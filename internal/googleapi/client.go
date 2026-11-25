package googleapi

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	statusNotFound      = 5
	statusAlreadyExists = 6
	statusUnavailable   = 14
	maxGoogleJSON       = 16 * 1024 * 1024
)

type googleError struct {
	operation string
	httpCode  int
	rpcCode   int
	reason    string
}

func (e *googleError) Error() string {
	if e.reason != "" {
		return e.operation + ": " + e.reason
	}
	if e.httpCode != 0 {
		return fmt.Sprintf("%s: HTTP %d", e.operation, e.httpCode)
	}
	if e.rpcCode != 0 {
		return fmt.Sprintf("%s: Google RPC code %d", e.operation, e.rpcCode)
	}
	return e.operation + ": invalid or interrupted Google response"
}

func (e *googleError) notFound() bool {
	return e.httpCode == http.StatusNotFound || e.rpcCode == statusNotFound || e.reason == "NOT_FOUND"
}

func IsNotFound(err error) bool {
	var upstream *googleError
	return errors.As(err, &upstream) && upstream.notFound()
}

func IsConflict(err error) bool {
	var upstream *googleError
	return errors.As(err, &upstream) &&
		(upstream.httpCode == http.StatusConflict || upstream.rpcCode == statusAlreadyExists || upstream.reason == "ALREADY_EXISTS")
}

func Retryable(err error) bool {
	var e *googleError
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &e) &&
		(e.rpcCode == 4 || e.rpcCode == 8 || e.rpcCode == 13 || e.rpcCode == statusUnavailable ||
			e.httpCode == 408 || e.httpCode == 429 || e.httpCode >= 500 ||
			e.reason == "DEADLINE_EXCEEDED" || e.reason == "RESOURCE_EXHAUSTED" || e.reason == "UNAVAILABLE" ||
			e.reason == "rateLimitExceeded" || e.reason == "userRateLimitExceeded" ||
			(e.httpCode == 0 && e.rpcCode == 0 && e.reason == "")))
}

func pause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func googleReason(message string) string {
	reasons := []string{
		"DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "UNAVAILABLE", "NOT_FOUND", "ALREADY_EXISTS",
		"FAILED_PRECONDITION", "PERMISSION_DENIED", "UNAUTHENTICATED", "INVALID_ARGUMENT",
		"rateLimitExceeded", "userRateLimitExceeded", "SERVICE_DISABLED", "ACCESS_TOKEN_SCOPE_INSUFFICIENT",
		"invalid_grant", "invalid_client", "unauthorized_client",
	}
	for _, reason := range reasons {
		if strings.Contains(message, reason) {
			return reason
		}
	}
	return ""
}

type accessToken struct {
	value  string
	expiry time.Time
}

type tokenSource struct {
	client                         *http.Client
	endpoint                       string
	clientID, secret, refreshToken string
	mu                             sync.Mutex
	cached                         accessToken
}

func (source *tokenSource) token(ctx context.Context) (accessToken, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if time.Until(source.cached.expiry) > 5*time.Minute {
		return source.cached, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	fields := url.Values{
		"grant_type": {"refresh_token"}, "client_id": {source.clientID},
		"client_secret": {source.secret}, "refresh_token": {source.refreshToken},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, source.endpoint, strings.NewReader(fields.Encode()))
	if err != nil {
		return accessToken{}, &googleError{operation: "build Google OAuth request"}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := source.client.Do(req)
	if err != nil {
		return accessToken{}, transportError(ctx, "refresh Google OAuth token")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return accessToken{}, responseError("refresh Google OAuth token", res)
	}
	var refreshed struct {
		Token   string `json:"access_token"`
		Seconds int    `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 64*1024)).Decode(&refreshed); err != nil || refreshed.Token == "" || refreshed.Seconds <= 0 {
		return accessToken{}, &googleError{operation: "decode Google OAuth token"}
	}
	source.cached = accessToken{refreshed.Token, time.Now().Add(time.Duration(refreshed.Seconds) * time.Second)}
	return source.cached, nil
}

func (source *tokenSource) invalidate(value string) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.cached.value == value {
		source.cached = accessToken{}
	}
}

type Client struct {
	http                          *http.Client
	tokens                        *tokenSource
	driveURL, uploadURL, colabURL string
	mcpURL                        string
}

type Options struct {
	HTTPClient                                      *http.Client
	TokenURL, DriveURL, UploadURL, ColabURL, MCPURL string
}

func New(clientID, clientSecret, refreshToken string, options Options) *Client {
	client := &http.Client{}
	if options.HTTPClient != nil {
		*client = *options.HTTPClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{
		http: client,
		tokens: &tokenSource{
			client: client, endpoint: cmp.Or(options.TokenURL, "https://oauth2.googleapis.com/token"),
			clientID: clientID, secret: clientSecret, refreshToken: refreshToken,
		},
		driveURL:  cmp.Or(options.DriveURL, "https://www.googleapis.com/drive/v3"),
		uploadURL: cmp.Or(options.UploadURL, "https://www.googleapis.com/upload/drive/v3"),
		colabURL:  cmp.Or(options.ColabURL, "https://colaboratory.googleapis.com"),
		mcpURL:    cmp.Or(options.MCPURL, "https://colaboratory.googleapis.com/mcp"),
	}
}

func (g *Client) AccessToken(ctx context.Context) (string, error) {
	token, err := g.tokens.token(ctx)
	return token.value, err
}

func transportError(ctx context.Context, operation string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return &googleError{operation: operation}
}

func responseError(operation string, response *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	return &googleError{operation: operation, httpCode: response.StatusCode, reason: googleReason(string(data))}
}

func (g *Client) request(ctx context.Context, method, address, operation, contentType, accept string, body []byte) (*http.Response, error) {
	for attempt := 0; attempt < 2; attempt++ {
		token, err := g.tokens.token(ctx)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(body))
		if err != nil {
			return nil, &googleError{operation: operation}
		}
		req.Header.Set("Authorization", "Bearer "+token.value)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		req.Header.Set("Accept", accept)
		res, err := g.http.Do(req)
		if err != nil {
			return nil, transportError(ctx, operation)
		}
		if res.StatusCode != http.StatusUnauthorized || attempt == 1 {
			return res, nil
		}
		res.Body.Close()
		g.tokens.invalidate(token.value)
	}
	return nil, &googleError{operation: operation}
}

func boundedBody(body io.Reader, limit int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, errors.New("response size exceeded the limit")
	}
	return data, nil
}

func (g *Client) rest(ctx context.Context, method, address, operation, contentType string, body []byte, output any) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	res, err := g.request(ctx, method, address, operation, contentType, "application/json", body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return responseError(operation, res)
	}
	if output == nil {
		return nil
	}
	data, err := boundedBody(res.Body, maxGoogleJSON)
	if err != nil {
		return transportError(ctx, operation)
	}
	if output, ok := output.(*[]byte); ok {
		*output = data
		return nil
	}
	if err := json.Unmarshal(data, output); err != nil {
		return &googleError{operation: operation}
	}
	return nil
}

func encodeJSON(value any) []byte {
	// Call sites supply only concrete structs, strings, and string-keyed maps.
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func queryURL(base string, values url.Values) string {
	return base + "?" + values.Encode()
}
