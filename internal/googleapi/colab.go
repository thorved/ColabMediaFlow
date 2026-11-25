package googleapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type runtimeSpec struct {
	Variant     string `json:"variant"`
	Accelerator string `json:"accelerator"`
	Shape       string `json:"shape"`
}

type colabRuntime struct {
	Name           string
	ConnectionInfo struct {
		Token string
		URL   string
	}
}

type operation struct {
	Name  string
	Done  bool
	Error *struct {
		Code    int
		Message string
	}
}

func (g *Client) getRuntime(ctx context.Context, name string) (colabRuntime, error) {
	var instance colabRuntime
	err := g.rest(ctx, http.MethodGet, g.colabURL+"/v1beta/"+name, "get Colab runtime", "", nil, &instance)
	if err == nil && (instance.Name != name || instance.ConnectionInfo.Token == "" || instance.ConnectionInfo.URL == "") {
		err = &googleError{operation: "read Colab runtime connection"}
	}
	return instance, err
}

func (g *Client) EnsureRuntime(ctx context.Context, name, requestID string) error {
	if _, err := g.getRuntime(ctx, name); !IsNotFound(err) {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	var specs struct {
		RuntimeSpecs []struct {
			Key      runtimeSpec
			Eligible bool
		}
	}
	if err := g.rest(ctx, http.MethodGet, g.colabURL+"/v1beta/runtimespecs", "list Colab runtime specs", "", nil, &specs); err != nil {
		return err
	}
	standard := runtimeSpec{"VARIANT_CPU", "NONE", "SHAPE_STANDARD"}
	eligible := false
	for _, spec := range specs.RuntimeSpecs {
		if spec.Eligible && spec.Key == standard {
			eligible = true
			break
		}
	}
	if !eligible {
		return errors.New("Google Colab reports that this account is ineligible for a standard CPU runtime")
	}
	address := queryURL(g.colabURL+"/v1beta/runtimes", url.Values{
		"runtimeId": {strings.TrimPrefix(name, "runtimes/")}, "requestId": {requestID},
	})
	var creation operation
	payload := encodeJSON(map[string]runtimeSpec{"runtimeSpec": standard})
	for attempt := 0; ; attempt++ {
		err := g.rest(ctx, http.MethodPost, address, "create Colab runtime", "application/json", payload, &creation)
		if IsConflict(err) {
			break
		}
		if err == nil {
			if err := g.waitRuntimeCreation(ctx, creation); err != nil && !IsConflict(err) {
				return err
			}
			break
		}
		if attempt == 3 || !Retryable(err) {
			return err
		}
		if err := pause(ctx, time.Second<<attempt); err != nil {
			return err
		}
	}
	for {
		_, err := g.getRuntime(ctx, name)
		if !IsNotFound(err) && !Retryable(err) {
			return err
		}
		if err := pause(ctx, 2*time.Second); err != nil {
			return fmt.Errorf("wait for Colab runtime connection: %w", err)
		}
	}
}

func (g *Client) waitRuntimeCreation(ctx context.Context, job operation) error {
	name := job.Name
	for !job.Done {
		if !strings.HasPrefix(name, "operations/") {
			return &googleError{operation: "read Colab runtime creation operation"}
		}
		if err := pause(ctx, 2*time.Second); err != nil {
			return err
		}
		var current operation
		err := g.rest(ctx, http.MethodGet, g.colabURL+"/v1/"+name, "poll Colab runtime creation", "", nil, &current)
		if err != nil && !Retryable(err) {
			return err
		}
		if err == nil {
			job = current
		}
	}
	if job.Error != nil {
		return &googleError{operation: "create Colab runtime", rpcCode: job.Error.Code, reason: googleReason(job.Error.Message)}
	}
	return nil
}

// Google's Colab MCP reference documents single-request tool calls and both
// JSON and SSE responses. The stream can carry notifications before the result.
type mcpResponse struct {
	Result *struct {
		Structured       json.RawMessage `json:"structuredContent"`
		LegacyStructured json.RawMessage `json:"structured_content"`
		IsError          bool            `json:"isError"`
		Content          []struct{ Text string }
	}
	Error *struct {
		Code    int
		Message string
		Data    json.RawMessage
	}
}

func (g *Client) callTool(ctx context.Context, tool string, arguments map[string]string, timeout time.Duration, output any) error {
	const operationPrefix = "Colab tool "
	operation := operationPrefix + tool
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	body := encodeJSON(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{"name": tool, "arguments": arguments},
	})
	res, err := g.request(ctx, http.MethodPost, g.mcpURL, operation, "application/json", "application/json, text/event-stream", body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return responseError(operation, res)
	}
	var msg mcpResponse
	if strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		msg, err = readMCPStream(res.Body)
	} else {
		var data []byte
		data, err = boundedBody(res.Body, maxGoogleJSON)
		if err == nil {
			err = json.Unmarshal(data, &msg)
		}
	}
	if err != nil {
		return transportError(ctx, operation)
	}
	if msg.Error != nil {
		return &googleError{operation: operation, rpcCode: msg.Error.Code,
			reason: googleReason(msg.Error.Message + " " + string(msg.Error.Data))}
	}
	if msg.Result == nil {
		return &googleError{operation: operation}
	}
	if msg.Result.IsError {
		var detail strings.Builder
		for _, block := range msg.Result.Content {
			detail.WriteString(block.Text)
		}
		detail.Write(msg.Result.Structured)
		var rpc struct {
			Code int
		}
		_ = json.Unmarshal(msg.Result.Structured, &rpc)
		return &googleError{operation: operation, rpcCode: rpc.Code, reason: googleReason(detail.String())}
	}
	if output == nil {
		return nil
	}
	data := msg.Result.Structured
	if len(data) == 0 {
		data = msg.Result.LegacyStructured
	}
	if len(data) == 0 && len(msg.Result.Content) == 1 {
		data = []byte(msg.Result.Content[0].Text)
	}
	if len(data) == 0 || json.Unmarshal(data, output) != nil {
		return &googleError{operation: operation}
	}
	return nil
}

func readMCPStream(body io.Reader) (mcpResponse, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 32*1024), maxGoogleJSON)
	var event bytes.Buffer
	for scanner.Scan() {
		line := scanner.Bytes()
		switch {
		case bytes.HasPrefix(line, []byte("data:")):
			if event.Len() > 0 {
				event.WriteByte('\n')
			}
			event.Write(bytes.TrimPrefix(line[5:], []byte(" ")))
			if event.Len() > maxGoogleJSON {
				return mcpResponse{}, errors.New("Colab MCP event exceeds size limit")
			}
		case len(line) == 0 && event.Len() > 0:
			msg, found := decodeMCPEvent(event.Bytes())
			if found {
				return msg, nil
			}
			event.Reset()
		}
	}
	if msg, found := decodeMCPEvent(event.Bytes()); found {
		return msg, nil
	}
	return mcpResponse{}, io.ErrUnexpectedEOF
}

func decodeMCPEvent(data []byte) (mcpResponse, bool) {
	var msg mcpResponse
	err := json.Unmarshal(data, &msg)
	return msg, err == nil && (msg.Result != nil || msg.Error != nil)
}

func (g *Client) CreateSession(ctx context.Context, runtimeName, sessionName string) error {
	id := strings.TrimPrefix(sessionName, runtimeName+"/sessions/")
	var session struct{ Name string }
	err := g.callTool(ctx, "create_session", map[string]string{"parent": runtimeName, "sessionId": id}, 2*time.Minute, &session)
	if err != nil {
		return err
	}
	if session.Name != sessionName {
		return errors.New("Colab did not return the requested kernel session")
	}
	return nil
}

func (g *Client) DeleteSession(ctx context.Context, sessionName string) error {
	err := g.callTool(ctx, "delete_session", map[string]string{"name": sessionName}, time.Minute, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

func (g *Client) ExecuteCode(ctx context.Context, sessionName, executionID, code string) (bool, error) {
	var result struct {
		ExecutionError *struct {
			Name string
		}
	}
	err := g.callTool(ctx, "execute_code", map[string]string{
		"session": sessionName, "executionId": executionID, "code": code,
	}, 11*time.Minute, &result)
	if err != nil {
		return false, err
	}
	if result.ExecutionError != nil {
		return true, errors.New("managed Colab worker raised a Python exception")
	}
	return true, nil
}

func (g *Client) proxy(ctx context.Context, runtimeName, method, endpoint, path string, body []byte, out *[]byte) error {
	const operation = "access managed Colab runtime files"
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	instance, err := g.getRuntime(ctx, runtimeName)
	if err != nil {
		return err
	}
	info := instance.ConnectionInfo
	base, err := url.Parse(info.URL)
	if err != nil || base.Host == "" || base.Scheme != "https" || base.User != nil || base.Fragment != "" {
		return errors.New("Google returned an invalid Colab proxy URL")
	}
	address := base.JoinPath(endpoint, path).String()
	req, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(body))
	if err != nil {
		return &googleError{operation: operation}
	}
	req.Header.Set("X-Colab-Runtime-Proxy-Token", info.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := g.http.Do(req)
	if err != nil {
		return transportError(ctx, operation)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return responseError(operation, res)
	}
	if out != nil {
		*out, err = boundedBody(res.Body, maxGoogleJSON)
		if err != nil {
			return &googleError{operation: "read managed Colab run report"}
		}
	}
	return nil
}

func (g *Client) WriteRuntimeFile(ctx context.Context, runtimeName, path string, data []byte) error {
	file := encodeJSON(map[string]string{"type": "file", "format": "text", "content": string(data)})
	return g.proxy(ctx, runtimeName, http.MethodPut, "api/contents", path, file, nil)
}

func (g *Client) CheckRuntime(ctx context.Context, name string) error {
	_, err := g.getRuntime(ctx, name)
	return err
}

func (g *Client) CreateRuntimeDirectory(ctx context.Context, runtimeName, path string) error {
	return g.proxy(ctx, runtimeName, http.MethodPut, "api/contents", path, encodeJSON(map[string]string{"type": "directory"}), nil)
}

func (g *Client) CheckRuntimeDirectory(ctx context.Context, runtimeName, path string) error {
	return g.proxy(ctx, runtimeName, http.MethodGet, "api/contents", path, nil, nil)
}

func (g *Client) ReadRuntimeFile(ctx context.Context, runtimeName, path string) ([]byte, error) {
	var data []byte
	err := g.proxy(ctx, runtimeName, http.MethodGet, "files", path, nil, &data)
	return data, err
}
