package googleapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeCreationReportsStructuredFailureReason(t *testing.T) {
	var job operation
	err := json.Unmarshal([]byte(`{"done":true,"error":{"code":9,"message":"internal details",
		"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"NO_CAPACITY","metadata":{"private":"value"}}]}}`), &job)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{}
	err = client.waitRuntimeCreation(t.Context(), job)
	if err == nil || err.Error() != "create Colab runtime: NO_CAPACITY" {
		t.Fatalf("unexpected runtime error: %v", err)
	}
}

func TestOAuthRefreshesExpiringAccessToken(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		fmt.Fprintf(w, "{\"access_token\":\"access-%d\",\"expires_in\":3600}", n)
	}))
	defer server.Close()
	client := New("test-client", "test-secret", "test-refresh", Options{TokenURL: server.URL})
	first, err := client.AccessToken(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.AccessToken(t.Context())
	if err != nil || first != second {
		t.Fatalf("fresh token was not reused: %v", err)
	}
	client.tokens.cached.expiry = time.Now().Add(3 * time.Minute)
	third, err := client.AccessToken(t.Context())
	if err != nil || third == first || requests.Load() != 2 {
		t.Fatalf("expiring token was not refreshed: %v", err)
	}
}
