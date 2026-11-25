package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thorved/ColabMediaFlow/internal/googleapi"
)

type apiFixture struct {
	t            *testing.T
	server       *httptest.Server
	mu           sync.Mutex
	notebook     googleapi.DriveFile
	notebookBody []byte
	runtimeName  string
	scratch      bool
	report       *report
	finalReport  *report
	tokenCalls   atomic.Int32
	workCalls    atomic.Int32
	createCalls  atomic.Int32
	updateCalls  atomic.Int32
	parentError  atomic.Bool
	executionIDs []string
	workStarted  chan struct{}
	continueWork chan struct{}
	resume       bool
}

func newFixture(t *testing.T, resume bool) *apiFixture {
	t.Helper()
	fixture := &apiFixture{
		t: t, resume: resume, workStarted: make(chan struct{}), continueWork: make(chan struct{}),
	}
	fixture.server = httptest.NewTLSServer(http.HandlerFunc(fixture.handle))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (f *apiFixture) cloud(cfg config) *googleapi.Client {
	return googleapi.New(cfg.clientID, cfg.clientSecret, cfg.refreshToken, googleapi.Options{
		HTTPClient: f.server.Client(),
		TokenURL:   f.server.URL + "/token",
		DriveURL:   f.server.URL + "/drive",
		UploadURL:  f.server.URL + "/upload",
		ColabURL:   f.server.URL,
		MCPURL:     f.server.URL + "/mcp",
	})
}

func (f *apiFixture) handle(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/proxy/") {
		if r.Header.Get("X-Colab-Runtime-Proxy-Token") != "proxy-token" || r.Header.Get("Authorization") != "" {
			f.t.Error("incorrect authentication sent to the Colab proxy")
		}
	} else if r.URL.Path != "/token" && !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer access-") {
		f.t.Error("OAuth bearer token was not sent to Google")
	}
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/token":
		if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "refresh_token" ||
			r.Form.Get("client_id") != "test-client" || r.Form.Get("client_secret") != "test-secret" ||
			r.Form.Get("refresh_token") != "test-refresh" {
			f.t.Error("incorrect OAuth refresh parameters")
		}
		number := f.tokenCalls.Add(1)
		fmt.Fprintf(w, "{\"access_token\":\"access-%d\",\"expires_in\":3600}", number)
	case r.URL.Path == "/drive/files/parent":
		if f.parentError.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeFixtureJSON(w, googleapi.DriveFile{ID: "parent", MimeType: googleapi.FolderMIME})
	case r.URL.Path == "/drive/files/generateIds":
		writeFixtureJSON(w, map[string][]string{"ids": {"notebook-id"}})
	case r.URL.Path == "/drive/files/notebook-id":
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case f.notebook.ID == "":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Query().Get("alt") == "media":
			w.Write(f.notebookBody)
		default:
			writeFixtureJSON(w, f.notebook)
		}
	case r.URL.Path == "/upload/files" || r.URL.Path == "/upload/files/notebook-id":
		f.upload(w, r)
	case r.URL.Path == "/v1beta/runtimespecs":
		w.Write([]byte("{\"runtimeSpecs\":[{\"key\":{\"variant\":\"VARIANT_CPU\",\"accelerator\":\"NONE\",\"shape\":\"SHAPE_STANDARD\"},\"eligible\":true}]}"))
	case r.URL.Path == "/v1beta/runtimes":
		f.mu.Lock()
		defer f.mu.Unlock()
		f.runtimeName = "runtimes/" + r.URL.Query().Get("runtimeId")
		if requestID := r.URL.Query().Get("requestId"); requestID == "" {
			f.t.Error("runtime creation needs an idempotency key")
		}
		writeFixtureJSON(w, map[string]bool{"done": true})
	case strings.HasPrefix(r.URL.Path, "/v1beta/runtimes/"):
		f.mu.Lock()
		defer f.mu.Unlock()
		if strings.TrimPrefix(r.URL.Path, "/v1beta/") != f.runtimeName {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeFixtureJSON(w, map[string]any{"name": f.runtimeName,
			"connectionInfo": map[string]string{"url": f.server.URL + "/proxy", "token": "proxy-token"}})
	case strings.HasPrefix(r.URL.Path, "/proxy/api/contents/") || strings.HasPrefix(r.URL.Path, "/proxy/files/"):
		f.proxy(w, r)
	case r.URL.Path == "/mcp":
		f.mcp(w, r)
	default:
		f.t.Errorf("unexpected mock request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
	}
}

func writeFixtureJSON(w http.ResponseWriter, value any) {
	_ = json.NewEncoder(w).Encode(value)
}

func (f *apiFixture) upload(w http.ResponseWriter, r *http.Request) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		f.t.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := multipart.NewReader(r.Body, params["boundary"])
	first, err := parts.NextPart()
	if err != nil {
		f.t.Error(err)
		return
	}
	var metadata googleapi.DriveFile
	if err := json.NewDecoder(first).Decode(&metadata); err != nil {
		f.t.Error(err)
		return
	}
	if r.Method == http.MethodPatch {
		f.notebook.Name = metadata.Name
		f.updateCalls.Add(1)
	} else {
		f.notebook = metadata
		f.createCalls.Add(1)
	}
	second, err := parts.NextPart()
	if err != nil {
		f.t.Error(err)
		return
	}
	f.notebookBody, err = io.ReadAll(second)
	if err != nil {
		f.t.Error(err)
		return
	}
	writeFixtureJSON(w, f.notebook)
}

func (f *apiFixture) proxy(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPut:
		var upload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&upload); err != nil {
			f.t.Error(err)
		}
		if upload["type"] == "directory" {
			f.scratch = true
			f.report = nil
		}
		if strings.HasSuffix(r.URL.Path, "/oauth.json") {
			if strings.Contains(upload["content"], "test-refresh") ||
				strings.Contains(upload["content"], "test-secret") || !strings.Contains(upload["content"], "access-") {
				f.t.Error("the proxy credential file must contain only the access token")
			}
		}
		w.WriteHeader(http.StatusCreated)
	case strings.HasSuffix(r.URL.Path, "report.json"):
		if f.report == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeFixtureJSON(w, f.report)
	case !f.scratch:
		w.WriteHeader(http.StatusNotFound)
	default:
		w.Write([]byte("{}"))
	}
}

func (f *apiFixture) mcp(w http.ResponseWriter, r *http.Request) {
	var call struct {
		Method string
		Params struct {
			Name      string
			Arguments map[string]string
		}
	}
	if err := json.NewDecoder(r.Body).Decode(&call); err != nil || call.Method != "tools/call" {
		f.t.Error("invalid MCP tool call")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	args := call.Params.Arguments
	reply := map[string]any{}
	switch call.Params.Name {
	case "create_session":
		reply["name"] = args["parent"] + "/sessions/" + args["sessionId"]
	case "delete_session":
	case "execute_code":
		if strings.Contains(args["code"], "MEDIA_FLOW_RUN_PATH = ") {
			n := f.workCalls.Add(1)
			f.mu.Lock()
			f.executionIDs = append(f.executionIDs, args["executionId"])
			f.mu.Unlock()
			if n == 1 && f.resume {
				close(f.workStarted)
				<-r.Context().Done()
				return
			}
			if n == 1 && !f.resume {
				close(f.workStarted)
				select {
				case <-f.continueWork:
				case <-r.Context().Done():
					return
				}
			}
			f.mu.Lock()
			f.report = f.finalReport
			if f.report == nil {
				f.report = &report{Done: true, ScanComplete: true, Failures: []archiveFailure{}}
			}
			f.mu.Unlock()
		} else if strings.Contains(args["code"], "shutil.rmtree(scratch)") {
			f.mu.Lock()
			f.scratch, f.report = false, nil
			f.mu.Unlock()
		} else {
			f.t.Error("unexpected code sent to Colab")
		}
	default:
		f.t.Errorf("unexpected Colab tool %s", call.Params.Name)
	}
	msg := map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"structuredContent": reply}}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Write([]byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n"))
	fmt.Fprintf(w, "event: message\ndata: %s\n\n", encodeJSON(msg))
}

func testConfig() config {
	return config{
		clientID: "test-client", clientSecret: "test-secret", refreshToken: "test-refresh",
		notebookParent: "parent", inputFolder: "input", apiKey: "local-api-key",
	}
}

func requestAPI(s *service, method, path, body, key string) *httptest.ResponseRecorder {
	res := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	s.router().ServeHTTP(res, req)
	return res
}

func expectAPI(t *testing.T, s *service, method, path, body string, status int) *httptest.ResponseRecorder {
	t.Helper()
	res := requestAPI(s, method, path, body, s.cfg.apiKey)
	if res.Code != status {
		t.Fatalf("%s %s: got %d want %d: %s", method, path, res.Code, status, res.Body)
	}
	return res
}

func testStore(t *testing.T) *store {
	t.Helper()
	s, err := openStore(filepath.Join(t.TempDir(), "state file.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.db.Close() })
	return s
}

func waitFor(t *testing.T, pred func() bool) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !pred() {
		select {
		case <-timer.C:
			t.Fatal("condition did not become true")
		case <-ticker.C:
		}
	}
}

func TestAPIAndScheduler(t *testing.T) {
	cfg := testConfig()
	cfg.interval = 15 * time.Millisecond
	f := newFixture(t, false)
	svc := newService(cfg, testStore(t), f.cloud(cfg))
	if r := requestAPI(svc, http.MethodPut, "/v1/notebook", "", ""); r.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated notebook PUT: %d", r.Code)
	}
	expectAPI(t, svc, http.MethodPost, "/v1/runs", "{}", http.StatusConflict)
	expectAPI(t, svc, http.MethodPost, "/v1/runs", "{\"code\":\"unsupported\"}", http.StatusBadRequest)
	expectAPI(t, svc, http.MethodPut, "/v1/notebook", "", http.StatusCreated)
	expectAPI(t, svc, http.MethodPut, "/v1/notebook", "", http.StatusOK)
	if f.createCalls.Load() != 1 {
		t.Fatal("repeated PUT created a duplicate")
	}

	res := expectAPI(t, svc, http.MethodPost, "/v1/runs", "{\"retry_failed\":true}", http.StatusAccepted)
	path := res.Header().Get("Location")
	var manual runResult
	if err := json.Unmarshal(res.Body.Bytes(), &manual); err != nil || manual.Status != runQueued || path != "/v1/runs/"+manual.ID {
		t.Fatalf("invalid run acknowledgment: %s (%v)", res.Body, err)
	}
	tickCtx, stopTimer := context.WithCancel(t.Context())
	ticksDone := make(chan struct{})
	go func() {
		defer close(ticksDone)
		svc.scheduler(tickCtx)
	}()
	t.Cleanup(func() { stopTimer(); <-ticksDone })
	workerCtx, stopWorker := context.WithCancel(t.Context())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		svc.worker(workerCtx)
	}()
	t.Cleanup(func() { stopWorker(); <-workerDone })
	waitFor(t, func() bool {
		select {
		case <-f.workStarted:
			return true
		default:
			return false
		}
	})
	conflict := expectAPI(t, svc, http.MethodPost, "/v1/runs", "", http.StatusConflict)
	if !strings.Contains(conflict.Body.String(), manual.ID) {
		t.Fatalf("missing active run ID: %s", conflict.Body)
	}
	expectAPI(t, svc, http.MethodPut, "/v1/notebook", "", http.StatusConflict)
	time.Sleep(5 * cfg.interval)
	if got := f.workCalls.Load(); got != 1 {
		t.Fatalf("%d workers while manual run was blocked", got)
	}
	close(f.continueWork)
	waitFor(t, func() bool {
		var status runResult
		res := expectAPI(t, svc, http.MethodGet, path, "", http.StatusOK)
		if err := json.Unmarshal(res.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		return status.Status == runSucceeded && status.Scanned == 0
	})
	waitFor(t, func() bool { return f.workCalls.Load() >= 2 })
	stopTimer()
	if f.tokenCalls.Load() != 1 {
		t.Fatal("OAuth token should be reused within its lifetime")
	}
}

func TestRestartReattachesToSameExecution(t *testing.T) {
	cfg := testConfig()
	f := newFixture(t, true)
	dbPath := filepath.Join(t.TempDir(), "restart.db")
	db, err := openStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	first := newService(cfg, db, f.cloud(cfg))
	expectAPI(t, first, http.MethodPut, "/v1/notebook", "", http.StatusCreated)
	run, accepted, err := first.queue(t.Context(), "manual", false)
	if !accepted || err != nil {
		t.Fatalf("queue before restart: %v", err)
	}
	firstCtx, stop := context.WithCancel(t.Context())
	workDone := make(chan error, 1)
	go func() { workDone <- first.advance(firstCtx) }()
	waitFor(t, func() bool {
		select {
		case <-f.workStarted:
			return true
		default:
			return false
		}
	})
	stop()
	<-workDone
	if err := db.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.db.Close()

	second := newService(cfg, db, f.cloud(cfg))
	before, err := db.activeRun(t.Context())
	if err != nil || before == nil || !before.Dispatched || before.SessionName == "" || before.ExecutionID == "" {
		t.Fatalf("incomplete persisted run: %+v, %v", before, err)
	}
	expectAPI(t, second, http.MethodPost, "/v1/runs", "", http.StatusConflict)
	if recovering, err := second.bootstrap(t.Context(), true); err != nil || !recovering {
		t.Fatalf("startup did not recognize the persisted run: %v, %v", recovering, err)
	}
	if err := second.advance(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := db.getRun(t.Context(), run.ID)
	if err != nil || after.Status != runSucceeded || after.FinishedAt == nil {
		t.Fatalf("recovered run: %+v, %v", after, err)
	}
	if recovering, err := second.bootstrap(t.Context(), false); err != nil || recovering {
		t.Fatalf("startup did not finish after recovery: %v, %v", recovering, err)
	}
	if latest, err := db.latestRun(t.Context()); err != nil || latest == nil || latest.ID != run.ID {
		t.Fatalf("startup duplicated the recovered run: %+v, %v", latest, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.executionIDs) != 2 || f.executionIDs[0] != f.executionIDs[1] || f.executionIDs[1] != before.ExecutionID {
		t.Fatalf("recovery changed execution IDs: %v", f.executionIDs)
	}
}

func TestArchiveFailureVersionsPersist(t *testing.T) {
	state := testStore(t)
	svc := newService(testConfig(), state, nil)
	ctx := t.Context()
	if err := state.setMetadata(ctx, "notebook_id", "notebook"); err != nil {
		t.Fatal(err)
	}
	run, queued, err := svc.queue(ctx, "manual", false)
	if !queued || err != nil {
		t.Fatal(err)
	}
	result := report{Failures: []archiveFailure{
		{FileID: "bad", Version: "12", Permanent: true},
		{FileID: "transient", Version: "9"},
	}}
	run.updateOutcome(result, true, nil)
	if err := state.recordReport(ctx, run, result); err != nil {
		t.Fatal(err)
	}
	versions, err := state.badVersions(ctx, "input")
	if err != nil || len(versions) != 1 || versions["bad"] != "12" {
		t.Fatalf("saved bad versions %v, %v", versions, err)
	}
	result = report{Resolved: []string{"bad"}}
	run.updateOutcome(result, true, nil)
	if err := state.recordReport(ctx, run, result); err != nil {
		t.Fatal(err)
	}
	versions, err = state.badVersions(ctx, "input")
	if err != nil || len(versions) != 0 {
		t.Fatalf("resolved bad versions %v, %v", versions, err)
	}
}
