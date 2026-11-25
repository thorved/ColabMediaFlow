package app

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/thorved/ColabMediaFlow/internal/googleapi"
)

// Run only on demand. This test creates and cleans up its own Drive folder and
// Colab runtime. It never uses the configured media folder as the input.
func TestLiveColabWorkflow(t *testing.T) {
	if os.Getenv("COLAB_MEDIA_FLOW_LIVE_TEST") != "1" {
		t.Skip("set COLAB_MEDIA_FLOW_LIVE_TEST=1 to run with real Google credentials")
	}
	cfg, err := readConfig(filepath.Join("..", "..", ".env"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	g := googleapi.New(cfg.clientID, cfg.clientSecret, cfg.refreshToken, googleapi.Options{})
	state, err := openStore(filepath.Join(t.TempDir(), "smoke.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.db.Close()

	var folder googleapi.DriveFile
	payload := encodeJSON(googleapi.DriveFile{
		Name: "ColabMediaFlow smoke " + uuid.NewString(), MimeType: googleapi.FolderMIME,
	})
	if err := liveGoogleREST(ctx, g, http.MethodPost, smokeDrive+"/files", payload, &folder, false); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := liveGoogleREST(cleanup, g, http.MethodPatch, smokeDrive+"/files/"+url.PathEscape(folder.ID), []byte("{\"trashed\":true}"), nil, false); err != nil {
			t.Errorf("clean up isolated Drive folder %s: %v", folder.ID, err)
		}
		name, err := state.metadata(cleanup, "runtime_name")
		if err != nil {
			t.Errorf("read the smoke-test runtime name: %v", err)
			return
		}
		if name != "" {
			err := liveGoogleREST(cleanup, g, http.MethodDelete, "https://colaboratory.googleapis.com/v1beta/"+name, nil, nil, true)
			if err != nil {
				t.Errorf("clean up test-only Colab runtime %s: %v", name, err)
			}
		}
	}()
	t.Logf("created isolated Drive folder %s", folder.ID)
	cfg.inputFolder, cfg.notebookParent, cfg.interval = folder.ID, folder.ID, 5*time.Second
	svc := newService(cfg, state, g)
	server := httptest.NewServer(svc.router())
	defer server.Close()

	var archive bytes.Buffer
	zipWriter := zip.NewWriter(&archive)
	member, err := zipWriter.CreateHeader(&zip.FileHeader{Name: "nested/sample.txt", Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("ColabMediaFlow smoke fixture\n"), 350_000)
	if _, err := member.Write(want); err != nil {
		t.Fatal(err)
	}
	if _, err := zipWriter.Create("nested/empty.txt"); err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := g.UploadDriveFile(ctx, googleapi.DriveFile{Name: "smoke.zip", Parents: []string{folder.ID}}, "application/zip", archive.Bytes(), "")
	if err != nil {
		t.Fatal(err)
	}
	expectedArchives := 1
	if rarPath := os.Getenv("COLAB_MEDIA_FLOW_LIVE_RAR_FIXTURE"); rarPath != "" {
		rar, err := os.ReadFile(rarPath)
		if err != nil {
			t.Fatal(err)
		}
		_, err = g.UploadDriveFile(ctx, googleapi.DriveFile{Name: "smoke-rar.rar", Parents: []string{folder.ID}},
			"application/vnd.rar", rar, "")
		if err != nil {
			t.Fatal(err)
		}
		expectedArchives++
	}
	var notebook struct {
		ID string `json:"file_id"`
	}
	liveAPI(t, ctx, server.URL, cfg.apiKey, http.MethodPut, "/v1/notebook", http.StatusCreated, &notebook)
	t.Logf("created managed notebook %s", notebook.ID)
	var reused struct {
		ID string `json:"file_id"`
	}
	liveAPI(t, ctx, server.URL, cfg.apiKey, http.MethodPut, "/v1/notebook", http.StatusOK, &reused)
	if notebook.ID != reused.ID {
		t.Fatal("notebook PUT created a duplicate")
	}

	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		svc.worker(workerCtx)
	}()
	defer func() {
		stopWorker()
		<-workerDone
	}()

	var run runResult
	liveAPI(t, ctx, server.URL, cfg.apiKey, http.MethodPost, "/v1/runs", http.StatusAccepted, &run)
	t.Logf("queued manual run %s", run.ID)
	finished := waitForLiveRun(t, ctx, server.URL, cfg.apiKey, run.ID)
	if finished.Status != runSucceeded || finished.Processed != expectedArchives || finished.Scanned != expectedArchives {
		t.Fatalf("manual run returned %+v", finished)
	}
	moved, err := g.ReadDriveFile(ctx, source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(moved.Parents) != 1 || moved.Parents[0] == folder.ID {
		t.Fatalf("archive is still in the input: %+v", moved)
	}
	target, err := g.ReadDriveFile(ctx, moved.Parents[0])
	if err != nil || target.Name != "compressed_files" {
		t.Fatalf("archive moved to unexpected folder: %+v, %v", target, err)
	}
	output := findLiveChild(t, ctx, g, folder.ID, "uncompressed_files")
	stem := findLiveChild(t, ctx, g, output.ID, "smoke")
	nested := findLiveChild(t, ctx, g, stem.ID, "nested")
	sample := findLiveChild(t, ctx, g, nested.ID, "sample.txt")
	extracted, err := g.DownloadDriveFile(ctx, sample.ID)
	if err != nil || !bytes.Equal(extracted, want) {
		t.Fatalf("downloaded extracted sample has %d bytes, expected %d: %v", len(extracted), len(want), err)
	}
	emptyFile := findLiveChild(t, ctx, g, nested.ID, "empty.txt")
	extracted, err = g.DownloadDriveFile(ctx, emptyFile.ID)
	if err != nil || len(extracted) != 0 {
		t.Fatalf("empty extracted file has %d bytes: %v", len(extracted), err)
	}

	timerCtx, stopTimer := context.WithCancel(ctx)
	defer stopTimer()
	go svc.scheduler(timerCtx)
	var scheduledID string
	for scheduledID == "" {
		active, err := state.activeRun(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if active != nil && active.Trigger == "scheduled" {
			scheduledID = active.ID
			stopTimer()
			break
		}
		if err := pause(ctx, 250*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("observed scheduled run %s", scheduledID)
	empty := waitForLiveRun(t, ctx, server.URL, cfg.apiKey, scheduledID)
	if empty.Status != runSucceeded || empty.Scanned != 0 {
		t.Fatalf("scheduled empty run returned %+v", empty)
	}
}

func findLiveChild(t *testing.T, ctx context.Context, g *googleapi.Client, parent, name string) googleapi.DriveFile {
	t.Helper()
	var found struct{ Files []googleapi.DriveFile }
	address := smokeDrive + "/files?" + (url.Values{
		"q":      {"trashed=false and '" + parent + "' in parents and name='" + name + "'"},
		"fields": {"files(id,name,mimeType)"},
	}).Encode()
	if err := liveGoogleREST(ctx, g, http.MethodGet, address, nil, &found, false); err != nil {
		t.Fatal(err)
	}
	if len(found.Files) != 1 {
		t.Fatalf("expected one %q under %s, found %d", name, parent, len(found.Files))
	}
	return found.Files[0]
}

func liveAPI(t *testing.T, ctx context.Context, origin, key, method, path string, status int, data any) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, origin+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != status {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		t.Fatalf("%s %s returned %d: %s", method, path, res.StatusCode, body)
	}
	if err := json.NewDecoder(res.Body).Decode(data); err != nil {
		t.Fatal(err)
	}
}

func waitForLiveRun(t *testing.T, ctx context.Context, origin, key, id string) runResult {
	t.Helper()
	path := fmt.Sprintf("/v1/runs/%s", id)
	for {
		var run runResult
		liveAPI(t, ctx, origin, key, http.MethodGet, path, http.StatusOK, &run)
		if run.FinishedAt != nil {
			return run
		}
		if err := pause(ctx, 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}
}

const smokeDrive = "https://www.googleapis.com/drive/v3"

func liveGoogleREST(ctx context.Context, g *googleapi.Client, method, address string, body []byte, out any, allowMissing bool) error {
	token, err := g.AccessToken(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if allowMissing && res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("Google request returned HTTP %d", res.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}
