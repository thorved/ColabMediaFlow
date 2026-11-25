package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestStartupProcessesAndPublishesLiveStatus(t *testing.T) {
	cfg := testConfig()
	cfg.interval = time.Hour
	f := newFixture(t, false)
	svc := newService(cfg, testStore(t), f.cloud(cfg))
	if res := requestAPI(svc, http.MethodGet, "/v1/status", "", ""); res.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status returned %d", res.Code)
	}
	if initial := readWorkflow(t, svc); initial.Notebook.Status != "initializing" || initial.Run != nil {
		t.Fatalf("unexpected initial state: %+v", initial)
	}

	ctx, stop := context.WithCancel(t.Context())
	automationDone, workerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(automationDone)
		svc.automate(ctx, false)
	}()
	go func() {
		defer close(workerDone)
		svc.worker(ctx)
	}()
	defer func() {
		stop()
		<-automationDone
		<-workerDone
	}()
	select {
	case <-f.workStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("automatic startup did not begin a run")
	}
	status := readWorkflow(t, svc)
	if status.Notebook.FileID != "notebook-id" || status.Notebook.Status != "ready" ||
		status.Run == nil || status.Run.Trigger != "startup" ||
		status.Run.Remaining != nil || status.Run.ProgressPercent != nil {
		t.Fatalf("unscanned startup run: %+v, %+v", status.Notebook, status.Run)
	}
	runID := status.Run.ID

	done, total := int64(500), int64(1000)
	failed := []archiveFailure{{FileID: "bad", Version: "1", Name: "bad.zip"}}
	f.mu.Lock()
	f.report = &report{
		ScanComplete: true, Scanned: 5, Processed: 1, Skipped: 1, Failures: failed,
		CurrentArchive: &archiveProgress{FileID: "current", Name: "large.rar", Phase: "downloading", BytesDone: &done, BytesTotal: &total},
	}
	f.finalReport = &report{Done: true, ScanComplete: true, Scanned: 5, Processed: 3, Skipped: 1, Failures: failed}
	f.mu.Unlock()
	waitFor(t, func() bool {
		run := readWorkflow(t, svc).Run
		return run != nil && run.CurrentArchive != nil
	})
	status = readWorkflow(t, svc)
	if run := status.Run; run.Status != runRunning || !run.ScanComplete || run.Scanned != 5 || run.Processed != 1 ||
		run.Skipped != 1 || run.Failed != 1 || run.Remaining == nil || *run.Remaining != 2 ||
		run.ProgressPercent == nil || *run.ProgressPercent != 60 || run.ProgressUpdatedAt == nil ||
		run.CurrentArchive.BytesDone == nil || *run.CurrentArchive.BytesDone != 500 {
		t.Fatalf("incorrect live startup progress: %+v", run)
	}
	res := expectAPI(t, svc, http.MethodGet, "/v1/runs/"+runID, "", http.StatusOK)
	var single runView
	if err := json.Unmarshal(res.Body.Bytes(), &single); err != nil || single.Remaining == nil || *single.Remaining != 2 {
		t.Fatalf("run endpoint did not share the progress snapshot: %s (%v)", res.Body, err)
	}

	close(f.continueWork)
	waitFor(t, func() bool { return readWorkflow(t, svc).Run.Status == runPartial })
	result := readWorkflow(t, svc).Run
	if result.ID != runID || result.CurrentArchive != nil || result.Processed != 3 ||
		result.Remaining == nil || *result.Remaining != 0 || *result.ProgressPercent != 100 ||
		f.createCalls.Load() != 1 {
		t.Fatalf("incorrect completed startup run: %+v", result)
	}
}

func TestStartupRetriesSetupAndPreservesManualMode(t *testing.T) {
	cfg := testConfig()
	f := newFixture(t, false)
	svc := newService(cfg, testStore(t), f.cloud(cfg))
	f.parentError.Store(true)
	if _, err := svc.bootstrap(t.Context(), false); err == nil {
		t.Fatal("expected temporary Drive error")
	}
	if view := readWorkflow(t, svc); view.Notebook.Status != "retrying" || view.Notebook.Error == "" || view.Run != nil {
		t.Fatalf("missing retry status: %+v, %+v", view.Notebook, view.Run)
	}
	f.parentError.Store(false)
	svc.automate(t.Context(), false)
	f.mu.Lock()
	f.notebookBody = []byte(`{"cells":[{"cell_type":"code","source":"previous release"}]}`)
	f.mu.Unlock()
	svc.automate(t.Context(), false)
	view := readWorkflow(t, svc)
	if view.Notebook.Status != "ready" || view.Notebook.Error != "" ||
		view.Run != nil || f.createCalls.Load() != 1 || f.updateCalls.Load() != 1 {
		t.Fatalf("manual-only startup should reuse the notebook without queuing: %+v, %+v", view.Notebook, view.Run)
	}
}

func readWorkflow(t *testing.T, svc *service) workflowView {
	t.Helper()
	res := expectAPI(t, svc, http.MethodGet, "/v1/status", "", http.StatusOK)
	var status workflowView
	if err := json.Unmarshal(res.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	return status
}
