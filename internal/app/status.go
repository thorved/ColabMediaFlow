package app

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type notebookSetup struct {
	Status   string `json:"status"`
	FileID   string `json:"file_id,omitempty"`
	ColabURL string `json:"colab_url,omitempty"`
	Error    string `json:"error,omitempty"`
}

func readyNotebook(id string) *notebookSetup {
	return &notebookSetup{
		Status: "ready", FileID: id, ColabURL: "https://colab.research.google.com/drive/" + id,
	}
}

type archiveProgress struct {
	FileID     string `json:"file_id"`
	Name       string `json:"name"`
	Phase      string `json:"phase"`
	Member     string `json:"member,omitempty"`
	BytesDone  *int64 `json:"bytes_done,omitempty"`
	BytesTotal *int64 `json:"bytes_total,omitempty"`
}

type runView struct {
	runResult
	Failed          int  `json:"failed"`
	Remaining       *int `json:"remaining"`
	ProgressPercent *int `json:"progress_percent"`
}

type workflowView struct {
	Notebook *notebookSetup `json:"notebook"`
	Run      *runView       `json:"run"`
}

func (r runResult) view() runView {
	view := runView{runResult: r, Failed: len(r.Failures)}
	// Existing SQLite rows predate the explicit scan flag.
	view.ScanComplete = r.ScanComplete || r.Scanned > 0 || r.Status == runSucceeded
	if view.ScanComplete {
		counted := r.Processed + r.Skipped + view.Failed
		remaining := max(0, r.Scanned-counted)
		percent := 100
		if r.Scanned > 0 {
			percent = (r.Scanned - remaining) * 100 / r.Scanned
		}
		view.Remaining, view.ProgressPercent = &remaining, &percent
	}
	return view
}

func (r report) hasProgress() bool {
	return r.ScanComplete || r.Scanned > 0 || r.CurrentArchive != nil
}

func (r *runResult) applyReport(result report) {
	if !result.hasProgress() && !result.Done && len(result.Failures) == 0 {
		return
	}
	r.ScanComplete = r.ScanComplete || result.ScanComplete || result.Scanned > 0 || (result.Done && result.Error == "")
	r.Scanned, r.Processed, r.Skipped = result.Scanned, result.Processed, result.Skipped
	if result.Failures != nil {
		r.Failures = result.Failures
	}
	r.CurrentArchive = result.CurrentArchive
	now := time.Now().UTC()
	r.ProgressUpdatedAt = &now
}

func (s *service) getStatus(c *gin.Context) {
	run, err := s.store.latestRun(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read run state"})
		return
	}
	setup := s.setup.Load()
	if setup == nil {
		setup = &notebookSetup{Status: "initializing"}
	}
	var view *runView
	if run != nil {
		result := run.view()
		view = &result
	}
	c.JSON(http.StatusOK, workflowView{Notebook: setup, Run: view})
}
