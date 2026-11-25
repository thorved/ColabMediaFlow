package app

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/thorved/ColabMediaFlow/internal/googleapi"
)

var (
	errNoNotebook   = errors.New("create the managed notebook with PUT /v1/notebook before running it")
	errNotebookBusy = errors.New("the managed notebook is being updated")
)

type service struct {
	cfg        config
	store      *store
	runner     *runner
	notebookMu sync.Mutex
	wake       chan struct{}
}

func newService(cfg config, db *store, google *googleapi.Client) *service {
	return &service{cfg: cfg, store: db, runner: &runner{google: google, state: db}, wake: make(chan struct{}, 1)}
}

func (s *service) signalWorker() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *service) queue(ctx context.Context, trigger string, retryFailed bool) (*runState, bool, error) {
	if !s.notebookMu.TryLock() {
		return nil, false, errNotebookBusy
	}
	defer s.notebookMu.Unlock()
	active, err := s.store.activeRun(ctx)
	if err != nil || active != nil {
		return active, false, err
	}
	id, err := s.store.metadata(ctx, "notebook_id")
	if err != nil {
		return nil, false, err
	}
	if id == "" {
		return nil, false, errNoNotebook
	}
	run := &runState{
		runResult: runResult{
			ID: uuid.NewString(), Trigger: trigger, Status: runQueued,
			CreatedAt: time.Now().UTC(), Failures: []archiveFailure{},
		},
		NotebookID: id, InputFolder: s.cfg.inputFolder, RetryFailed: retryFailed,
	}
	inserted, err := s.store.insertRun(ctx, run)
	if err != nil {
		return nil, false, err
	}
	if !inserted {
		active, err := s.store.activeRun(ctx)
		return active, false, err
	}
	log.Printf("queued %s run %s", trigger, run.ID)
	s.signalWorker()
	return run, true, nil
}

func (s *service) scheduler(ctx context.Context) {
	if s.cfg.interval == 0 {
		return
	}
	ticker := time.NewTicker(s.cfg.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _, err := s.queue(ctx, "scheduled", false)
			if err != nil && !errors.Is(err, errNoNotebook) && !errors.Is(err, errNotebookBusy) && ctx.Err() == nil {
				log.Printf("could not queue scheduled run: %v", err)
			}
		}
	}
}

func (s *service) worker(ctx context.Context) {
	retry := time.NewTicker(15 * time.Second)
	defer retry.Stop()
	s.signalWorker()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-retry.C:
		}
		if err := s.advance(ctx); err != nil && ctx.Err() == nil {
			log.Printf("could not advance run: %v", err)
		}
	}
}

func (s *service) advance(ctx context.Context) error {
	run, err := s.store.activeRun(ctx)
	if err != nil || run == nil {
		return err
	}
	if run.Status == runQueued {
		now := time.Now().UTC()
		run.StartedAt = &now
		run.Status = runRunning
		if err := s.store.saveRun(ctx, run); err != nil {
			return err
		}
	}
	if run.Status == runRunning {
		runCtx, cancel := context.WithDeadline(ctx, run.StartedAt.Add(runBudget))
		result, settled, runErr := s.runner.execute(runCtx, run)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if runErr != nil && run.Dispatched && !result.Done {
			recoveryCtx, stop := context.WithTimeout(ctx, 45*time.Second)
			provisional, err := s.runner.report(recoveryCtx, run)
			stop()
			if err == nil && (provisional.Done || provisional.Scanned >= result.Scanned) {
				result = provisional
				if result.Done {
					runErr = nil
				}
			}
		}
		run.updateOutcome(result, settled, runErr)
		if err := s.store.recordReport(ctx, run, result); err != nil {
			return err
		}
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if err := s.runner.clean(cleanupCtx, run); err != nil {
		return err
	}
	if err := s.store.finish(ctx, run); err != nil {
		return err
	}
	log.Printf("run %s %s, scanned=%d processed=%d skipped=%d failures=%d", run.ID, run.Status, run.Scanned, run.Processed, run.Skipped, len(run.Failures))
	return nil
}

func (run *runState) updateOutcome(result report, settled bool, executionErr error) {
	run.Status = runCleaning
	run.Settled = settled
	run.Scanned, run.Processed, run.Skipped = result.Scanned, result.Processed, result.Skipped
	if result.Failures != nil {
		run.Failures = result.Failures
	}
	run.Error = result.Error
	if executionErr != nil {
		run.Error = executionErr.Error()
	}
	switch {
	case run.Error != "":
		run.FinalStatus = runFailed
	case len(result.Failures) > 0 && result.Processed > 0:
		run.FinalStatus = runPartial
	case len(result.Failures) > 0:
		run.FinalStatus = runFailed
	default:
		run.FinalStatus = runSucceeded
	}
}

func (s *service) router() *gin.Engine {
	router := gin.New()
	// Gin's default panic logger dumps request headers, including the local key.
	router.Use(gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, _ any) {
		log.Printf("recovered API handler panic")
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
	}))
	api := router.Group("/v1", func(c *gin.Context) {
		bearer, found := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
		if !found || subtle.ConstantTimeCompare([]byte(bearer), []byte(s.cfg.apiKey)) != 1 {
			c.Header("WWW-Authenticate", "Bearer")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "valid local API bearer key required"})
			return
		}
	})
	api.PUT("/notebook", s.putNotebook)
	api.POST("/runs", s.postRun)
	api.GET("/runs/:id", s.getRun)
	return router
}

func (s *service) putNotebook(c *gin.Context) {
	if !s.notebookMu.TryLock() {
		c.JSON(http.StatusConflict, gin.H{"error": errNotebookBusy.Error()})
		return
	}
	defer s.notebookMu.Unlock()
	ctx := c.Request.Context()
	run, err := s.store.activeRun(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read run state"})
		return
	}
	if run != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "a notebook run is active", "active_run_id": run.ID})
		return
	}
	id, created, err := ensureNotebook(ctx, s.runner.google, s.store, s.cfg.notebookParent)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	c.JSON(status, gin.H{"file_id": id, "colab_url": "https://colab.research.google.com/drive/" + id})
}

func (s *service) postRun(c *gin.Context) {
	var request *struct {
		RetryFailed bool `json:"retry_failed"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err == nil {
		if request == nil || decoder.Decode(new(any)) != io.EOF {
			c.JSON(http.StatusBadRequest, gin.H{"error": "expected one run options object"})
			return
		}
	} else if err != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"error": "expected a JSON object with optional retry_failed"})
		return
	}
	run, inserted, err := s.queue(c.Request.Context(), "manual", request != nil && request.RetryFailed)
	switch {
	case errors.Is(err, errNoNotebook) || errors.Is(err, errNotebookBusy):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not queue notebook run"})
	case !inserted && run != nil:
		c.JSON(http.StatusConflict, gin.H{"error": "a notebook run is active", "active_run_id": run.ID})
	case !inserted:
		c.JSON(http.StatusConflict, gin.H{"error": "the run slot changed, retry the request"})
	default:
		c.Header("Location", "/v1/runs/"+run.ID)
		c.JSON(http.StatusAccepted, run.runResult)
	}
}

func (s *service) getRun(c *gin.Context) {
	run, err := s.store.getRun(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read run state"})
		return
	}
	if run == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	c.JSON(http.StatusOK, run.runResult)
}
