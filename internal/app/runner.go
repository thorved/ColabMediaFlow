package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/thorved/ColabMediaFlow/internal/googleapi"
)

const runBudget = 6 * time.Hour

type runner struct {
	google         *googleapi.Client
	state          *store
	notebookParent string
}

func runDirectory(id string) string {
	return "content/colab-media-flow-" + id
}

func (r *runner) prepare(ctx context.Context, run *runState) error {
	if run.Code == "" {
		data, err := r.google.DownloadDriveFile(ctx, run.NotebookID)
		if err != nil && !googleapi.IsNotFound(err) {
			return err
		}
		cells, err := verifiedCode(data)
		if err != nil {
			// A queued run from a previous release has no remote execution to
			// reattach to. Restore the current worker before dispatching it.
			id, _, err := ensureNotebook(ctx, r.google, r.state, r.notebookParent)
			if err != nil {
				return err
			}
			cells = notebookCode()
			run.NotebookID = id
		}
		run.Code = "MEDIA_FLOW_RUN_PATH = " + string(encodeJSON("/"+runDirectory(run.ID))) + "\n" + strings.Join(cells, "\n")
		run.RuntimeName, err = r.state.metadata(ctx, "runtime_name")
		if err != nil {
			return err
		}
		if run.RuntimeName == "" {
			run.RuntimeName = "runtimes/media-flow-" + uuid.NewString()
			if err := r.state.setMetadata(ctx, "runtime_name", run.RuntimeName); err != nil {
				return err
			}
		}
		run.ExecutionID = uuid.NewString()
		if err := r.state.saveRun(ctx, run); err != nil {
			return err
		}
	}
	if run.Dispatched {
		err := r.google.CheckRuntime(ctx, run.RuntimeName)
		if googleapi.IsNotFound(err) {
			return errors.New("the Colab runtime stopped while the notebook run was active")
		}
		return err
	}
	if err := r.google.EnsureRuntime(ctx, run.RuntimeName, run.ID); err != nil {
		return err
	}
	run.SessionName = run.RuntimeName + "/sessions/run-" + run.ID
	if err := r.state.saveRun(ctx, run); err != nil {
		return err
	}
	if err := r.google.CreateSession(ctx, run.RuntimeName, run.SessionName); err != nil {
		return err
	}
	directory := runDirectory(run.ID)
	if err := r.google.CreateRuntimeDirectory(ctx, run.RuntimeName, directory); err != nil {
		return err
	}
	versions, err := r.state.badVersions(ctx, run.InputFolder)
	if err != nil {
		return err
	}
	settings := map[string]any{"input_folder": run.InputFolder, "retry_failed": run.RetryFailed, "bad_versions": versions}
	return r.google.WriteRuntimeFile(ctx, run.RuntimeName, directory+"/settings.json", encodeJSON(settings))
}

func (r *runner) report(ctx context.Context, run *runState) (report, error) {
	data, err := r.google.ReadRuntimeFile(ctx, run.RuntimeName, runDirectory(run.ID)+"/report.json")
	if googleapi.IsNotFound(err) {
		return report{}, nil
	}
	if err != nil {
		return report{}, err
	}
	var result report
	if err := json.Unmarshal(data, &result); err != nil {
		return report{}, errors.New("decode Colab worker report")
	}
	return result, nil
}

func (r *runner) sendToken(ctx context.Context, run *runState, previous string) (string, error) {
	token, err := r.google.AccessToken(ctx)
	if err != nil {
		return previous, err
	}
	if token != previous {
		data := encodeJSON(map[string]string{"access_token": token})
		if err := r.google.WriteRuntimeFile(ctx, run.RuntimeName, runDirectory(run.ID)+"/oauth.json", data); err != nil {
			return previous, err
		}
	}
	return token, nil
}

func (r *runner) refreshDuringRun(ctx context.Context, run *runState, last string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var err error
			last, err = r.sendToken(ctx, run, last)
			if err != nil && ctx.Err() == nil {
				log.Printf("run %s: refresh runtime access token: %v", run.ID, err)
			}
		}
	}
}

func (r *runner) watchProgress(ctx context.Context, snapshot runState) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	var last report
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		result, err := r.report(ctx, &snapshot)
		if err != nil || (!result.hasProgress() && !result.Done) || reflect.DeepEqual(last, result) {
			continue
		}
		snapshot.applyReport(result)
		if err := r.state.saveRun(ctx, &snapshot); err == nil {
			last = result
		} else if ctx.Err() == nil {
			log.Printf("run %s: store progress: %v", snapshot.ID, err)
		}
	}
}

func (r *runner) execute(ctx context.Context, run *runState) (report, bool, error) {
	if err := r.prepare(ctx, run); err != nil {
		return report{}, false, err
	}
	// Check for a completed report before calling execute_code again. Colab can
	// execute the same ID a second time after it has already delivered a result.
	if run.Dispatched {
		result, err := r.report(ctx, run)
		if err == nil && result.Done {
			return result, false, nil // Observe the report, then stop the recovered kernel.
		}
		if err != nil && !googleapi.Retryable(err) {
			return result, false, err
		}
	}
	token, err := r.sendToken(ctx, run, "")
	if err != nil {
		return report{}, false, err
	}
	run.Dispatched = true
	if err := r.state.saveRun(ctx, run); err != nil {
		return report{}, false, err
	}
	monitorCtx, stopMonitors := context.WithCancel(ctx)
	var monitors sync.WaitGroup
	monitors.Go(func() { r.refreshDuringRun(monitorCtx, run, token) })
	snapshot := *run
	monitors.Go(func() { r.watchProgress(monitorCtx, snapshot) })
	defer func() {
		stopMonitors()
		monitors.Wait()
	}()

	delay := time.Second
	for {
		settled, err := r.google.ExecuteCode(ctx, run.SessionName, run.ExecutionID, run.Code)
		var result report
		var reportErr error
		if ctx.Err() == nil {
			result, reportErr = r.report(ctx, run)
		}
		if result.Done {
			return result, settled, nil
		}
		if ctx.Err() != nil {
			return result, settled, ctx.Err()
		}
		if settled {
			// Colab has returned the terminal execution response. Retry reading
			// the report independently, so the notebook cannot execute again.
			for attempt := 0; reportErr != nil && googleapi.Retryable(reportErr) && attempt < 3; attempt++ {
				if pause(ctx, time.Second<<attempt) != nil {
					break
				}
				result, reportErr = r.report(ctx, run)
			}
			if result.Done {
				return result, true, nil
			}
			if err != nil {
				return result, true, err
			}
			if reportErr != nil {
				return result, true, reportErr
			}
			return result, true, errors.New("managed notebook did not write its final run report")
		}
		if !googleapi.Retryable(err) {
			return result, false, err
		}
		if err := pause(ctx, delay); err != nil {
			return result, false, err
		}
		delay = min(delay*2, 15*time.Second)
	}
}

func (r *runner) clean(ctx context.Context, run *runState) error {
	if run.SessionName == "" {
		return nil
	}
	if err := r.google.CheckRuntime(ctx, run.RuntimeName); err != nil {
		if googleapi.IsNotFound(err) {
			return nil
		}
		return err
	}

	directory := runDirectory(run.ID)
	cleanupSession := run.RuntimeName + "/sessions/cleanup-" + run.ID
	targetSession := run.SessionName
	// Stop potentially running notebook work before removing its scratch files.
	if !run.Settled {
		if err := r.google.DeleteSession(ctx, targetSession); err != nil {
			return err
		}
		targetSession = cleanupSession
	}
	err := r.google.CheckRuntimeDirectory(ctx, run.RuntimeName, directory)
	if err != nil && !googleapi.IsNotFound(err) {
		return err
	}
	if err == nil {
		if err := r.google.CreateSession(ctx, run.RuntimeName, targetSession); err != nil {
			return err
		}
		// shutil removes the data directly. The proxy's Contents DELETE can
		// move large temporary files into the runtime's trash instead.
		code := "import shutil\nfrom pathlib import Path\nscratch = Path(" + string(encodeJSON("/"+directory)) +
			")\nif scratch.exists():\n    shutil.rmtree(scratch)\n"
		if _, err := r.google.ExecuteCode(ctx, targetSession, run.ID, code); err != nil {
			return fmt.Errorf("remove Colab scratch files: %w", err)
		}
	}
	return r.google.DeleteSession(ctx, targetSession)
}

// Callers supply only JSON-compatible structs, strings, and maps.
func encodeJSON(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func pause(ctx context.Context, delay time.Duration) error {
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
