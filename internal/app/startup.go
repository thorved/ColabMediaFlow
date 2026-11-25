package app

import (
	"context"
	"errors"
	"log"
	"time"
)

func (s *service) automate(ctx context.Context, recovering bool) {
	startImmediately := !recovering && s.cfg.interval > 0
	var lastError string
	for ctx.Err() == nil {
		active, err := s.bootstrap(ctx, startImmediately)
		switch {
		case err == nil && !active:
			s.scheduler(ctx)
			return
		case active:
			// The recovered run takes the place of an immediate startup run.
			startImmediately = false
		case !errors.Is(err, errNotebookBusy) && ctx.Err() == nil && err.Error() != lastError:
			lastError = err.Error()
			log.Printf("notebook startup will retry: %v", err)
		}
		if pause(ctx, 5*time.Second) != nil {
			return
		}
	}
}

func (s *service) bootstrap(ctx context.Context, startImmediately bool) (bool, error) {
	if !s.notebookMu.TryLock() {
		return false, errNotebookBusy
	}
	defer s.notebookMu.Unlock()
	active, err := s.store.activeRun(ctx)
	if err == nil && active != nil {
		s.setup.Store(&notebookSetup{Status: "recovering"})
		return true, nil
	}
	if err == nil {
		var id string
		id, _, err = ensureNotebook(ctx, s.runner.google, s.store, s.cfg.notebookParent)
		if err == nil && startImmediately {
			_, _, err = s.enqueue(ctx, id, "startup", false)
		}
		if err == nil {
			s.setup.Store(readyNotebook(id))
			return false, nil
		}
	}
	if ctx.Err() == nil {
		s.setup.Store(&notebookSetup{Status: "retrying", Error: err.Error()})
	}
	return false, err
}
