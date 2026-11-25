package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const (
	runQueued    = "queued"
	runRunning   = "running"
	runCleaning  = "cleaning_up"
	runSucceeded = "succeeded"
	runPartial   = "partial"
	runFailed    = "failed"
)

type archiveFailure struct {
	FileID    string `json:"file_id"`
	Version   string `json:"version"`
	Name      string `json:"name"`
	Error     string `json:"error"`
	Permanent bool   `json:"permanent"`
}

type report struct {
	Done      bool             `json:"done"`
	Scanned   int              `json:"scanned"`
	Processed int              `json:"processed"`
	Skipped   int              `json:"skipped"`
	Failures  []archiveFailure `json:"failures"`
	Resolved  []string         `json:"resolved"`
	Error     string           `json:"error"`
}

type runResult struct {
	ID         string           `json:"id"`
	Trigger    string           `json:"trigger"`
	Status     string           `json:"status"`
	CreatedAt  time.Time        `json:"created_at"`
	StartedAt  *time.Time       `json:"started_at,omitempty"`
	FinishedAt *time.Time       `json:"finished_at,omitempty"`
	Scanned    int              `json:"scanned"`
	Processed  int              `json:"processed"`
	Skipped    int              `json:"skipped"`
	Failures   []archiveFailure `json:"failures"`
	Error      string           `json:"error,omitempty"`
}

type runState struct {
	runResult
	NotebookID  string
	InputFolder string
	RetryFailed bool
	RuntimeName string
	SessionName string
	ExecutionID string
	Code        string
	Dispatched  bool
	Settled     bool
	FinalStatus string
}

type store struct {
	db *sql.DB
}

func openStore(path string) (*store, error) {
	p := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" {
		p = "/" + p
	}
	dsn := (&url.URL{Scheme: "file", Path: p, RawQuery: "_pragma=busy_timeout%285000%29"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	statements := []string{
		"PRAGMA journal_mode = WAL",
		"CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
		"CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, active INTEGER NOT NULL, body TEXT NOT NULL)",
		"CREATE UNIQUE INDEX IF NOT EXISTS only_one_active_run ON runs (active) WHERE active = 1",
		"CREATE TABLE IF NOT EXISTS bad_archives (folder_id TEXT NOT NULL, file_id TEXT NOT NULL, version TEXT NOT NULL, PRIMARY KEY(folder_id, file_id))",
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize state database: %w", err)
		}
	}
	return &store{db}, nil
}

func (s *store) metadata(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key = ?", key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (s *store) setMetadata(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value)
	return err
}

func (s *store) getRun(ctx context.Context, id string) (*runState, error) {
	return scanRun(s.db.QueryRowContext(ctx, "SELECT body FROM runs WHERE id = ?", id))
}

func (s *store) activeRun(ctx context.Context) (*runState, error) {
	return scanRun(s.db.QueryRowContext(ctx, "SELECT body FROM runs WHERE active = 1"))
}

func scanRun(row *sql.Row) (*runState, error) {
	var data string
	if err := row.Scan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var run runState
	err := json.Unmarshal([]byte(data), &run)
	return &run, err
}

func (s *store) insertRun(ctx context.Context, run *runState) (bool, error) {
	data, err := json.Marshal(run)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx,
		"INSERT INTO runs(id,active,body) VALUES(?,1,?) ON CONFLICT(active) WHERE active=1 DO NOTHING", run.ID, string(data))
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

func (s *store) saveRun(ctx context.Context, run *runState) error {
	return saveActiveRun(ctx, s.db, run, true)
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func saveActiveRun(ctx context.Context, db execer, run *runState, active bool) error {
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "UPDATE runs SET active=?, body=? WHERE id=? AND active=1", active, string(data), run.ID)
	return err
}

func (s *store) recordReport(ctx context.Context, run *runState, result report) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, id := range result.Resolved {
		if _, err := tx.ExecContext(ctx, "DELETE FROM bad_archives WHERE folder_id=? AND file_id=?", run.InputFolder, id); err != nil {
			return err
		}
	}
	for _, failure := range result.Failures {
		if !failure.Permanent || failure.Version == "" {
			continue
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO bad_archives(folder_id,file_id,version) VALUES(?,?,?) ON CONFLICT(folder_id,file_id) DO UPDATE SET version=excluded.version",
			run.InputFolder, failure.FileID, failure.Version)
		if err != nil {
			return err
		}
	}
	if err := saveActiveRun(ctx, tx, run, true); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *store) badVersions(ctx context.Context, folderID string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT file_id,version FROM bad_archives WHERE folder_id=?", folderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	versions := make(map[string]string)
	for rows.Next() {
		var id, version string
		if err := rows.Scan(&id, &version); err != nil {
			return nil, err
		}
		versions[id] = version
	}
	return versions, rows.Err()
}

func (s *store) finish(ctx context.Context, run *runState) error {
	now := time.Now().UTC()
	run.FinishedAt = &now
	run.Status = run.FinalStatus
	run.Code = ""
	return saveActiveRun(ctx, s.db, run, false)
}
