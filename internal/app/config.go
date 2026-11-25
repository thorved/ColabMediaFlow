package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/thorved/ColabMediaFlow/internal/googleapi"
)

type config struct {
	clientID, clientSecret, refreshToken string
	inputFolder, notebookParent, apiKey  string

	dbPath, listenAddr string
	interval           time.Duration
}

func readConfig(envFile string) (config, error) {
	if err := godotenv.Load(envFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return config{}, errors.New("could not read .env; check its permissions and KEY=value syntax")
	}
	return configFromEnv(os.Getenv)
}

func configFromEnv(getenv func(string) string) (config, error) {
	var cfg config
	required := []struct {
		key    string
		target *string
	}{
		{"GOOGLE_OAUTH_CLIENT_ID", &cfg.clientID},
		{"GOOGLE_OAUTH_CLIENT_SECRET", &cfg.clientSecret},
		{"GOOGLE_OAUTH_REFRESH_TOKEN", &cfg.refreshToken},
		{"DRIVE_DOWNLOAD_FOLDER_ID", &cfg.inputFolder},
		{"NOTEBOOK_PARENT_FOLDER_ID", &cfg.notebookParent},
		{"API_KEY", &cfg.apiKey},
	}
	for _, item := range required {
		value := strings.TrimSpace(getenv(item.key))
		if value == "" || strings.HasPrefix(value, "replace-with-") {
			return config{}, fmt.Errorf("set %s in .env or the process environment", item.key)
		}
		*item.target = value
	}
	if !googleapi.ValidDriveID(cfg.inputFolder) || !googleapi.ValidDriveID(cfg.notebookParent) {
		return config{}, errors.New("Drive folder settings must be file IDs")
	}
	cfg.interval = time.Minute
	if value := strings.TrimSpace(getenv("RUN_INTERVAL")); value != "" {
		interval, err := time.ParseDuration(value)
		if err != nil || interval < 0 {
			return config{}, errors.New("RUN_INTERVAL must be a Go duration such as 1m or 30s, or 0")
		}
		cfg.interval = interval
	}
	cfg.listenAddr = strings.TrimSpace(getenv("LISTEN_ADDR"))
	if cfg.listenAddr == "" {
		cfg.listenAddr = "127.0.0.1:8080"
	}
	cfg.dbPath = strings.TrimSpace(getenv("STATE_DB"))
	if cfg.dbPath == "" {
		cfg.dbPath = "colabmediaflow.db"
	}
	path, err := filepath.Abs(cfg.dbPath)
	if err != nil {
		return config{}, fmt.Errorf("resolve STATE_DB: %w", err)
	}
	cfg.dbPath = path
	return cfg, nil
}
