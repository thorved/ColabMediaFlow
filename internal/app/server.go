package app

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/thorved/ColabMediaFlow/internal/googleapi"
)

func Run(ctx context.Context) error {
	cfg, err := readConfig(".env")
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	state, err := openStore(cfg.dbPath)
	if err != nil {
		return err
	}
	defer state.db.Close()
	svc := newService(cfg, state, googleapi.New(cfg.clientID, cfg.clientSecret, cfg.refreshToken, googleapi.Options{}))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gin.SetMode(gin.ReleaseMode)
	server := &http.Server{
		Handler: svc.router(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}

	var background sync.WaitGroup
	background.Go(func() { svc.worker(ctx) })
	background.Go(func() { svc.scheduler(ctx) })
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	log.Printf("ColabMediaFlow listening on http://%s", listener.Addr())

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-served:
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
	}
	background.Wait()
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}
