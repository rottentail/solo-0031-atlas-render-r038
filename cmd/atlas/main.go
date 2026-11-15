package main

import (
	"atlas-render/internal/engine"
	"atlas-render/internal/httpapi"
	"atlas-render/internal/persist"
	"atlas-render/internal/service"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	if e := run(); e != nil {
		log.Print(e)
		os.Exit(1)
	}
}
func run() error {
	mode := "serve"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	path := os.Getenv("ATLAS_STATE")
	if path == "" {
		path = "var/atlas-state.json"
	}
	st, e := persist.Open(path)
	if e != nil {
		return e
	}
	defer st.Close()
	switch mode {
	case "serve":
		return serve(st)
	case "worker":
		return work(st)
	default:
		return errors.New("usage: atlas [serve|worker]")
	}
}

func serve(st *persist.Store) error {
	addr := os.Getenv("ATLAS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8763"
	}
	lease := durationEnv("ATLAS_LEASE", engine.DefaultLease)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The API reaps expired leases so a dead worker's job is visible as queued
	// even before a replacement execution process starts.
	go engine.ReapLoop(ctx, st, lease, log.Printf)
	srv := &http.Server{Addr: addr, Handler: (httpapi.API{Service: &service.Service{Store: st}}).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	errc := make(chan error, 1)
	go func() {
		log.Printf("Atlas Render API listening on %s (state %s)", addr, st.Path())
		errc <- srv.ListenAndServe()
	}()
	select {
	case e := <-errc:
		stop()
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	return nil
}

func work(st *persist.Store) error {
	cfg := engine.DefaultConfig()
	cfg.Lease = durationEnv("ATLAS_LEASE", engine.DefaultLease)
	cfg.Poll = durationEnv("ATLAS_POLL", engine.DefaultPoll)
	cfg.Concurrency = intEnv("ATLAS_WORKERS", engine.DefaultConcurrency)
	worker := engine.New(st, cfg)
	if e := worker.Recover(); e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("Atlas Render worker %s consuming %s (concurrency %d, lease %s)", cfg.ID, st.Path(), cfg.Concurrency, cfg.Lease)
	worker.Run(ctx)
	return nil
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if d, e := time.ParseDuration(v); e == nil && d > 0 {
			return d
		}
	}
	return fallback
}

func intEnv(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n > 0 {
			return n
		}
	}
	return fallback
}
