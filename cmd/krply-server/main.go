// Command krply-server runs the krply query API and serves the web UI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/krply/krply/internal/api"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/materialize"
	"github.com/krply/krply/internal/metrics"
	"github.com/krply/krply/internal/replay"
	"github.com/krply/krply/internal/storage"
	"github.com/krply/krply/internal/version"
)

var buildVersion = version.Version

func main() {
	if err := run(); err != nil {
		slog.Error("krply-server failed", "err", err)
		os.Exit(1)
	}
}

// seedDemo copies the events and snapshot refs from a demo SQLite fixture into
// an empty journal. It is idempotent: when the target journal already has
// events, it is left untouched.
func seedDemo(ctx context.Context, store storage.Store, demoPath string) error {
	existing, err := store.Events(ctx, storage.EventFilter{Limit: 1})
	if err != nil {
		return fmt.Errorf("check existing events: %w", err)
	}
	if len(existing) > 0 {
		return nil
	}

	demo, err := storage.NewSQLiteStore(demoPath)
	if err != nil {
		return fmt.Errorf("open demo store: %w", err)
	}
	defer demo.Close()

	recs, err := demo.Events(ctx, storage.EventFilter{})
	if err != nil {
		return fmt.Errorf("read demo events: %w", err)
	}
	if len(recs) > 0 {
		ptrs := make([]*event.Record, 0, len(recs))
		for i := range recs {
			ptrs = append(ptrs, &recs[i])
		}
		if _, err := store.Appends(ctx, ptrs); err != nil {
			return fmt.Errorf("append demo events: %w", err)
		}
	}

	snaps, err := demo.Snapshots(ctx)
	if err != nil {
		return fmt.Errorf("read demo snapshots: %w", err)
	}
	for i := range snaps {
		if err := store.SaveSnapshot(ctx, &snaps[i]); err != nil {
			return fmt.Errorf("append demo snapshot: %w", err)
		}
	}

	slog.Info("seeded demo journal", "events", len(recs), "snapshots", len(snaps))
	return nil
}

func run() error {
	var (
		storePath    = flag.String("store", "krply.db", "path to the SQLite journal")
		listen       = flag.String("listen", ":8080", "listen address")
		demoPath     = flag.String("demo", "", "seed an empty journal from this SQLite demo fixture")
		showVer      = flag.Bool("version", false, "print version and exit")
		storeFlagSet bool
	)
	flag.Parse()

	if *showVer {
		fmt.Println(buildVersion)
		return nil
	}

	listenAddr := *listen
	listenFlagSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "listen" {
			listenFlagSet = true
		}
		if f.Name == "store" {
			storeFlagSet = true
		}
	})
	if !listenFlagSet && os.Getenv("PORT") != "" {
		listenAddr = ":" + os.Getenv("PORT")
	}
	if !listenFlagSet && os.Getenv("LISTEN_ADDR") != "" {
		listenAddr = os.Getenv("LISTEN_ADDR")
	}
	if !storeFlagSet && os.Getenv("STORE_PATH") != "" {
		*storePath = os.Getenv("STORE_PATH")
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.NewSQLiteStore(*storePath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	if *demoPath != "" {
		if err := seedDemo(ctx, store, *demoPath); err != nil {
			return fmt.Errorf("seed demo journal: %w", err)
		}
	}

	mat := materialize.NewMaterializer(store)
	planner := replay.NewPlanner(store, mat, replay.DefaultPolicy())

	m := metrics.New()
	m.RefreshFromStore(ctx, store)
	planner.SetMetrics(m)

	// Store-derived gauges are refreshed on a ticker; nothing else in the
	// process would update degraded streams, gap counts, or store size.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.RefreshFromStore(ctx, store)
			}
		}
	}()

	srv, err := api.NewServer(store, mat, planner, m, buildVersion)
	if err != nil {
		return fmt.Errorf("build api server: %w", err)
	}

	httpSrv := &http.Server{
		Addr:              listenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("krply-server listening", "addr", listenAddr, "store", *storePath, "version", buildVersion)
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
