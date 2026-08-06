// Package watch implements the krply collector: it lists and watches
// Kubernetes collections with exact resource-version semantics, bookmarks,
// reconnects, 410 Gone relists, and baseline handling.
package watch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"k8s.io/client-go/dynamic"

	"github.com/krply/krply/internal/discovery"
	"github.com/krply/krply/internal/metrics"
	"github.com/krply/krply/internal/storage"
	"github.com/krply/krply/internal/version"
)

// Config configures a Collector.
type Config struct {
	// KubeConfig is the path to a kubeconfig file. Empty means in-cluster.
	KubeConfig string
	// Context is the kubeconfig context to use (empty means the current one).
	Context string
	// ClusterID is the stable cluster identifier (see ClusterID).
	ClusterID string
	// Resources are the collections to watch. Empty defaults to
	// discovery.DefaultResources().
	Resources []discovery.ResourceSpec
	// Selector is an optional label selector applied to every stream.
	Selector string
	// Store is the durable journal. It is required.
	Store storage.Store
	// Log is the collector logger. Empty defaults to a discard handler.
	Log *slog.Logger
	// Bookmarks enables allowWatchBookmarks on watch requests. Bookmarks
	// advance checkpoints only.
	Bookmarks bool
	// SendInitial is accepted for API compatibility; the collector uses the
	// conventional list-plus-watch path with synthetic baseline events.
	SendInitial bool
	// AgentName is set as the client-go User-Agent when building the client.
	AgentName string
	// MinBackoff and MaxBackoff bound the reconnect backoff. Defaults are
	// 500ms and 30s when zero.
	MinBackoff time.Duration
	MaxBackoff time.Duration

	// WatchIdleTimeout forces a reconnect when no watch event or bookmark has
	// arrived within the window. Zero uses a 10 minute default. It prevents a
	// silently dead connection from stalling the stream forever.
	WatchIdleTimeout time.Duration

	// Metrics, when non-nil, receives ingest counters for this collector.
	Metrics *metrics.Metrics

	// DynamicClient, when non-nil, is used instead of building a client from
	// KubeConfig. It exists to make the collector testable with fake clients.
	DynamicClient dynamic.Interface
}

// Collector runs one list-and-watch stream per configured resource.
type Collector struct {
	cfg Config
	dyn dynamic.Interface
}

// NewCollector validates the configuration and builds the dynamic client.
func NewCollector(cfg Config) (*Collector, error) {
	if cfg.Store == nil {
		return nil, errors.New("watch: Store is required")
	}
	applyConfigDefaults(&cfg)

	var dyn dynamic.Interface
	if cfg.DynamicClient != nil {
		dyn = cfg.DynamicClient
	} else {
		restCfg, err := clientConfig(cfg.KubeConfig, cfg.Context)
		if err != nil {
			return nil, fmt.Errorf("watch: build rest config: %w", err)
		}
		restCfg.UserAgent = userAgent(cfg)
		dyn, err = dynamic.NewForConfig(restCfg)
		if err != nil {
			return nil, fmt.Errorf("watch: new dynamic client: %w", err)
		}
	}

	if len(cfg.Resources) == 0 {
		cfg.Resources = discovery.DefaultResources()
	}
	return &Collector{cfg: cfg, dyn: dyn}, nil
}

func applyConfigDefaults(cfg *Config) {
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.MinBackoff > cfg.MaxBackoff {
		cfg.MinBackoff = cfg.MaxBackoff
	}
	if cfg.WatchIdleTimeout <= 0 {
		cfg.WatchIdleTimeout = 10 * time.Minute
	}
	if cfg.ClusterID == "" {
		cfg.ClusterID = "cluster-unknown"
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

func userAgent(cfg Config) string {
	if cfg.AgentName != "" {
		return cfg.AgentName
	}
	return "krply/" + version.Version
}

// Run starts one stream goroutine per configured resource and waits for all of
// them to stop. It returns nil when the context is cancelled and the first
// unrecoverable error (for example a closed store) otherwise. When one stream
// fails, the remaining streams are cancelled so no goroutine keeps writing to
// the store after Run has returned.
func (c *Collector) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(c.cfg.Resources))

	for _, spec := range c.cfg.Resources {
		spec := spec
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					select {
					case errCh <- fmt.Errorf("watch stream panic: %v", r):
					case <-ctx.Done():
					}
				}
			}()
			if err := c.runStream(ctx, spec); err != nil {
				select {
				case errCh <- err:
				case <-ctx.Done():
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		<-done
		return nil
	case err := <-errCh:
		cancel()
		<-done
		return err
	case <-done:
		return nil
	}
}
