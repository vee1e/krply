// Package metrics exposes Prometheus metrics for krply per design §19.
//
// The watch collector and api server share a *Metrics: the collector bumps the
// exported counters, and the api server serves them at /metrics. Store-derived
// state (degraded streams, storage bytes, per-stream gap counts) is refreshed
// through RefreshFromStore so a scrape reflects the durable journal.
package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/krply/krply/internal/storage"
)

const streamLabel = "stream"

// Metrics holds the concrete Prometheus instruments for krply. All fields are
// safe for concurrent use; the watch collector and api server may bump them
// directly.
type Metrics struct {
	reg *prometheus.Registry

	// Reconnects counts watch reconnects per stream.
	Reconnects *prometheus.CounterVec
	// Gone410 counts 410 Gone resyncs per stream.
	Gone410 *prometheus.CounterVec
	// EventsIngested counts raw watch events written per stream.
	EventsIngested *prometheus.CounterVec
	// EventsDeduped counts duplicate watch events skipped per stream.
	EventsDeduped *prometheus.CounterVec
	// Gaps counts gap records written per stream.
	Gaps *prometheus.CounterVec

	// IngestLag is the wall-clock lag between observed and ingested events.
	IngestLag prometheus.Gauge
	// StorageBytes is the current on-disk store size, when the store exposes it.
	StorageBytes prometheus.Gauge
	// DegradedStreams is the number of streams currently marked degraded.
	DegradedStreams prometheus.Gauge

	// ReplayPlanFailures counts replay plan failures.
	ReplayPlanFailures prometheus.Counter

	// gapGauges is the store-derived per-stream gap count, refreshed by
	// RefreshFromStore. It is intentionally distinct from Gaps, which counts
	// gap records as they are written by the watch collector.
	gapGauges *prometheus.GaugeVec
}

// New returns a Metrics with all instruments registered on its own internal
// registry. Use Registry or Handler to expose them.
func New() *Metrics {
	return newMetrics(prometheus.NewRegistry())
}

// NewRegistry returns a fresh registry with all krply metrics registered and
// seeded from the store. It is the wiring used by the api server; the returned
// registry contains the ingest counters as well as the store-derived gauges.
func NewRegistry(store storage.Store) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	m.RefreshFromStore(context.Background(), store)
	return reg
}

func newMetrics(reg *prometheus.Registry) *Metrics {
	streamLabels := []string{streamLabel}
	m := &Metrics{
		reg: reg,
		Reconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krply_watch_reconnects_total",
			Help: "Total watch reconnects by stream.",
		}, streamLabels),
		Gone410: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krply_410_gone_total",
			Help: "Total 410 Gone watch resyncs by stream.",
		}, streamLabels),
		EventsIngested: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krply_events_ingested_total",
			Help: "Total raw watch events ingested by stream.",
		}, streamLabels),
		EventsDeduped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krply_events_deduplicated_total",
			Help: "Total duplicate watch events deduplicated by stream.",
		}, streamLabels),
		Gaps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "krply_gaps_total",
			Help: "Total gap records written by stream.",
		}, streamLabels),
		IngestLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "krply_ingest_lag_seconds",
			Help: "Wall-clock lag between observed and ingested events, in seconds.",
		}),
		StorageBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "krply_storage_bytes",
			Help: "Current store size in bytes, when the store exposes it.",
		}),
		DegradedStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "krply_degraded_streams",
			Help: "Number of streams currently marked degraded.",
		}),
		ReplayPlanFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "krply_replay_plan_failures_total",
			Help: "Total replay plan failures.",
		}),
		gapGauges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "krply_gaps_current",
			Help: "Current gap count per stream as recorded in the store.",
		}, streamLabels),
	}
	reg.MustRegister(
		m.Reconnects,
		m.Gone410,
		m.EventsIngested,
		m.EventsDeduped,
		m.Gaps,
		m.IngestLag,
		m.StorageBytes,
		m.DegradedStreams,
		m.ReplayPlanFailures,
		m.gapGauges,
	)
	return m
}

// Registry returns the Prometheus registry backing this Metrics.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.reg
}

// Handler returns an HTTP handler that serves the Prometheus exposition format
// for this Metrics registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// sizeReporter is implemented by stores that can report their own size.
type sizeReporter interface {
	SizeBytes() (int64, error)
}

// RefreshFromStore reconciles the store-derived gauges with the current state
// of the journal: the degraded stream count, the per-stream gap counts, and
// the store size (best effort). Ingest lag is set by the watch collector and
// is left untouched.
func (m *Metrics) RefreshFromStore(ctx context.Context, store storage.Store) {
	streams, err := store.Streams(ctx)
	if err != nil {
		return
	}
	var degraded int
	m.gapGauges.Reset()
	for _, s := range streams {
		if s.Degraded {
			degraded++
		}
		m.gapGauges.WithLabelValues(s.StreamID).Set(float64(s.GapCount))
	}
	m.DegradedStreams.Set(float64(degraded))
	if sr, ok := store.(sizeReporter); ok {
		if n, err := sr.SizeBytes(); err == nil {
			m.StorageBytes.Set(float64(n))
		}
	}
}
