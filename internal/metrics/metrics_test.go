package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
)

func TestCountersIncrement(t *testing.T) {
	m := New()
	stream := "c/apps/v1/deployments/default/"

	m.Reconnects.WithLabelValues(stream).Inc()
	m.Gone410.WithLabelValues(stream).Inc()
	m.EventsIngested.WithLabelValues(stream).Inc()
	m.EventsDeduped.WithLabelValues(stream).Inc()
	m.Gaps.WithLabelValues(stream).Inc()
	m.ReplayPlanFailures.Inc()
	m.IngestLag.Set(1.5)
	m.StorageBytes.Set(4096)

	if got := testutil.ToFloat64(m.Reconnects.WithLabelValues(stream)); got != 1 {
		t.Errorf("Reconnects = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.Gone410.WithLabelValues(stream)); got != 1 {
		t.Errorf("Gone410 = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.EventsIngested.WithLabelValues(stream)); got != 1 {
		t.Errorf("EventsIngested = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.EventsDeduped.WithLabelValues(stream)); got != 1 {
		t.Errorf("EventsDeduped = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.Gaps.WithLabelValues(stream)); got != 1 {
		t.Errorf("Gaps = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ReplayPlanFailures); got != 1 {
		t.Errorf("ReplayPlanFailures = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.IngestLag); got != 1.5 {
		t.Errorf("IngestLag = %v, want 1.5", got)
	}
	if got := testutil.ToFloat64(m.StorageBytes); got != 4096 {
		t.Errorf("StorageBytes = %v, want 4096", got)
	}
}

func TestRefreshFromStore(t *testing.T) {
	ctx := context.Background()
	store := storage.NewInMemory()
	stream := "c/apps/v1/deployments/default/"

	if _, err := store.Append(ctx, &event.Record{
		StreamID:  stream,
		ClusterID: "c",
		Type:      event.TypeGap,
	}); err != nil {
		t.Fatalf("append gap: %v", err)
	}

	m := New()
	m.RefreshFromStore(ctx, store)

	if got := testutil.ToFloat64(m.DegradedStreams); got != 1 {
		t.Errorf("DegradedStreams = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.gapGauges.WithLabelValues(stream)); got != 1 {
		t.Errorf("gapGauges[%s] = %v, want 1", stream, got)
	}
	if got := testutil.ToFloat64(m.StorageBytes); got != 0 {
		t.Errorf("StorageBytes = %v, want 0 (memory store does not report size)", got)
	}

	m.Reconnects.WithLabelValues(stream).Inc()
	m.Gone410.WithLabelValues(stream).Inc()
	m.EventsIngested.WithLabelValues(stream).Inc()
	m.EventsDeduped.WithLabelValues(stream).Inc()
	m.Gaps.WithLabelValues(stream).Inc()
	m.IngestLag.Set(0.25)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, name := range []string{
		"krply_watch_reconnects_total",
		"krply_410_gone_total",
		"krply_events_ingested_total",
		"krply_events_deduplicated_total",
		"krply_gaps_total",
		"krply_gaps_current",
		"krply_ingest_lag_seconds",
		"krply_storage_bytes",
		"krply_degraded_streams",
		"krply_replay_plan_failures_total",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("metrics output missing %q", name)
		}
	}
}

func TestNewRegistrySeedsFromStore(t *testing.T) {
	ctx := context.Background()
	store := storage.NewInMemory()
	stream := "c/apps/v1/deployments/default/"
	if _, err := store.Append(ctx, &event.Record{
		StreamID:  stream,
		ClusterID: "c",
		Type:      event.TypeGap,
	}); err != nil {
		t.Fatalf("append gap: %v", err)
	}

	reg := NewRegistry(store)
	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, "krply_degraded_streams 1") {
		t.Errorf("NewRegistry output missing degraded count, got:\n%s", body)
	}
	if !strings.Contains(body, `krply_gaps_current{stream="`+stream+`"} 1`) {
		t.Errorf("NewRegistry output missing per-stream gap gauge, got:\n%s", body)
	}
}
