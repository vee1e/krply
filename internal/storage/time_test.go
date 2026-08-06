package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/krply/krply/internal/event"
)

// TestSQLiteSubSecondBoundaries verifies that since/until and ObjectAt compare
// correctly across whole-second and sub-second boundaries. The old
// variable-width RFC3339Nano storage made lexicographic comparison wrong at
// sub-second precision; the fixed-width format must not.
func TestSQLiteSubSecondBoundaries(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	sid := event.Stream{ClusterID: "c", Group: "apps", Version: "v1", Resource: "deployments", Namespace: "default"}.ID()
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	// An event exactly at a whole second and one half a second later.
	whole := &event.Record{
		ClusterID: "c", StreamID: sid, Type: event.TypeEvent, EventID: "w",
		ObservedAt: base,
		WatchType:  event.WatchAdded,
		Resource:   event.ResourceRef{Namespace: "default", Name: "a", Kind: "Deployment", ResourceVersion: "1"},
		Object:     []byte(`{}`),
	}
	half := &event.Record{
		ClusterID: "c", StreamID: sid, Type: event.TypeEvent, EventID: "h",
		ObservedAt: base.Add(500 * time.Millisecond),
		WatchType:  event.WatchAdded,
		Resource:   event.ResourceRef{Namespace: "default", Name: "b", Kind: "Deployment", ResourceVersion: "2"},
		Object:     []byte(`{}`),
	}
	for _, r := range []*event.Record{whole, half} {
		if _, err := store.Append(ctx, r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// since = whole second must include both events (both are at or after it).
	recs, err := store.Events(ctx, EventFilter{ClusterID: "c", Since: base})
	if err != nil {
		t.Fatalf("Events since whole second: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("Events since whole second = %d, want 2", len(recs))
	}

	// until = whole second + 200ms must include only the whole-second event.
	recs, err = store.Events(ctx, EventFilter{ClusterID: "c", Until: base.Add(200 * time.Millisecond)})
	if err != nil {
		t.Fatalf("Events until sub-second: %v", err)
	}
	if len(recs) != 1 || recs[0].EventID != "w" {
		t.Fatalf("Events until sub-second = %+v, want only 'w'", recs)
	}

	// ObjectAt at whole second + 200ms must return the whole-second event.
	obj, err := store.ObjectAt(ctx, ObjectRef{ClusterID: "c", StreamID: sid, Namespace: "default", Name: "a"}, base.Add(200*time.Millisecond))
	if err != nil {
		t.Fatalf("ObjectAt: %v", err)
	}
	if obj.Resource.ResourceVersion != "1" {
		t.Fatalf("ObjectAt RV = %q, want 1", obj.Resource.ResourceVersion)
	}
}

// TestMemoryStoreDedupAndPagination verifies the in-memory store matches the
// SQLite store: event_id dedup, SinceSeq cursors, and Limit/Offset.
func TestMemoryStoreDedupAndPagination(t *testing.T) {
	store := NewInMemory()
	ctx := context.Background()
	sid := event.Stream{ClusterID: "c", Group: "", Version: "v1", Resource: "pods", Namespace: "default"}.ID()
	at := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	mk := func(id string) *event.Record {
		return &event.Record{
			ClusterID: "c", StreamID: sid, Type: event.TypeEvent, EventID: id,
			ObservedAt: at, WatchType: event.WatchAdded,
			Resource: event.ResourceRef{Namespace: "default", Name: id, Kind: "Pod", ResourceVersion: id},
			Object:   []byte(`{}`),
		}
	}
	for _, r := range []*event.Record{mk("a"), mk("b"), mk("a"), mk("c"), mk("d")} {
		if _, err := store.Append(ctx, r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// "a" was appended twice; dedup must collapse it to one record.
	recs, err := store.Events(ctx, EventFilter{ClusterID: "c"})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(recs) != 4 {
		t.Fatalf("deduped events = %d, want 4 (a,b,c,d)", len(recs))
	}

	// Cursor pagination: page of 2 after cursor 0.
	page1, err := store.Events(ctx, EventFilter{ClusterID: "c", Limit: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 2 || page1[0].EventID != "a" || page1[1].EventID != "b" {
		t.Fatalf("page1 = %+v", page1)
	}
	page2, err := store.Events(ctx, EventFilter{ClusterID: "c", Limit: 2, SinceSeq: page1[1].IngestSeq})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 2 || page2[0].EventID != "c" || page2[1].EventID != "d" {
		t.Fatalf("page2 = %+v", page2)
	}
}
