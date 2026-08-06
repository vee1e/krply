package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/krply/krply/internal/event"
)

func TestSQLiteStore(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	cluster := "cluster-a"
	stream := event.Stream{
		ClusterID: cluster,
		Group:     "apps",
		Version:   "v1",
		Resource:  "deployments",
		Namespace: "default",
	}
	streamID := stream.ID()

	t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	t2 := t0.Add(2 * time.Minute)
	t3 := t0.Add(3 * time.Minute)
	t4 := t0.Add(4 * time.Minute)
	t5 := t0.Add(5 * time.Minute)

	mkEvent := func(id, name, rv string, wt event.WatchType, at time.Time) *event.Record {
		obj := json.RawMessage(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"` +
			name + `","namespace":"default","resourceVersion":"` + rv + `","uid":"uid-` + id + `"}}`)
		return &event.Record{
			ClusterID:  cluster,
			StreamID:   streamID,
			Type:       event.TypeEvent,
			EventID:    id,
			ObservedAt: at,
			WatchType:  wt,
			Resource: event.ResourceRef{
				Group: "apps", Version: "v1", Kind: "Deployment",
				Namespace: "default", Name: name, UID: "uid-" + id, ResourceVersion: rv,
			},
			ObjectHash: event.ObjectHash(obj),
			Object:     obj,
		}
	}

	e1 := mkEvent("e1", "web-0", "1", event.WatchAdded, t0)
	e2 := mkEvent("e2", "web-0", "2", event.WatchModified, t1)
	e3 := mkEvent("e3", "web-0", "3", event.WatchDeleted, t2)
	e4 := mkEvent("e4", "db-0", "1", event.WatchAdded, t3)

	seqs, err := store.Appends(ctx, []*event.Record{e1, e2, e3, e4})
	if err != nil {
		t.Fatalf("Appends: %v", err)
	}
	if len(seqs) != 4 {
		t.Fatalf("Appends returned %d seqs, want 4", len(seqs))
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("seqs not increasing: %v", seqs)
		}
	}

	gapRec := &event.Record{
		ClusterID:  cluster,
		StreamID:   streamID,
		Type:       event.TypeGap,
		EventID:    "gap-1",
		ObservedAt: t4,
		Gap: &event.GapInfo{
			FromResourceVersion: "3",
			ToResourceVersion:   "9",
			Reason:              "410 Gone",
			DetectedAt:          t4,
		},
		Resource: event.ResourceRef{Group: "apps", Version: "v1", Kind: "Deployment"},
	}
	baseRec := &event.Record{
		ClusterID:  cluster,
		StreamID:   streamID,
		Type:       event.TypeBaseline,
		EventID:    "base-1",
		ObservedAt: t5,
		Synthetic:  true,
		Resource: event.ResourceRef{
			Group: "apps", Version: "v1", Kind: "Deployment", ResourceVersion: "9",
		},
	}
	if _, err := store.Append(ctx, gapRec); err != nil {
		t.Fatalf("Append gap: %v", err)
	}
	if _, err := store.Append(ctx, baseRec); err != nil {
		t.Fatalf("Append baseline: %v", err)
	}

	t.Run("dedup", func(t *testing.T) {
		dup := mkEvent("e2", "web-0", "2", event.WatchModified, t1)
		seq, err := store.Append(ctx, dup)
		if err != nil {
			t.Fatalf("Append duplicate: %v", err)
		}
		if seq != seqs[1] {
			t.Errorf("duplicate seq = %d, want %d", seq, seqs[1])
		}
		recs, err := store.Events(ctx, EventFilter{StreamID: streamID, RecordType: event.TypeEvent})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		count := 0
		for _, r := range recs {
			if r.EventID == "e2" {
				count++
			}
		}
		if count != 1 {
			t.Errorf("duplicate event_id appears %d times, want 1", count)
		}
	})

	t.Run("events_filter", func(t *testing.T) {
		recs, err := store.Events(ctx, EventFilter{
			ClusterID: cluster,
			StreamID:  streamID,
			Namespace: "default",
			Name:      "web-0",
		})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if len(recs) != 3 {
			t.Fatalf("Events returned %d records, want 3", len(recs))
		}
		for i, want := range []string{"e1", "e2", "e3"} {
			if recs[i].EventID != want {
				t.Errorf("Events[%d].EventID = %s, want %s", i, recs[i].EventID, want)
			}
			if recs[i].IngestSeq <= 0 {
				t.Errorf("Events[%d].IngestSeq = %d, want > 0", i, recs[i].IngestSeq)
			}
		}

		window, err := store.Events(ctx, EventFilter{
			StreamID:  streamID,
			Namespace: "default",
			Name:      "web-0",
			Since:     t0,
			Until:     t1,
		})
		if err != nil {
			t.Fatalf("Events window: %v", err)
		}
		if len(window) != 2 || window[0].EventID != "e1" || window[1].EventID != "e2" {
			t.Errorf("Events window = %v, want e1,e2", window)
		}

		onlyGap, err := store.Events(ctx, EventFilter{StreamID: streamID, RecordType: event.TypeGap})
		if err != nil {
			t.Fatalf("Events gap type: %v", err)
		}
		if len(onlyGap) != 1 || onlyGap[0].EventID != "gap-1" {
			t.Errorf("Events gap type = %v, want [gap-1]", onlyGap)
		}
	})

	t.Run("object_history", func(t *testing.T) {
		hist, err := store.ObjectHistory(ctx, ObjectRef{
			ClusterID: cluster,
			StreamID:  streamID,
			Namespace: "default",
			Name:      "web-0",
		})
		if err != nil {
			t.Fatalf("ObjectHistory: %v", err)
		}
		if len(hist) != 3 {
			t.Fatalf("ObjectHistory returned %d records, want 3", len(hist))
		}
		for i, want := range []string{"e1", "e2", "e3"} {
			if hist[i].EventID != want {
				t.Errorf("hist[%d].EventID = %s, want %s", i, hist[i].EventID, want)
			}
		}
		if hist[0].IngestSeq >= hist[1].IngestSeq || hist[1].IngestSeq >= hist[2].IngestSeq {
			t.Errorf("history not in ingest order: %v, %v, %v",
				hist[0].IngestSeq, hist[1].IngestSeq, hist[2].IngestSeq)
		}
	})

	t.Run("object_at", func(t *testing.T) {
		ref := ObjectRef{ClusterID: cluster, StreamID: streamID, Namespace: "default", Name: "web-0"}

		atT0, err := store.ObjectAt(ctx, ref, t0)
		if err != nil {
			t.Fatalf("ObjectAt t0: %v", err)
		}
		if atT0.EventID != "e1" {
			t.Errorf("ObjectAt t0 = %s, want e1", atT0.EventID)
		}

		atT1, err := store.ObjectAt(ctx, ref, t1)
		if err != nil {
			t.Fatalf("ObjectAt t1: %v", err)
		}
		if atT1.EventID != "e2" {
			t.Errorf("ObjectAt t1 = %s, want e2", atT1.EventID)
		}

		if _, err := store.ObjectAt(ctx, ref, t2); err != ErrNotFound {
			t.Errorf("ObjectAt t2 err = %v, want ErrNotFound (deleted)", err)
		}
		if _, err := store.ObjectAt(ctx, ref, t0.Add(-time.Second)); err != ErrNotFound {
			t.Errorf("ObjectAt before first err = %v, want ErrNotFound", err)
		}
	})

	t.Run("stream_events", func(t *testing.T) {
		all, err := store.StreamEvents(ctx, streamID, time.Time{})
		if err != nil {
			t.Fatalf("StreamEvents all: %v", err)
		}
		if len(all) != 6 {
			t.Fatalf("StreamEvents all returned %d records, want 6", len(all))
		}
		until, err := store.StreamEvents(ctx, streamID, t2)
		if err != nil {
			t.Fatalf("StreamEvents until: %v", err)
		}
		if len(until) != 3 {
			t.Fatalf("StreamEvents until t2 returned %d records, want 3", len(until))
		}
	})

	t.Run("baselines_and_gaps", func(t *testing.T) {
		baselines, err := store.Baselines(ctx, streamID)
		if err != nil {
			t.Fatalf("Baselines: %v", err)
		}
		if len(baselines) != 1 || baselines[0].EventID != "base-1" {
			t.Errorf("Baselines = %v, want [base-1]", baselines)
		}
		if !baselines[0].Synthetic {
			t.Errorf("baseline Synthetic = false, want true")
		}
		gaps, err := store.Gaps(ctx, streamID)
		if err != nil {
			t.Fatalf("Gaps: %v", err)
		}
		if len(gaps) != 1 || gaps[0].EventID != "gap-1" {
			t.Errorf("Gaps = %v, want [gap-1]", gaps)
		}
	})

	t.Run("stream_meta", func(t *testing.T) {
		meta, err := store.StreamMeta(ctx, streamID)
		if err != nil {
			t.Fatalf("StreamMeta: %v", err)
		}
		if meta.GapCount != 1 {
			t.Errorf("GapCount = %d, want 1", meta.GapCount)
		}
		if !meta.HasGaps {
			t.Errorf("HasGaps = false, want true")
		}
		if meta.Degraded {
			t.Errorf("Degraded = true, want false after baseline")
		}
		if !meta.Available {
			t.Errorf("Available = false, want true after baseline")
		}
		if meta.LastResourceVersion != "9" {
			t.Errorf("LastResourceVersion = %q, want 9", meta.LastResourceVersion)
		}
		if !meta.FirstObservedAt.Equal(t0) {
			t.Errorf("FirstObservedAt = %v, want %v", meta.FirstObservedAt, t0)
		}
		if !meta.LastObservedAt.Equal(t5) {
			t.Errorf("LastObservedAt = %v, want %v", meta.LastObservedAt, t5)
		}

		if _, err := store.StreamMeta(ctx, "nope"); err != ErrStreamNotFound {
			t.Errorf("StreamMeta missing err = %v, want ErrStreamNotFound", err)
		}

		streams, err := store.Streams(ctx)
		if err != nil {
			t.Fatalf("Streams: %v", err)
		}
		if len(streams) != 1 || streams[0].StreamID != streamID {
			t.Errorf("Streams = %v, want one stream %q", streams, streamID)
		}

		clusters, err := store.ListClusters(ctx)
		if err != nil {
			t.Fatalf("ListClusters: %v", err)
		}
		if len(clusters) != 1 || clusters[0] != cluster {
			t.Errorf("ListClusters = %v, want [%s]", clusters, cluster)
		}
	})

	t.Run("snapshots", func(t *testing.T) {
		now := time.Now().UTC()
		if err := store.SaveSnapshot(ctx, &SnapshotRef{ID: "snap-1", ClusterID: cluster, Name: "initial", At: now}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		if err := store.SaveSnapshot(ctx, &SnapshotRef{ClusterID: cluster, Name: "second", At: now.Add(time.Hour)}); err != nil {
			t.Fatalf("SaveSnapshot auto-id: %v", err)
		}
		snaps, err := store.Snapshots(ctx)
		if err != nil {
			t.Fatalf("Snapshots: %v", err)
		}
		if len(snaps) != 2 {
			t.Fatalf("Snapshots returned %d, want 2", len(snaps))
		}
		if snaps[0].ID != "snap-1" {
			t.Errorf("Snapshots[0].ID = %s, want snap-1 (ordered by at)", snaps[0].ID)
		}
		if snaps[1].Name != "second" || snaps[1].ID == "" {
			t.Errorf("Snapshots[1] = %+v, want auto-generated ID", snaps[1])
		}
		if !snaps[0].At.Before(snaps[1].At) {
			t.Errorf("snapshots not ordered by at: %v then %v", snaps[0].At, snaps[1].At)
		}
	})
}

func TestSQLiteStoreMemory(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore(:memory:): %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	streamID := "cluster-a//v1/deployments/default/"
	rec := &event.Record{
		ClusterID:  "cluster-a",
		StreamID:   streamID,
		Type:       event.TypeEvent,
		EventID:    "m1",
		ObservedAt: time.Now().UTC(),
		WatchType:  event.WatchAdded,
		Resource:   event.ResourceRef{Kind: "Deployment", Namespace: "default", Name: "x"},
	}
	if _, err := store.Append(ctx, rec); err != nil {
		t.Fatalf("Append: %v", err)
	}
	recs, err := store.Events(ctx, EventFilter{ClusterID: "cluster-a"})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(recs) != 1 || recs[0].EventID != "m1" {
		t.Errorf("Events = %v, want [m1]", recs)
	}
}

// TestSpecialRecordsNotDeduplicated ensures baselines, gaps, and checkpoints
// (which have no event_id) are never collapsed by the dedup index. Multiple
// relists must produce multiple BASELINE records.
func TestSpecialRecordsNotDeduplicated(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore(:memory:): %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	streamID := "cluster-a/apps/v1/deployments/default/"
	mk := func(typ event.RecordType) *event.Record {
		return &event.Record{
			ClusterID:  "cluster-a",
			StreamID:   streamID,
			Type:       typ,
			ObservedAt: time.Now().UTC(),
			Resource:   event.ResourceRef{Group: "apps", Version: "v1", Kind: "Deployment", Namespace: "default"},
		}
	}
	for _, typ := range []event.RecordType{event.TypeBaseline, event.TypeGap, event.TypeBaseline, event.TypeCheckpoint} {
		if _, err := store.Append(ctx, mk(typ)); err != nil {
			t.Fatalf("Append(%s): %v", typ, err)
		}
	}
	for _, typ := range []event.RecordType{event.TypeBaseline, event.TypeGap, event.TypeCheckpoint} {
		recs, err := store.Events(ctx, EventFilter{ClusterID: "cluster-a", RecordType: typ})
		if err != nil {
			t.Fatalf("Events(%s): %v", typ, err)
		}
		want := 2
		if typ != event.TypeBaseline {
			want = 1
		}
		if len(recs) != want {
			t.Errorf("Events(%s) = %d records, want %d", typ, len(recs), want)
		}
	}
}
