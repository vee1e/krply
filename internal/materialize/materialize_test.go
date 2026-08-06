package materialize

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
)

func deployObject(name, image, rv string) json.RawMessage {
	obj := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":              name,
			"namespace":         "default",
			"uid":               "uid-" + name,
			"resourceVersion":   rv,
			"generation":        len(rv),
			"creationTimestamp": "2026-01-01T00:00:00Z",
			"managedFields": []any{
				map[string]any{"manager": "kube-controller-manager", "operation": "Update"},
			},
		},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "app", "image": image},
					},
				},
			},
		},
		"status": map[string]any{"readyReplicas": 1},
	}
	raw, _ := json.Marshal(obj)
	return raw
}

func configMapObject(name, rv string) json.RawMessage {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       "default",
			"uid":             "uid-" + name,
			"resourceVersion": rv,
		},
		"data": map[string]any{"key": "value"},
	}
	raw, _ := json.Marshal(obj)
	return raw
}

func ev(cluster, stream string, wt event.WatchType, res event.ResourceRef, obj json.RawMessage, at time.Time) *event.Record {
	return &event.Record{
		ClusterID:  cluster,
		StreamID:   stream,
		Type:       event.TypeEvent,
		WatchType:  wt,
		Resource:   res,
		Object:     obj,
		ObservedAt: at,
	}
}

func baseline(cluster, stream string, at time.Time) *event.Record {
	return &event.Record{
		ClusterID:  cluster,
		StreamID:   stream,
		Type:       event.TypeBaseline,
		ObservedAt: at,
	}
}

func gap(cluster, stream string, at time.Time) *event.Record {
	return &event.Record{
		ClusterID:  cluster,
		StreamID:   stream,
		Type:       event.TypeGap,
		ObservedAt: at,
		Gap:        &event.GapInfo{Reason: "test gap"},
	}
}

func appendRec(t *testing.T, store storage.Store, rec *event.Record) {
	t.Helper()
	if _, err := store.Append(context.Background(), rec); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func deployRef(name string) event.ResourceRef {
	return event.ResourceRef{Namespace: "default", Name: name, Kind: "Deployment"}
}

func TestSnapshotMaterialization(t *testing.T) {
	store := storage.NewInMemory()
	mat := NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	streamDeploy := event.Stream{ClusterID: cluster, Group: "apps", Version: "v1", Resource: "deployments", Namespace: "default"}.ID()
	streamCM := event.Stream{ClusterID: cluster, Group: "", Version: "v1", Resource: "configmaps", Namespace: "default"}.ID()
	streamGap := event.Stream{ClusterID: cluster, Group: "", Version: "v1", Resource: "secrets", Namespace: ""}.ID()

	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	appendRec(t, store, baseline(cluster, streamDeploy, base))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchAdded, deployRef("A"), deployObject("A", "nginx:1.20", "1"), base.Add(1*time.Minute)))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchModified, deployRef("A"), deployObject("A", "nginx:1.21", "2"), base.Add(2*time.Minute)))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchAdded, deployRef("B"), deployObject("B", "nginx:1.30", "1"), base.Add(3*time.Minute)))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchDeleted, deployRef("B"), deployObject("B", "nginx:1.30", "1"), base.Add(4*time.Minute)))

	appendRec(t, store, baseline(cluster, streamCM, base))
	appendRec(t, store, ev(cluster, streamCM, event.WatchAdded, event.ResourceRef{Namespace: "default", Name: "C", Kind: "ConfigMap"}, configMapObject("C", "1"), base.Add(3*time.Minute+30*time.Second)))

	appendRec(t, store, gap(cluster, streamGap, base.Add(5*time.Minute)))

	snapAt := base.Add(6 * time.Minute)
	snap, err := mat.Snapshot(ctx, cluster, snapAt, "daily")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if len(snap.Objects) != 2 {
		t.Fatalf("Snapshot objects = %d, want 2: %+v", len(snap.Objects), snap.Objects)
	}
	if snap.Objects[0].Name != "C" || snap.Objects[1].Name != "A" {
		t.Fatalf("Snapshot objects not sorted by stream/namespace/name: %+v", snap.Objects)
	}
	byName := map[string]ObjectState{}
	for _, o := range snap.Objects {
		byName[o.Name] = o
	}
	if _, ok := byName["B"]; ok {
		t.Fatalf("expected deleted object B to be absent")
	}
	a, ok := byName["A"]
	if !ok {
		t.Fatalf("expected object A present")
	}
	var aDeploy struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Image string `json:"image"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(a.Object, &aDeploy); err != nil {
		t.Fatalf("unmarshal A: %v", err)
	}
	if got := aDeploy.Spec.Template.Spec.Containers[0].Image; got != "nginx:1.21" {
		t.Fatalf("A image = %q, want nginx:1.21 (modified)", got)
	}

	if snap.Complete {
		t.Fatalf("Snapshot Complete = true, want false due to gap")
	}
	if len(snap.Missing) != 1 || snap.Missing[0] != streamGap {
		t.Fatalf("Missing = %v, want [%s]", snap.Missing, streamGap)
	}
	if !strings.Contains(snap.Warning, streamGap) {
		t.Fatalf("Warning = %q, want mention of %s", snap.Warning, streamGap)
	}

	snaps, err := store.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots: %v", err)
	}
	if len(snaps) != 1 || snaps[0].Name != "daily" || !strings.HasPrefix(snaps[0].ID, "snap-") {
		t.Fatalf("stored snapshots = %+v", snaps)
	}
}

func TestObjectAt(t *testing.T) {
	store := storage.NewInMemory()
	mat := NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	streamDeploy := event.Stream{ClusterID: cluster, Group: "apps", Version: "v1", Resource: "deployments", Namespace: "default"}.ID()

	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	appendRec(t, store, baseline(cluster, streamDeploy, base))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchAdded, deployRef("A"), deployObject("A", "nginx:1.20", "1"), base.Add(1*time.Minute)))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchModified, deployRef("A"), deployObject("A", "nginx:1.21", "2"), base.Add(2*time.Minute)))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchAdded, deployRef("B"), deployObject("B", "nginx:1.30", "1"), base.Add(3*time.Minute)))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchDeleted, deployRef("B"), deployObject("B", "nginx:1.30", "1"), base.Add(4*time.Minute)))

	if _, err := mat.ObjectAt(ctx, cluster, streamDeploy, "default", "A", base.Add(-time.Second)); !errors.Is(err, ErrNoObjectAtTime) {
		t.Fatalf("before creation: err = %v, want ErrNoObjectAtTime", err)
	}

	image := func(rec *event.Record) string {
		t.Helper()
		var d struct {
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Image string `json:"image"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(rec.Object, &d); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return d.Spec.Template.Spec.Containers[0].Image
	}

	rec, err := mat.ObjectAt(ctx, cluster, streamDeploy, "default", "A", base.Add(1*time.Minute+30*time.Second))
	if err != nil {
		t.Fatalf("ObjectAt v1: %v", err)
	}
	if got := image(rec); got != "nginx:1.20" {
		t.Fatalf("A at v1 time = %q, want nginx:1.20", got)
	}

	rec, err = mat.ObjectAt(ctx, cluster, streamDeploy, "default", "A", base.Add(2*time.Minute+30*time.Second))
	if err != nil {
		t.Fatalf("ObjectAt v2: %v", err)
	}
	if got := image(rec); got != "nginx:1.21" {
		t.Fatalf("A at v2 time = %q, want nginx:1.21", got)
	}

	if _, err := mat.ObjectAt(ctx, cluster, streamDeploy, "default", "B", base.Add(5*time.Minute)); !errors.Is(err, ErrNoObjectAtTime) {
		t.Fatalf("deleted object: err = %v, want ErrNoObjectAtTime", err)
	}
}

func TestDiff(t *testing.T) {
	store := storage.NewInMemory()
	mat := NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	streamDeploy := event.Stream{ClusterID: cluster, Group: "apps", Version: "v1", Resource: "deployments", Namespace: "default"}.ID()
	streamCM := event.Stream{ClusterID: cluster, Group: "", Version: "v1", Resource: "configmaps", Namespace: "default"}.ID()
	streamGap := event.Stream{ClusterID: cluster, Group: "", Version: "v1", Resource: "secrets", Namespace: ""}.ID()

	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	appendRec(t, store, baseline(cluster, streamDeploy, base))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchAdded, deployRef("A"), deployObject("A", "nginx:1.20", "1"), base.Add(1*time.Minute)))
	appendRec(t, store, ev(cluster, streamDeploy, event.WatchModified, deployRef("A"), deployObject("A", "nginx:1.21", "2"), base.Add(2*time.Minute)))
	appendRec(t, store, baseline(cluster, streamCM, base))
	appendRec(t, store, ev(cluster, streamCM, event.WatchAdded, event.ResourceRef{Namespace: "default", Name: "C", Kind: "ConfigMap"}, configMapObject("C", "1"), base.Add(3*time.Minute)))
	appendRec(t, store, gap(cluster, streamGap, base.Add(5*time.Minute)))

	before := base.Add(1*time.Minute + 30*time.Second)
	after := base.Add(6 * time.Minute)
	res, err := mat.Diff(ctx, cluster, "default", before, after)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}

	if !res.HasGaps {
		t.Fatalf("Diff HasGaps = false, want true")
	}
	if !strings.Contains(res.Warning, streamGap) {
		t.Fatalf("Diff Warning = %q, want mention of %s", res.Warning, streamGap)
	}
	if len(res.Changes) != 2 {
		t.Fatalf("Diff changes = %d, want 2 (A modified, C added): %+v", len(res.Changes), res.Changes)
	}

	var aDiff, cDiff *ObjectDiff
	for i := range res.Changes {
		switch res.Changes[i].Name {
		case "A":
			aDiff = &res.Changes[i]
		case "C":
			cDiff = &res.Changes[i]
		}
	}
	if aDiff == nil {
		t.Fatalf("missing diff for A")
	}
	if cDiff == nil {
		t.Fatalf("missing diff for C")
	}

	if len(aDiff.Changes) != 1 {
		t.Fatalf("A changes = %+v, want exactly 1", aDiff.Changes)
	}
	ch := aDiff.Changes[0]
	if ch.Path != "spec.template.spec.containers[0].image" {
		t.Fatalf("A change path = %q, want spec.template.spec.containers[0].image", ch.Path)
	}
	if ch.Before != "nginx:1.20" || ch.After != "nginx:1.21" {
		t.Fatalf("A change = before %v, after %v", ch.Before, ch.After)
	}
	for _, skip := range []string{"resourceVersion", "generation", "managedFields", "uid", "status", "creationTimestamp"} {
		if strings.Contains(ch.Path, skip) {
			t.Fatalf("A change path %q should ignore %q", ch.Path, skip)
		}
	}

	if len(cDiff.Changes) != 1 || !cDiff.Changes[0].Added || cDiff.Changes[0].Path != "" {
		t.Fatalf("C changes = %+v, want single whole-object Added", cDiff.Changes)
	}
}

// TestGapHealingAndBaselineReset verifies that a BASELINE after a gap heals
// the open gap and resets object state, so an object deleted during the gap
// disappears while a surviving object is re-materialized from the synthetic
// ADDED that follows the baseline.
func TestGapHealingAndBaselineReset(t *testing.T) {
	store := storage.NewInMemory()
	mat := NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	sid := event.Stream{ClusterID: cluster, Group: "apps", Version: "v1", Resource: "deployments", Namespace: "default"}.ID()
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	appendRec(t, store, baseline(cluster, sid, base))
	appendRec(t, store, ev(cluster, sid, event.WatchAdded, deployRef("A"), deployObject("A", "nginx:1.20", "1"), base.Add(time.Minute)))
	appendRec(t, store, gap(cluster, sid, base.Add(2*time.Minute)))
	// After the gap: B survived, A was deleted during the gap.
	appendRec(t, store, baseline(cluster, sid, base.Add(3*time.Minute)))
	appendRec(t, store, ev(cluster, sid, event.WatchAdded, deployRef("B"), deployObject("B", "nginx:1.30", "2"), base.Add(3*time.Minute+time.Second)))

	res, err := mat.StreamState(ctx, sid, base.Add(4*time.Minute))
	if err != nil {
		t.Fatalf("StreamState: %v", err)
	}
	if res.HasGaps {
		t.Fatalf("HasGaps = true, want false (baseline healed the gap)")
	}
	names := map[string]bool{}
	for _, o := range res.Objects {
		names[o.Name] = true
	}
	if !names["B"] {
		t.Fatalf("object B missing from state after healed gap: %+v", names)
	}
	if names["A"] {
		t.Fatalf("object A (deleted during the gap) still present: %+v", names)
	}
}

// TestDiffIgnoreIsPathScoped verifies that server-metadata fields are ignored
// only under metadata, while user fields with the same names are kept.
func TestDiffIgnoreIsPathScoped(t *testing.T) {
	store := storage.NewInMemory()
	mat := NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	sid := event.Stream{ClusterID: cluster, Group: "", Version: "v1", Resource: "configmaps", Namespace: "default"}.ID()
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	mk := func(rv string, dataStatus string, dataUID string) json.RawMessage {
		obj := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name": "app", "namespace": "default",
				"uid": "u", "resourceVersion": rv, "generation": 1,
			},
			"data": map[string]any{"status": dataStatus, "uid": dataUID},
		}
		raw, _ := json.Marshal(obj)
		return raw
	}

	ref := event.ResourceRef{Namespace: "default", Name: "app", Kind: "ConfigMap"}
	appendRec(t, store, baseline(cluster, sid, base))
	appendRec(t, store, ev(cluster, sid, event.WatchAdded, ref, mk("1", "up", "v1"), base.Add(time.Minute)))
	appendRec(t, store, ev(cluster, sid, event.WatchModified, ref, mk("2", "down", "v2"), base.Add(2*time.Minute)))

	res, err := mat.Diff(ctx, cluster, "default", base.Add(90*time.Second), base.Add(150*time.Second))
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(res.Changes) != 1 {
		t.Fatalf("changed objects = %d, want 1", len(res.Changes))
	}
	paths := map[string]bool{}
	for _, ch := range res.Changes[0].Changes {
		paths[ch.Path] = true
	}
	if !paths["data.status"] {
		t.Fatalf("data.status change missing (user field dropped): %v", paths)
	}
	if !paths["data.uid"] {
		t.Fatalf("data.uid change missing (user field dropped): %v", paths)
	}
	for _, p := range []string{"metadata.uid", "metadata.resourceVersion", "metadata.generation", "status"} {
		if paths[p] {
			t.Fatalf("server metadata field %q leaked into the diff: %v", p, paths)
		}
	}
}

// TestDiffNestedAddedRemovedFlags verifies nested adds/removes carry the
// Added/Removed flags so the UI and CLI can render them correctly.
func TestDiffNestedAddedRemovedFlags(t *testing.T) {
	store := storage.NewInMemory()
	mat := NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	sid := event.Stream{ClusterID: cluster, Group: "apps", Version: "v1", Resource: "deployments", Namespace: "default"}.ID()
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	ref := deployRef("A")

	mk := func(rv string, label string) json.RawMessage {
		obj := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata":   map[string]any{"name": "A", "namespace": "default", "labels": map[string]any{"team": label}},
		}
		raw, _ := json.Marshal(obj)
		return raw
	}

	appendRec(t, store, baseline(cluster, sid, base))
	appendRec(t, store, ev(cluster, sid, event.WatchAdded, ref, mk("1", "a"), base.Add(time.Minute)))
	appendRec(t, store, ev(cluster, sid, event.WatchModified, ref, mk("2", "b"), base.Add(2*time.Minute)))

	res, err := mat.Diff(ctx, cluster, "default", base.Add(90*time.Second), base.Add(150*time.Second))
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(res.Changes) != 1 || len(res.Changes[0].Changes) != 1 {
		t.Fatalf("expected 1 change, got %+v", res.Changes)
	}
	ch := res.Changes[0].Changes[0]
	if ch.Path != "metadata.labels.team" || ch.Added || ch.Removed || ch.Before != "a" || ch.After != "b" {
		t.Fatalf("changed field = %+v, want metadata.labels.team a -> b", ch)
	}
}

// TestSnapshotWritesRecordAndWatermarks verifies Snapshot persists a
// TypeSnapshot journal record and per-stream watermarks.
func TestSnapshotWritesRecordAndWatermarks(t *testing.T) {
	store := storage.NewInMemory()
	mat := NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	sid := event.Stream{ClusterID: cluster, Group: "", Version: "v1", Resource: "configmaps", Namespace: "default"}.ID()
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	appendRec(t, store, baseline(cluster, sid, base))
	appendRec(t, store, ev(cluster, sid, event.WatchAdded, event.ResourceRef{Namespace: "default", Name: "cm", Kind: "ConfigMap", ResourceVersion: "42"}, configMapObject("cm", "42"), base.Add(time.Minute)))

	snap, err := mat.Snapshot(ctx, cluster, base.Add(2*time.Minute), "test")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !snap.Complete {
		t.Fatalf("Snapshot not complete: %s", snap.Warning)
	}
	if len(snap.Watermarks) != 1 {
		t.Fatalf("watermarks = %d, want 1", len(snap.Watermarks))
	}
	if snap.Watermarks[0].LastResourceVersion != "42" || snap.Watermarks[0].LastObservedAt.IsZero() {
		t.Fatalf("watermark = %+v", snap.Watermarks[0])
	}

	found := false
	for _, rec := range storeEvents(ctx, store) {
		if rec.Type == event.TypeSnapshot {
			found = true
			if rec.Snapshot == nil || rec.Snapshot.Name != "test" {
				t.Fatalf("snapshot record missing name: %+v", rec.Snapshot)
			}
		}
	}
	if !found {
		t.Fatal("no TypeSnapshot journal record was written")
	}
}

func storeEvents(ctx context.Context, store storage.Store) []event.Record {
	recs, err := store.Events(ctx, storage.EventFilter{})
	if err != nil {
		return nil
	}
	return recs
}
