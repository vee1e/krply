//go:build e2e

// Package e2e_test runs the full record -> timeline -> snapshot -> replay-plan
// pipeline against the fake apiserver without a real cluster.
package e2e_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	"github.com/krply/krply/internal/discovery"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/materialize"
	"github.com/krply/krply/internal/replay"
	"github.com/krply/krply/internal/storage"
	"github.com/krply/krply/internal/watch"
	"github.com/krply/krply/test/integration/fakeapiserver"
)

const clusterID = "e2e"

func e2eDynamicClient(t *testing.T, url string) dynamic.Interface {
	t.Helper()
	cfg := &rest.Config{
		Host: url,
		ContentConfig: rest.ContentConfig{
			NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			GroupVersion:         &schema.GroupVersion{Group: "", Version: "v1"},
		},
		BearerToken: "ignored",
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("dynamic.NewForConfig: %v", err)
	}
	return dyn
}

func e2eCollector(t *testing.T, dyn dynamic.Interface, store storage.Store) *watch.Collector {
	t.Helper()
	col, err := watch.NewCollector(watch.Config{
		ClusterID: clusterID,
		Resources: []discovery.ResourceSpec{
			{APIGroup: "", Version: "v1", Resource: "configmaps", Kind: "ConfigMap", Namespace: "default"},
		},
		Store:         store,
		DynamicClient: dyn,
		Bookmarks:     true,
		MinBackoff:    10 * time.Millisecond,
		MaxBackoff:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("watch.NewCollector: %v", err)
	}
	return col
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func eventsOf(ctx context.Context, store storage.Store) []event.Record {
	recs, err := store.Events(ctx, storage.EventFilter{})
	if err != nil {
		return nil
	}
	return recs
}

func hasDelete(store storage.Store, name string) func() bool {
	return func() bool {
		for _, r := range eventsOf(context.Background(), store) {
			if r.Type == event.TypeEvent && r.WatchType == event.WatchDeleted && r.Resource.Name == name {
				return true
			}
		}
		return false
	}
}

func TestEndToEndPipeline(t *testing.T) {
	fake, err := fakeapiserver.NewFake(map[string]string{
		"cm-a": `{"metadata":{"name":"cm-a"},"data":{"key":"v1"}}`,
		"cm-b": `{"metadata":{"name":"cm-b"},"data":{"key":"v1"}}`,
	})
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer fake.Close()

	store := storage.NewInMemory()
	col := e2eCollector(t, e2eDynamicClient(t, fake.URL), store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		var baseline, sawA, sawB bool
		for _, r := range eventsOf(ctx, store) {
			switch {
			case r.Type == event.TypeBaseline:
				baseline = true
			case r.Type == event.TypeEvent && r.Synthetic && r.WatchType == event.WatchAdded && r.Resource.Name == "cm-a":
				sawA = true
			case r.Type == event.TypeEvent && r.Synthetic && r.WatchType == event.WatchAdded && r.Resource.Name == "cm-b":
				sawB = true
			}
		}
		return baseline && sawA && sawB
	}, "baseline + synthetic ADDED for cm-a and cm-b")

	fake.AddOrUpdate("cm-a", map[string]any{
		"metadata": map[string]any{"name": "cm-a"},
		"data":     map[string]any{"key": "v2"},
	}, "")
	fake.Delete("cm-b")

	waitFor(t, 5*time.Second, hasDelete(store, "cm-b"), "DELETED event for cm-b")
	time.Sleep(50 * time.Millisecond)

	streams, err := store.Streams(ctx)
	if err != nil {
		t.Fatalf("Streams: %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	sm := streams[0]
	if !sm.Available || sm.HasGaps || sm.GapCount != 0 {
		t.Fatalf("stream meta = %+v, want available with no gaps", sm)
	}
	if sm.LastResourceVersion != "102" {
		t.Fatalf("last resource version = %q, want 102", sm.LastResourceVersion)
	}

	history, err := store.ObjectHistory(ctx, storage.ObjectRef{
		ClusterID: clusterID,
		StreamID:  sm.StreamID,
		Namespace: "default",
		Name:      "cm-a",
	})
	if err != nil {
		t.Fatalf("ObjectHistory: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("cm-a history = %d records, want 2 (ADDED, MODIFIED)", len(history))
	}
	if history[0].WatchType != event.WatchAdded || !history[0].Synthetic {
		t.Fatalf("first history record = %+v, want synthetic ADDED", history[0])
	}
	if history[1].WatchType != event.WatchModified {
		t.Fatalf("second history record watch type = %q, want MODIFIED", history[1].WatchType)
	}

	mat := materialize.NewMaterializer(store)
	at := time.Now().UTC().Add(2 * time.Second)
	snap, err := mat.Snapshot(ctx, clusterID, at, "e2e-snap")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !snap.Complete {
		t.Fatalf("snapshot complete = false, missing = %v, warning = %q", snap.Missing, snap.Warning)
	}
	if len(snap.Objects) != 1 {
		t.Fatalf("snapshot objects = %d, want 1 (deleted cm-b must not be present)", len(snap.Objects))
	}
	if snap.Objects[0].Name != "cm-a" || snap.Objects[0].Kind != "ConfigMap" {
		t.Fatalf("snapshot object = %+v, want cm-a ConfigMap", snap.Objects[0])
	}

	pol := replay.DefaultPolicy()
	pol.AllowGaps = true
	planner := replay.NewPlanner(store, mat, pol)
	plan, err := planner.Plan(ctx, clusterID, snap.ID, "default", "")
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Status != "planned" || plan.SnapshotID != snap.ID {
		t.Fatalf("plan = %+v, want status planned for snapshot %s", plan, snap.ID)
	}
	if !plan.CoverageComplete {
		t.Fatalf("plan coverage complete = false, want true: the only stream has a baseline and no gaps")
	}
	if len(plan.Objects) != 1 {
		t.Fatalf("plan objects = %d, want 1", len(plan.Objects))
	}
	po := plan.Objects[0]
	if po.Name != "cm-a" || po.Kind != "ConfigMap" || po.Namespace != "default" {
		t.Fatalf("plan object = %+v, want cm-a ConfigMap in default", po)
	}
	meta, _ := po.Object["metadata"].(map[string]any)
	for _, key := range []string{"uid", "resourceVersion", "namespace"} {
		if _, ok := meta[key]; ok {
			t.Fatalf("plan object metadata still contains %q: %+v", key, meta)
		}
	}
	if _, ok := po.Object["status"]; ok {
		t.Fatalf("plan object still contains status")
	}
	data, _ := po.Object["data"].(map[string]any)
	if data["key"] != "v2" {
		t.Fatalf("plan object data = %v, want key=v2 (mutated value)", data)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("collector Run returned %v", err)
	}
}
