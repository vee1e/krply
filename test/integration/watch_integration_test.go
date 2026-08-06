//go:build integration

// Package integration_test runs the real watch collector and storage against
// a fake Kubernetes apiserver over the network.
package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	"github.com/krply/krply/internal/discovery"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
	"github.com/krply/krply/internal/watch"
	"github.com/krply/krply/test/integration/fakeapiserver"
)

const cmA = `{"metadata":{"name":"cm-a"},"data":{"key":"v1"}}`

func newDynamicClient(t *testing.T, url, token string) dynamic.Interface {
	t.Helper()
	cfg := &rest.Config{
		Host: url,
		ContentConfig: rest.ContentConfig{
			NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			GroupVersion:         &schema.GroupVersion{Group: "", Version: "v1"},
		},
	}
	if token != "" {
		cfg.BearerToken = token
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("dynamic.NewForConfig: %v", err)
	}
	return dyn
}

func newCollector(t *testing.T, dyn dynamic.Interface, store storage.Store) *watch.Collector {
	t.Helper()
	col, err := watch.NewCollector(watch.Config{
		ClusterID: "itest",
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

func storeEvents(ctx context.Context, store storage.Store) []event.Record {
	recs, err := store.Events(ctx, storage.EventFilter{})
	if err != nil {
		return nil
	}
	return recs
}

func baselineAndSynthetic(store storage.Store, name string) func() bool {
	return func() bool {
		var baseline, synthetic bool
		for _, r := range storeEvents(context.Background(), store) {
			switch {
			case r.Type == event.TypeBaseline:
				baseline = true
			case r.Type == event.TypeEvent && r.Synthetic && r.WatchType == event.WatchAdded && r.Resource.Name == name:
				synthetic = true
			}
		}
		return baseline && synthetic
	}
}

// TestCollectorRecordsBaselineAndModified verifies the initial list produces a
// BASELINE plus a synthetic ADDED, and that a mutation through the fake
// apiserver reaches the store as a single MODIFIED event with a stable
// EventID.
func TestCollectorRecordsBaselineAndModified(t *testing.T) {
	fake, err := fakeapiserver.NewFake(map[string]string{"cm-a": cmA})
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer fake.Close()

	store := storage.NewInMemory()
	col := newCollector(t, newDynamicClient(t, fake.URL, "ignored"), store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()

	waitFor(t, 5*time.Second, baselineAndSynthetic(store, "cm-a"), "baseline + synthetic ADDED for cm-a")

	fake.AddOrUpdate("cm-a", map[string]any{
		"metadata": map[string]any{"name": "cm-a"},
		"data":     map[string]any{"key": "v2"},
	}, "")

	var modified *event.Record
	waitFor(t, 5*time.Second, func() bool {
		for _, r := range storeEvents(ctx, store) {
			if r.Type == event.TypeEvent && r.WatchType == event.WatchModified && r.Resource.Name == "cm-a" {
				modified = &r
				return true
			}
		}
		return false
	}, "MODIFIED event for cm-a")

	if modified.WatchType != event.WatchModified {
		t.Fatalf("watch type = %q, want MODIFIED", modified.WatchType)
	}
	var obj map[string]any
	if err := json.Unmarshal(modified.Object, &obj); err != nil {
		t.Fatalf("decode modified object: %v", err)
	}
	data, _ := obj["data"].(map[string]any)
	if data["key"] != "v2" {
		t.Fatalf("modified data = %v, want key=v2", data)
	}
	if modified.EventID == "" {
		t.Fatal("MODIFIED event missing EventID")
	}

	count := 0
	for _, r := range storeEvents(ctx, store) {
		if r.EventID == modified.EventID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("EventID %s appeared %d times, want exactly once", modified.EventID, count)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("collector Run returned %v", err)
	}
}

// TestCollectorRecordsCheckpoint verifies bookmarks from the fake apiserver
// advance checkpoints only and are not stored as events with object payloads.
func TestCollectorRecordsCheckpoint(t *testing.T) {
	fake, err := fakeapiserver.NewFake(map[string]string{"cm-a": cmA})
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer fake.Close()
	fake.SetBookmarkEvery(1)

	store := storage.NewInMemory()
	col := newCollector(t, newDynamicClient(t, fake.URL, "ignored"), store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()
	defer cancel()

	waitFor(t, 5*time.Second, baselineAndSynthetic(store, "cm-a"), "baseline + synthetic ADDED for cm-a")

	fake.AddOrUpdate("cm-a", map[string]any{
		"metadata": map[string]any{"name": "cm-a"},
		"data":     map[string]any{"key": "v3"},
	}, "")

	var checkpoint *event.Record
	waitFor(t, 5*time.Second, func() bool {
		for _, r := range storeEvents(ctx, store) {
			if r.Type == event.TypeCheckpoint && r.Checkpoint != nil && r.Checkpoint.ResourceVersion != "" {
				checkpoint = &r
				return true
			}
		}
		return false
	}, "CHECKPOINT record")

	if checkpoint.Checkpoint == nil || checkpoint.Checkpoint.ResourceVersion == "" {
		t.Fatal("CHECKPOINT missing resource version")
	}
	if len(checkpoint.Object) != 0 {
		t.Fatalf("CHECKPOINT must not carry an object payload, got %d bytes", len(checkpoint.Object))
	}
	if checkpoint.WatchType != "" {
		t.Fatalf("CHECKPOINT must not be an EVENT, got watch type %q", checkpoint.WatchType)
	}

	select {
	case err := <-done:
		t.Fatalf("collector Run returned early with %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("collector Run returned %v", err)
	}
}

// TestCollectorRelistsAfterGap verifies that a forced 410 on a reconnect
// writes a GAP record and triggers a relist producing a second BASELINE.
func TestCollectorRelistsAfterGap(t *testing.T) {
	fake, err := fakeapiserver.NewFake(map[string]string{"cm-a": cmA})
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer fake.Close()

	store := storage.NewInMemory()
	col := newCollector(t, newDynamicClient(t, fake.URL, "ignored"), store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()
	defer cancel()

	waitFor(t, 5*time.Second, baselineAndSynthetic(store, "cm-a"), "baseline + synthetic ADDED for cm-a")

	fake.ForceGap()
	fake.CloseCurrentWatch()

	waitFor(t, 5*time.Second, func() bool {
		for _, r := range storeEvents(ctx, store) {
			if r.Type == event.TypeGap && r.Gap != nil && r.Gap.Reason == "410 Gone" {
				return true
			}
		}
		return false
	}, "GAP record with reason 410 Gone")

	waitFor(t, 5*time.Second, func() bool {
		baselines := 0
		for _, r := range storeEvents(ctx, store) {
			if r.Type == event.TypeBaseline {
				baselines++
			}
		}
		return baselines >= 2
	}, "relist after gap (second BASELINE)")

	waitFor(t, 5*time.Second, func() bool {
		synthetic := 0
		for _, r := range storeEvents(ctx, store) {
			if r.Type == event.TypeEvent && r.Synthetic && r.WatchType == event.WatchAdded && r.Resource.Name == "cm-a" {
				synthetic++
			}
		}
		return synthetic >= 2
	}, "second synthetic ADDED for cm-a after relist")

	gaps := 0
	baselines := 0
	for _, r := range storeEvents(ctx, store) {
		switch r.Type {
		case event.TypeGap:
			gaps++
		case event.TypeBaseline:
			baselines++
		}
	}
	if gaps != 1 {
		t.Fatalf("GAP records = %d, want exactly 1", gaps)
	}
	if baselines != 2 {
		t.Fatalf("BASELINE records = %d, want exactly 2", baselines)
	}
}

// TestCollectorRetriesOnRbacDenial verifies that a collector whose dynamic
// client is denied by the apiserver keeps retrying instead of crashing or
// exiting, and stops only when the context is cancelled.
func TestCollectorRetriesOnRbacDenial(t *testing.T) {
	fake, err := fakeapiserver.NewFake(nil)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer fake.Close()
	fake.RequireToken("super-secret")

	store := storage.NewInMemory()
	col := newCollector(t, newDynamicClient(t, fake.URL, "wrong-token"), store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("collector Run returned early (err = %v); RBAC denial should retry", err)
	default:
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("collector Run returned %v after cancel, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("collector Run did not return after cancel")
	}

	if recs := storeEvents(ctx, store); len(recs) != 0 {
		t.Fatalf("RBAC-denied collector wrote %d records, want none", len(recs))
	}
}
