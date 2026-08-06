package watch

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"

	"github.com/krply/krply/internal/discovery"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
)

func newTestConfig(dyn dynamic.Interface, store storage.Store) Config {
	return Config{
		ClusterID:     "cluster-test",
		Store:         store,
		Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		DynamicClient: dyn,
		MinBackoff:    10 * time.Millisecond,
		MaxBackoff:    50 * time.Millisecond,
		Resources: []discovery.ResourceSpec{
			{APIGroup: "", Version: "v1", Resource: "configmaps", Kind: "ConfigMap", Namespace: ""},
		},
	}
}

// TestCollectorWritesBaselineAndSyntheticEvents verifies the initial list
// produces a BASELINE record plus one synthetic ADDED record per object.
func TestCollectorWritesBaselineAndSyntheticEvents(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "app-config", Namespace: "default"},
		Data:       map[string]string{"app": "prod"},
	}
	dyn := dynamicfake.NewSimpleDynamicClient(scheme.Scheme, cm)
	store := storage.NewInMemory()

	col, err := NewCollector(newTestConfig(dyn, store))
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := col.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	recs, err := store.Events(ctx, storage.EventFilter{})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	var baselines, synthetic []event.Record
	for _, r := range recs {
		switch {
		case r.Type == event.TypeBaseline:
			baselines = append(baselines, r)
		case r.Type == event.TypeEvent && r.Synthetic:
			synthetic = append(synthetic, r)
		}
	}

	if len(baselines) != 1 {
		t.Fatalf("expected 1 BASELINE record, got %d", len(baselines))
	}
	b := baselines[0]
	if b.Resource.ResourceVersion == "" {
		t.Error("BASELINE missing resource version")
	}
	if b.StreamID == "" {
		t.Error("BASELINE missing stream id")
	}

	if len(synthetic) != 1 {
		t.Fatalf("expected 1 synthetic ADDED event, got %d", len(synthetic))
	}
	s := synthetic[0]
	if s.Resource.Name != "app-config" || s.Resource.Namespace != "default" {
		t.Errorf("synthetic event resource = %+v", s.Resource)
	}
	if s.WatchType != event.WatchAdded {
		t.Errorf("synthetic event watch type = %q, want ADDED", s.WatchType)
	}
	if !s.Synthetic {
		t.Error("synthetic event not marked synthetic")
	}
	if len(s.Object) == 0 || s.ObjectHash == "" {
		t.Error("synthetic event missing object payload or hash")
	}
}

// TestCollectorRelistsOn410Gone verifies a 410 Gone error writes a GAP record
// and triggers a relist (a second BASELINE).
func TestCollectorRelistsOn410Gone(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "app-config", Namespace: "default"},
	}
	dyn := dynamicfake.NewSimpleDynamicClient(scheme.Scheme, cm)

	var calls atomic.Int32
	fw := watch.NewRaceFreeFake()
	dyn.PrependWatchReactor("configmaps", func(action k8stesting.Action) (bool, watch.Interface, error) {
		if calls.Add(1) == 1 {
			fw.Error(&metav1.Status{Status: metav1.StatusFailure, Code: http.StatusGone, Reason: metav1.StatusReasonExpired})
			fw.Stop()
		}
		return true, fw, nil
	})

	store := storage.NewInMemory()
	col, err := NewCollector(newTestConfig(dyn, store))
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := col.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	recs, err := store.Events(ctx, storage.EventFilter{})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	var gaps, baselines int
	for _, r := range recs {
		switch r.Type {
		case event.TypeGap:
			gaps++
			if r.Gap == nil || r.Gap.Reason != "410 Gone" {
				t.Errorf("gap record = %+v", r)
			}
		case event.TypeBaseline:
			baselines++
		}
	}
	if gaps != 1 {
		t.Errorf("expected 1 GAP record, got %d", gaps)
	}
	if baselines < 2 {
		t.Errorf("expected relist to write a second BASELINE, got %d baselines", baselines)
	}
}
