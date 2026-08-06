package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	queryv1 "github.com/krply/krply/api/query/v1"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/materialize"
	"github.com/krply/krply/internal/metrics"
	"github.com/krply/krply/internal/replay"
	"github.com/krply/krply/internal/storage"
)

const (
	testCluster   = "c1"
	testCMStream  = "c1//v1/configmaps//"
	testDEPStream = "c1//v1/deployments//"
)

func testRecord(streamID string, typ event.RecordType, wt event.WatchType, synthetic bool, ns, name, kind, rv string, obj map[string]any, at time.Time) *event.Record {
	rec := &event.Record{
		ClusterID:  testCluster,
		StreamID:   streamID,
		Type:       typ,
		WatchType:  wt,
		Synthetic:  synthetic,
		ObservedAt: at,
		Resource: event.ResourceRef{
			Version:         "v1",
			Kind:            kind,
			Namespace:       ns,
			Name:            name,
			ResourceVersion: rv,
		},
	}
	if obj != nil {
		b, _ := json.Marshal(obj)
		rec.Object = b
		rec.ObjectHash = event.ObjectHash(b)
	}
	return rec
}

// seedStore writes a baseline, synthetic added, modified, a gap, and a
// bookmark checkpoint for one configmap and one deployment into a fresh
// in-memory store.
func seedStore(t *testing.T) storage.Store {
	t.Helper()
	store := storage.NewInMemory()
	ctx := context.Background()

	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(5 * time.Minute)
	t2 := t0.Add(15 * time.Minute)
	t3 := t0.Add(20 * time.Minute)

	recs := []*event.Record{
		// configmaps: baseline, synthetic added, modified, gap, checkpoint
		{
			ClusterID:  testCluster,
			StreamID:   testCMStream,
			Type:       event.TypeBaseline,
			ObservedAt: t0,
			Resource:   event.ResourceRef{Kind: "ConfigMap", ResourceVersion: "rv1"},
		},
		testRecord(testCMStream, event.TypeEvent, event.WatchAdded, true, "default", "cm-1", "ConfigMap", "rv1",
			map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-1", "namespace": "default"}, "data": map[string]any{"v1": "a"}}, t0),
		testRecord(testCMStream, event.TypeEvent, event.WatchModified, false, "default", "cm-1", "ConfigMap", "rv2",
			map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-1", "namespace": "default"}, "data": map[string]any{"v1": "b"}}, t1),
		{
			ClusterID:  testCluster,
			StreamID:   testCMStream,
			Type:       event.TypeGap,
			ObservedAt: t2,
			Resource:   event.ResourceRef{Kind: "ConfigMap", Namespace: "default"},
			Gap:        &event.GapInfo{FromResourceVersion: "rv2", ToResourceVersion: "rv3", Reason: "410 gone", DetectedAt: t2},
		},
		{
			ClusterID:  testCluster,
			StreamID:   testCMStream,
			Type:       event.TypeCheckpoint,
			ObservedAt: t3,
			Checkpoint: &event.CheckpointInfo{ResourceVersion: "rv9", BookmarkedAt: t3},
		},
		// deployments: baseline, synthetic added, modified
		{
			ClusterID:  testCluster,
			StreamID:   testDEPStream,
			Type:       event.TypeBaseline,
			ObservedAt: t0,
			Resource:   event.ResourceRef{Kind: "Deployment", ResourceVersion: "rv1"},
		},
		testRecord(testDEPStream, event.TypeEvent, event.WatchAdded, true, "default", "web", "Deployment", "rv1",
			map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "web", "namespace": "default"}, "spec": map[string]any{"replicas": float64(1)}}, t0),
		testRecord(testDEPStream, event.TypeEvent, event.WatchModified, false, "default", "web", "Deployment", "rv2",
			map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "web", "namespace": "default"}, "spec": map[string]any{"replicas": float64(3)}}, t1),
	}
	if _, err := store.Appends(ctx, recs); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	return store
}

func newTestServer(t *testing.T, store storage.Store) *httptest.Server {
	t.Helper()
	mat := materialize.NewMaterializer(store)
	planner := replay.NewPlanner(store, mat, replay.DefaultPolicy())
	srv, err := NewServer(store, mat, planner, metrics.New(), "test")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func doJSON(t *testing.T, method, url string, body any, out any) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	if out != nil {
		if err := json.NewDecoder(res.Body).Decode(out); err != nil {
			t.Fatalf("decode %s %s response: %v", method, url, err)
		}
	}
	return res
}

func historyURL(base, cluster, stream, ns, name string) string {
	ref := EncodeObjectRef(storage.ObjectRef{
		ClusterID: cluster,
		StreamID:  stream,
		Namespace: ns,
		Name:      name,
	})
	return base + "/v1/objects/" + url.PathEscape(ref) + "/history"
}

func TestHealth(t *testing.T) {
	ts := newTestServer(t, storage.NewInMemory())
	var h queryv1.Health
	res := doJSON(t, http.MethodGet, ts.URL+"/v1/health", nil, &h)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", res.StatusCode)
	}
	if h.Status != "ok" {
		t.Errorf("health status field = %q, want ok", h.Status)
	}
	if h.Version != "test" {
		t.Errorf("health version = %q, want test", h.Version)
	}
	if h.Storage != "sqlite" {
		t.Errorf("health storage = %q, want sqlite", h.Storage)
	}
}

func TestClusters(t *testing.T) {
	ts := newTestServer(t, seedStore(t))
	var clusters []queryv1.Cluster
	res := doJSON(t, http.MethodGet, ts.URL+"/v1/clusters", nil, &clusters)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("clusters status = %d", res.StatusCode)
	}
	if len(clusters) != 1 || clusters[0].ID != testCluster {
		t.Fatalf("clusters = %+v", clusters)
	}
	if clusters[0].Generation != "1" {
		t.Errorf("cluster generation = %q, want 1", clusters[0].Generation)
	}
	if clusters[0].FirstSeen.IsZero() || clusters[0].LastSeen.IsZero() {
		t.Errorf("cluster first_seen/last_seen not derived: %+v", clusters[0])
	}
}

func TestStreamsHasGaps(t *testing.T) {
	ts := newTestServer(t, seedStore(t))
	var streams []queryv1.Stream
	res := doJSON(t, http.MethodGet, ts.URL+"/v1/streams", nil, &streams)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("streams status = %d", res.StatusCode)
	}
	var cm *queryv1.Stream
	for i := range streams {
		if streams[i].ID == testCMStream {
			cm = &streams[i]
		}
	}
	if cm == nil {
		t.Fatalf("no configmaps stream in %+v", streams)
	}
	if !cm.HasGaps {
		t.Error("configmaps stream should report has_gaps")
	}
	if cm.GapCount != 1 {
		t.Errorf("configmaps gap_count = %d, want 1", cm.GapCount)
	}
}

func TestObjectHistoryTimeline(t *testing.T) {
	ts := newTestServer(t, seedStore(t))
	u := historyURL(ts.URL, testCluster, testCMStream, "default", "cm-1")
	var h queryv1.ObjectHistory
	res := doJSON(t, http.MethodGet, u, nil, &h)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("history status = %d", res.StatusCode)
	}
	if h.Kind != "ConfigMap" {
		t.Errorf("history kind = %q, want ConfigMap", h.Kind)
	}
	if !h.HasGaps {
		t.Error("history should report has_gaps")
	}
	if len(h.Gaps) != 1 {
		t.Errorf("history gaps = %d, want 1", len(h.Gaps))
	}

	var modified *queryv1.TimelineItem
	for i := range h.Items {
		if h.Items[i].WatchType == "MODIFIED" {
			modified = &h.Items[i]
		}
		if h.Items[i].WatchType == "BOOKMARK" {
			t.Error("bookmark record leaked into timeline")
		}
	}
	if modified == nil {
		t.Fatalf("no MODIFIED item in %+v", h.Items)
	}
	found := false
	for _, p := range modified.ChangedFields {
		if p == "data.v1" {
			found = true
		}
	}
	if !found {
		t.Errorf("modified item changed_fields = %v, want data.v1", modified.ChangedFields)
	}
	if modified.Summary != "data.v1 changed" {
		t.Errorf("modified item summary = %q, want %q", modified.Summary, "data.v1 changed")
	}
}

func TestDiff(t *testing.T) {
	ts := newTestServer(t, seedStore(t))
	before := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	after := before.Add(10 * time.Minute)
	u := fmt.Sprintf("%s/v1/diff?cluster_id=%s&namespace=default&since=%s&until=%s",
		ts.URL, testCluster, before.Format(time.RFC3339), after.Format(time.RFC3339))
	var d queryv1.DiffResult
	res := doJSON(t, http.MethodGet, u, nil, &d)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("diff status = %d", res.StatusCode)
	}
	for _, od := range d.Changed {
		if od.Name != "cm-1" {
			continue
		}
		for _, fc := range od.Changes {
			if fc.Path == "data.v1" && fc.Before == "a" && fc.After == "b" {
				return
			}
		}
	}
	t.Errorf("diff did not detect configmap data.v1 a->b: %+v", d.Changed)
}

func TestEventsPagination(t *testing.T) {
	ts := newTestServer(t, seedStore(t))
	var page queryv1.EventPage
	res := doJSON(t, http.MethodGet, ts.URL+"/v1/events?limit=2", nil, &page)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("events status = %d", res.StatusCode)
	}
	if len(page.Items) != 2 {
		t.Errorf("page items = %d, want 2", len(page.Items))
	}
	if !page.HasMore {
		t.Error("expected has_more=true")
	}
	if page.NextCursor == "" {
		t.Error("expected next_cursor")
	}

	var next queryv1.EventPage
	res = doJSON(t, http.MethodGet, ts.URL+"/v1/events?limit=2&cursor="+url.QueryEscape(page.NextCursor), nil, &next)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("events(cursor) status = %d", res.StatusCode)
	}
	if len(next.Items) != 2 {
		t.Errorf("second page items = %d, want 2", len(next.Items))
	}
	if next.Items[0].IngestSeq <= page.Items[len(page.Items)-1].IngestSeq {
		t.Error("cursor page should only return newer records")
	}
}

func TestSnapshotsList(t *testing.T) {
	store := seedStore(t)
	mat := materialize.NewMaterializer(store)
	at := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	if _, err := mat.Snapshot(context.Background(), testCluster, at, "preview"); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	ts := newTestServer(t, store)
	var snaps []queryv1.Snapshot
	res := doJSON(t, http.MethodGet, ts.URL+"/v1/snapshots", nil, &snaps)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("snapshots status = %d", res.StatusCode)
	}
	if len(snaps) != 1 {
		t.Fatalf("snapshots = %d, want 1", len(snaps))
	}
	if snaps[0].ID == "" || snaps[0].ClusterID != testCluster {
		t.Errorf("snapshot ref = %+v", snaps[0])
	}
	if snaps[0].Complete {
		t.Error("gapped snapshot should be incomplete")
	}
	if snaps[0].ObjectCount != 2 || snaps[0].Streams != 2 {
		t.Errorf("snapshot details = objects:%d streams:%d, want 2/2", snaps[0].ObjectCount, snaps[0].Streams)
	}
	if len(snaps[0].Missing) != 1 || snaps[0].Missing[0] != testCMStream {
		t.Errorf("snapshot missing = %v, want [%s]", snaps[0].Missing, testCMStream)
	}
}

func TestReplayPlans(t *testing.T) {
	store := seedStore(t)
	mat := materialize.NewMaterializer(store)
	at := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	snap, err := mat.Snapshot(context.Background(), testCluster, at, "preview")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	ts := newTestServer(t, store)

	// allow_gaps=true on the gapped store -> 200 with plan objects.
	var plan queryv1.ReplayPlan
	res := doJSON(t, http.MethodPost, ts.URL+"/v1/replay-plans", map[string]any{
		"cluster_id":  testCluster,
		"snapshot_id": snap.ID,
		"allow_gaps":  true,
	}, &plan)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("create plan status = %d, want 200", res.StatusCode)
	}
	if len(plan.Objects) != 2 {
		t.Errorf("plan objects = %d, want 2", len(plan.Objects))
	}
	if plan.ID == "" || plan.SnapshotID != snap.ID {
		t.Errorf("plan identity = %+v", plan)
	}

	var plans []queryv1.ReplayPlan
	res = doJSON(t, http.MethodGet, ts.URL+"/v1/replay-plans", nil, &plans)
	if res.StatusCode != http.StatusOK || len(plans) != 1 || plans[0].ID != plan.ID {
		t.Fatalf("list plans status=%d plans=%+v", res.StatusCode, plans)
	}

	// Registered plan is fetchable by id.
	var got queryv1.ReplayPlan
	res = doJSON(t, http.MethodGet, ts.URL+"/v1/replay-plans/"+plan.ID, nil, &got)
	if res.StatusCode != http.StatusOK || got.ID != plan.ID {
		t.Fatalf("get plan status=%d id=%q", res.StatusCode, got.ID)
	}

	// Unknown id -> 404.
	res = doJSON(t, http.MethodGet, ts.URL+"/v1/replay-plans/plan-missing", nil, nil)
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("missing plan status = %d, want 404", res.StatusCode)
	}

	// allow_gaps=false on the gapped store -> 409.
	var errResp map[string]string
	res = doJSON(t, http.MethodPost, ts.URL+"/v1/replay-plans", map[string]any{
		"cluster_id":  testCluster,
		"snapshot_id": snap.ID,
		"allow_gaps":  false,
	}, &errResp)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("gapped plan status = %d, want 409 (body=%v)", res.StatusCode, errResp)
	}
}

// TestReplayPlansCleanStore verifies that a gap-free store with a directly
// saved snapshot yields a complete, 200 plan under the default (no gaps)
// policy.
func TestReplayPlansCleanStore(t *testing.T) {
	store := storage.NewInMemory()
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	recs := []*event.Record{
		{
			ClusterID:  testCluster,
			StreamID:   testCMStream,
			Type:       event.TypeBaseline,
			ObservedAt: t0,
			Resource:   event.ResourceRef{Kind: "ConfigMap", ResourceVersion: "rv1"},
		},
		testRecord(testCMStream, event.TypeEvent, event.WatchAdded, true, "default", "cm-1", "ConfigMap", "rv1",
			map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-1", "namespace": "default"}, "data": map[string]any{"v1": "a"}}, t0),
	}
	if _, err := store.Appends(ctx, recs); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := store.SaveSnapshot(ctx, &storage.SnapshotRef{
		ID: "snap-clean", ClusterID: testCluster, Name: "clean", At: t0.Add(time.Hour),
	}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	ts := newTestServer(t, store)
	var plan queryv1.ReplayPlan
	res := doJSON(t, http.MethodPost, ts.URL+"/v1/replay-plans", map[string]any{
		"cluster_id":  testCluster,
		"snapshot_id": "snap-clean",
	}, &plan)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("clean plan status = %d, want 200", res.StatusCode)
	}
	if !plan.CoverageComplete {
		t.Error("clean store plan should be coverage_complete=true")
	}
	if len(plan.Objects) != 1 {
		t.Errorf("clean plan objects = %d, want 1", len(plan.Objects))
	}
}
