package replay

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/materialize"
	"github.com/krply/krply/internal/storage"
)

func rawObject(obj map[string]any) json.RawMessage {
	raw, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return raw
}

func appendRec(t *testing.T, store storage.Store, rec *event.Record) {
	t.Helper()
	if _, err := store.Append(context.Background(), rec); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func eventRec(cluster, stream string, wt event.WatchType, res event.ResourceRef, obj map[string]any, at time.Time) *event.Record {
	return &event.Record{
		ClusterID:  cluster,
		StreamID:   stream,
		Type:       event.TypeEvent,
		WatchType:  wt,
		Resource:   res,
		Object:     rawObject(obj),
		ObservedAt: at,
	}
}

func baselineRec(cluster, stream string, at time.Time) *event.Record {
	return &event.Record{
		ClusterID:  cluster,
		StreamID:   stream,
		Type:       event.TypeBaseline,
		ObservedAt: at,
	}
}

func gapRec(cluster, stream string, at time.Time) *event.Record {
	return &event.Record{
		ClusterID:  cluster,
		StreamID:   stream,
		Type:       event.TypeGap,
		ObservedAt: at,
		Gap:        &event.GapInfo{Reason: "test gap"},
	}
}

func deploymentObject(name, namespace string) map[string]any {
	return map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":              name,
			"namespace":         namespace,
			"uid":               "uid-" + name,
			"resourceVersion":   "42",
			"generation":        2,
			"creationTimestamp": "2026-08-01T00:00:00Z",
			"managedFields": []any{
				map[string]any{"manager": "kubectl", "operation": "Update"},
			},
			"annotations": map[string]any{
				"user.example.com/owner":                           "team",
				"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"apps/v1","kind":"Deployment"}`,
			},
		},
		"spec": map[string]any{
			"replicas": 3,
			"selector": map[string]any{"matchLabels": map[string]any{"app": name}},
			"template": map[string]any{
				"metadata": map[string]any{
					"creationTimestamp": "2026-08-01T00:00:00Z",
					"uid":               "uid-pod-" + name,
					"resourceVersion":   "42",
					"labels":            map[string]any{"app": name},
				},
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "app", "image": "nginx:1.21"},
					},
				},
			},
		},
		"status": map[string]any{"readyReplicas": 1, "replicas": 3},
	}
}

func secretObject(name string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       "default",
			"uid":             "uid-" + name,
			"resourceVersion": "1",
		},
		"type": "Opaque",
		"data": map[string]any{"password": "c2VjcmV0"},
	}
}

func loadBalancerService(name string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       "default",
			"uid":             "uid-" + name,
			"resourceVersion": "1",
		},
		"spec": map[string]any{
			"type":      "LoadBalancer",
			"clusterIP": "10.0.0.1",
		},
		"status": map[string]any{
			"loadBalancer": map[string]any{"ingress": []any{map[string]any{"ip": "203.0.113.5"}}},
		},
	}
}

func saveSnapshot(t *testing.T, store storage.Store, id, cluster string, at time.Time) {
	t.Helper()
	if err := store.SaveSnapshot(context.Background(), &storage.SnapshotRef{ID: id, ClusterID: cluster, Name: "daily", At: at}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
}

func TestPlanSanitizesAndExcludes(t *testing.T) {
	store := storage.NewInMemory()
	mat := materialize.NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	streamDeploy := event.Stream{ClusterID: cluster, Group: "apps", Version: "v1", Resource: "deployments", Namespace: ""}.ID()
	streamSecret := event.Stream{ClusterID: cluster, Group: "", Version: "v1", Resource: "secrets", Namespace: ""}.ID()
	streamSvc := event.Stream{ClusterID: cluster, Group: "", Version: "v1", Resource: "services", Namespace: ""}.ID()

	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	appendRec(t, store, baselineRec(cluster, streamDeploy, base))
	appendRec(t, store, eventRec(cluster, streamDeploy, event.WatchAdded, event.ResourceRef{Namespace: "default", Name: "web", Kind: "Deployment"}, deploymentObject("web", "default"), base.Add(1*time.Minute)))
	appendRec(t, store, baselineRec(cluster, streamSecret, base))
	appendRec(t, store, eventRec(cluster, streamSecret, event.WatchAdded, event.ResourceRef{Namespace: "default", Name: "creds", Kind: "Secret"}, secretObject("creds"), base.Add(1*time.Minute)))
	appendRec(t, store, baselineRec(cluster, streamSvc, base))
	appendRec(t, store, eventRec(cluster, streamSvc, event.WatchAdded, event.ResourceRef{Namespace: "default", Name: "lb", Kind: "Service"}, loadBalancerService("lb"), base.Add(1*time.Minute)))

	snapAt := base.Add(10 * time.Minute)
	saveSnapshot(t, store, "snap-test", cluster, snapAt)

	planner := NewPlanner(store, mat, DefaultPolicy())
	plan, err := planner.Plan(ctx, cluster, "snap-test", "", "")
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	if plan.Status != "planned" {
		t.Fatalf("Status = %q, want planned", plan.Status)
	}
	if !plan.CoverageComplete {
		t.Fatalf("CoverageComplete = false, want true")
	}
	if plan.ID == "" || plan.FieldManager != "krply-plan-"+plan.ID {
		t.Fatalf("ID = %q, FieldManager = %q, want krply-plan-<id>", plan.ID, plan.FieldManager)
	}
	if len(plan.Objects) != 1 {
		t.Fatalf("objects = %d, want 1: %+v", len(plan.Objects), plan.Objects)
	}

	po := plan.Objects[0]
	if po.Kind != "Deployment" || po.Name != "web" || po.Order != 1 {
		t.Fatalf("object = %s/%s order %d, want Deployment/web order 1", po.Kind, po.Name, po.Order)
	}
	meta, _ := po.Object["metadata"].(map[string]any)
	for _, f := range []string{"uid", "resourceVersion", "creationTimestamp", "generation", "managedFields", "ownerReferences"} {
		if _, ok := meta[f]; ok {
			t.Errorf("metadata.%s still present", f)
		}
	}
	if anns, ok := meta["annotations"].(map[string]any); ok {
		if _, ok := anns["kubectl.kubernetes.io/last-applied-configuration"]; ok {
			t.Errorf("last-applied annotation still present")
		}
		if _, ok := anns["user.example.com/owner"]; !ok {
			t.Errorf("user annotation dropped")
		}
	}
	if _, ok := po.Object["status"]; ok {
		t.Errorf("status still present")
	}
	spec, _ := po.Object["spec"].(map[string]any)
	if spec["replicas"] != float64(3) {
		t.Errorf("spec.replicas = %v, want 3", spec["replicas"])
	}
	tmplMeta, _ := spec["template"].(map[string]any)["metadata"].(map[string]any)
	for _, f := range []string{"creationTimestamp", "uid", "resourceVersion"} {
		if _, ok := tmplMeta[f]; ok {
			t.Errorf("spec.template.metadata.%s still present", f)
		}
	}

	excluded := map[string]string{}
	for _, ex := range plan.Excluded {
		excluded[ex.Kind] = ex.Reason
	}
	if got := excluded["Secret"]; got != "secret excluded by default" {
		t.Errorf("Secret exclusion = %q, want secret excluded by default", got)
	}
	if got := excluded["Service"]; got != "load balancer service not safe to replay" {
		t.Errorf("Service exclusion = %q, want load balancer service not safe to replay", got)
	}
}

func TestPlanRefusesIncompleteCoverage(t *testing.T) {
	store := storage.NewInMemory()
	mat := materialize.NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	streamDeploy := event.Stream{ClusterID: cluster, Group: "apps", Version: "v1", Resource: "deployments", Namespace: ""}.ID()

	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	appendRec(t, store, baselineRec(cluster, streamDeploy, base))
	appendRec(t, store, eventRec(cluster, streamDeploy, event.WatchAdded, event.ResourceRef{Namespace: "default", Name: "web", Kind: "Deployment"}, deploymentObject("web", "default"), base.Add(1*time.Minute)))
	appendRec(t, store, gapRec(cluster, streamDeploy, base.Add(2*time.Minute)))

	snapAt := base.Add(10 * time.Minute)
	saveSnapshot(t, store, "snap-gap", cluster, snapAt)

	planner := NewPlanner(store, mat, DefaultPolicy())
	_, err := planner.Plan(ctx, cluster, "snap-gap", "", "")
	if err == nil {
		t.Fatalf("Plan with gaps = nil error, want refusal")
	}
	if !strings.Contains(err.Error(), "incomplete coverage") {
		t.Fatalf("error %q does not mention incomplete coverage", err)
	}

	allow := DefaultPolicy()
	allow.AllowGaps = true
	planner2 := NewPlanner(store, mat, allow)
	plan, err := planner2.Plan(ctx, cluster, "snap-gap", "", "")
	if err != nil {
		t.Fatalf("Plan with allow-gaps: %v", err)
	}
	if plan.CoverageComplete {
		t.Fatalf("CoverageComplete = true, want false")
	}
}

func TestPlanNamespaceMapping(t *testing.T) {
	store := storage.NewInMemory()
	mat := materialize.NewMaterializer(store)
	ctx := context.Background()

	cluster := "c1"
	streamDeploy := event.Stream{ClusterID: cluster, Group: "apps", Version: "v1", Resource: "deployments", Namespace: ""}.ID()

	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	appendRec(t, store, baselineRec(cluster, streamDeploy, base))
	appendRec(t, store, eventRec(cluster, streamDeploy, event.WatchAdded, event.ResourceRef{Namespace: "default", Name: "web", Kind: "Deployment"}, deploymentObject("web", "default"), base.Add(1*time.Minute)))
	appendRec(t, store, eventRec(cluster, streamDeploy, event.WatchAdded, event.ResourceRef{Namespace: "other", Name: "other-web", Kind: "Deployment"}, deploymentObject("other-web", "other"), base.Add(2*time.Minute)))

	snapAt := base.Add(10 * time.Minute)
	saveSnapshot(t, store, "snap-map", cluster, snapAt)

	planner := NewPlanner(store, mat, DefaultPolicy())
	plan, err := planner.Plan(ctx, cluster, "snap-map", "default", "sandbox")
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	if len(plan.Objects) != 1 {
		t.Fatalf("objects = %d, want 1 (only source namespace objects)", len(plan.Objects))
	}
	po := plan.Objects[0]
	if po.Name != "web" {
		t.Fatalf("object = %s, want web", po.Name)
	}
	if po.Namespace != "sandbox" {
		t.Fatalf("object namespace = %q, want sandbox", po.Namespace)
	}
	meta, _ := po.Object["metadata"].(map[string]any)
	if got := meta["namespace"]; got != "sandbox" {
		t.Fatalf("metadata.namespace = %v, want sandbox", got)
	}

	var foundOutside bool
	for _, ex := range plan.Excluded {
		if ex.Kind == "Deployment" && ex.Name == "other-web" && ex.Reason == "outside source namespace" {
			foundOutside = true
		}
	}
	if !foundOutside {
		t.Fatalf("expected other-web excluded as outside source namespace: %+v", plan.Excluded)
	}

	var mapped bool
	for _, w := range plan.Warnings {
		if strings.Contains(w, "namespace mapping") {
			mapped = true
		}
	}
	if !mapped {
		t.Fatalf("plan warnings %v lack a namespace mapping note", plan.Warnings)
	}
}
