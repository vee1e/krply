package discovery

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
)

func newFakeClient() *fakeclientset.Clientset {
	cli := fakeclientset.NewSimpleClientset()
	cli.Discovery().(*fakediscovery.FakeDiscovery).Resources = []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "namespaces", Kind: "Namespace", Namespaced: false},
				{Name: "configmaps", Kind: "ConfigMap", Namespaced: true},
				{Name: "services", Kind: "Service", Namespaced: true},
				{Name: "pods", Kind: "Pod", Namespaced: true},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Kind: "Deployment", Namespaced: true},
				{Name: "statefulsets", Kind: "StatefulSet", Namespaced: true},
				{Name: "daemonsets", Kind: "DaemonSet", Namespaced: true},
			},
		},
		{
			GroupVersion: "node.k8s.io/v1",
			APIResources: []metav1.APIResource{
				{Name: "runtimeclasses", Kind: "RuntimeClass", Namespaced: false},
			},
		},
	}
	return cli
}

func TestDefaultResources(t *testing.T) {
	specs := DefaultResources()
	if len(specs) != 8 {
		t.Fatalf("DefaultResources len = %d, want 8", len(specs))
	}
	byResource := map[string]ResourceSpec{}
	for _, s := range specs {
		byResource[s.Resource] = s
	}
	want := []string{"namespaces", "configmaps", "services", "deployments", "statefulsets", "daemonsets", "runtimeclasses", "pods"}
	for _, r := range want {
		if _, ok := byResource[r]; !ok {
			t.Errorf("DefaultResources missing %q", r)
		}
	}
	if got := byResource["deployments"]; got.APIGroup != "apps" || got.Version != "v1" || got.Kind != "Deployment" {
		t.Errorf("deployments spec = %+v", got)
	}
	if got := byResource["runtimeclasses"]; got.APIGroup != "node.k8s.io" || got.Version != "v1" || got.Kind != "RuntimeClass" {
		t.Errorf("runtimeclasses spec = %+v", got)
	}
}

func TestDiscoverResolvesDefaults(t *testing.T) {
	cli := newFakeClient()
	specs, err := Discover(context.Background(), cli)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(specs) != 8 {
		t.Fatalf("Discover len = %d, want 8", len(specs))
	}
	for _, s := range specs {
		if s.Version == "" || s.Kind == "" {
			t.Errorf("spec not fully resolved: %+v", s)
		}
	}
}

func TestResolveAliases(t *testing.T) {
	cli := newFakeClient()
	inputs := []ResourceSpec{
		{Resource: "deploy"},
		{Resource: "sts"},
		{Resource: "ds"},
		{Resource: "cm"},
		{Resource: "po"},
		{Resource: "svc"},
		{Resource: "ns"},
		{Resource: "rc"},
	}
	resolved, err := Resolve(context.Background(), cli, inputs)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := []struct{ resource, group, kind string }{
		{"deployments", "apps", "Deployment"},
		{"statefulsets", "apps", "StatefulSet"},
		{"daemonsets", "apps", "DaemonSet"},
		{"configmaps", "", "ConfigMap"},
		{"pods", "", "Pod"},
		{"services", "", "Service"},
		{"namespaces", "", "Namespace"},
		{"runtimeclasses", "node.k8s.io", "RuntimeClass"},
	}
	for i, w := range want {
		got := resolved[i]
		if got.Resource != w.resource || got.APIGroup != w.group || got.Kind != w.kind {
			t.Errorf("alias %d: got %+v, want resource=%q group=%q kind=%q", i, got, w.resource, w.group, w.kind)
		}
	}
}

func TestResolvePartialWithKnownVersion(t *testing.T) {
	cli := newFakeClient()
	resolved, err := Resolve(context.Background(), cli, []ResourceSpec{
		{APIGroup: "apps", Version: "v1", Resource: "deployments"},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(resolved) != 1 {
		t.Fatalf("Resolve len = %d", len(resolved))
	}
	s := resolved[0]
	if s.Kind != "Deployment" {
		t.Errorf("kind = %q, want Deployment", s.Kind)
	}
	if s.Namespace != "" {
		t.Errorf("deployments should be namespaced with empty namespace filter, got %q", s.Namespace)
	}
}
