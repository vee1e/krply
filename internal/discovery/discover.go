// Package discovery resolves resource specifications into full group/version/
// resource triples with scope information.
//
// A ResourceSpec is "full" when it carries a Version and a Kind. Partial specs
// (for example just "deploy" or "pods") are resolved through well-known
// aliases and the Kubernetes discovery API.
package discovery

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
)

// ResourceSpec identifies one collection to watch.
//
// APIGroup is empty for core/v1 resources. A Namespace of "" means cluster
// scope (or all namespaces for a namespaced resource).
type ResourceSpec struct {
	APIGroup  string
	Version   string
	Resource  string
	Kind      string
	Namespace string
}

// DefaultResources returns the MVP watch list.
//
// Namespaced resources use an empty Namespace so the collector watches the
// collection cluster-wide. Cluster-scoped resources (namespaces, runtime
// classes) are resolved to Namespace "" by discovery.
func DefaultResources() []ResourceSpec {
	return []ResourceSpec{
		{APIGroup: "", Version: "v1", Resource: "namespaces", Kind: "Namespace", Namespace: ""},
		{APIGroup: "", Version: "v1", Resource: "configmaps", Kind: "ConfigMap", Namespace: ""},
		{APIGroup: "", Version: "v1", Resource: "services", Kind: "Service", Namespace: ""},
		{APIGroup: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment", Namespace: ""},
		{APIGroup: "apps", Version: "v1", Resource: "statefulsets", Kind: "StatefulSet", Namespace: ""},
		{APIGroup: "apps", Version: "v1", Resource: "daemonsets", Kind: "DaemonSet", Namespace: ""},
		{APIGroup: "node.k8s.io", Version: "v1", Resource: "runtimeclasses", Kind: "RuntimeClass", Namespace: ""},
		{APIGroup: "", Version: "v1", Resource: "pods", Kind: "Pod", Namespace: ""},
	}
}

// Discover resolves the default resource list against the cluster's discovery
// API and returns the full specs. Partial specs are filled in via aliases and
// discovery.
func Discover(ctx context.Context, client kubernetes.Interface) ([]ResourceSpec, error) {
	return Resolve(ctx, client, DefaultResources())
}

// Resolve fills missing APIGroup/Version/Kind for every spec, resolves
// well-known aliases, and clears Namespace for cluster-scoped resources.
func Resolve(ctx context.Context, client kubernetes.Interface, specs []ResourceSpec) ([]ResourceSpec, error) {
	out := make([]ResourceSpec, 0, len(specs))
	for _, spec := range specs {
		resolved, err := resolveOne(ctx, client, spec)
		if err != nil {
			return nil, err
		}
		out = append(out, resolved)
	}
	return out, nil
}

func resolveOne(ctx context.Context, client kubernetes.Interface, spec ResourceSpec) (ResourceSpec, error) {
	if alias, ok := aliases[strings.ToLower(spec.Resource)]; ok {
		spec = mergeAlias(spec, alias)
	}
	if spec.Version != "" && spec.Kind != "" {
		return spec, nil
	}
	return resolveViaDiscovery(ctx, client, spec)
}

// mergeAlias fills empty fields on spec from the alias. Explicit fields on the
// user's spec are never overwritten so a fully- or partially-qualified spec
// whose plural name happens to match an alias (for example a CRD named "pods")
// is not silently rewritten to the built-in resource.
func mergeAlias(spec, alias ResourceSpec) ResourceSpec {
	if spec.APIGroup == "" {
		spec.APIGroup = alias.APIGroup
	}
	if spec.Version == "" {
		spec.Version = alias.Version
	}
	if spec.Kind == "" {
		spec.Kind = alias.Kind
	}
	if alias.Resource != "" && !strings.EqualFold(spec.Resource, alias.Resource) {
		spec.Resource = alias.Resource
	}
	return spec
}

func resolveViaDiscovery(ctx context.Context, client kubernetes.Interface, spec ResourceSpec) (ResourceSpec, error) {
	if spec.Version != "" {
		return resolveInGroupVersion(ctx, client, spec)
	}
	lists, err := client.Discovery().ServerPreferredResources()
	for _, l := range lists {
		if l == nil {
			continue
		}
		for _, api := range l.APIResources {
			if !strings.EqualFold(api.Name, spec.Resource) {
				continue
			}
			gv, parseErr := schema.ParseGroupVersion(l.GroupVersion)
			if parseErr != nil {
				continue
			}
			spec.APIGroup = gv.Group
			spec.Version = gv.Version
			spec.Kind = api.Kind
			if !api.Namespaced {
				spec.Namespace = ""
			}
			return spec, nil
		}
	}
	if err != nil {
		return spec, fmt.Errorf("discover %q: %w", spec.Resource, err)
	}
	return spec, fmt.Errorf("discover %q: not found in ServerPreferredResources", spec.Resource)
}

func resolveInGroupVersion(ctx context.Context, client kubernetes.Interface, spec ResourceSpec) (ResourceSpec, error) {
	gv := spec.Version
	if spec.APIGroup != "" {
		gv = spec.APIGroup + "/" + spec.Version
	}
	list, err := client.Discovery().ServerResourcesForGroupVersion(gv)
	if err != nil {
		return spec, fmt.Errorf("discover %q in %q: %w", spec.Resource, gv, err)
	}
	for _, api := range list.APIResources {
		if !strings.EqualFold(api.Name, spec.Resource) {
			continue
		}
		spec.Kind = api.Kind
		if !api.Namespaced {
			spec.Namespace = ""
		}
		return spec, nil
	}
	return spec, fmt.Errorf("discover %q: not found in group version %q", spec.Resource, gv)
}

// aliases maps common kubectl-style names to canonical specs.
var aliases = map[string]ResourceSpec{
	"deploy":                 {APIGroup: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment"},
	"deployment":             {APIGroup: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment"},
	"deployments":            {APIGroup: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment"},
	"sts":                    {APIGroup: "apps", Version: "v1", Resource: "statefulsets", Kind: "StatefulSet"},
	"statefulset":            {APIGroup: "apps", Version: "v1", Resource: "statefulsets", Kind: "StatefulSet"},
	"statefulsets":           {APIGroup: "apps", Version: "v1", Resource: "statefulsets", Kind: "StatefulSet"},
	"ds":                     {APIGroup: "apps", Version: "v1", Resource: "daemonsets", Kind: "DaemonSet"},
	"daemonset":              {APIGroup: "apps", Version: "v1", Resource: "daemonsets", Kind: "DaemonSet"},
	"daemonsets":             {APIGroup: "apps", Version: "v1", Resource: "daemonsets", Kind: "DaemonSet"},
	"cm":                     {Version: "v1", Resource: "configmaps", Kind: "ConfigMap"},
	"configmap":              {Version: "v1", Resource: "configmaps", Kind: "ConfigMap"},
	"configmaps":             {Version: "v1", Resource: "configmaps", Kind: "ConfigMap"},
	"po":                     {Version: "v1", Resource: "pods", Kind: "Pod"},
	"pod":                    {Version: "v1", Resource: "pods", Kind: "Pod"},
	"pods":                   {Version: "v1", Resource: "pods", Kind: "Pod"},
	"svc":                    {Version: "v1", Resource: "services", Kind: "Service"},
	"service":                {Version: "v1", Resource: "services", Kind: "Service"},
	"services":               {Version: "v1", Resource: "services", Kind: "Service"},
	"ns":                     {Version: "v1", Resource: "namespaces", Kind: "Namespace"},
	"namespace":              {Version: "v1", Resource: "namespaces", Kind: "Namespace"},
	"namespaces":             {Version: "v1", Resource: "namespaces", Kind: "Namespace"},
	"rc":                     {Version: "v1", Resource: "replicationcontrollers", Kind: "ReplicationController"},
	"replicationcontroller":  {Version: "v1", Resource: "replicationcontrollers", Kind: "ReplicationController"},
	"replicationcontrollers": {Version: "v1", Resource: "replicationcontrollers", Kind: "ReplicationController"},
	"rs":                     {APIGroup: "apps", Version: "v1", Resource: "replicasets", Kind: "ReplicaSet"},
	"replicaset":             {APIGroup: "apps", Version: "v1", Resource: "replicasets", Kind: "ReplicaSet"},
	"replicasets":            {APIGroup: "apps", Version: "v1", Resource: "replicasets", Kind: "ReplicaSet"},
	"runtimeclass":           {APIGroup: "node.k8s.io", Version: "v1", Resource: "runtimeclasses", Kind: "RuntimeClass"},
	"runtimeclasses":         {APIGroup: "node.k8s.io", Version: "v1", Resource: "runtimeclasses", Kind: "RuntimeClass"},
}
