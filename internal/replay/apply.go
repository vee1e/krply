package replay

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// DryRunResult reports the outcome of a server-side dry run.
type DryRunResult struct {
	Applied   int
	Conflicts []DryRunItem
	Errors    []DryRunItem
	Skipped   []DryRunItem
	OK        bool
}

// DryRunItem describes one object that failed to apply or conflicted.
type DryRunItem struct {
	Namespace string
	Name      string
	Kind      string
	Manager   string
	Message   string
}

// ApplyResult reports the outcome of an applied plan.
type ApplyResult struct {
	PlanID  string
	Applied int
	Errors  []DryRunItem
	Skipped []DryRunItem
}

// DryRun runs a server-side apply dry run against the target cluster. It never
// sends Force, so ownership conflicts are reported instead of overwritten. OK
// is true only when there are no conflicts, no errors, and nothing skipped.
func (pl *Plan) DryRun(ctx context.Context, kubeconfig, targetContext string) (*DryRunResult, error) {
	pl.Mu.Lock()
	defer pl.Mu.Unlock()
	dyn, err := pl.dynamicClient(kubeconfig, targetContext)
	if err != nil {
		return nil, err
	}
	res := &DryRunResult{}
	items, skipped := pl.applyItems()
	res.Skipped = skipped
	for _, it := range items {
		if err := applyOne(ctx, dyn, it, pl.FieldManager, true); err != nil {
			if apierrors.IsConflict(err) {
				res.Conflicts = append(res.Conflicts, DryRunItem{
					Namespace: it.namespace,
					Name:      it.name,
					Kind:      it.kind,
					Manager:   conflictManager(err),
					Message:   err.Error(),
				})
			} else if apierrors.IsNotFound(err) && it.namespace != "" {
				// The target namespace does not exist yet; Apply creates it
				// first, so this is not a real failure for the dry run.
			} else {
				res.Errors = append(res.Errors, DryRunItem{
					Namespace: it.namespace,
					Name:      it.name,
					Kind:      it.kind,
					Message:   err.Error(),
				})
			}
			continue
		}
		res.Applied++
	}
	res.OK = len(res.Conflicts) == 0 && len(res.Errors) == 0 && len(res.Skipped) == 0
	if res.OK {
		pl.Status = "dry-run-ok"
	} else {
		pl.Status = "dry-run-failed"
	}
	return res, nil
}

// Apply applies the plan to the target cluster with server-side apply using
// the synthetic field manager. It refuses to run without explicit confirmation
// and without a previously successful dry run (the plan status must be
// "dry-run-ok"). Failures are collected per object and never stop the loop.
func (pl *Plan) Apply(ctx context.Context, kubeconfig, targetContext string, confirm bool) (*ApplyResult, error) {
	if !confirm {
		return nil, errors.New("refusing apply without --confirm")
	}
	pl.Mu.Lock()
	defer pl.Mu.Unlock()
	if pl.Status != "dry-run-ok" {
		return nil, fmt.Errorf("refusing apply: plan %s has not passed a dry run (status %q)", pl.ID, pl.Status)
	}
	dyn, err := pl.dynamicClient(kubeconfig, targetContext)
	if err != nil {
		return nil, err
	}
	result := &ApplyResult{PlanID: pl.ID}
	items, skipped := pl.applyItems()
	result.Skipped = skipped

	ensured := map[string]bool{}
	for _, it := range items {
		if it.namespace != "" && it.gvr.Resource != "namespaces" && !ensured[it.namespace] {
			ensured[it.namespace] = true
			if err := ensureNamespace(ctx, dyn, it.namespace); err != nil {
				result.Errors = append(result.Errors, DryRunItem{
					Namespace: it.namespace,
					Name:      it.name,
					Kind:      it.kind,
					Message:   "ensure namespace: " + err.Error(),
				})
				continue
			}
		}
		if err := applyOne(ctx, dyn, it, pl.FieldManager, false); err != nil {
			result.Errors = append(result.Errors, DryRunItem{
				Namespace: it.namespace,
				Name:      it.name,
				Kind:      it.kind,
				Message:   err.Error(),
			})
			continue
		}
		result.Applied++
	}
	pl.Status = "applied"
	return result, nil
}

func (pl *Plan) dynamicClient(kubeconfig, targetContext string) (dynamic.Interface, error) {
	cfg, err := restConfig(kubeconfig, targetContext)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("replay: new dynamic client: %w", err)
	}
	pl.TargetContext = targetContext
	return dyn, nil
}

// restConfig builds a rest.Config from a kubeconfig path and context. When the
// path is empty it resolves through the standard chain: in-cluster, then the
// KUBECONFIG env var, then ~/.kube/config.
func restConfig(kubeconfig, contextName string) (*rest.Config, error) {
	if kubeconfig == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
		cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(),
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("replay: no cluster connection (no in-cluster service account and no kubeconfig): %w", err)
		}
		return cfg, nil
	}
	raw, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("replay: load kubeconfig %q: %w", kubeconfig, err)
	}
	cc := clientcmd.NewNonInteractiveClientConfig(*raw, contextName, &clientcmd.ConfigOverrides{}, nil)
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("replay: resolve config for context %q: %w", contextName, err)
	}
	return cfg, nil
}

// applyItem pairs a sanitized object with its target GVR and coordinates.
type applyItem struct {
	gvr       schema.GroupVersionResource
	name      string
	namespace string
	kind      string
	object    map[string]any
}

// applyItems converts plan objects into apply items, skipping objects whose
// apiVersion or kind has no supported GVR mapping. Skipped objects are
// returned explicitly so callers can report them instead of silently dropping
// objects while claiming success.
func (pl *Plan) applyItems() ([]applyItem, []DryRunItem) {
	items := make([]applyItem, 0, len(pl.Objects))
	var skipped []DryRunItem
	for _, po := range pl.Objects {
		gvr, err := gvrFor(po.Object, po.Kind)
		if err != nil {
			pl.Warnings = append(pl.Warnings, fmt.Sprintf("replay: skipping %s/%s: %v", po.Kind, po.Name, err))
			skipped = append(skipped, DryRunItem{
				Namespace: po.Namespace,
				Name:      po.Name,
				Kind:      po.Kind,
				Message:   err.Error(),
			})
			continue
		}
		items = append(items, applyItem{
			gvr:       gvr,
			name:      po.Name,
			namespace: po.Namespace,
			kind:      po.Kind,
			object:    po.Object,
		})
	}
	return items, skipped
}

// kindToResource maps object kinds to their plural resource names for the
// dynamic client. Group and version come from the object's apiVersion.
var kindToResource = map[string]string{
	"Deployment":                    "deployments",
	"StatefulSet":                   "statefulsets",
	"DaemonSet":                     "daemonsets",
	"ReplicaSet":                    "replicasets",
	"Service":                       "services",
	"ConfigMap":                     "configmaps",
	"Namespace":                     "namespaces",
	"RuntimeClass":                  "runtimeclasses",
	"Secret":                        "secrets",
	"ServiceAccount":                "serviceaccounts",
	"Role":                          "roles",
	"ClusterRole":                   "clusterroles",
	"RoleBinding":                   "rolebindings",
	"ClusterRoleBinding":            "clusterrolebindings",
	"Job":                           "jobs",
	"CronJob":                       "cronjobs",
	"Pod":                           "pods",
	"PersistentVolume":              "persistentvolumes",
	"PersistentVolumeClaim":         "persistentvolumeclaims",
	"StorageClass":                  "storageclasses",
	"VolumeAttachment":              "volumeattachments",
	"MutatingWebhookConfiguration":  "mutatingwebhookconfigurations",
	"ValidatingWebhookConfiguration": "validatingwebhookconfigurations",
	"CustomResourceDefinition":      "customresourcedefinitions",
	"Ingress":                       "ingresses",
	"NetworkPolicy":                 "networkpolicies",
	"LimitRange":                    "limitranges",
	"ResourceQuota":                 "resourcequotas",
	"HorizontalPodAutoscaler":       "horizontalpodautoscalers",
	"PodDisruptionBudget":           "poddisruptionbudgets",
	"Endpoints":                     "endpoints",
	"PriorityClass":                 "priorityclasses",
	"Lease":                         "leases",
}

func gvrFor(obj map[string]any, kind string) (schema.GroupVersionResource, error) {
	apiVersion, _ := obj["apiVersion"].(string)
	if apiVersion == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("replay: %s has no apiVersion", kind)
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	resource, ok := kindToResource[kind]
	if !ok {
		return schema.GroupVersionResource{}, fmt.Errorf("replay: unsupported kind %q", kind)
	}
	return schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: resource}, nil
}

func applyOne(ctx context.Context, dyn dynamic.Interface, it applyItem, manager string, dryRun bool) error {
	opts := metav1.ApplyOptions{FieldManager: manager, Force: false}
	if dryRun {
		opts.DryRun = []string{metav1.DryRunAll}
	}
	obj := &unstructured.Unstructured{Object: it.object}
	resource := dyn.Resource(it.gvr)
	if it.namespace != "" {
		_, err := resource.Namespace(it.namespace).Apply(ctx, it.name, obj, opts)
		return err
	}
	_, err := resource.Apply(ctx, it.name, obj, opts)
	return err
}

// ensureNamespace creates the target namespace when it does not exist.
func ensureNamespace(ctx context.Context, dyn dynamic.Interface, ns string) error {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	_, err := dyn.Resource(gvr).Get(ctx, ns, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": ns},
	}}
	_, err = dyn.Resource(gvr).Create(ctx, obj, metav1.CreateOptions{})
	return err
}

// conflictManager extracts the competing field manager name from an SSA
// conflict status, falling back to an empty string when it is not available.
func conflictManager(err error) string {
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) && statusErr.ErrStatus.Details != nil {
		for _, c := range statusErr.ErrStatus.Details.Causes {
			if c.Field == "manager" || strings.Contains(c.Message, "conflict") {
				return c.Message
			}
		}
	}
	return ""
}
