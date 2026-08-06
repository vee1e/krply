package replay

import "strings"

// lastAppliedAnnotation is dropped from replayed objects. It is server-side
// kubectl bookkeeping, not desired state.
const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// sanitizeObject strips server-generated and sensitive fields from a single
// object and returns the sanitized object plus any warnings. The input map
// is mutated in place.
func sanitizeObject(obj map[string]any, kind string, pol Policy) (map[string]any, []string) {
	if obj == nil {
		obj = map[string]any{}
	}
	var warnings []string

	stripMetadata(obj)
	warnings = append(warnings, sanitizeFinalizers(obj, pol)...)

	// Status is controller-owned server state and is never declarative.
	delete(obj, "status")

	switch kind {
	case "Service":
		warnings = append(warnings, sanitizeService(obj)...)
	case "Deployment", "StatefulSet", "DaemonSet":
		stripWorkloadTemplate(obj)
	}

	switch kind {
	case "StatefulSet":
		warnings = append(warnings, "statefulset replay requires storage awareness")
	case "Secret":
		warnings = append(warnings, "secret data included raw; redaction is the caller's responsibility")
	case "RuntimeClass":
		warnings = append(warnings, "runtimeclass is plan-only")
	case "ConfigMap":
		warnings = append(warnings, "configmap may contain sensitive data")
	}

	return obj, warnings
}

// stripMetadata removes server-owned metadata fields, drops the last-applied
// annotation while keeping user annotations, and removes owner references
// (design section 11: remove generated owner references by default).
func stripMetadata(obj map[string]any) {
	m, _ := obj["metadata"].(map[string]any)
	if m == nil {
		return
	}
	for _, f := range []string{
		"uid",
		"resourceVersion",
		"creationTimestamp",
		"generation",
		"managedFields",
		"deletionTimestamp",
		"deletionGracePeriodSeconds",
		"selfLink",
		"clusterName",
		"ownerReferences",
	} {
		delete(m, f)
	}
	if anns, ok := m["annotations"].(map[string]any); ok {
		delete(anns, lastAppliedAnnotation)
		if len(anns) == 0 {
			delete(m, "annotations")
		}
	}
}

// sanitizeFinalizers removes every finalizer not present in
// pol.AllowFinalizers, reporting each removal as a warning.
func sanitizeFinalizers(obj map[string]any, pol Policy) []string {
	var warnings []string
	m, _ := obj["metadata"].(map[string]any)
	if m == nil {
		return nil
	}
	raw, ok := m["finalizers"].([]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	keep := make([]any, 0, len(raw))
	for _, f := range raw {
		s, _ := f.(string)
		if s != "" && containsString(pol.AllowFinalizers, s) {
			keep = append(keep, s)
			continue
		}
		warnings = append(warnings, "removed finalizer "+s)
	}
	if len(keep) == 0 {
		delete(m, "finalizers")
	} else {
		m["finalizers"] = keep
	}
	return warnings
}

// sanitizeService strips dynamic cluster IP fields from ClusterIP and NodePort
// services. Headless services (clusterIP: "None") keep their explicit value,
// which is user-desired state. LoadBalancer services are rejected by the
// caller via excludeReason.
func sanitizeService(obj map[string]any) []string {
	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		return nil
	}
	switch serviceType(obj) {
	case "", "ClusterIP":
		if spec["clusterIP"] == "None" {
			return nil
		}
		delete(spec, "clusterIP")
		delete(spec, "clusterIPs")
		return []string{"service cluster IP removed"}
	case "NodePort":
		// NodePort services also receive a server-assigned cluster IP, and a
		// fixed nodePort from the source cluster would collide in the target.
		delete(spec, "clusterIP")
		delete(spec, "clusterIPs")
		if ports, ok := spec["ports"].([]any); ok {
			for _, p := range ports {
				if pm, ok := p.(map[string]any); ok {
					delete(pm, "nodePort")
				}
			}
		}
		return []string{"service cluster IP and nodePort removed for reassignment"}
	}
	return nil
}

func serviceType(obj map[string]any) string {
	spec, _ := obj["spec"].(map[string]any)
	t, _ := spec["type"].(string)
	return t
}

// stripWorkloadTemplate removes server-owned fields from the nested pod
// template metadata of workload objects.
func stripWorkloadTemplate(obj map[string]any) {
	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		return
	}
	tmpl, _ := spec["template"].(map[string]any)
	if tmpl == nil {
		return
	}
	tmeta, _ := tmpl["metadata"].(map[string]any)
	if tmeta == nil {
		return
	}
	for _, f := range []string{"creationTimestamp", "uid", "resourceVersion", "generation", "managedFields"} {
		delete(tmeta, f)
	}
}

// excludeReason returns the reason an object must be excluded from the plan,
// if any. Kind defaults follow design section 13 and are overridden by the
// policy's Include* toggles. Token secrets and LoadBalancer services are
// always excluded regardless of policy.
func excludeReason(obj map[string]any, kind, name string, pol Policy) (string, bool) {
	switch kind {
	case "Secret":
		if strings.Contains(name, "-token-") {
			return "token secret excluded by default", true
		}
		if !pol.IncludeSecrets {
			return "secret excluded by default", true
		}
	case "ServiceAccount":
		return "serviceaccount excluded by default", true
	case "Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding":
		if !pol.IncludeRoles {
			return "rbac object excluded by default", true
		}
	case "Job", "CronJob":
		if !pol.IncludeJobs {
			return "job object excluded by default", true
		}
	case "Pod":
		if !pol.IncludePods {
			return "pod excluded by default", true
		}
	case "PersistentVolume", "PersistentVolumeClaim", "StorageClass":
		if !pol.IncludePV {
			return "storage object excluded by default", true
		}
	case "VolumeAttachment":
		return "storage object excluded by default", true
	case "MutatingWebhookConfiguration", "ValidatingWebhookConfiguration":
		if !pol.IncludeWebhooks {
			return "webhook excluded by default", true
		}
	case "CustomResourceDefinition":
		if !pol.IncludeCRDs {
			return "crd excluded by default", true
		}
	case "Service":
		if serviceType(obj) == "LoadBalancer" {
			return "load balancer service not safe to replay", true
		}
	}
	return "", false
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
