// Package replay reconstructs sanitized Kubernetes state from a snapshot and
// plans safe, reviewable replays into a target cluster. It never reuses the
// original field manager, refuses plans with incomplete coverage unless
// explicitly allowed, and requires explicit confirmation before applying.
package replay

// Policy controls which object kinds a replay plan includes and how
// aggressively server-generated fields are stripped. The zero value is the
// conservative default: everything sensitive is excluded and every
// server-owned field is removed.
type Policy struct {
	IncludeSecrets  bool
	IncludeRoles    bool
	IncludePods     bool
	IncludeJobs     bool
	IncludePV       bool
	IncludeWebhooks bool
	IncludeCRDs     bool
	AllowFinalizers []string
	MapNamespaces   bool
	AllowGaps       bool
}

// DefaultPolicy returns the sanitization defaults from design section 13:
// secrets, RBAC, pods, jobs, storage, webhooks, and CRDs are excluded;
// finalizers are removed unless allowlisted; namespaces are mapped; plans
// with incomplete coverage are refused.
func DefaultPolicy() Policy {
	return Policy{
		MapNamespaces: true,
	}
}
