package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/krply/krply/internal/materialize"
	"github.com/krply/krply/internal/storage"
)

// PlanObject is one sanitized, dependency-ordered object in a replay plan.
type PlanObject struct {
	Namespace string
	Name      string
	Kind      string
	Order     int
	Object    map[string]any
	Warnings  []string
}

// Excluded records an object that was intentionally left out of a plan.
type Excluded struct {
	Namespace string
	Name      string
	Kind      string
	Reason    string
}

// Plan is a sanitized, reviewable replay of a snapshot into a target cluster.
type Plan struct {
	ID               string
	ClusterID        string
	SnapshotID       string
	SourceNamespace  string
	TargetNamespace  string
	TargetContext    string
	FieldManager     string
	Objects          []PlanObject
	Warnings         []string
	Excluded         []Excluded
	CoverageComplete bool
	Status           string
}

// Planner builds replay plans from a store and a materializer.
type Planner struct {
	store  storage.Store
	mat    *materialize.Materializer
	policy Policy
}

// NewPlanner returns a Planner backed by store and mat using policy.
func NewPlanner(store storage.Store, mat *materialize.Materializer, policy Policy) *Planner {
	return &Planner{store: store, mat: mat, policy: policy}
}

// Plan reconstructs the objects of the given snapshot, sanitizes them, and
// returns an ordered plan. It refuses to plan when stream coverage was
// incomplete unless the policy allows gaps.
func (p *Planner) Plan(ctx context.Context, clusterID, snapshotID, sourceNS, targetNS string) (*Plan, error) {
	snap, err := p.lookupSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, err
	}

	states, complete, err := p.mat.StateAt(ctx, snap.ClusterID, snap.At)
	if err != nil {
		return nil, err
	}

	if !complete && !p.policy.AllowGaps {
		return nil, errors.New("refusing plan: incomplete coverage (stream gaps) — pass allow-gaps to override")
	}

	id := "plan-" + uuid.NewString()[:8]
	plan := &Plan{
		ID:               id,
		ClusterID:        snap.ClusterID,
		SnapshotID:       snapshotID,
		SourceNamespace:  sourceNS,
		TargetNamespace:  targetNS,
		FieldManager:     "krply-plan-" + id,
		Status:           "planned",
		CoverageComplete: complete,
	}

	mapped := false
	for _, st := range states {
		var obj map[string]any
		if err := json.Unmarshal(st.Object, &obj); err != nil {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("replay: cannot decode %s/%s: %v", st.Kind, st.Name, err))
			continue
		}

		kind := kindOf(obj, st.Kind)
		name := nameOf(obj, st.Name)
		namespace := namespaceOf(obj, st.Namespace)

		if sourceNS != "" && namespace != "" && namespace != sourceNS {
			plan.Excluded = append(plan.Excluded, Excluded{Namespace: namespace, Name: name, Kind: kind, Reason: "outside source namespace"})
			continue
		}

		if reason, excluded := excludeReason(obj, kind, name, p.policy); excluded {
			plan.Excluded = append(plan.Excluded, Excluded{Namespace: namespace, Name: name, Kind: kind, Reason: reason})
			if kind == "Service" {
				plan.Warnings = append(plan.Warnings, fmt.Sprintf("excluded service %s/%s: %s", namespace, name, reason))
			}
			continue
		}

		clean, warnings := sanitizeObject(obj, kind, p.policy)
		effectiveNS := mapNamespace(clean, namespace, sourceNS, targetNS, p.policy, &mapped, &plan.Warnings)

		plan.Objects = append(plan.Objects, PlanObject{
			Namespace: effectiveNS,
			Name:      name,
			Kind:      kind,
			Order:     orderFor(kind),
			Object:    clean,
			Warnings:  warnings,
		})
	}

	sort.Slice(plan.Objects, func(i, j int) bool {
		a, b := plan.Objects[i], plan.Objects[j]
		if a.Order != b.Order {
			return a.Order < b.Order
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})

	return plan, nil
}

func (p *Planner) lookupSnapshot(ctx context.Context, snapshotID string) (*storage.SnapshotRef, error) {
	snaps, err := p.store.Snapshots(ctx)
	if err != nil {
		return nil, err
	}
	for i := range snaps {
		if snaps[i].ID == snapshotID {
			return &snaps[i], nil
		}
	}
	return nil, errors.New("snapshot not found")
}

// mapNamespace applies namespace remapping and returns the effective namespace
// the object will be applied to. When no remap is requested, the namespace is
// dropped from the payload (the caller sets it at apply time).
func mapNamespace(obj map[string]any, namespace, sourceNS, targetNS string, pol Policy, mapped *bool, warnings *[]string) string {
	if namespace == "" {
		return ""
	}
	m := meta(obj)

	switch {
	case pol.MapNamespaces && sourceNS != "" && targetNS != "":
		m["namespace"] = targetNS
		remapSpecNamespace(obj, targetNS)
		if !*mapped {
			*warnings = append(*warnings, fmt.Sprintf("namespace mapping: %s -> %s", sourceNS, targetNS))
			*mapped = true
		}
		return targetNS
	case sourceNS == "" && targetNS != "":
		m["namespace"] = targetNS
		if !*mapped {
			*warnings = append(*warnings, fmt.Sprintf("namespace mapping: * -> %s", targetNS))
			*mapped = true
		}
		return targetNS
	default:
		delete(m, "namespace")
		return namespace
	}
}

func remapSpecNamespace(obj map[string]any, targetNS string) {
	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		return
	}
	if _, ok := spec["namespace"]; ok {
		spec["namespace"] = targetNS
	}
}

// orderFor returns the dependency order: namespaces first so target namespaces
// exist before any namespaced object is applied, then everything else.
func orderFor(kind string) int {
	if kind == "Namespace" {
		return 0
	}
	return 1
}

func kindOf(obj map[string]any, fallback string) string {
	if k, ok := obj["kind"].(string); ok && k != "" {
		return k
	}
	return fallback
}

func nameOf(obj map[string]any, fallback string) string {
	if n, ok := meta(obj)["name"].(string); ok && n != "" {
		return n
	}
	return fallback
}

func namespaceOf(obj map[string]any, fallback string) string {
	if n, ok := meta(obj)["namespace"].(string); ok && n != "" {
		return n
	}
	return fallback
}

func meta(obj map[string]any) map[string]any {
	m, _ := obj["metadata"].(map[string]any)
	if m == nil {
		m = map[string]any{}
		obj["metadata"] = m
	}
	return m
}
