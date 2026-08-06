package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

// DiffResult is the semantic diff between two points in time.
type DiffResult struct {
	Before  time.Time
	After   time.Time
	Changes []ObjectDiff
	HasGaps bool
	Warning string
}

// ObjectDiff is the set of field changes for one object.
type ObjectDiff struct {
	Namespace string
	Name      string
	Kind      string
	Changes   []FieldChange
}

// FieldChange is one changed field, addressed by a dotted JSON path.
type FieldChange struct {
	Path    string
	Before  any
	After   any
	Added   bool
	Removed bool
}

// metadataIgnoreKeys are server-owned metadata fields, ignored only when they
// appear directly under metadata.
var metadataIgnoreKeys = map[string]bool{
	"uid":               true,
	"resourceVersion":   true,
	"creationTimestamp": true,
	"generation":        true,
	"managedFields":     true,
	"deletionTimestamp": true,
	"ownerReferences":   true,
}

// ignoreKey reports whether a map key at parentPath is server-owned noise.
// Ignoring is path-scoped so a user field named "status", "uid" or
// "resourceVersion" inside data or spec is never silently dropped.
func ignoreKey(parentPath, key string) bool {
	if key == "status" && parentPath == "" {
		return true
	}
	if parentPath == "metadata" && metadataIgnoreKeys[key] {
		return true
	}
	if key == "kubectl.kubernetes.io/last-applied-configuration" && parentPath == "metadata.annotations" {
		return true
	}
	return false
}

// Diff reconstructs cluster state before and after, intersects object keys,
// and reports semantic field changes, ignoring server-owned metadata.
func (m *Materializer) Diff(ctx context.Context, clusterID, namespace string, before, after time.Time) (*DiffResult, error) {
	if !before.IsZero() && !after.IsZero() && before.After(after) {
		return nil, fmt.Errorf("materialize: before (%s) must not be after after (%s)", before.Format(time.RFC3339), after.Format(time.RFC3339))
	}
	streams, err := m.store.Streams(ctx)
	if err != nil {
		return nil, err
	}

	res := &DiffResult{Before: before, After: after}
	beforeStates := map[string]ObjectState{}
	afterStates := map[string]ObjectState{}
	var gapped []string

	for _, s := range streams {
		if s.ClusterID != clusterID {
			continue
		}
		if !before.IsZero() {
			b, err := m.StreamState(ctx, s.StreamID, before)
			if err != nil {
				return nil, err
			}
			if b.HasGaps {
				gapped = append(gapped, s.StreamID)
			}
			for _, st := range b.Objects {
				if namespace != "" && st.Namespace != namespace {
					continue
				}
				beforeStates[st.StreamID+"|"+st.Namespace+"/"+st.Name] = st
			}
		}
		a, err := m.StreamState(ctx, s.StreamID, after)
		if err != nil {
			return nil, err
		}
		if a.HasGaps {
			gapped = append(gapped, s.StreamID)
		}
		for _, st := range a.Objects {
			if namespace != "" && st.Namespace != namespace {
				continue
			}
			afterStates[st.StreamID+"|"+st.Namespace+"/"+st.Name] = st
		}
	}

	keys := make([]string, 0, len(beforeStates)+len(afterStates))
	seen := map[string]bool{}
	for k := range beforeStates {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for k := range afterStates {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	for _, k := range keys {
		b, okB := beforeStates[k]
		a, okA := afterStates[k]
		od := ObjectDiff{}
		var beforeAny, afterAny any
		if okB {
			od.Namespace, od.Name, od.Kind = b.Namespace, b.Name, b.Kind
			beforeAny = parseObject(b.Object)
		}
		if okA {
			od.Namespace, od.Name, od.Kind = a.Namespace, a.Name, a.Kind
			afterAny = parseObject(a.Object)
		}
		switch {
		case okB && !okA:
			od.Changes = []FieldChange{{Path: "", Removed: true, Before: beforeAny}}
		case !okB && okA:
			od.Changes = []FieldChange{{Path: "", Added: true, After: afterAny}}
		default:
			diffValue("", beforeAny, afterAny, &od.Changes)
			sort.Slice(od.Changes, func(i, j int) bool { return od.Changes[i].Path < od.Changes[j].Path })
			if len(od.Changes) == 0 {
				continue
			}
		}
		res.Changes = append(res.Changes, od)
	}

	sort.Slice(res.Changes, func(i, j int) bool {
		if res.Changes[i].Namespace != res.Changes[j].Namespace {
			return res.Changes[i].Namespace < res.Changes[j].Namespace
		}
		if res.Changes[i].Name != res.Changes[j].Name {
			return res.Changes[i].Name < res.Changes[j].Name
		}
		return res.Changes[i].Kind < res.Changes[j].Kind
	})

	if len(gapped) > 0 {
		sort.Strings(gapped)
		res.HasGaps = true
		res.Warning = "coverage incomplete for stream(s): " + strings.Join(gapped, ", ")
	}

	return res, nil
}

// DiffObjects computes the field changes between two raw object payloads,
// ignoring server-owned metadata. It is used for timeline summaries.
func DiffObjects(before, after json.RawMessage) []FieldChange {
	var out []FieldChange
	if len(before) == 0 && len(after) == 0 {
		return out
	}
	if len(before) == 0 {
		return []FieldChange{{Path: "", Added: true, After: parseObject(after)}}
	}
	if len(after) == 0 {
		return []FieldChange{{Path: "", Removed: true, Before: parseObject(before)}}
	}
	diffValue("", parseObject(before), parseObject(after), &out)
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// parseObject decodes a raw object into an arbitrary JSON value.
func parseObject(raw json.RawMessage) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}

// diffValue recurses into before/after and appends field changes at out.
// Arrays are compared by index; a length change is reported as one change at
// the array's path. Map objects recurse by key; keys present on only one side
// are reported as a single Added/Removed change.
func diffValue(path string, before, after any, out *[]FieldChange) {
	if reflect.DeepEqual(before, after) {
		return
	}
	bm, bOK := before.(map[string]any)
	am, aOK := after.(map[string]any)
	if bOK && aOK {
		keys := map[string]bool{}
		for k := range bm {
			keys[k] = true
		}
		for k := range am {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			if ignoreKey(path, k) {
				continue
			}
			childPath := joinPath(path, k)
			_, beforeHas := bm[k]
			_, afterHas := am[k]
			switch {
			case beforeHas && !afterHas:
				*out = append(*out, FieldChange{Path: childPath, Before: bm[k], Removed: true})
			case !beforeHas && afterHas:
				*out = append(*out, FieldChange{Path: childPath, After: am[k], Added: true})
			default:
				diffValue(childPath, bm[k], am[k], out)
			}
		}
		return
	}
	bl, bArr := before.([]any)
	al, aArr := after.([]any)
	if bArr && aArr {
		if len(bl) != len(al) {
			*out = append(*out, FieldChange{Path: path, Before: before, After: after})
			return
		}
		for i := range bl {
			diffValue(fmt.Sprintf("%s[%d]", path, i), bl[i], al[i], out)
		}
		return
	}
	*out = append(*out, FieldChange{Path: path, Before: before, After: after})
}

// joinPath builds a dotted path. Segments that could collide with the path
// syntax ('.', '[', ']', '\') are backslash-escaped.
func joinPath(parent, key string) string {
	key = escapePathSegment(key)
	if parent == "" {
		return key
	}
	return parent + "." + key
}

func escapePathSegment(k string) string {
	k = strings.ReplaceAll(k, `\`, `\\`)
	k = strings.ReplaceAll(k, `.`, `\.`)
	k = strings.ReplaceAll(k, `[`, `\[`)
	k = strings.ReplaceAll(k, `]`, `\]`)
	return k
}
