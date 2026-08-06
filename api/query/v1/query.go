// Package queryv1 defines the public query API response types.
//
// These types are the stable wire contract served by krply-server and
// consumed by the CLI and web UI. Internal representations stay in
// internal/event until the schema stabilizes.
package queryv1

import "time"

// Cluster identifies a source Kubernetes cluster.
type Cluster struct {
	ID         string    `json:"id"`
	Context    string    `json:"context"`
	Generation string    `json:"generation"`
	Agent      string    `json:"agent,omitempty"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
}

// Stream describes one watched collection and its coverage.
type Stream struct {
	ID                  string    `json:"id"`
	ClusterID           string    `json:"cluster_id"`
	Group               string    `json:"group"`
	Version             string    `json:"version"`
	Resource            string    `json:"resource"`
	Kind                string    `json:"kind"`
	Namespace           string    `json:"namespace"`
	Selector            string    `json:"selector"`
	Available           bool      `json:"available"`
	FirstObservedAt     time.Time `json:"first_observed_at"`
	LastObservedAt      time.Time `json:"last_observed_at"`
	LastResourceVersion string    `json:"last_resource_version"`
	GapCount            int64     `json:"gap_count"`
	HasGaps             bool      `json:"has_gaps"`
	Degraded            bool      `json:"degraded"`
}

// Event is a normalized journal event for API consumption.
type Event struct {
	ClusterID       string    `json:"cluster_id"`
	StreamID        string    `json:"stream_id"`
	RecordType      string    `json:"record_type"`
	EventID         string    `json:"event_id"`
	IngestSeq       int64     `json:"ingest_seq"`
	ObservedAt      time.Time `json:"observed_at"`
	WatchType       string    `json:"watch_type,omitempty"`
	Synthetic       bool      `json:"synthetic,omitempty"`
	Namespace       string    `json:"namespace,omitempty"`
	Name            string    `json:"name,omitempty"`
	UID             string    `json:"uid,omitempty"`
	Kind            string    `json:"kind,omitempty"`
	ResourceVersion string    `json:"resource_version,omitempty"`
	ObjectHash      string    `json:"object_hash,omitempty"`
	Object          any       `json:"object,omitempty"`
	Provenance      any       `json:"provenance,omitempty"`
}

// EventQuery is the request for GET /v1/events.
type EventQuery struct {
	ClusterID string    `json:"cluster_id,omitempty"`
	StreamID  string    `json:"stream_id,omitempty"`
	Namespace string    `json:"namespace,omitempty"`
	Name      string    `json:"name,omitempty"`
	Kind      string    `json:"kind,omitempty"`
	Since     time.Time `json:"since,omitempty"`
	Until     time.Time `json:"until,omitempty"`
	Limit     int       `json:"limit,omitempty"`
	Cursor    string    `json:"cursor,omitempty"`
}

// EventPage is a cursor-paginated event result.
type EventPage struct {
	Items      []Event `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
	HasMore    bool    `json:"has_more"`
}

// Gap is a continuity loss on a stream.
type Gap struct {
	StreamID            string    `json:"stream_id"`
	FromResourceVersion string    `json:"from_resource_version"`
	ToResourceVersion   string    `json:"to_resource_version"`
	Reason              string    `json:"reason"`
	DetectedAt          time.Time `json:"detected_at"`
}

// TimelineItem is one entry in an object history timeline.
type TimelineItem struct {
	ObservedAt      time.Time `json:"observed_at"`
	ResourceVersion string    `json:"resource_version"`
	WatchType       string    `json:"watch_type"`
	Synthetic       bool      `json:"synthetic,omitempty"`
	Summary         string    `json:"summary"`
	ObjectHash      string    `json:"object_hash,omitempty"`
	ChangedFields   []string  `json:"changed_fields,omitempty"`
	Provenance      any       `json:"provenance,omitempty"`
}

// ObjectHistory is one object's timeline with coverage metadata.
type ObjectHistory struct {
	ClusterID string         `json:"cluster_id"`
	StreamID  string         `json:"stream_id"`
	Namespace string         `json:"namespace"`
	Name      string         `json:"name"`
	Kind      string         `json:"kind"`
	Items     []TimelineItem `json:"items"`
	HasGaps   bool           `json:"has_gaps"`
	Gaps      []Gap          `json:"gaps,omitempty"`
	Warning   string         `json:"warning,omitempty"`
}

// FieldChange is one changed field in a diff.
type FieldChange struct {
	Path    string `json:"path"`
	Before  any    `json:"before"`
	After   any    `json:"after"`
	Added   bool   `json:"added,omitempty"`
	Removed bool   `json:"removed,omitempty"`
}

// DiffResult compares two time boundaries for a scope.
type DiffResult struct {
	ClusterID string       `json:"cluster_id"`
	Scope     string       `json:"scope"`
	BeforeAt  time.Time    `json:"before_at"`
	AfterAt   time.Time    `json:"after_at"`
	Changed   []ObjectDiff `json:"changed"`
	HasGaps   bool         `json:"has_gaps"`
	Warning   string       `json:"warning,omitempty"`
}

// ObjectDiff is the set of field changes for one object.
type ObjectDiff struct {
	Namespace string        `json:"namespace"`
	Name      string        `json:"name"`
	Kind      string        `json:"kind"`
	Changes   []FieldChange `json:"changes"`
}

// Snapshot describes a materialized state snapshot.
type Snapshot struct {
	ID          string    `json:"id"`
	ClusterID   string    `json:"cluster_id"`
	Name        string    `json:"name"`
	At          time.Time `json:"at"`
	ObjectCount int       `json:"object_count"`
	Streams     int       `json:"streams"`
	Complete    bool      `json:"complete"`
	Missing     []string  `json:"missing,omitempty"`
	Warning     string    `json:"warning,omitempty"`
}

// ReplayPlan is a sanitized, reviewed apply plan.
type ReplayPlan struct {
	ID               string           `json:"id"`
	ClusterID        string           `json:"cluster_id"`
	SnapshotID       string           `json:"snapshot_id"`
	SourceNamespace  string           `json:"source_namespace"`
	TargetNamespace  string           `json:"target_namespace"`
	TargetContext    string           `json:"target_context"`
	CreatedAt        time.Time        `json:"created_at"`
	FieldManager     string           `json:"field_manager"`
	Objects          []PlanObject     `json:"objects"`
	Warnings         []string         `json:"warnings"`
	Excluded         []ExcludedObject `json:"excluded"`
	CoverageComplete bool             `json:"coverage_complete"`
	DryRunResult     *DryRunResult    `json:"dry_run_result,omitempty"`
	Status           string           `json:"status"`
}

// PlanObject is one declarative object in a replay plan.
type PlanObject struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Kind      string   `json:"kind"`
	Order     int      `json:"order"`
	Object    any      `json:"object"`
	Warnings  []string `json:"warnings,omitempty"`
}

// ExcludedObject explains why an object was excluded from a plan.
type ExcludedObject struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Reason    string `json:"reason"`
}

// DryRunResult is the outcome of a server-side dry run.
type DryRunResult struct {
	Applied   int          `json:"applied"`
	Conflicts []DryRunItem `json:"conflicts"`
	Errors    []DryRunItem `json:"errors"`
	Skipped   []DryRunItem `json:"skipped,omitempty"`
	OK        bool         `json:"ok"`
}

// DryRunItem is one failed or conflicting apply result.
type DryRunItem struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Manager   string `json:"manager,omitempty"`
	Message   string `json:"message"`
}

// ReplayRun is one approved apply execution.
type ReplayRun struct {
	ID         string       `json:"id"`
	PlanID     string       `json:"plan_id"`
	Status     string       `json:"status"`
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
	Applied    int          `json:"applied"`
	Errors     []DryRunItem `json:"errors,omitempty"`
	Skipped    []DryRunItem `json:"skipped,omitempty"`
}

// Health is the server health response.
type Health struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	Storage string `json:"storage"`
}
