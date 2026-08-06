// Package event defines the durable record types stored in the krply journal.
//
// Every record is an immutable, flat entry. The raw watch payload is kept
// separate from normalized fields so unknown CRD fields survive intact.
package event

import (
	"encoding/json"
	"time"
)

// RecordType discriminates the kinds of entries in the journal.
type RecordType string

const (
	// TypeEvent is a watch event (ADDED, MODIFIED, DELETED, BOOKMARK, ERROR).
	TypeEvent RecordType = "EVENT"
	// TypeBaseline is a full list result used as a relist baseline.
	TypeBaseline RecordType = "BASELINE"
	// TypeGap records that watch continuity was lost.
	TypeGap RecordType = "GAP"
	// TypeCoverageChange records a resource becoming available or newly discovered.
	TypeCoverageChange RecordType = "COVERAGE_CHANGE"
	// TypeAuditCorrelation records optional request provenance.
	TypeAuditCorrelation RecordType = "AUDIT_CORRELATION"
	// TypeSnapshot records a materialized set of stream boundaries.
	TypeSnapshot RecordType = "SNAPSHOT"
	// TypeCheckpoint records a progress-only bookmark advance.
	TypeCheckpoint RecordType = "CHECKPOINT"
)

// WatchType is the original Kubernetes watch event type.
type WatchType string

const (
	WatchAdded    WatchType = "ADDED"
	WatchModified WatchType = "MODIFIED"
	WatchDeleted  WatchType = "DELETED"
	WatchBookmark WatchType = "BOOKMARK"
	WatchError    WatchType = "ERROR"
)

// ResourceRef identifies an object. Resource versions and UIDs are only
// meaningful within one cluster and one API resource.
type ResourceRef struct {
	Group           string `json:"group"`
	Version         string `json:"version"`
	Kind            string `json:"kind"`
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resource_version"`
}

// ObjectKey returns the namespace/name scoped key, used for materialization.
func (r ResourceRef) ObjectKey() string {
	if r.Namespace == "" {
		return r.Name
	}
	return r.Namespace + "/" + r.Name
}

// GVK returns the group/version/kind triple.
func (r ResourceRef) GVK() (string, string, string) {
	return r.Group, r.Version, r.Kind
}

// Provenance records optional audit-log correlation. It is only present when
// an audit record can be matched to an event.
type Provenance struct {
	RequestID   string            `json:"request_id"`
	User        string            `json:"user"`
	UserAgent   string            `json:"user_agent"`
	SourceIPs   []string          `json:"source_ips,omitempty"`
	Verb        string            `json:"verb"`
	Stage       string            `json:"stage"`
	Response    int               `json:"response_code"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// GapInfo describes a continuity loss on a stream.
type GapInfo struct {
	FromResourceVersion string    `json:"from_resource_version"`
	ToResourceVersion   string    `json:"to_resource_version"`
	Reason              string    `json:"reason"`
	DetectedAt          time.Time `json:"detected_at"`
}

// CoverageInfo describes a change in stream availability.
type CoverageInfo struct {
	Previous *CoverageState `json:"previous,omitempty"`
	Current  CoverageState  `json:"current"`
	Reason   string         `json:"reason"`
}

// CoverageState is one side of a coverage transition.
type CoverageState struct {
	Available           bool   `json:"available"`
	Resource            string `json:"resource"`
	Namespace           string `json:"namespace"`
	LastResourceVersion string `json:"last_resource_version"`
}

// CheckpointInfo records a durable progress marker.
type CheckpointInfo struct {
	ResourceVersion string    `json:"resource_version"`
	BookmarkedAt    time.Time `json:"bookmarked_at"`
}

// SnapshotInfo records a materialized snapshot boundary.
type SnapshotInfo struct {
	Name string `json:"name"`
}

// Record is the immutable journal entry.
type Record struct {
	ClusterID  string          `json:"cluster_id"`
	StreamID   string          `json:"stream_id"`
	Type       RecordType      `json:"record_type"`
	EventID    string          `json:"event_id"`
	IngestSeq  int64           `json:"ingest_seq"`
	ObservedAt time.Time       `json:"observed_at"`
	WatchType  WatchType       `json:"watch_type,omitempty"`
	Synthetic  bool            `json:"synthetic,omitempty"`
	Resource   ResourceRef     `json:"resource,omitempty"`
	ObjectHash string          `json:"object_hash,omitempty"`
	Object     json.RawMessage `json:"object,omitempty"`
	Provenance *Provenance     `json:"provenance,omitempty"`
	Gap        *GapInfo        `json:"gap,omitempty"`
	Coverage   *CoverageInfo   `json:"coverage,omitempty"`
	Checkpoint *CheckpointInfo `json:"checkpoint,omitempty"`
	Snapshot   *SnapshotInfo   `json:"snapshot,omitempty"`
}
