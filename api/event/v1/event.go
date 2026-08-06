// Package eventv1 defines the versioned wire format for journal records.
//
// It is the stable, public serialization of the krply journal for export,
// import, and cross-service transmission. Internal processing uses
// internal/event, which is designed to serialize to exactly this shape.
package eventv1

import (
	"encoding/json"
	"time"
)

// RecordType mirrors internal/event.RecordType.
type RecordType string

const (
	TypeEvent            RecordType = "EVENT"
	TypeBaseline         RecordType = "BASELINE"
	TypeGap              RecordType = "GAP"
	TypeCoverageChange   RecordType = "COVERAGE_CHANGE"
	TypeAuditCorrelation RecordType = "AUDIT_CORRELATION"
	TypeSnapshot         RecordType = "SNAPSHOT"
	TypeCheckpoint       RecordType = "CHECKPOINT"
)

// WatchType mirrors internal/event.WatchType.
type WatchType string

const (
	WatchAdded    WatchType = "ADDED"
	WatchModified WatchType = "MODIFIED"
	WatchDeleted  WatchType = "DELETED"
	WatchBookmark WatchType = "BOOKMARK"
	WatchError    WatchType = "ERROR"
)

// ResourceRef identifies one object in one cluster.
type ResourceRef struct {
	Group           string `json:"group"`
	Version         string `json:"version"`
	Kind            string `json:"kind"`
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resource_version"`
}

// Provenance is optional audit correlation.
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

// Record is the versioned wire record. IngestSeq is assigned by storage.
type Record struct {
	SchemaVersion int             `json:"schema_version"`
	ClusterID     string          `json:"cluster_id"`
	StreamID      string          `json:"stream_id"`
	Type          RecordType      `json:"record_type"`
	EventID       string          `json:"event_id"`
	ObservedAt    time.Time       `json:"observed_at"`
	WatchType     WatchType       `json:"watch_type,omitempty"`
	Synthetic     bool            `json:"synthetic,omitempty"`
	Resource      *ResourceRef    `json:"resource,omitempty"`
	ObjectHash    string          `json:"object_hash,omitempty"`
	Object        json.RawMessage `json:"object,omitempty"`
	Provenance    *Provenance     `json:"provenance,omitempty"`
	Gap           *GapInfo        `json:"gap,omitempty"`
	Coverage      *CoverageInfo   `json:"coverage,omitempty"`
	Checkpoint    *CheckpointInfo `json:"checkpoint,omitempty"`
	Snapshot      *SnapshotInfo   `json:"snapshot,omitempty"`
}

// GapInfo mirrors internal/event.GapInfo.
type GapInfo struct {
	FromResourceVersion string    `json:"from_resource_version"`
	ToResourceVersion   string    `json:"to_resource_version"`
	Reason              string    `json:"reason"`
	DetectedAt          time.Time `json:"detected_at"`
}

// CoverageInfo mirrors internal/event.CoverageInfo.
type CoverageInfo struct {
	Previous *CoverageState `json:"previous,omitempty"`
	Current  CoverageState  `json:"current"`
	Reason   string         `json:"reason"`
}

// CoverageState mirrors internal/event.CoverageState.
type CoverageState struct {
	Available           bool   `json:"available"`
	Resource            string `json:"resource"`
	Namespace           string `json:"namespace"`
	LastResourceVersion string `json:"last_resource_version"`
}

// CheckpointInfo mirrors internal/event.CheckpointInfo.
type CheckpointInfo struct {
	ResourceVersion string    `json:"resource_version"`
	BookmarkedAt    time.Time `json:"bookmarked_at"`
}

// SnapshotInfo mirrors internal/event.SnapshotInfo.
type SnapshotInfo struct {
	Name string `json:"name"`
}
