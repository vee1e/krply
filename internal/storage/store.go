// Package storage defines the durable journal and event store interfaces.
//
// The contract is deliberately small. Writers append immutable records and
// advance checkpoints; readers reconstruct state, coverage, and timelines.
package storage

import (
	"context"
	"time"

	"github.com/krply/krply/internal/event"
)

// StreamMeta is the observable health of one stream.
type StreamMeta struct {
	StreamID            string
	ClusterID           string
	Group               string
	Version             string
	Resource            string
	Kind                string
	Namespace           string
	Selector            string
	Available           bool
	FirstObservedAt     time.Time
	LastObservedAt      time.Time
	LastResourceVersion string
	GapCount            int64
	HasGaps             bool
	Degraded            bool
}

// EventFilter selects events from the store.
type EventFilter struct {
	ClusterID  string
	StreamID   string
	Namespace  string
	Name       string
	Kind       string
	RecordType event.RecordType
	Since      time.Time
	Until      time.Time
	SinceSeq   int64 // return only records with ingest_seq > SinceSeq (cursor)
	Limit      int
	Offset     int64
}

// ObjectRef selects one object's history.
type ObjectRef struct {
	ClusterID string
	StreamID  string
	Namespace string
	Name      string
}

// SnapshotRef identifies a materialized snapshot.
type SnapshotRef struct {
	ID        string
	ClusterID string
	Name      string
	At        time.Time
}

// Store is the durable journal.
//
// Implementations must guarantee: append is atomic with its checkpoint
// advance; appends never reorder within a stream; reads return records in
// ingest order.
type Store interface {
	// Append writes one record and its checkpoint in the same transaction.
	// It returns the assigned ingest sequence.
	Append(ctx context.Context, rec *event.Record) (int64, error)

	// Appends writes records in order, atomically advancing checkpoints.
	Appends(ctx context.Context, recs []*event.Record) ([]int64, error)

	// ListClusters returns distinct cluster IDs.
	ListClusters(ctx context.Context) ([]string, error)

	// Streams returns metadata for every stream.
	Streams(ctx context.Context) ([]StreamMeta, error)

	// StreamMeta returns metadata for one stream.
	StreamMeta(ctx context.Context, streamID string) (StreamMeta, error)

	// Events returns records matching the filter in ingest order.
	Events(ctx context.Context, f EventFilter) ([]event.Record, error)

	// ObjectHistory returns the event history for one object.
	ObjectHistory(ctx context.Context, ref ObjectRef) ([]event.Record, error)

	// ObjectAt returns the last event for an object at or before ts.
	ObjectAt(ctx context.Context, ref ObjectRef, ts time.Time) (*event.Record, error)

	// StreamEvents returns all event records for a stream up to ts.
	StreamEvents(ctx context.Context, streamID string, until time.Time) ([]event.Record, error)

	// Baselines returns the baseline records for a stream.
	Baselines(ctx context.Context, streamID string) ([]event.Record, error)

	// Gaps returns gap records for a stream.
	Gaps(ctx context.Context, streamID string) ([]event.Record, error)

	// Snapshot stores a materialized snapshot.
	SaveSnapshot(ctx context.Context, snap *SnapshotRef) error

	// Snapshots lists stored snapshots.
	Snapshots(ctx context.Context) ([]SnapshotRef, error)

	// Close releases the underlying resources.
	Close() error
}
