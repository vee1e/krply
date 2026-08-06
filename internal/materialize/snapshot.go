package materialize

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
)

// Snapshot is a materialized view of a cluster at a point in time.
type Snapshot struct {
	ID          string
	ClusterID   string
	Name        string
	At          time.Time
	Objects     []ObjectState
	Streams     []string
	Watermarks  []StreamWatermark
	Complete    bool
	Missing     []string
	Warning     string
}

// StreamWatermark is the durable boundary of one stream at snapshot time.
type StreamWatermark struct {
	StreamID            string
	LastObservedAt      time.Time
	LastResourceVersion string
}

// Snapshot materializes every watched object of the cluster at at, records
// per-stream coverage and watermarks, persists a TypeSnapshot journal entry,
// and stores the snapshot metadata.
func (m *Materializer) Snapshot(ctx context.Context, clusterID string, at time.Time, name string) (*Snapshot, error) {
	streams, err := m.store.Streams(ctx)
	if err != nil {
		return nil, err
	}

	snap := &Snapshot{
		ID:        "snap-" + uuid.NewString()[:8] + "-" + name,
		ClusterID: clusterID,
		Name:      name,
		At:        at,
		Complete:  true,
	}

	var missing []string
	for _, s := range streams {
		if s.ClusterID != clusterID {
			continue
		}
		ss, err := m.StreamState(ctx, s.StreamID, at)
		if err != nil {
			return nil, err
		}
		snap.Objects = append(snap.Objects, ss.Objects...)
		snap.Streams = append(snap.Streams, s.StreamID)
		snap.Watermarks = append(snap.Watermarks, StreamWatermark{
			StreamID:            s.StreamID,
			LastObservedAt:      ss.LastObservedAt,
			LastResourceVersion: ss.LastResourceVersion,
		})

		if !ss.HasBaseline || ss.HasGaps {
			missing = append(missing, s.StreamID)
		}
	}

	sort.Strings(snap.Streams)
	sortObjects(snap.Objects)
	sort.Slice(snap.Watermarks, func(i, j int) bool { return snap.Watermarks[i].StreamID < snap.Watermarks[j].StreamID })
	sort.Strings(missing)

	if len(missing) > 0 {
		snap.Complete = false
		snap.Missing = missing
		snap.Warning = "coverage incomplete for stream(s): " + strings.Join(missing, ", ")
	}

	if err := m.store.SaveSnapshot(ctx, &storage.SnapshotRef{
		ID:        snap.ID,
		ClusterID: clusterID,
		Name:      name,
		At:        at,
	}); err != nil {
		return nil, err
	}

	// Persist a TypeSnapshot journal record so the snapshot is reconstructable
	// and provable from the event stream, not only from the metadata table.
	if _, err := m.store.Append(ctx, &event.Record{
		ClusterID:  clusterID,
		ObservedAt: at,
		Type:       event.TypeSnapshot,
		Snapshot:   &event.SnapshotInfo{Name: name},
	}); err != nil {
		return nil, err
	}

	return snap, nil
}
