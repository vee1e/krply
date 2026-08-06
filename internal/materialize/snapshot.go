package materialize

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/krply/krply/internal/storage"
)

// Snapshot is a materialized view of a cluster at a point in time.
type Snapshot struct {
	ID        string
	ClusterID string
	Name      string
	At        time.Time
	Objects   []ObjectState
	Streams   []string
	Complete  bool
	Missing   []string
	Warning   string
}

// Snapshot materializes every watched object of the cluster at at, records
// per-stream coverage, and persists the snapshot metadata. The materialized
// objects are reconstructable on demand; the snapshot reference is durable.
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

		if !ss.HasBaseline || ss.HasGaps {
			missing = append(missing, s.StreamID)
		}
	}

	sort.Strings(snap.Streams)
	sortObjects(snap.Objects)
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

	return snap, nil
}
