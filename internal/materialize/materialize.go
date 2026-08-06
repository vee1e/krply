// Package materialize reduces the event journal into object state,
// reconstructs state at points in time, materializes snapshots, and computes
// semantic field diffs.
package materialize

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
)

// ErrNoObjectAtTime reports that no object existed at the requested time.
var ErrNoObjectAtTime = errors.New("materialize: no object state at time")

// ObjectState is one materialized object at a point in time.
type ObjectState struct {
	ClusterID string
	StreamID  string
	Namespace string
	Name      string
	Kind      string
	Object    json.RawMessage
	At        time.Time
}

// Materializer reduces stored events into object state.
type Materializer struct {
	store storage.Store
}

// NewMaterializer returns a Materializer backed by store.
func NewMaterializer(store storage.Store) *Materializer {
	return &Materializer{store: store}
}

// ObjectAt returns the last event record for the object at or before at.
// It returns ErrNoObjectAtTime if no object existed at that time.
func (m *Materializer) ObjectAt(ctx context.Context, clusterID, streamID, namespace, name string, at time.Time) (*event.Record, error) {
	rec, err := m.store.ObjectAt(ctx, storage.ObjectRef{
		ClusterID: clusterID,
		StreamID:  streamID,
		Namespace: namespace,
		Name:      name,
	}, at)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrNoObjectAtTime
		}
		return nil, err
	}
	return rec, nil
}

// StateAt reconstructs the object state of every stream in a cluster at time
// at, without persisting anything. It reports coverage completeness.
func (m *Materializer) StateAt(ctx context.Context, clusterID string, at time.Time) ([]ObjectState, bool, error) {
	streams, err := m.store.Streams(ctx)
	if err != nil {
		return nil, false, err
	}
	var all []ObjectState
	complete := true
	for _, sm := range streams {
		if sm.ClusterID != clusterID {
			continue
		}
		res, err := m.StreamState(ctx, sm.StreamID, at)
		if err != nil {
			return nil, false, err
		}
		if !res.HasBaseline || res.HasGaps {
			complete = false
		}
		all = append(all, res.Objects...)
	}
	sortObjects(all)
	return all, complete, nil
}

// StreamStateResult is the reduced state of one stream.
type StreamStateResult struct {
	Objects     []ObjectState
	HasGaps     bool
	GapCount    int
	HasBaseline bool
}

// StreamState reduces every event of a stream observed up to at into object
// state, reporting whether coverage was lost along the way.
func (m *Materializer) StreamState(ctx context.Context, streamID string, at time.Time) (StreamStateResult, error) {
	recs, err := m.store.StreamEvents(ctx, streamID, at)
	if err != nil {
		return StreamStateResult{}, err
	}
	red := reduce(streamID, recs)
	objs := make([]ObjectState, 0, len(red.states))
	for _, st := range red.states {
		objs = append(objs, st)
	}
	sortObjects(objs)
	return StreamStateResult{Objects: objs, HasGaps: red.hasGaps, GapCount: red.gapCount, HasBaseline: red.hasBaseline}, nil
}

// reduction is the per-stream reduce result.
type reduction struct {
	states      map[string]ObjectState
	hasBaseline bool
	hasGaps     bool
	gapCount    int
}

// reduce folds records into per-object state. DELETED removes the object,
// BASELINE marks full coverage from that point, and GAP marks lost coverage.
func reduce(streamID string, recs []event.Record) reduction {
	red := reduction{states: map[string]ObjectState{}}
	for i := range recs {
		rec := &recs[i]
		switch rec.Type {
		case event.TypeBaseline:
			red.hasBaseline = true
		case event.TypeGap:
			red.hasGaps = true
			red.gapCount++
		case event.TypeEvent:
			switch rec.WatchType {
			case event.WatchDeleted:
				delete(red.states, rec.Resource.ObjectKey())
			case event.WatchBookmark, event.WatchError:
				// No object payload.
			default:
				if len(rec.Object) == 0 {
					continue
				}
				red.states[rec.Resource.ObjectKey()] = ObjectState{
					ClusterID: rec.ClusterID,
					StreamID:  streamID,
					Namespace: rec.Resource.Namespace,
					Name:      rec.Resource.Name,
					Kind:      rec.Resource.Kind,
					Object:    rec.Object,
					At:        rec.ObservedAt,
				}
			}
		}
	}
	return red
}

func sortObjects(objs []ObjectState) {
	sort.Slice(objs, func(i, j int) bool {
		if objs[i].StreamID != objs[j].StreamID {
			return objs[i].StreamID < objs[j].StreamID
		}
		if objs[i].Namespace != objs[j].Namespace {
			return objs[i].Namespace < objs[j].Namespace
		}
		return objs[i].Name < objs[j].Name
	})
}
