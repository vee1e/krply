package storage

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/krply/krply/internal/event"
)

// NewInMemory returns a Store backed by RAM. It is for tests and for running
// the query API without a persistent journal. It satisfies the same atomic
// append+checkpoint contract as the SQLite store.
func NewInMemory() Store {
	m := &memoryStore{
		records:   map[string][]*event.Record{},
		seq:       0,
		streams:   map[string]*StreamMeta{},
		snapshots: map[string]*SnapshotRef{},
	}
	return m
}

type memoryStore struct {
	mu        sync.Mutex
	records   map[string][]*event.Record // streamID -> records in ingest order
	seq       int64
	streams   map[string]*StreamMeta
	snapshots map[string]*SnapshotRef
}

func (m *memoryStore) Append(ctx context.Context, rec *event.Record) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendLocked(rec)
}

func (m *memoryStore) Appends(ctx context.Context, recs []*event.Record) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seqs := make([]int64, 0, len(recs))
	for _, r := range recs {
		s, err := m.appendLocked(r)
		if err != nil {
			return nil, err
		}
		seqs = append(seqs, s)
	}
	return seqs, nil
}

func (m *memoryStore) appendLocked(rec *event.Record) (int64, error) {
	m.seq++
	rec.IngestSeq = m.seq
	if rec.ObservedAt.IsZero() {
		rec.ObservedAt = time.Now().UTC()
	}
	m.records[rec.StreamID] = append(m.records[rec.StreamID], rec)
	m.updateMeta(rec)
	return m.seq, nil
}

func (m *memoryStore) updateMeta(rec *event.Record) {
	meta := m.streams[rec.StreamID]
	if meta == nil {
		meta = &StreamMeta{StreamID: rec.StreamID, ClusterID: rec.ClusterID}
		m.streams[rec.StreamID] = meta
	}
	if meta.FirstObservedAt.IsZero() {
		meta.FirstObservedAt = rec.ObservedAt
	}
	if rec.ObservedAt.After(meta.LastObservedAt) {
		meta.LastObservedAt = rec.ObservedAt
	}
	switch rec.Type {
	case event.TypeGap:
		meta.GapCount++
		meta.HasGaps = true
		meta.Degraded = true
	case event.TypeBaseline:
		meta.Available = true
		meta.Degraded = false
		if rec.Resource.ResourceVersion != "" {
			meta.LastResourceVersion = rec.Resource.ResourceVersion
		}
	case event.TypeCoverageChange:
		if rec.Coverage != nil {
			meta.Available = rec.Coverage.Current.Available
			if !meta.Available {
				meta.Degraded = true
			}
		}
	case event.TypeCheckpoint:
		if rec.Checkpoint != nil {
			meta.LastResourceVersion = rec.Checkpoint.ResourceVersion
		}
	case event.TypeEvent:
		if rec.Resource.ResourceVersion != "" {
			meta.LastResourceVersion = rec.Resource.ResourceVersion
		}
	}
}

func (m *memoryStore) ListClusters(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, recs := range m.records {
		for _, r := range recs {
			if r.ClusterID != "" && !seen[r.ClusterID] {
				seen[r.ClusterID] = true
				out = append(out, r.ClusterID)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *memoryStore) Streams(ctx context.Context) ([]StreamMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]StreamMeta, 0, len(m.streams))
	for _, s := range m.streams {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StreamID < out[j].StreamID })
	return out, nil
}

func (m *memoryStore) StreamMeta(ctx context.Context, streamID string) (StreamMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[streamID]
	if !ok {
		return StreamMeta{}, ErrStreamNotFound
	}
	return *s, nil
}

func (m *memoryStore) Events(ctx context.Context, f EventFilter) ([]event.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []event.Record
	for _, recs := range m.records {
		for _, r := range recs {
			if !matches(r, f) {
				continue
			}
			out = append(out, *r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IngestSeq < out[j].IngestSeq })
	return out, nil
}

func (m *memoryStore) ObjectHistory(ctx context.Context, ref ObjectRef) ([]event.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []event.Record
	for _, recs := range m.records {
		for _, r := range recs {
			if r.Type != event.TypeEvent {
				continue
			}
			if r.ClusterID != ref.ClusterID || r.StreamID != ref.StreamID {
				continue
			}
			if r.Resource.Namespace != ref.Namespace || r.Resource.Name != ref.Name {
				continue
			}
			out = append(out, *r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IngestSeq < out[j].IngestSeq })
	return out, nil
}

func (m *memoryStore) ObjectAt(ctx context.Context, ref ObjectRef, ts time.Time) (*event.Record, error) {
	recs, err := m.ObjectHistory(ctx, ref)
	if err != nil {
		return nil, err
	}
	var last *event.Record
	for i := range recs {
		if recs[i].ObservedAt.After(ts) {
			break
		}
		if recs[i].WatchType == event.WatchDeleted {
			last = nil
			continue
		}
		cp := recs[i]
		last = &cp
	}
	if last == nil {
		return nil, ErrNotFound
	}
	return last, nil
}

func (m *memoryStore) StreamEvents(ctx context.Context, streamID string, until time.Time) ([]event.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []event.Record
	for _, r := range m.records[streamID] {
		if until.IsZero() || !r.ObservedAt.After(until) {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (m *memoryStore) Baselines(ctx context.Context, streamID string) ([]event.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []event.Record
	for _, r := range m.records[streamID] {
		if r.Type == event.TypeBaseline {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (m *memoryStore) Gaps(ctx context.Context, streamID string) ([]event.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []event.Record
	for _, r := range m.records[streamID] {
		if r.Type == event.TypeGap {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (m *memoryStore) SaveSnapshot(ctx context.Context, snap *SnapshotRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *snap
	if cp.ID == "" {
		cp.ID = "snap-" + time.Now().UTC().Format("20060102T150405")
	}
	m.snapshots[cp.ID] = &cp
	return nil
}

func (m *memoryStore) Snapshots(ctx context.Context) ([]SnapshotRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SnapshotRef, 0, len(m.snapshots))
	for _, s := range m.snapshots {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out, nil
}

func (m *memoryStore) Close() error { return nil }

func matches(r *event.Record, f EventFilter) bool {
	if f.ClusterID != "" && r.ClusterID != f.ClusterID {
		return false
	}
	if f.StreamID != "" && r.StreamID != f.StreamID {
		return false
	}
	if f.RecordType != "" && r.Type != f.RecordType {
		return false
	}
	if f.Namespace != "" && r.Resource.Namespace != f.Namespace {
		return false
	}
	if f.Name != "" && r.Resource.Name != f.Name {
		return false
	}
	if f.Kind != "" && r.Resource.Kind != f.Kind {
		return false
	}
	if !f.Since.IsZero() && r.ObservedAt.Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && r.ObservedAt.After(f.Until) {
		return false
	}
	return true
}

// Sentinel errors shared by all Store implementations.
var (
	ErrStreamNotFound = &storeError{"stream not found"}
	ErrNotFound       = &storeError{"not found"}
)

type storeError struct{ msg string }

func (e *storeError) Error() string { return e.msg }
