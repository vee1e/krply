package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/krply/krply/internal/event"
)

// sqliteStore is the durable, persistent Store backed by SQLite.
//
// Writes are serialized through a single mutex so that append and its
// checkpoint (stream meta) advance atomically and never interleave. Reads
// run concurrently and do not need the lock.
type sqliteStore struct {
	db *sql.DB
	mu sync.Mutex
}

// NewSQLiteStore opens (or creates) a persistent SQLite store at path and
// returns it as a Store. The special path ":memory:" yields an in-memory
// database. WAL mode is enabled via the DSN pragmas.
func NewSQLiteStore(path string) (Store, error) {
	dsn := ":memory:"
	if path != ":memory:" {
		dsn = "file:" + (&url.URL{Path: path}).EscapedPath() + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store %q: %w", path, err)
	}
	if path == ":memory:" {
		// Each pooled connection to ":memory:" is its own database, so pin the
		// pool to a single connection.
		db.SetMaxOpenConns(1)
	}
	s := &sqliteStore{db: db}
	if err := s.init(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("init sqlite store: %w", err)
	}
	return s, nil
}

const (
	recordsDDL = `CREATE TABLE IF NOT EXISTS records (
		ingest_seq       INTEGER PRIMARY KEY AUTOINCREMENT,
		cluster_id       TEXT,
		stream_id        TEXT,
		record_type      TEXT,
		event_id         TEXT,
		observed_at      TEXT,
		watch_type       TEXT,
		synthetic        INTEGER,
		grp              TEXT,
		version          TEXT,
		kind             TEXT,
		namespace        TEXT,
		name             TEXT,
		uid              TEXT,
		resource_version TEXT,
		object_hash      TEXT,
		object           BLOB,
		provenance       TEXT,
		gap              TEXT,
		coverage         TEXT,
		checkpoint       TEXT,
		snapshot         TEXT
	)`

	dedupIndexDDL = `CREATE UNIQUE INDEX IF NOT EXISTS idx_records_dedup
		ON records (cluster_id, stream_id, event_id) WHERE event_id <> ''`

	lookupIndexDDL = `CREATE INDEX IF NOT EXISTS idx_records_lookup
		ON records (cluster_id, stream_id, observed_at, namespace, name)`

	streamsDDL = `CREATE TABLE IF NOT EXISTS streams (
		stream_id            TEXT PRIMARY KEY,
		cluster_id           TEXT,
		grp                  TEXT,
		version              TEXT,
		resource             TEXT,
		kind                 TEXT,
		namespace            TEXT,
		selector             TEXT,
		available            INTEGER,
		first_observed_at    TEXT,
		last_observed_at     TEXT,
		last_resource_version TEXT,
		gap_count            INTEGER,
		degraded             INTEGER
	)`

	snapshotsDDL = `CREATE TABLE IF NOT EXISTS snapshots (
		id         TEXT PRIMARY KEY,
		cluster_id TEXT,
		name       TEXT,
		at         TEXT
	)`
)

func (s *sqliteStore) init(ctx context.Context) error {
	for _, ddl := range []string{recordsDDL, dedupIndexDDL, lookupIndexDDL, streamsDDL, snapshotsDDL} {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	// migrateTimeColumns normalizes timestamps written before the fixed-width
	// format to the same shape so lexicographic comparisons stay chronological.
	var userVersion int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&userVersion); err != nil {
		return err
	}
	if userVersion < 1 {
		for _, q := range []string{
			`UPDATE records SET observed_at = strftime('%Y-%m-%dT%H:%M:%fZ', observed_at) WHERE observed_at <> ''`,
			`UPDATE streams SET first_observed_at = strftime('%Y-%m-%dT%H:%M:%fZ', first_observed_at) WHERE first_observed_at <> ''`,
			`UPDATE streams SET last_observed_at = strftime('%Y-%m-%dT%H:%M:%fZ', last_observed_at) WHERE last_observed_at <> ''`,
			`UPDATE snapshots SET at = strftime('%Y-%m-%dT%H:%M:%fZ', at) WHERE at <> ''`,
		} {
			if _, err := s.db.ExecContext(ctx, q); err != nil {
				return err
			}
		}
		if _, err := s.db.ExecContext(ctx, `PRAGMA user_version = 1`); err != nil {
			return err
		}
	}
	return nil
}

// recordCols is the SELECT column list for the records table in schema order.
const recordCols = `ingest_seq, cluster_id, stream_id, record_type, event_id, observed_at,
	watch_type, synthetic, grp, version, kind, namespace, name, uid, resource_version,
	object_hash, object, provenance, gap, coverage, checkpoint, snapshot`

// streamCols is the SELECT column list for the streams meta table.
const streamCols = `stream_id, cluster_id, grp, version, resource, kind, namespace, selector,
	available, first_observed_at, last_observed_at, last_resource_version, gap_count, degraded`

func (s *sqliteStore) Append(ctx context.Context, rec *event.Record) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	seq, err := s.appendRecord(ctx, tx, rec)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return 0, err
	}
	return seq, nil
}

func (s *sqliteStore) Appends(ctx context.Context, recs []*event.Record) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	seqs := make([]int64, 0, len(recs))
	for _, r := range recs {
		seq, err := s.appendRecord(ctx, tx, r)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		seqs = append(seqs, seq)
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return nil, err
	}
	return seqs, nil
}

func (s *sqliteStore) appendRecord(ctx context.Context, tx *sql.Tx, rec *event.Record) (int64, error) {
	if rec.ObservedAt.IsZero() {
		rec.ObservedAt = time.Now().UTC()
	}
	observedAt := formatTime(rec.ObservedAt)
	synthetic := 0
	if rec.Synthetic {
		synthetic = 1
	}
	var object []byte
	if len(rec.Object) > 0 {
		object = rec.Object
	}
	provenance, err := marshalJSON(rec.Provenance)
	if err != nil {
		return 0, err
	}
	gap, err := marshalJSON(rec.Gap)
	if err != nil {
		return 0, err
	}
	coverage, err := marshalJSON(rec.Coverage)
	if err != nil {
		return 0, err
	}
	checkpoint, err := marshalJSON(rec.Checkpoint)
	if err != nil {
		return 0, err
	}
	snapshot, err := marshalJSON(rec.Snapshot)
	if err != nil {
		return 0, err
	}

	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO records (
		cluster_id, stream_id, record_type, event_id, observed_at, watch_type, synthetic,
		grp, version, kind, namespace, name, uid, resource_version, object_hash, object,
		provenance, gap, coverage, checkpoint, snapshot
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.ClusterID, rec.StreamID, string(rec.Type), rec.EventID, observedAt,
		string(rec.WatchType), synthetic,
		rec.Resource.Group, rec.Resource.Version, rec.Resource.Kind, rec.Resource.Namespace,
		rec.Resource.Name, rec.Resource.UID, rec.Resource.ResourceVersion, rec.ObjectHash, object,
		provenance, gap, coverage, checkpoint, snapshot,
	)
	if err != nil {
		return 0, err
	}

	inserted, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	var seq int64
	if inserted > 0 {
		if err := tx.QueryRowContext(ctx, `SELECT last_insert_rowid()`).Scan(&seq); err != nil {
			return 0, err
		}
		if rec.StreamID != "" {
			if err := s.upsertMeta(ctx, tx, rec, observedAt); err != nil {
				return 0, err
			}
		}
	} else {
		// Duplicate event_id delivery: re-return the existing ingest sequence.
		if err := tx.QueryRowContext(ctx, `SELECT ingest_seq FROM records
			WHERE cluster_id=? AND stream_id=? AND event_id=?`,
			rec.ClusterID, rec.StreamID, rec.EventID).Scan(&seq); err != nil {
			return 0, err
		}
	}
	rec.IngestSeq = seq
	return seq, nil
}

// upsertMeta mirrors the checkpoint-advance logic of the in-memory store's
// updateMeta, but persisted. It runs inside the same transaction as the
// record insert so the two are atomic.
func (s *sqliteStore) upsertMeta(ctx context.Context, tx *sql.Tx, rec *event.Record, observedAt string) error {
	var (
		available, degraded int
		gapCount            int64
		first, last, lastRV string
	)
	err := tx.QueryRowContext(ctx, `SELECT available, degraded, gap_count, first_observed_at,
		last_observed_at, last_resource_version FROM streams WHERE stream_id=?`, rec.StreamID).
		Scan(&available, &degraded, &gapCount, &first, &last, &lastRV)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if first == "" {
		first = observedAt
	}
	if last == "" || observedAt > last {
		last = observedAt
	}

	switch rec.Type {
	case event.TypeGap:
		gapCount++
		degraded = 1
	case event.TypeBaseline:
		available = 1
		degraded = 0
		if rec.Resource.ResourceVersion != "" {
			lastRV = rec.Resource.ResourceVersion
		}
	case event.TypeCoverageChange:
		if rec.Coverage != nil {
			if rec.Coverage.Current.Available {
				available = 1
				degraded = 0
			} else {
				available = 0
				degraded = 1
			}
		}
	case event.TypeCheckpoint:
		if rec.Checkpoint != nil {
			lastRV = rec.Checkpoint.ResourceVersion
		}
	case event.TypeEvent:
		if rec.Resource.ResourceVersion != "" {
			lastRV = rec.Resource.ResourceVersion
		}
	}

	grp, version, resource, kind, namespace, selector := metaParts(rec.StreamID, rec)

	_, err = tx.ExecContext(ctx, `INSERT INTO streams (
		stream_id, cluster_id, grp, version, resource, kind, namespace, selector,
		available, first_observed_at, last_observed_at, last_resource_version, gap_count, degraded
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(stream_id) DO UPDATE SET
		cluster_id=excluded.cluster_id,
		grp=excluded.grp,
		version=excluded.version,
		resource=excluded.resource,
		kind=excluded.kind,
		namespace=excluded.namespace,
		selector=excluded.selector,
		available=excluded.available,
		first_observed_at=excluded.first_observed_at,
		last_observed_at=excluded.last_observed_at,
		last_resource_version=excluded.last_resource_version,
		gap_count=excluded.gap_count,
		degraded=excluded.degraded`,
		rec.StreamID, rec.ClusterID, grp, version, resource, kind, namespace, selector,
		available, first, last, lastRV, gapCount, degraded,
	)
	return err
}

func metaParts(streamID string, rec *event.Record) (grp, version, resource, kind, namespace, selector string) {
	if st, err := event.StreamID(streamID); err == nil {
		return st.Group, st.Version, st.Resource, rec.Resource.Kind, st.Namespace, st.Selector
	}
	return rec.Resource.Group, rec.Resource.Version, "", rec.Resource.Kind, rec.Resource.Namespace, ""
}

func (s *sqliteStore) ListClusters(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT cluster_id FROM streams
		WHERE cluster_id<>'' ORDER BY cluster_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *sqliteStore) Streams(ctx context.Context) ([]StreamMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+streamCols+` FROM streams ORDER BY stream_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StreamMeta
	for rows.Next() {
		m, err := scanStreamMeta(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *sqliteStore) StreamMeta(ctx context.Context, streamID string) (StreamMeta, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+streamCols+` FROM streams WHERE stream_id=?`, streamID)
	m, err := scanStreamMeta(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return StreamMeta{}, ErrStreamNotFound
	}
	if err != nil {
		return StreamMeta{}, err
	}
	return m, nil
}

func (s *sqliteStore) Events(ctx context.Context, f EventFilter) ([]event.Record, error) {
	q := `SELECT ` + recordCols + ` FROM records WHERE 1=1`
	var args []any
	if f.ClusterID != "" {
		q += " AND cluster_id=?"
		args = append(args, f.ClusterID)
	}
	if f.StreamID != "" {
		q += " AND stream_id=?"
		args = append(args, f.StreamID)
	}
	if f.RecordType != "" {
		q += " AND record_type=?"
		args = append(args, string(f.RecordType))
	}
	if f.Namespace != "" {
		q += " AND namespace=?"
		args = append(args, f.Namespace)
	}
	if f.Name != "" {
		q += " AND name=?"
		args = append(args, f.Name)
	}
	if f.Kind != "" {
		q += " AND kind=?"
		args = append(args, f.Kind)
	}
	if !f.Since.IsZero() {
		q += " AND observed_at>=?"
		args = append(args, formatTime(f.Since))
	}
	if !f.Until.IsZero() {
		q += " AND observed_at<=?"
		args = append(args, formatTime(f.Until))
	}
	if f.SinceSeq > 0 {
		q += " AND ingest_seq>?"
		args = append(args, f.SinceSeq)
	}
	q += " ORDER BY ingest_seq ASC"
	if f.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, f.Limit)
		if f.Offset > 0 {
			q += " OFFSET ?"
			args = append(args, f.Offset)
		}
	} else if f.Offset > 0 {
		q += " LIMIT -1 OFFSET ?"
		args = append(args, f.Offset)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []event.Record
	for rows.Next() {
		r, err := scanRecord(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sqliteStore) ObjectHistory(ctx context.Context, ref ObjectRef) ([]event.Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+recordCols+` FROM records
		WHERE record_type=? AND cluster_id=? AND stream_id=? AND namespace=? AND name=?
		ORDER BY ingest_seq ASC`,
		string(event.TypeEvent), ref.ClusterID, ref.StreamID, ref.Namespace, ref.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []event.Record
	for rows.Next() {
		r, err := scanRecord(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sqliteStore) ObjectAt(ctx context.Context, ref ObjectRef, ts time.Time) (*event.Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+recordCols+` FROM records
		WHERE record_type=? AND cluster_id=? AND stream_id=? AND namespace=? AND name=? AND observed_at<=?
		ORDER BY ingest_seq ASC`,
		string(event.TypeEvent), ref.ClusterID, ref.StreamID, ref.Namespace, ref.Name, formatTime(ts))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var last *event.Record
	for rows.Next() {
		r, err := scanRecord(rows.Scan)
		if err != nil {
			return nil, err
		}
		if r.WatchType == event.WatchDeleted {
			last = nil
			continue
		}
		cp := r
		last = &cp
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if last == nil {
		return nil, ErrNotFound
	}
	return last, nil
}

func (s *sqliteStore) StreamEvents(ctx context.Context, streamID string, until time.Time) ([]event.Record, error) {
	q := `SELECT ` + recordCols + ` FROM records WHERE stream_id=?`
	args := []any{streamID}
	if !until.IsZero() {
		q += " AND observed_at<=?"
		args = append(args, formatTime(until))
	}
	q += " ORDER BY ingest_seq ASC"
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []event.Record
	for rows.Next() {
		r, err := scanRecord(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sqliteStore) Baselines(ctx context.Context, streamID string) ([]event.Record, error) {
	return s.recordsByType(ctx, streamID, event.TypeBaseline)
}

func (s *sqliteStore) Gaps(ctx context.Context, streamID string) ([]event.Record, error) {
	return s.recordsByType(ctx, streamID, event.TypeGap)
}

func (s *sqliteStore) recordsByType(ctx context.Context, streamID string, typ event.RecordType) ([]event.Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+recordCols+` FROM records
		WHERE stream_id=? AND record_type=? ORDER BY ingest_seq ASC`, streamID, string(typ))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []event.Record
	for rows.Next() {
		r, err := scanRecord(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sqliteStore) SaveSnapshot(ctx context.Context, snap *SnapshotRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *snap
	if cp.ID == "" {
		cp.ID = "snap-" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO snapshots (id, cluster_id, name, at)
		VALUES (?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET cluster_id=excluded.cluster_id, name=excluded.name, at=excluded.at`,
		cp.ID, cp.ClusterID, cp.Name, formatTime(cp.At))
	return err
}

func (s *sqliteStore) Snapshots(ctx context.Context) ([]SnapshotRef, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, cluster_id, name, at FROM snapshots ORDER BY at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotRef
	for rows.Next() {
		var snap SnapshotRef
		var at string
		if err := rows.Scan(&snap.ID, &snap.ClusterID, &snap.Name, &at); err != nil {
			return nil, err
		}
		t, err := parseTime(at)
		if err != nil {
			return nil, err
		}
		snap.At = t
		out = append(out, snap)
	}
	return out, rows.Err()
}

func (s *sqliteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// scanFunc matches both *sql.Rows.Scan and *sql.Row.Scan.
type scanFunc func(dest ...any) error

func scanRecord(scan scanFunc) (event.Record, error) {
	var r event.Record
	var (
		observedAt, watchType                                                 string
		synthetic                                                             int
		grp, version, kind, namespace, name, uid, resourceVersion, objectHash string
		object                                                                []byte
		provenance, gap, coverage, checkpoint, snapshot                       string
	)
	if err := scan(&r.IngestSeq, &r.ClusterID, &r.StreamID, &r.Type, &r.EventID, &observedAt,
		&watchType, &synthetic, &grp, &version, &kind, &namespace, &name, &uid, &resourceVersion,
		&objectHash, &object, &provenance, &gap, &coverage, &checkpoint, &snapshot); err != nil {
		return event.Record{}, err
	}
	t, err := parseTime(observedAt)
	if err != nil {
		return event.Record{}, err
	}
	r.ObservedAt = t
	r.WatchType = event.WatchType(watchType)
	r.Synthetic = synthetic != 0
	r.Resource = event.ResourceRef{
		Group: grp, Version: version, Kind: kind, Namespace: namespace, Name: name,
		UID: uid, ResourceVersion: resourceVersion,
	}
	r.ObjectHash = objectHash
	r.Object = json.RawMessage(object)
	if err := unmarshalJSON(provenance, &r.Provenance); err != nil {
		return event.Record{}, err
	}
	if err := unmarshalJSON(gap, &r.Gap); err != nil {
		return event.Record{}, err
	}
	if err := unmarshalJSON(coverage, &r.Coverage); err != nil {
		return event.Record{}, err
	}
	if err := unmarshalJSON(checkpoint, &r.Checkpoint); err != nil {
		return event.Record{}, err
	}
	if err := unmarshalJSON(snapshot, &r.Snapshot); err != nil {
		return event.Record{}, err
	}
	return r, nil
}

func scanStreamMeta(scan scanFunc) (StreamMeta, error) {
	var m StreamMeta
	var available, degraded int
	var first, last string
	if err := scan(&m.StreamID, &m.ClusterID, &m.Group, &m.Version, &m.Resource, &m.Kind,
		&m.Namespace, &m.Selector, &available, &first, &last, &m.LastResourceVersion,
		&m.GapCount, &degraded); err != nil {
		return StreamMeta{}, err
	}
	m.Available = available != 0
	m.Degraded = degraded != 0
	m.HasGaps = m.GapCount > 0
	t, err := parseTime(first)
	if err != nil {
		return StreamMeta{}, err
	}
	m.FirstObservedAt = t
	t, err = parseTime(last)
	if err != nil {
		return StreamMeta{}, err
	}
	m.LastObservedAt = t
	return m, nil
}

// formatTime renders t as a fixed-width UTC timestamp. Variable-width RFC3339
// output breaks lexicographic string comparisons at sub-second boundaries, so
// the fraction is always padded to milliseconds and the zone is always "Z".
// All comparisons against observed_at use this exact shape.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05.000") + "Z"
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func marshalJSON(v any) (string, error) {
	if v == nil {
		return "", nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func unmarshalJSON(s string, v any) error {
	if s == "" {
		return nil
	}
	return json.Unmarshal([]byte(s), v)
}
