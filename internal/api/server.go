// Package api implements the krply HTTP query API served by krply-server.
//
// It exposes the stable queryv1 wire types over a small set of read and
// replay endpoints, backs them with the storage journal, and serves the
// materialized web UI when a web/dist directory is present.
package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	queryv1 "github.com/krply/krply/api/query/v1"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/materialize"
	"github.com/krply/krply/internal/metrics"
	"github.com/krply/krply/internal/replay"
	"github.com/krply/krply/internal/storage"
)

// maxEventLimit caps the number of events returned by a single page.
const maxEventLimit = 1000

// Server serves the krply query API.
type Server struct {
	store   storage.Store
	mat     *materialize.Materializer
	planner *replay.Planner
	metrics *metrics.Metrics
	version string
	static  fs.FS

	planMu    sync.RWMutex
	plans     map[string]*replay.Plan
	planTimes map[string]time.Time

	corsOrigins []string
}

// NewServer wires a Server onto the given store, materializer, planner, and
// metrics registry. The metrics instance may be nil; a fresh one is created
// in that case. The static web assets are served from web/dist relative to
// the current working directory when the directory exists.
func NewServer(store storage.Store, mat *materialize.Materializer, planner *replay.Planner, m *metrics.Metrics, version string) (*Server, error) {
	if store == nil {
		return nil, errors.New("api: nil store")
	}
	if mat == nil {
		return nil, errors.New("api: nil materializer")
	}
	if planner == nil {
		return nil, errors.New("api: nil planner")
	}
	if m == nil {
		m = metrics.New()
	}
	var static fs.FS
	if fi, err := os.Stat("web/dist"); err == nil && fi.IsDir() {
		static = os.DirFS("web/dist")
	}
	origins := strings.Split(os.Getenv("KRPLY_CORS_ORIGINS"), ",")
	if len(origins) == 1 && origins[0] == "" {
		origins = []string{"*"}
	}
	return &Server{
		store:       store,
		mat:         mat,
		planner:     planner,
		metrics:     m,
		version:     version,
		static:      static,
		plans:       map[string]*replay.Plan{},
		planTimes:   map[string]time.Time{},
		corsOrigins: origins,
	}, nil
}

// cors wraps the mux with CORS headers so the static web UI can query the API
// from a different origin. Origins are allowed when they match KRPLY_CORS_ORIGINS
// (comma-separated, "*" for any). Preflight OPTIONS requests are short-circuited.
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			if s.allowsOrigin(origin) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
				w.Header().Set("Vary", "Origin")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) allowsOrigin(origin string) bool {
	for _, o := range s.corsOrigins {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("GET /v1/clusters", s.handleClusters)
	mux.HandleFunc("GET /v1/streams", s.handleStreams)
	mux.HandleFunc("GET /v1/streams/{id}", s.handleStreamByID)
	mux.HandleFunc("GET /v1/events", s.handleEvents)
	mux.HandleFunc("GET /v1/objects/{ref}/history", s.handleObjectHistory)
	mux.HandleFunc("GET /v1/diff", s.handleDiff)
	mux.HandleFunc("GET /v1/snapshots", s.handleSnapshots)
	mux.HandleFunc("POST /v1/replay-plans", s.handleCreatePlan)
	mux.HandleFunc("GET /v1/replay-plans", s.handleListPlans)
	mux.HandleFunc("GET /v1/replay-plans/{id}", s.handleGetPlan)
	mux.HandleFunc("POST /v1/replay-plans/{id}/dry-run", s.handleDryRun)
	mux.HandleFunc("POST /v1/replay-runs", s.handleReplayRun)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("/", s.handleStatic)
	return s.cors(mux)
}

// planByID returns the registered plan and whether it exists.
func (s *Server) planByID(id string) (*replay.Plan, bool) {
	s.planMu.RLock()
	defer s.planMu.RUnlock()
	p, ok := s.plans[id]
	return p, ok
}

func (s *Server) planRecord(id string) (*replay.Plan, time.Time, bool) {
	s.planMu.RLock()
	defer s.planMu.RUnlock()
	p, ok := s.plans[id]
	if !ok {
		return nil, time.Time{}, false
	}
	return p, s.planTimes[id], true
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.metrics.Handler().ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, queryv1.Health{Status: "ok", Version: s.version, Storage: "sqlite"})
}

func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusters, err := s.store.ListClusters(ctx)
	if err != nil {
		s.internalError(w, "list clusters", err)
		return
	}
	streams, err := s.store.Streams(ctx)
	if err != nil {
		s.internalError(w, "list streams", err)
		return
	}
	first := map[string]time.Time{}
	last := map[string]time.Time{}
	for _, st := range streams {
		if first[st.ClusterID].IsZero() || st.FirstObservedAt.Before(first[st.ClusterID]) {
			first[st.ClusterID] = st.FirstObservedAt
		}
		if st.LastObservedAt.After(last[st.ClusterID]) {
			last[st.ClusterID] = st.LastObservedAt
		}
	}
	out := make([]queryv1.Cluster, 0, len(clusters))
	for _, id := range clusters {
		out = append(out, queryv1.Cluster{
			ID:         id,
			Generation: "1",
			FirstSeen:  first[id],
			LastSeen:   last[id],
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	streams, err := s.store.Streams(r.Context())
	if err != nil {
		s.internalError(w, "list streams", err)
		return
	}
	out := make([]queryv1.Stream, 0, len(streams))
	for _, m := range streams {
		out = append(out, mapStream(m))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleStreamByID(w http.ResponseWriter, r *http.Request) {
	m, err := s.store.StreamMeta(r.Context(), r.PathValue("id"))
	if errors.Is(err, storage.ErrStreamNotFound) {
		writeError(w, http.StatusNotFound, "stream not found")
		return
	}
	if err != nil {
		s.internalError(w, "stream meta", err)
		return
	}
	writeJSON(w, http.StatusOK, mapStream(m))
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := storage.EventFilter{
		ClusterID: q.Get("cluster_id"),
		StreamID:  q.Get("stream_id"),
		Namespace: q.Get("namespace"),
		Name:      q.Get("name"),
		Kind:      q.Get("kind"),
	}
	if rt := q.Get("record_type"); rt != "" {
		f.RecordType = event.RecordType(rt)
	}

	limit := 100
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = min(n, maxEventLimit)
	}

	var err error
	if f.Since, err = parseTimeParam(q.Get("since")); err != nil {
		writeError(w, http.StatusBadRequest, "invalid since: "+err.Error())
		return
	}
	if f.Until, err = parseTimeParam(q.Get("until")); err != nil {
		writeError(w, http.StatusBadRequest, "invalid until: "+err.Error())
		return
	}

	var cursorSeq int64
	if c := q.Get("cursor"); c != "" {
		seq, err := decodeCursor(c)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		cursorSeq = seq
	}

	f.Limit = limit + 1
	recs, err := s.store.Events(r.Context(), f)
	if err != nil {
		s.internalError(w, "events", err)
		return
	}

	if cursorSeq > 0 {
		filtered := recs[:0]
		for _, rec := range recs {
			if rec.IngestSeq > cursorSeq {
				filtered = append(filtered, rec)
			}
		}
		recs = filtered
	}

	hasMore := len(recs) > limit
	if hasMore {
		recs = recs[:limit]
	}
	items := make([]queryv1.Event, 0, len(recs))
	for _, rec := range recs {
		items = append(items, mapEvent(rec))
	}
	page := queryv1.EventPage{Items: items, HasMore: hasMore}
	if hasMore && len(items) > 0 {
		page.NextCursor = encodeCursor(recs[len(recs)-1].IngestSeq)
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleObjectHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ref, err := DecodeObjectRef(r.PathValue("ref"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid object ref: "+err.Error())
		return
	}

	q := r.URL.Query()
	since, err := parseTimeParam(q.Get("since"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid since: "+err.Error())
		return
	}
	until, err := parseTimeParam(q.Get("until"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid until: "+err.Error())
		return
	}

	recs, err := s.store.ObjectHistory(ctx, ref)
	if err != nil {
		s.internalError(w, "object history", err)
		return
	}
	recs = filterByTime(recs, since, until)

	gaps, err := s.store.Gaps(ctx, ref.StreamID)
	if err != nil {
		s.internalError(w, "gaps", err)
		return
	}

	history := queryv1.ObjectHistory{
		ClusterID: ref.ClusterID,
		StreamID:  ref.StreamID,
		Namespace: ref.Namespace,
		Name:      ref.Name,
		Kind:      objectKind(recs),
		Items:     s.buildTimeline(recs, gaps),
		HasGaps:   len(gaps) > 0,
		Gaps:      mapGaps(gaps),
	}
	if len(gaps) > 0 {
		history.Warning = "watch history has gaps; timeline may be incomplete"
	}
	writeJSON(w, http.StatusOK, history)
}

func (s *Server) handleDiff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	clusterID := q.Get("cluster_id")
	namespace := q.Get("namespace")
	before, err := parseTimeParam(q.Get("since"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid since: "+err.Error())
		return
	}
	after, err := parseTimeParam(q.Get("until"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid until: "+err.Error())
		return
	}
	if before.IsZero() || after.IsZero() {
		writeError(w, http.StatusBadRequest, "since and until are required")
		return
	}
	if !before.Before(after) {
		writeError(w, http.StatusBadRequest, "since must be before until")
		return
	}
	res, err := s.mat.Diff(r.Context(), clusterID, namespace, before, after)
	if err != nil {
		s.internalError(w, "diff", err)
		return
	}
	writeJSON(w, http.StatusOK, mapDiffResult(clusterID, namespace, res))
}

func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	snaps, err := s.store.Snapshots(r.Context())
	if err != nil {
		s.internalError(w, "snapshots", err)
		return
	}
	streams, err := s.store.Streams(ctx)
	if err != nil {
		s.internalError(w, "snapshot streams", err)
		return
	}
	out := make([]queryv1.Snapshot, 0, len(snaps))
	for _, sn := range snaps {
		item := queryv1.Snapshot{
			ID:        sn.ID,
			ClusterID: sn.ClusterID,
			Name:      sn.Name,
			At:        sn.At,
			Complete:  true,
		}
		for _, stream := range streams {
			if stream.ClusterID != sn.ClusterID {
				continue
			}
			item.Streams++
			state, err := s.mat.StreamState(ctx, stream.StreamID, sn.At)
			if err != nil {
				s.internalError(w, "snapshot state", err)
				return
			}
			item.ObjectCount += len(state.Objects)
			if !state.HasBaseline || state.HasGaps {
				item.Complete = false
				item.Missing = append(item.Missing, stream.StreamID)
			}
		}
		if !item.Complete {
			item.Warning = "coverage incomplete for stream(s): " + strings.Join(item.Missing, ", ")
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleListPlans(w http.ResponseWriter, r *http.Request) {
	s.planMu.RLock()
	items := make([]queryv1.ReplayPlan, 0, len(s.plans))
	for id, plan := range s.plans {
		items = append(items, s.mapPlan(plan, s.planTimes[id]))
	}
	s.planMu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	writeJSON(w, http.StatusOK, items)
}

type createPlanRequest struct {
	ClusterID       string `json:"cluster_id"`
	SnapshotID      string `json:"snapshot_id"`
	SourceNamespace string `json:"source_namespace"`
	TargetNamespace string `json:"target_namespace"`
	AllowGaps       bool   `json:"allow_gaps"`
}

func (s *Server) handleCreatePlan(w http.ResponseWriter, r *http.Request) {
	var req createPlanRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.ClusterID == "" || req.SnapshotID == "" {
		writeError(w, http.StatusBadRequest, "cluster_id and snapshot_id are required")
		return
	}
	policy := replay.DefaultPolicy()
	policy.AllowGaps = req.AllowGaps
	planner := s.planner
	if req.AllowGaps {
		planner = replay.NewPlanner(s.store, s.mat, policy)
	}
	plan, err := planner.Plan(r.Context(), req.ClusterID, req.SnapshotID, req.SourceNamespace, req.TargetNamespace)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "incomplete coverage"):
			writeError(w, http.StatusConflict, err.Error())
		case strings.Contains(err.Error(), "snapshot not found"):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			s.internalError(w, "plan", err)
		}
		return
	}
	now := time.Now().UTC()
	s.planMu.Lock()
	s.plans[plan.ID] = plan
	s.planTimes[plan.ID] = now
	s.planMu.Unlock()
	writeJSON(w, http.StatusOK, s.mapPlan(plan, now))
}

func (s *Server) handleGetPlan(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, created, ok := s.planRecord(id)
	if !ok {
		writeError(w, http.StatusNotFound, "plan not found")
		return
	}
	writeJSON(w, http.StatusOK, s.mapPlan(p, created))
}

type dryRunRequest struct {
	Kubeconfig    string `json:"kubeconfig"`
	TargetContext string `json:"target_context"`
}

func (s *Server) handleDryRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, created, ok := s.planRecord(id)
	if !ok {
		writeError(w, http.StatusNotFound, "plan not found")
		return
	}
	var req dryRunRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	res, err := p.DryRun(r.Context(), req.Kubeconfig, req.TargetContext)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	plan := s.mapPlan(p, created)
	plan.DryRunResult = mapDryRunResult(res)
	writeJSON(w, http.StatusOK, plan)
}

type replayRunRequest struct {
	PlanID        string `json:"plan_id"`
	Kubeconfig    string `json:"kubeconfig"`
	TargetContext string `json:"target_context"`
	Confirm       bool   `json:"confirm"`
}

func (s *Server) handleReplayRun(w http.ResponseWriter, r *http.Request) {
	var req replayRunRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.PlanID == "" {
		writeError(w, http.StatusBadRequest, "plan_id is required")
		return
	}
	p, ok := s.planByID(req.PlanID)
	if !ok {
		writeError(w, http.StatusNotFound, "plan not found")
		return
	}
	if !req.Confirm {
		writeError(w, http.StatusBadRequest, "refusing apply without confirm=true")
		return
	}
	res, err := p.Apply(r.Context(), req.Kubeconfig, req.TargetContext, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	writeJSON(w, http.StatusOK, queryv1.ReplayRun{
		ID:         req.PlanID + "-run",
		PlanID:     req.PlanID,
		Status:     "applied",
		StartedAt:  now,
		FinishedAt: &now,
		Applied:    res.Applied,
		Errors:     mapDryRunItems(res.Errors),
	})
}

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/v1") || strings.HasPrefix(r.URL.Path, "/metrics") {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if s.static == nil {
		http.NotFound(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	if !fileExists(s.static, path) {
		path = "index.html"
	}
	http.ServeFileFS(w, r, s.static, path)
}

func (s *Server) internalError(w http.ResponseWriter, op string, err error) {
	writeError(w, http.StatusInternalServerError, op+": "+err.Error())
}

// timelineEntry is a chronologically mergeable event or gap marker.
type timelineEntry struct {
	at  time.Time
	evt *event.Record
	gap *event.Record
}

// buildTimeline reduces the object's event records into a timeline, inserting
// stream gap markers and skipping bookmarks and errors. Summaries are derived
// by diffing consecutive object payloads.
func (s *Server) buildTimeline(recs []event.Record, gaps []event.Record) []queryv1.TimelineItem {
	merged := make([]timelineEntry, 0, len(recs)+len(gaps))
	for i := range recs {
		merged = append(merged, timelineEntry{at: recs[i].ObservedAt, evt: &recs[i]})
	}
	for i := range gaps {
		merged = append(merged, timelineEntry{at: gaps[i].ObservedAt, gap: &gaps[i]})
	}
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].at.Before(merged[j].at) })

	var items []queryv1.TimelineItem
	var prev *event.Record
	for _, e := range merged {
		if e.gap != nil {
			items = append(items, queryv1.TimelineItem{
				ObservedAt: e.gap.ObservedAt,
				Summary:    "GAP: watch history unavailable",
			})
			continue
		}
		r := e.evt
		if r.Type != event.TypeEvent {
			continue
		}
		if r.WatchType == event.WatchBookmark || r.WatchType == event.WatchError {
			continue
		}
		item := queryv1.TimelineItem{
			ObservedAt:      r.ObservedAt,
			ResourceVersion: r.Resource.ResourceVersion,
			WatchType:       string(r.WatchType),
			Synthetic:       r.Synthetic,
			ObjectHash:      r.ObjectHash,
		}
		if r.Provenance != nil {
			item.Provenance = r.Provenance
		}

		var summary string
		var changed []string
		switch {
		case r.Synthetic:
			summary = "baseline (synthetic)"
		case r.WatchType == event.WatchDeleted:
			summary = "object deleted"
		default:
			var prevObj json.RawMessage
			if prev != nil {
				prevObj = prev.Object
			}
			for _, c := range materialize.DiffObjects(prevObj, r.Object) {
				if c.Path == "" {
					if c.Added {
						summary = "object added"
					} else if c.Removed {
						summary = "object deleted"
					}
				} else {
					changed = append(changed, c.Path)
				}
			}
			if summary == "" && len(changed) > 0 {
				sort.Strings(changed)
				summary = changed[0] + " changed"
			}
		}
		item.Summary = summary
		item.ChangedFields = changed
		items = append(items, item)

		if r.WatchType == event.WatchDeleted {
			prev = nil
		} else {
			prev = r
		}
	}
	return items
}

func filterByTime(recs []event.Record, since, until time.Time) []event.Record {
	out := make([]event.Record, 0, len(recs))
	for _, r := range recs {
		if !since.IsZero() && r.ObservedAt.Before(since) {
			continue
		}
		if !until.IsZero() && r.ObservedAt.After(until) {
			continue
		}
		out = append(out, r)
	}
	return out
}

func objectKind(recs []event.Record) string {
	for _, r := range recs {
		if r.Resource.Kind != "" {
			return r.Resource.Kind
		}
	}
	return ""
}

func mapGaps(recs []event.Record) []queryv1.Gap {
	var out []queryv1.Gap
	for _, r := range recs {
		g := r.Gap
		if g == nil {
			continue
		}
		out = append(out, queryv1.Gap{
			StreamID:            r.StreamID,
			FromResourceVersion: g.FromResourceVersion,
			ToResourceVersion:   g.ToResourceVersion,
			Reason:              g.Reason,
			DetectedAt:          g.DetectedAt,
		})
	}
	return out
}

func mapDiffResult(clusterID, namespace string, d *materialize.DiffResult) queryv1.DiffResult {
	scope := namespace
	if scope == "" {
		scope = "all namespaces"
	}
	out := queryv1.DiffResult{
		ClusterID: clusterID,
		Scope:     scope,
		BeforeAt:  d.Before,
		AfterAt:   d.After,
		HasGaps:   d.HasGaps,
		Warning:   d.Warning,
	}
	for _, od := range d.Changes {
		changes := make([]queryv1.FieldChange, 0, len(od.Changes))
		for _, fc := range od.Changes {
			changes = append(changes, queryv1.FieldChange{
				Path:    fc.Path,
				Before:  fc.Before,
				After:   fc.After,
				Added:   fc.Added,
				Removed: fc.Removed,
			})
		}
		out.Changed = append(out.Changed, queryv1.ObjectDiff{
			Namespace: od.Namespace,
			Name:      od.Name,
			Kind:      od.Kind,
			Changes:   changes,
		})
	}
	return out
}

func (s *Server) mapPlan(p *replay.Plan, createdAt time.Time) queryv1.ReplayPlan {
	objects := make([]queryv1.PlanObject, 0, len(p.Objects))
	for _, o := range p.Objects {
		objects = append(objects, queryv1.PlanObject{
			Namespace: o.Namespace,
			Name:      o.Name,
			Kind:      o.Kind,
			Order:     o.Order,
			Object:    o.Object,
			Warnings:  o.Warnings,
		})
	}
	excluded := make([]queryv1.ExcludedObject, 0, len(p.Excluded))
	for _, e := range p.Excluded {
		excluded = append(excluded, queryv1.ExcludedObject{
			Namespace: e.Namespace,
			Name:      e.Name,
			Kind:      e.Kind,
			Reason:    e.Reason,
		})
	}
	return queryv1.ReplayPlan{
		ID:               p.ID,
		ClusterID:        p.ClusterID,
		SnapshotID:       p.SnapshotID,
		SourceNamespace:  p.SourceNamespace,
		TargetNamespace:  p.TargetNamespace,
		TargetContext:    p.TargetContext,
		CreatedAt:        createdAt,
		FieldManager:     p.FieldManager,
		Objects:          objects,
		Warnings:         p.Warnings,
		Excluded:         excluded,
		CoverageComplete: p.CoverageComplete,
		Status:           p.Status,
	}
}

func mapDryRunResult(d *replay.DryRunResult) *queryv1.DryRunResult {
	return &queryv1.DryRunResult{
		Applied:   d.Applied,
		Conflicts: mapDryRunItems(d.Conflicts),
		Errors:    mapDryRunItems(d.Errors),
		OK:        d.OK,
	}
}

func mapDryRunItems(items []replay.DryRunItem) []queryv1.DryRunItem {
	out := make([]queryv1.DryRunItem, 0, len(items))
	for _, it := range items {
		out = append(out, queryv1.DryRunItem{
			Namespace: it.Namespace,
			Name:      it.Name,
			Kind:      it.Kind,
			Manager:   it.Manager,
			Message:   it.Message,
		})
	}
	return out
}

// EncodeObjectRef serializes an object reference into a single URL-safe token.
// It exists because stream IDs contain slashes and cannot be path segments.
func EncodeObjectRef(ref storage.ObjectRef) string {
	raw := ref.ClusterID + "\x00" + ref.StreamID + "\x00" + ref.Namespace + "\x00" + ref.Name
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeObjectRef reverses EncodeObjectRef.
func DecodeObjectRef(token string) (storage.ObjectRef, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return storage.ObjectRef{}, fmt.Errorf("decode: %w", err)
	}
	parts := strings.SplitN(string(raw), "\x00", 4)
	if len(parts) != 4 {
		return storage.ObjectRef{}, fmt.Errorf("malformed ref token")
	}
	return storage.ObjectRef{
		ClusterID: parts[0],
		StreamID:  parts[1],
		Namespace: parts[2],
		Name:      parts[3],
	}, nil
}

// mapEvent normalizes a journal record for API consumption.
func mapEvent(r event.Record) queryv1.Event {
	var object any
	if len(r.Object) > 0 {
		var v any
		if err := json.Unmarshal(r.Object, &v); err == nil {
			object = v
		}
	}
	var provenance any
	if r.Provenance != nil {
		provenance = r.Provenance
	}
	return queryv1.Event{
		ClusterID:       r.ClusterID,
		StreamID:        r.StreamID,
		RecordType:      string(r.Type),
		EventID:         r.EventID,
		IngestSeq:       r.IngestSeq,
		ObservedAt:      r.ObservedAt,
		WatchType:       string(r.WatchType),
		Synthetic:       r.Synthetic,
		Namespace:       r.Resource.Namespace,
		Name:            r.Resource.Name,
		UID:             r.Resource.UID,
		Kind:            r.Resource.Kind,
		ResourceVersion: r.Resource.ResourceVersion,
		ObjectHash:      r.ObjectHash,
		Object:          object,
		Provenance:      provenance,
	}
}

// mapStream normalizes a storage stream metadata for API consumption.
func mapStream(m storage.StreamMeta) queryv1.Stream {
	return queryv1.Stream{
		ID:                  m.StreamID,
		ClusterID:           m.ClusterID,
		Group:               m.Group,
		Version:             m.Version,
		Resource:            m.Resource,
		Kind:                m.Kind,
		Namespace:           m.Namespace,
		Selector:            m.Selector,
		Available:           m.Available,
		FirstObservedAt:     m.FirstObservedAt,
		LastObservedAt:      m.LastObservedAt,
		LastResourceVersion: m.LastResourceVersion,
		GapCount:            m.GapCount,
		HasGaps:             m.HasGaps,
		Degraded:            m.Degraded,
	}
}

func encodeCursor(seq int64) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(seq, 10)))
}

func decodeCursor(c string) (int64, error) {
	raw, err := base64.StdEncoding.DecodeString(c)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(raw), 10, 64)
}

// parseTimeParam accepts "now", a Go duration relative to now (for example
// "30m"), or an RFC3339 timestamp. An empty string means no bound.
func parseTimeParam(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if strings.EqualFold(s, "now") {
		return time.Now(), nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return time.Now().Add(-d), nil
	}
	return time.Parse(time.RFC3339, s)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func fileExists(fsys fs.FS, name string) bool {
	_, err := fs.Stat(fsys, name)
	return err == nil
}
