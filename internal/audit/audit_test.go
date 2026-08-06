package audit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
)

func TestCorrelatorIngest(t *testing.T) {
	ctx := context.Background()
	store := storage.NewInMemory()

	stream := event.Stream{
		ClusterID: "c1",
		Group:     "",
		Version:   "v1",
		Resource:  "configmaps",
		Namespace: "default",
	}
	ref := event.ResourceRef{
		Namespace:       "default",
		Name:            "my-cm",
		UID:             "uid-1",
		ResourceVersion: "1234",
		Kind:            "ConfigMap",
		Version:         "v1",
	}
	watched := &event.Record{
		ClusterID:  "c1",
		StreamID:   stream.ID(),
		Type:       event.TypeEvent,
		EventID:    event.EventID(stream, ref, event.WatchModified, 0),
		ObservedAt: time.Now().UTC(),
		WatchType:  event.WatchModified,
		Resource:   ref,
		Object:     json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"my-cm","resourceVersion":"1234","uid":"uid-1"}}`),
	}
	if _, err := store.Append(ctx, watched); err != nil {
		t.Fatalf("append watched event: %v", err)
	}

	evt := AuditEvent{
		ClusterID:       "c1",
		RequestID:       "req-1",
		Verb:            "update",
		Resource:        "configmaps",
		Namespace:       "default",
		Name:            "my-cm",
		UID:             "uid-1",
		ResourceVersion: "1234",
		User:            "alice",
		UserAgent:       "kubectl/v1.30.0",
		SourceIPs:       []string{"10.0.0.1"},
		ResponseCode:    200,
		Stage:           "ResponseComplete",
		Object:          json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"my-cm"}}`),
		Timestamp:       time.Now().UTC(),
	}

	c := NewCorrelator(store)
	matchedID, err := c.Match(ctx, evt)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if matchedID != watched.EventID {
		t.Fatalf("Match event id = %q, want %q", matchedID, watched.EventID)
	}

	if err := c.Ingest(ctx, []AuditEvent{evt}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	recs, err := store.Events(ctx, storage.EventFilter{RecordType: event.TypeAuditCorrelation})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("correlation records = %d, want 1", len(recs))
	}
	rec := recs[0]
	if rec.Type != event.TypeAuditCorrelation {
		t.Fatalf("record type = %q, want %q", rec.Type, event.TypeAuditCorrelation)
	}
	if rec.ClusterID != "c1" {
		t.Errorf("cluster id = %q, want c1", rec.ClusterID)
	}
	if rec.StreamID != stream.ID() {
		t.Errorf("stream id = %q, want %q", rec.StreamID, stream.ID())
	}
	if rec.Resource != ref {
		t.Errorf("resource = %+v, want %+v", rec.Resource, ref)
	}
	if !strings.HasPrefix(rec.EventID, "audit-") {
		t.Errorf("event id %q has no audit- prefix", rec.EventID)
	}
	if rec.Provenance == nil {
		t.Fatal("provenance is nil")
	}
	if rec.Provenance.User != "alice" {
		t.Errorf("provenance user = %q, want alice", rec.Provenance.User)
	}
	if rec.Provenance.Verb != "update" {
		t.Errorf("provenance verb = %q, want update", rec.Provenance.Verb)
	}
	if rec.Provenance.Response != 200 {
		t.Errorf("provenance response = %d, want 200", rec.Provenance.Response)
	}
	if rec.Provenance.RequestID != "req-1" {
		t.Errorf("provenance request id = %q, want req-1", rec.Provenance.RequestID)
	}
}

func TestCorrelatorIngestSkipsUnmatched(t *testing.T) {
	ctx := context.Background()
	store := storage.NewInMemory()

	evt := AuditEvent{
		ClusterID:       "c1",
		RequestID:       "req-unmatched",
		Verb:            "update",
		Resource:        "configmaps",
		Namespace:       "default",
		Name:            "other-cm",
		UID:             "uid-x",
		ResourceVersion: "9999",
	}

	c := NewCorrelator(store)
	if _, err := c.Match(ctx, evt); err != ErrNoMatch {
		t.Fatalf("Match err = %v, want ErrNoMatch", err)
	}
	if err := c.Ingest(ctx, []AuditEvent{evt}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	recs, err := store.Events(ctx, storage.EventFilter{RecordType: event.TypeAuditCorrelation})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("correlation records = %d, want 0 (best-effort skip)", len(recs))
	}
}

func TestParseAuditLog(t *testing.T) {
	const log = "" +
		`{"kind":"Event","apiVersion":"audit.k8s.io/v1","level":"RequestResponse","auditID":"11111111-1111-1111-1111-111111111111","stage":"ResponseComplete","requestURI":"/api/v1/namespaces/default/configmaps","verb":"create","user":{"username":"alice","groups":["system:masters"],"uid":"u-1"},"sourceIPs":["10.0.0.1","192.168.1.2"],"userAgent":"kubectl/v1.30.0","objectRef":{"namespace":"default","name":"web-cm","uid":"uid-a","resource":"configmaps","resourceVersion":"1001"},"responseStatus":{"code":201},"requestObject":{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"web-cm"}},"responseObject":{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"web-cm","resourceVersion":"1001","uid":"uid-a"}},"annotations":{"authorization.k8s.io/decision":"allow"},"requestReceivedTimestamp":"2026-08-06T10:00:00Z"}` + "\n" +
		`{"kind":"Event","apiVersion":"audit.k8s.io/v1","level":"Request","auditID":"22222222-2222-2222-2222-222222222222","stage":"RequestReceived","requestURI":"/api/v1/namespaces/default/configmaps/web-cm","verb":"update","user":{"username":"bob"},"sourceIPs":["10.0.0.2"],"userAgent":"kubelet/v1.30.0","objectRef":{"namespace":"default","name":"web-cm","uid":"uid-a","resource":"configmaps","resourceVersion":"1002"},"responseStatus":{"code":200},"requestObject":{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"web-cm","resourceVersion":"1002"}},"requestReceivedTimestamp":"2026-08-06T10:01:00Z"}` + "\n" +
		`this line is not valid json` + "\n"

	evts, err := ParseAuditLog(strings.NewReader(log))
	if err != nil {
		t.Fatalf("ParseAuditLog: %v", err)
	}
	if len(evts) != 2 {
		t.Fatalf("events = %d, want 2 (malformed line skipped)", len(evts))
	}

	first := evts[0]
	if first.RequestID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("request id = %q", first.RequestID)
	}
	if first.Verb != "create" {
		t.Errorf("verb = %q, want create", first.Verb)
	}
	if first.Resource != "configmaps" {
		t.Errorf("resource = %q, want configmaps", first.Resource)
	}
	if first.Namespace != "default" || first.Name != "web-cm" || first.UID != "uid-a" {
		t.Errorf("objectRef = %s/%s uid %s", first.Namespace, first.Name, first.UID)
	}
	if first.ResourceVersion != "1001" {
		t.Errorf("resource version = %q, want 1001", first.ResourceVersion)
	}
	if first.User != "alice" {
		t.Errorf("user = %q, want alice", first.User)
	}
	if len(first.SourceIPs) != 2 || first.SourceIPs[0] != "10.0.0.1" || first.SourceIPs[1] != "192.168.1.2" {
		t.Errorf("source ips = %v", first.SourceIPs)
	}
	if !strings.HasPrefix(first.UserAgent, "kubectl") {
		t.Errorf("user agent = %q", first.UserAgent)
	}
	if first.ResponseCode != 201 {
		t.Errorf("response code = %d, want 201", first.ResponseCode)
	}
	if first.Stage != "ResponseComplete" {
		t.Errorf("stage = %q, want ResponseComplete", first.Stage)
	}
	wantTS := time.Date(2026, 8, 6, 10, 0, 0, 0, time.UTC)
	if !first.Timestamp.Equal(wantTS) {
		t.Errorf("timestamp = %v, want %v", first.Timestamp, wantTS)
	}
	if len(first.Object) == 0 || !strings.Contains(string(first.Object), `"resourceVersion":"1001"`) {
		t.Errorf("object = %s, want responseObject payload", first.Object)
	}
	if len(first.ResponseObject) == 0 {
		t.Error("response object is empty")
	}

	second := evts[1]
	if second.ResponseCode != 200 {
		t.Errorf("second response code = %d, want 200", second.ResponseCode)
	}
	if len(second.ResponseObject) != 0 {
		t.Errorf("second response object = %s, want empty", second.ResponseObject)
	}
	if len(second.Object) == 0 || !strings.Contains(string(second.Object), `"resourceVersion":"1002"`) {
		t.Errorf("second object = %s, want requestObject payload", second.Object)
	}
}
