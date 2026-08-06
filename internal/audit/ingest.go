package audit

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/krply/krply/internal/event"
)

// Ingest correlates audit events with stored journal events and appends a
// TypeAuditCorrelation record for each match. Events that match nothing are
// skipped silently. Multiple matches are written in a single Appends batch.
func (c *Correlator) Ingest(ctx context.Context, events []AuditEvent) error {
	var batch []*event.Record
	for i := range events {
		evt := events[i]
		matched, err := c.matchRecord(ctx, evt)
		if errors.Is(err, ErrNoMatch) {
			continue
		}
		if err != nil {
			return err
		}
		batch = append(batch, correlationRecord(evt, matched))
	}
	if len(batch) == 0 {
		return nil
	}
	_, err := c.store.Appends(ctx, batch)
	return err
}

// correlationRecord builds the deterministic TypeAuditCorrelation record that
// links an audit event to its matched journal event.
func correlationRecord(evt AuditEvent, matched *event.Record) *event.Record {
	return &event.Record{
		ClusterID:  evt.ClusterID,
		StreamID:   matched.StreamID,
		Type:       event.TypeAuditCorrelation,
		EventID:    auditEventID(evt),
		ObservedAt: evt.Timestamp,
		Resource:   matched.Resource,
		Provenance: &event.Provenance{
			RequestID:   evt.RequestID,
			User:        evt.User,
			UserAgent:   evt.UserAgent,
			SourceIPs:   evt.SourceIPs,
			Verb:        evt.Verb,
			Stage:       evt.Stage,
			Response:    evt.ResponseCode,
			Annotations: evt.Annotations,
		},
	}
}

// auditEventID derives a deterministic key for an audit event. The same
// request against the same object always produces the same ID.
func auditEventID(evt AuditEvent) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\x00%s", evt.ClusterID, evt.RequestID, evt.Namespace, evt.Name, evt.UID, evt.ResourceVersion)
	return "audit-" + hex.EncodeToString(h.Sum(nil))
}

// auditV1Event is the minimal shape of a Kubernetes audit event
// (k8s.io/apiserver/pkg/apis/audit.v1.Event) needed for correlation.
type auditV1Event struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Level      string `json:"level"`
	AuditID    string `json:"auditID"`
	Stage      string `json:"stage"`
	RequestURI string `json:"requestURI"`
	Verb       string `json:"verb"`
	User       struct {
		Username string   `json:"username"`
		Groups   []string `json:"groups"`
		UID      string   `json:"uid"`
	} `json:"user"`
	SourceIPs []string `json:"sourceIPs"`
	UserAgent string   `json:"userAgent"`
	ObjectRef struct {
		Namespace       string `json:"namespace"`
		Name            string `json:"name"`
		UID             string `json:"uid"`
		Resource        string `json:"resource"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"objectRef"`
	ResponseStatus struct {
		Code int `json:"code"`
	} `json:"responseStatus"`
	RequestObject            json.RawMessage   `json:"requestObject"`
	ResponseObject           json.RawMessage   `json:"responseObject"`
	Annotations              map[string]string `json:"annotations"`
	RequestReceivedTimestamp time.Time         `json:"requestReceivedTimestamp"`
}

// ParseAuditLog parses newline-delimited JSON Kubernetes audit entries. Lines
// that fail to parse are skipped. The responseObject is preferred as Object
// when present, falling back to requestObject.
func ParseAuditLog(r io.Reader) ([]AuditEvent, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var events []AuditEvent
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev auditV1Event
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}
		events = append(events, ev.auditEvent())
	}
	return events, sc.Err()
}

func (e *auditV1Event) auditEvent() AuditEvent {
	object := e.ResponseObject
	if len(object) == 0 {
		object = e.RequestObject
	}
	return AuditEvent{
		RequestID:       e.AuditID,
		Verb:            e.Verb,
		Resource:        e.ObjectRef.Resource,
		Namespace:       e.ObjectRef.Namespace,
		Name:            e.ObjectRef.Name,
		UID:             e.ObjectRef.UID,
		ResourceVersion: e.ObjectRef.ResourceVersion,
		User:            e.User.Username,
		UserAgent:       e.UserAgent,
		SourceIPs:       e.SourceIPs,
		ResponseCode:    e.ResponseStatus.Code,
		Stage:           e.Stage,
		Object:          object,
		ResponseObject:  e.ResponseObject,
		Annotations:     e.Annotations,
		Timestamp:       e.RequestReceivedTimestamp,
	}
}
