// Package audit correlates Kubernetes audit-log entries with events stored in
// the journal. Correlation is best-effort: an audit entry is only persisted as
// a TypeAuditCorrelation record when it matches an already-recorded watch
// event on (cluster, namespace, name, uid, resourceVersion).
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
)

// ErrNoMatch is returned when no stored event matches an audit entry.
var ErrNoMatch = errors.New("no matching journal event")

// AuditEvent is a normalized view of one Kubernetes audit-log entry.
// Resource is the plural resource name (e.g. "configmaps").
type AuditEvent struct {
	ClusterID       string
	RequestID       string
	Verb            string
	Resource        string
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
	User            string
	UserAgent       string
	SourceIPs       []string
	ResponseCode    int
	Stage           string
	Object          json.RawMessage
	ResponseObject  json.RawMessage
	Annotations     map[string]string
	Timestamp       time.Time
}

// Correlator matches audit entries against stored events and writes
// TypeAuditCorrelation records into a Store.
type Correlator struct {
	store storage.Store
}

// NewCorrelator returns a Correlator backed by the given Store.
func NewCorrelator(store storage.Store) *Correlator {
	return &Correlator{store: store}
}

// Match searches the store for an EVENT record whose (ClusterID,
// Resource.Namespace, Resource.Name, Resource.UID) equal the audit event's and
// whose Resource.ResourceVersion equals evt.ResourceVersion. It returns the
// matched record's EventID, or ErrNoMatch when no event matches.
func (c *Correlator) Match(ctx context.Context, evt AuditEvent) (string, error) {
	rec, err := c.matchRecord(ctx, evt)
	if err != nil {
		return "", err
	}
	return rec.EventID, nil
}

func (c *Correlator) matchRecord(ctx context.Context, evt AuditEvent) (*event.Record, error) {
	recs, err := c.store.Events(ctx, storage.EventFilter{
		ClusterID:  evt.ClusterID,
		Namespace:  evt.Namespace,
		Name:       evt.Name,
		RecordType: event.TypeEvent,
	})
	if err != nil {
		return nil, err
	}
	for i := range recs {
		r := &recs[i]
		if r.Resource.UID == evt.UID && r.Resource.ResourceVersion == evt.ResourceVersion {
			return r, nil
		}
	}
	return nil, ErrNoMatch
}
