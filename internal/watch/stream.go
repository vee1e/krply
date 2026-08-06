package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	"github.com/krply/krply/internal/discovery"
	"github.com/krply/krply/internal/event"
)

// Sentinel errors. Only errStore is unrecoverable; the others trigger a relist
// or a reconnect from the last durable resource version.
var (
	errGone      = errors.New("watch: 410 Gone received")
	errReconnect = errors.New("watch: stream ended, reconnect")
	errStore     = errors.New("watch: store failure")
)

func storeErrf(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errStore, fmt.Sprintf(format, a...))
}

// runStream runs one list-and-watch loop for a single resource. It never
// returns on transient errors; it returns nil on ctx.Done and errStore (or
// another wrapped unrecoverable error) otherwise.
func (c *Collector) runStream(ctx context.Context, spec discovery.ResourceSpec) error {
	stream, sid := streamIDFor(spec, c.cfg.ClusterID)
	stream.Selector = c.cfg.Selector
	sid = stream.ID()
	log := c.cfg.Log.With("stream", sid)

	gvr := gvrFor(spec)
	nsri := c.dyn.Resource(gvr)
	var ri dynamic.ResourceInterface = nsri
	if spec.Namespace != "" {
		ri = nsri.Namespace(spec.Namespace)
	}

	var lastRV string
	backoff := c.cfg.MinBackoff

	for {
		if ctx.Err() != nil {
			return nil
		}

		if lastRV == "" {
			rv, err := c.listAndBaseline(ctx, stream, sid, spec, ri)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, errStore) {
					return err
				}
				log.Error("list failed", "error", err)
				if err := c.sleepBackoff(ctx, &backoff); err != nil {
					return nil
				}
				continue
			}
			if rv == "" {
				// A relist without a resource version would restart the watch
				// from an arbitrary point and loop forever.
				log.Warn("list returned an empty resourceVersion; backing off and relisting")
				if err := c.sleepBackoff(ctx, &backoff); err != nil {
					return nil
				}
				continue
			}
			lastRV = rv
			backoff = c.cfg.MinBackoff
		}

		w, err := ri.Watch(ctx, metav1.ListOptions{
			ResourceVersion:     lastRV,
			LabelSelector:       c.cfg.Selector,
			AllowWatchBookmarks: c.cfg.Bookmarks,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Warn("watch start failed", "error", err)
			if err := c.sleepBackoff(ctx, &backoff); err != nil {
				return nil
			}
			continue
		}
		if m := c.cfg.Metrics; m != nil {
			m.Reconnects.WithLabelValues(sid).Inc()
		}

		err = c.drainWatch(ctx, stream, sid, spec, w, &lastRV)
		w.Stop()
		if err == nil || ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errStore) {
			return err
		}
		if errors.Is(err, errGone) {
			// Persistent 410s (aggressive etcd compaction, flaky aggregated
			// servers) would otherwise hammer the apiserver with an unthrottled
			// list/watch/gap loop.
			log.Warn("410 Gone received; relisting after backoff")
			lastRV = ""
			if err := c.sleepBackoff(ctx, &backoff); err != nil {
				return nil
			}
			continue
		}
		log.Warn("watch stream ended; reconnecting from last durable RV", "error", err)
		if err := c.sleepBackoff(ctx, &backoff); err != nil {
			return nil
		}
	}
}

// listAndBaseline lists the collection, persists a BASELINE record, then one
// synthetic ADDED record per item, and returns the collection resource version
// to watch from.
func (c *Collector) listAndBaseline(ctx context.Context, stream event.Stream, sid string, spec discovery.ResourceSpec, ri dynamic.ResourceInterface) (string, error) {
	list, err := ri.List(ctx, metav1.ListOptions{ResourceVersion: "0", LabelSelector: c.cfg.Selector})
	if err != nil {
		return "", err
	}
	listRV := list.GetResourceVersion()
	now := time.Now().UTC()

	recs := make([]*event.Record, 0, len(list.Items)+1)
	recs = append(recs, &event.Record{
		ClusterID:  c.cfg.ClusterID,
		StreamID:   sid,
		Type:       event.TypeBaseline,
		ObservedAt: now,
		Resource: event.ResourceRef{
			Group:           spec.APIGroup,
			Version:         spec.Version,
			Kind:            spec.Kind,
			Namespace:       spec.Namespace,
			ResourceVersion: listRV,
		},
	})
	for i := range list.Items {
		obj := &list.Items[i]
		raw, err := json.Marshal(obj.Object)
		if err != nil {
			return "", fmt.Errorf("marshal listed object: %w", err)
		}
		ref := refFromObject(spec, obj)
		recs = append(recs, &event.Record{
			ClusterID:  c.cfg.ClusterID,
			StreamID:   sid,
			Type:       event.TypeEvent,
			EventID:    event.EventID(stream, ref, event.WatchAdded, now.UnixNano()),
			ObservedAt: now,
			WatchType:  event.WatchAdded,
			Synthetic:  true,
			Resource:   ref,
			ObjectHash: event.ObjectHash(raw),
			Object:     raw,
		})
	}
	if _, err := c.cfg.Store.Appends(ctx, recs); err != nil {
		return "", storeErrf("append baseline: %v", err)
	}
	return listRV, nil
}

// drainWatch consumes a watch stream, persisting every event. It returns nil
// when the context is cancelled, errGone on 410 (gap already recorded),
// errReconnect on other errors, errStore on a store failure, or a plain error
// when the channel closes. Only a 410 loses data; every other teardown resumes
// from the last durable resource version, so no gap is recorded for them.
func (c *Collector) drainWatch(ctx context.Context, stream event.Stream, sid string, spec discovery.ResourceSpec, w watch.Interface, lastRV *string) error {
	var idle *time.Timer
	var idleC <-chan time.Time
	if c.cfg.WatchIdleTimeout > 0 {
		idle = time.NewTimer(c.cfg.WatchIdleTimeout)
		defer idle.Stop()
		idleC = idle.C
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-idleC:
			// No event (and no bookmark) for a full idle window: the connection
			// is likely dead without a close. Force a reconnect so recording
			// does not stall silently.
			return errReconnect
		case ev, ok := <-w.ResultChan():
			if !ok {
				return errors.New("watch channel closed")
			}
			if idle != nil {
				if !idle.Reset(c.cfg.WatchIdleTimeout) {
					select {
					case <-idle.C:
					default:
					}
				}
			}
			switch ev.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				obj, ok := ev.Object.(*unstructured.Unstructured)
				if !ok {
					return fmt.Errorf("unexpected watch object type %T for %s", ev.Object, ev.Type)
				}
				rec, err := c.watchEventRecord(stream, sid, spec, event.WatchType(ev.Type), obj, false)
				if err != nil {
					return err
				}
				if _, err := c.cfg.Store.Append(ctx, rec); err != nil {
					return storeErrf("append watch event: %v", err)
				}
				if m := c.cfg.Metrics; m != nil {
					m.EventsIngested.WithLabelValues(sid).Inc()
					m.IngestLag.Set(time.Since(rec.ObservedAt).Seconds())
				}
				if rec.Resource.ResourceVersion != "" {
					*lastRV = rec.Resource.ResourceVersion
				}

			case watch.Bookmark:
				rv := objRV(ev.Object)
				if rv == "" {
					continue
				}
				if *lastRV != "" && resourceVersionBefore(rv, *lastRV) {
					continue
				}
				now := time.Now().UTC()
				rec := &event.Record{
					ClusterID:  c.cfg.ClusterID,
					StreamID:   sid,
					Type:       event.TypeCheckpoint,
					ObservedAt: now,
					Resource: event.ResourceRef{
						Group:           spec.APIGroup,
						Version:         spec.Version,
						Kind:            spec.Kind,
						Namespace:       spec.Namespace,
						ResourceVersion: rv,
					},
					Checkpoint: &event.CheckpointInfo{
						ResourceVersion: rv,
						BookmarkedAt:    now,
					},
				}
				if _, err := c.cfg.Store.Append(ctx, rec); err != nil {
					return storeErrf("append bookmark: %v", err)
				}
				*lastRV = rv

			case watch.Error:
				if errorCode(ev.Object) == goneCode {
					if err := c.writeGap(ctx, sid, spec, *lastRV, "", "410 Gone"); err != nil {
						return err
					}
					if m := c.cfg.Metrics; m != nil {
						m.Gone410.WithLabelValues(sid).Inc()
					}
					return errGone
				}
				reason := "watch error"
				if st := statusOf(ev.Object); st != nil && st.Message != "" {
					reason = st.Message
				}
				c.cfg.Log.Warn("watch error (not a gap); reconnecting from last durable RV", "reason", reason)
				return errReconnect
			}
		}
	}
}

// resourceVersionBefore reports whether a < b when both parse as integers.
// Non-numeric resource versions are opaque and treated as incomparable.
func resourceVersionBefore(a, b string) bool {
	ai, aerr := strconv.ParseInt(a, 10, 64)
	bi, berr := strconv.ParseInt(b, 10, 64)
	if aerr != nil || berr != nil {
		return false
	}
	return ai < bi
}

func (c *Collector) watchEventRecord(stream event.Stream, sid string, spec discovery.ResourceSpec, wt event.WatchType, obj *unstructured.Unstructured, synthetic bool) (*event.Record, error) {
	raw, err := json.Marshal(obj.Object)
	if err != nil {
		return nil, fmt.Errorf("marshal watch object: %w", err)
	}
	ref := refFromObject(spec, obj)
	return &event.Record{
		ClusterID:  c.cfg.ClusterID,
		StreamID:   sid,
		Type:       event.TypeEvent,
		EventID:    event.EventID(stream, ref, wt, 0),
		ObservedAt: time.Now().UTC(),
		WatchType:  wt,
		Synthetic:  synthetic,
		Resource:   ref,
		ObjectHash: event.ObjectHash(raw),
		Object:     raw,
	}, nil
}

func (c *Collector) writeGap(ctx context.Context, sid string, spec discovery.ResourceSpec, fromRV, toRV, reason string) error {
	now := time.Now().UTC()
	rec := &event.Record{
		ClusterID:  c.cfg.ClusterID,
		StreamID:   sid,
		Type:       event.TypeGap,
		ObservedAt: now,
		Resource: event.ResourceRef{
			Group:     spec.APIGroup,
			Version:   spec.Version,
			Kind:      spec.Kind,
			Namespace: spec.Namespace,
		},
		Gap: &event.GapInfo{
			FromResourceVersion: fromRV,
			ToResourceVersion:   toRV,
			Reason:              reason,
			DetectedAt:          now,
		},
	}
	if _, err := c.cfg.Store.Append(ctx, rec); err != nil {
		return storeErrf("append gap: %v", err)
	}
	if m := c.cfg.Metrics; m != nil {
		m.Gaps.WithLabelValues(sid).Inc()
	}
	return nil
}

// sleepBackoff waits with exponential growth and jitter between MinBackoff and
// MaxBackoff. It returns ctx.Err() when cancelled.
func (c *Collector) sleepBackoff(ctx context.Context, current *time.Duration) error {
	d := *current
	if d < c.cfg.MinBackoff {
		d = c.cfg.MinBackoff
	}
	jitter := time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
	t := time.NewTimer(jitter)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}
	next := d * 2
	if next > c.cfg.MaxBackoff {
		next = c.cfg.MaxBackoff
	}
	*current = next
	return nil
}
