package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	queryv1 "github.com/krply/krply/api/query/v1"
	"github.com/krply/krply/internal/api"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/materialize"
	"github.com/krply/krply/internal/storage"
)

var (
	timelineNamespace string
	timelineSince     string
	timelineKind      string
)

var timelineCmd = &cobra.Command{
	Use:   "timeline <name>",
	Short: "show the event timeline for one object",
	Args:  cobra.ExactArgs(1),
	RunE:  runTimeline,
}

func init() {
	rootCmd.AddCommand(timelineCmd)
	f := timelineCmd.Flags()
	f.StringVar(&timelineNamespace, "namespace", "", "namespace of the object (auto-detected when empty)")
	f.StringVar(&timelineSince, "since", "", "only events observed after this time (RFC3339 or duration like 30m)")
	f.StringVar(&timelineKind, "kind", "", "kind of the object (disambiguates multiple streams)")
}

func runTimeline(cmd *cobra.Command, args []string) error {
	name := args[0]
	since, err := parseTimeFlag(timelineSince, time.Now())
	if err != nil {
		return err
	}
	if serverURL != "" {
		return timelineServer(cmd.Context(), name, since)
	}

	store, err := openStore(storePath)
	if err != nil {
		return err
	}
	defer closeStore(store)

	namespace, err := resolveObjectNamespace(cmd.Context(), store, name, timelineNamespace, timelineKind)
	if err != nil {
		return err
	}

	streams, err := streamsForObject(cmd.Context(), store, namespace, timelineKind)
	if err != nil {
		return err
	}
	if len(streams) == 0 {
		return fmt.Errorf("no stream found for object %s/%s", namespace, name)
	}
	stream := streams[0]

	recs, err := store.ObjectHistory(cmd.Context(), storage.ObjectRef{
		ClusterID: stream.ClusterID,
		StreamID:  stream.StreamID,
		Namespace: namespace,
		Name:      name,
	})
	if err != nil {
		return err
	}

	gaps, err := store.Gaps(cmd.Context(), stream.StreamID)
	if err != nil {
		return err
	}

	baselines, err := store.Baselines(cmd.Context(), stream.StreamID)
	if err != nil {
		return err
	}

	rows := buildTimeline(recs, gaps, baselines, since)
	printTimeline(rows)
	return nil
}

func timelineServer(ctx context.Context, name string, since time.Time) error {
	c := newAPIClient(serverURL)
	var streams []queryv1.Stream
	if err := c.get("/v1/streams", &streams); err != nil {
		return err
	}
	var match *queryv1.Stream
	for i := range streams {
		s := &streams[i]
		if timelineNamespace != "" && s.Namespace != "" && s.Namespace != timelineNamespace {
			continue
		}
		if timelineKind != "" && !strings.EqualFold(s.Kind, timelineKind) {
			continue
		}
		match = s
		break
	}
	if match == nil {
		return fmt.Errorf("no stream found for object %s/%s", nsLabel(timelineNamespace), name)
	}

	ref := api.EncodeObjectRef(storage.ObjectRef{
		ClusterID: match.ClusterID,
		StreamID:  match.ID,
		Namespace: timelineNamespace,
		Name:      name,
	})
	path := "/v1/objects/" + url.PathEscape(ref) + "/history"
	q := url.Values{}
	if !since.IsZero() {
		q.Set("since", since.Format(time.RFC3339))
	}
	var hist queryv1.ObjectHistory
	if err := c.get(withQuery(path, q), &hist); err != nil {
		return err
	}

	rows := make([]tlRow, 0, len(hist.Items))
	for _, it := range hist.Items {
		rows = append(rows, tlRow{
			observed:  it.ObservedAt,
			watchType: it.WatchType,
			rv:        it.ResourceVersion,
			summary:   it.Summary,
			synthetic: it.Synthetic,
		})
	}
	for _, g := range hist.Gaps {
		rows = append(rows, tlRow{
			observed:  g.DetectedAt,
			watchType: "GAP",
			summary:   fmt.Sprintf("gap %s -> %s: %s", g.FromResourceVersion, g.ToResourceVersion, g.Reason),
			synthetic: true,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].observed.Before(rows[j].observed) })
	printTimeline(rows)
	return nil
}

// streamsForObject returns the streams that could contain the object, ordered
// by preference: exact namespace match first, cluster-wide streams next.
func streamsForObject(ctx context.Context, store storage.Store, namespace, kind string) ([]storage.StreamMeta, error) {
	streams, err := store.Streams(ctx)
	if err != nil {
		return nil, err
	}
	var out []storage.StreamMeta
	for _, s := range streams {
		if namespace != "" && s.Namespace != "" && s.Namespace != namespace {
			continue
		}
		if kind != "" && !strings.EqualFold(s.Kind, kind) {
			continue
		}
		out = append(out, s)
	}
	sort.SliceStable(out, func(i, j int) bool {
		rank := func(ns string) int {
			switch {
			case namespace != "" && ns == namespace:
				return 0
			case ns == "":
				return 1
			default:
				return 2
			}
		}
		return rank(out[i].Namespace) < rank(out[j].Namespace)
	})
	return out, nil
}

// resolveObjectNamespace finds the namespace of an object when it was not
// provided, by searching the journal for the object's name.
func resolveObjectNamespace(ctx context.Context, store storage.Store, name, namespace, kind string) (string, error) {
	if namespace != "" {
		return namespace, nil
	}
	recs, err := store.Events(ctx, storage.EventFilter{Name: name, Kind: kind})
	if err != nil {
		return "", err
	}
	for i := range recs {
		if recs[i].Resource.Namespace != "" {
			return recs[i].Resource.Namespace, nil
		}
	}
	return "", fmt.Errorf("could not determine namespace for object %q (pass --namespace)", name)
}

type tlRow struct {
	observed  time.Time
	watchType string
	rv        string
	summary   string
	synthetic bool
}

func buildTimeline(recs []event.Record, gaps []event.Record, baselines []event.Record, since time.Time) []tlRow {
	var rows []tlRow

	markers := make([]event.Record, 0, len(gaps)+len(baselines))
	markers = append(markers, gaps...)
	markers = append(markers, baselines...)
	for i := range markers {
		m := markers[i]
		if !since.IsZero() && m.ObservedAt.Before(since) {
			continue
		}
		switch m.Type {
		case event.TypeGap:
			from, to, reason := "?", "?", ""
			if m.Gap != nil {
				from, to, reason = m.Gap.FromResourceVersion, m.Gap.ToResourceVersion, m.Gap.Reason
			}
			rows = append(rows, tlRow{observed: m.ObservedAt, watchType: "GAP", summary: fmt.Sprintf("gap %s -> %s: %s", from, to, reason), synthetic: true})
		case event.TypeBaseline:
			rows = append(rows, tlRow{observed: m.ObservedAt, watchType: "BASELINE", rv: m.Resource.ResourceVersion, summary: "relist", synthetic: true})
		}
	}

	var prev json.RawMessage
	for i := range recs {
		rec := &recs[i]
		if !since.IsZero() && rec.ObservedAt.Before(since) {
			if rec.WatchType != event.WatchDeleted {
				prev = rec.Object
			} else {
				prev = nil
			}
			continue
		}
		switch rec.WatchType {
		case event.WatchDeleted:
			rows = append(rows, tlRow{
				observed:  rec.ObservedAt,
				watchType: string(rec.WatchType),
				rv:        rec.Resource.ResourceVersion,
				summary:   "deleted",
				synthetic: rec.Synthetic,
			})
			prev = nil
		case event.WatchBookmark:
			rows = append(rows, tlRow{
				observed:  rec.ObservedAt,
				watchType: string(rec.WatchType),
				rv:        rec.Resource.ResourceVersion,
				summary:   "progress only",
				synthetic: rec.Synthetic,
			})
		default:
			summary := "initial state"
			if len(prev) > 0 || len(rec.Object) > 0 {
				summary = summarizeChanges(materialize.DiffObjects(prev, rec.Object))
			}
			rows = append(rows, tlRow{
				observed:  rec.ObservedAt,
				watchType: string(rec.WatchType),
				rv:        rec.Resource.ResourceVersion,
				summary:   summary,
				synthetic: rec.Synthetic,
			})
			prev = rec.Object
		}
	}

	sort.SliceStable(rows, func(i, j int) bool { return rows[i].observed.Before(rows[j].observed) })
	return rows
}

func printTimeline(rows []tlRow) {
	if len(rows) == 0 {
		out("no events recorded\n")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "OBSERVED\tTYPE\tRV\tSUMMARY")
	for _, r := range rows {
		mark := ""
		if r.synthetic {
			mark = "*"
		}
		fmt.Fprintf(w, "%s\t%s%s\t%s\t%s\n", r.observed.Format(time.RFC3339), r.watchType, mark, r.rv, r.summary)
	}
	w.Flush()
}
