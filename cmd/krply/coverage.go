package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	queryv1 "github.com/krply/krply/api/query/v1"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/storage"
)

var coverageCmd = &cobra.Command{
	Use:   "coverage [stream-id]",
	Short: "show stream coverage and gap details",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runCoverage,
}

func init() {
	rootCmd.AddCommand(coverageCmd)
}

func runCoverage(cmd *cobra.Command, args []string) error {
	streamID := ""
	if len(args) > 0 {
		streamID = args[0]
	}
	if serverURL != "" {
		return coverageServer(cmd.Context(), streamID)
	}

	store, err := openStore(storePath)
	if err != nil {
		return err
	}
	defer closeStore(store)

	if streamID == "" {
		streams, err := store.Streams(cmd.Context())
		if err != nil {
			return err
		}
		if len(streams) == 0 {
			out("no streams recorded\n")
			return nil
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "STREAM\tRESOURCE\tNAMESPACE\tAVAIL\tLAST-RV\tGAPS\tCOVERAGE")
		for _, s := range streams {
			fmt.Fprintf(w, "%s\t%s\t%s\t%v\t%s\t%d\t%s\n",
				s.StreamID, resourceLabel(s.Group, s.Resource), nsLabel(s.Namespace),
				s.Available, s.LastResourceVersion, s.GapCount,
				coverageLabel(s.HasGaps, s.Degraded, s.Available))
		}
		w.Flush()
		return nil
	}

	meta, err := store.StreamMeta(cmd.Context(), streamID)
	if err != nil {
		return err
	}
	printStreamMeta(meta)

	gaps, err := store.Gaps(cmd.Context(), streamID)
	if err != nil {
		return err
	}
	printGapRecords(gaps)
	return nil
}

func coverageServer(ctx context.Context, streamID string) error {
	c := newAPIClient(serverURL)
	var streams []queryv1.Stream
	if err := c.get("/v1/streams", &streams); err != nil {
		return err
	}

	if streamID == "" {
		if len(streams) == 0 {
			out("no streams recorded\n")
			return nil
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "STREAM\tRESOURCE\tNAMESPACE\tAVAIL\tLAST-RV\tGAPS\tCOVERAGE")
		for _, s := range streams {
			fmt.Fprintf(w, "%s\t%s\t%s\t%v\t%s\t%d\t%s\n",
				s.ID, resourceLabel(s.Group, s.Resource), nsLabel(s.Namespace),
				s.Available, s.LastResourceVersion, s.GapCount,
				coverageLabel(s.HasGaps, s.Degraded, s.Available))
		}
		w.Flush()
		return nil
	}

	var found *queryv1.Stream
	for i := range streams {
		if streams[i].ID == streamID {
			found = &streams[i]
			break
		}
	}
	if found == nil {
		return fmt.Errorf("stream %q not found", streamID)
	}
	printStreamMeta(queryv1StreamMeta(*found))

	q := url.Values{}
	q.Set("stream_id", streamID)
	q.Set("record_type", string(event.TypeGap))
	var page queryv1.EventPage
	if err := c.get(withQuery("/v1/events", q), &page); err != nil {
		return err
	}
	if len(page.Items) == 0 {
		out("no gaps recorded\n")
		return nil
	}
	out("GAPS (%d):\n", len(page.Items))
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "FROM-RV\tTO-RV\tREASON\tDETECTED")
	for _, it := range page.Items {
		from, to, reason := gapFromEvent(it)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", from, to, reason, it.ObservedAt.Format(time.RFC3339))
	}
	w.Flush()
	return nil
}

func queryv1StreamMeta(s queryv1.Stream) storage.StreamMeta {
	return storage.StreamMeta{
		StreamID:            s.ID,
		ClusterID:           s.ClusterID,
		Group:               s.Group,
		Version:             s.Version,
		Resource:            s.Resource,
		Kind:                s.Kind,
		Namespace:           s.Namespace,
		Selector:            s.Selector,
		Available:           s.Available,
		FirstObservedAt:     s.FirstObservedAt,
		LastObservedAt:      s.LastObservedAt,
		LastResourceVersion: s.LastResourceVersion,
		GapCount:            s.GapCount,
		HasGaps:             s.HasGaps,
		Degraded:            s.Degraded,
	}
}

func printStreamMeta(meta storage.StreamMeta) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "STREAM\t%s\n", meta.StreamID)
	fmt.Fprintf(w, "CLUSTER\t%s\n", meta.ClusterID)
	fmt.Fprintf(w, "RESOURCE\t%s\n", resourceLabel(meta.Group, meta.Resource))
	fmt.Fprintf(w, "KIND\t%s\n", meta.Kind)
	fmt.Fprintf(w, "NAMESPACE\t%s\n", nsLabel(meta.Namespace))
	fmt.Fprintf(w, "SELECTOR\t%s\n", meta.Selector)
	fmt.Fprintf(w, "AVAILABLE\t%v\n", meta.Available)
	fmt.Fprintf(w, "FIRST OBSERVED\t%s\n", meta.FirstObservedAt.Format(time.RFC3339))
	fmt.Fprintf(w, "LAST OBSERVED\t%s\n", meta.LastObservedAt.Format(time.RFC3339))
	fmt.Fprintf(w, "LAST RESOURCE VERSION\t%s\n", meta.LastResourceVersion)
	fmt.Fprintf(w, "GAP COUNT\t%d\n", meta.GapCount)
	fmt.Fprintf(w, "DEGRADED\t%v\n", meta.Degraded)
	fmt.Fprintf(w, "COVERAGE\t%s\n", coverageLabel(meta.HasGaps, meta.Degraded, meta.Available))
	w.Flush()
}

func printGapRecords(gaps []event.Record) {
	if len(gaps) == 0 {
		out("no gaps recorded\n")
		return
	}
	out("GAPS (%d):\n", len(gaps))
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "FROM-RV\tTO-RV\tREASON\tDETECTED")
	for i := range gaps {
		g := gaps[i]
		if g.Gap == nil {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", "?", "?", string(g.Type), g.ObservedAt.Format(time.RFC3339))
			continue
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			g.Gap.FromResourceVersion, g.Gap.ToResourceVersion, g.Gap.Reason, g.Gap.DetectedAt.Format(time.RFC3339))
	}
	w.Flush()
}

func gapFromEvent(it queryv1.Event) (from, to, reason string) {
	m, ok := it.Object.(map[string]any)
	if !ok {
		return "", "", ""
	}
	get := func(k string) string {
		if v, ok := m[k].(string); ok {
			return v
		}
		return ""
	}
	return get("from_resource_version"), get("to_resource_version"), get("reason")
}
