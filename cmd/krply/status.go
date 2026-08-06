package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"

	queryv1 "github.com/krply/krply/api/query/v1"
	"github.com/krply/krply/internal/storage"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "show cluster and stream coverage status",
	Args:  cobra.NoArgs,
	RunE:  runStatus,
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
	if serverURL != "" {
		return statusServer(cmd.Context())
	}
	store, err := openStore(storePath)
	if err != nil {
		return err
	}
	defer closeStore(store)

	clusters, err := store.ListClusters(cmd.Context())
	if err != nil {
		return err
	}
	if len(clusters) == 0 {
		out("no clusters recorded\n")
		return nil
	}
	streams, err := store.Streams(cmd.Context())
	if err != nil {
		return err
	}
	printStreams(streamsToRows(streams))
	return nil
}

func statusServer(ctx context.Context) error {
	c := newAPIClient(serverURL)
	var clusters []queryv1.Cluster
	if err := c.get("/v1/clusters", &clusters); err != nil {
		return err
	}
	if len(clusters) == 0 {
		out("no clusters recorded\n")
		return nil
	}
	var streams []queryv1.Stream
	if err := c.get("/v1/streams", &streams); err != nil {
		return err
	}
	rows := make([]streamRow, 0, len(streams))
	for _, s := range streams {
		rows = append(rows, streamRow{
			cluster:   s.ClusterID,
			stream:    s.ID,
			resource:  resourceLabel(s.Group, s.Resource),
			namespace: s.Namespace,
			lastRV:    s.LastResourceVersion,
			available: s.Available,
			gaps:      s.GapCount,
			hasGaps:   s.HasGaps,
			degraded:  s.Degraded,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].cluster != rows[j].cluster {
			return rows[i].cluster < rows[j].cluster
		}
		return rows[i].stream < rows[j].stream
	})
	printStreams(rows)
	return nil
}

type streamRow struct {
	cluster, stream, resource, namespace, lastRV string
	available                                    bool
	gaps                                         int64
	hasGaps, degraded                            bool
}

func streamsToRows(streams []storage.StreamMeta) []streamRow {
	rows := make([]streamRow, 0, len(streams))
	for _, s := range streams {
		rows = append(rows, streamRow{
			cluster:   s.ClusterID,
			stream:    s.StreamID,
			resource:  resourceLabel(s.Group, s.Resource),
			namespace: s.Namespace,
			lastRV:    s.LastResourceVersion,
			available: s.Available,
			gaps:      s.GapCount,
			hasGaps:   s.HasGaps,
			degraded:  s.Degraded,
		})
	}
	return rows
}

func resourceLabel(group, resource string) string {
	if group == "" {
		return resource
	}
	return group + "/" + resource
}

func printStreams(rows []streamRow) {
	if len(rows) == 0 {
		out("no streams recorded\n")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tSTREAM\tRESOURCE\tNAMESPACE\tLAST-RV\tAVAIL\tGAPS\tCOVERAGE")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%v\t%d\t%s\n",
			r.cluster, r.stream, r.resource, nsLabel(r.namespace), r.lastRV, r.available, r.gaps,
			coverageLabel(r.hasGaps, r.degraded, r.available))
	}
	w.Flush()
}
