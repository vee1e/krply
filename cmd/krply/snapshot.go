package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	queryv1 "github.com/krply/krply/api/query/v1"
	"github.com/krply/krply/internal/materialize"
)

var (
	snapshotAt        string
	snapshotName      string
	snapshotClusterID string
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "materialize a state snapshot (local) or list snapshots (server)",
	Args:  cobra.NoArgs,
	RunE:  runSnapshot,
}

func init() {
	rootCmd.AddCommand(snapshotCmd)
	f := snapshotCmd.Flags()
	f.StringVar(&snapshotAt, "at", "", "point in time for the snapshot (RFC3339 or duration like 30m; default now)")
	f.StringVar(&snapshotName, "name", "", "snapshot name (default: manual)")
	f.StringVar(&snapshotClusterID, "cluster-id", "", "cluster to snapshot (default: the first recorded cluster)")
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	now := time.Now()
	at, err := parseTimeFlag(snapshotAt, now)
	if err != nil {
		return err
	}
	if at.IsZero() {
		at = now
	}
	if serverURL != "" {
		return snapshotServer(cmd.Context())
	}

	store, err := openStore(storePath)
	if err != nil {
		return err
	}
	defer closeStore(store)

	clusterID := snapshotClusterID
	if clusterID == "" {
		clusters, err := store.ListClusters(cmd.Context())
		if err != nil {
			return err
		}
		clusterID, err = firstClusterID(clusters)
		if err != nil {
			return err
		}
	}

	name := snapshotName
	if name == "" {
		name = "manual"
	}

	mat := materialize.NewMaterializer(store)
	snap, err := mat.Snapshot(cmd.Context(), clusterID, at, name)
	if err != nil {
		return err
	}
	printSnapshot(snap)
	return nil
}

func snapshotServer(ctx context.Context) error {
	c := newAPIClient(serverURL)
	var snaps []queryv1.Snapshot
	if err := c.get("/v1/snapshots", &snaps); err != nil {
		return err
	}
	if len(snaps) == 0 {
		out("no snapshots recorded\n")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCLUSTER\tNAME\tAT\tOBJECTS\tSTREAMS\tCOMPLETE\tMISSING")
	for _, s := range snaps {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%v\t%s\n",
			s.ID, s.ClusterID, s.Name, s.At.Format(time.RFC3339), s.ObjectCount, s.Streams,
			s.Complete, strings.Join(s.Missing, ","))
	}
	w.Flush()
	return nil
}

func printSnapshot(snap *materialize.Snapshot) {
	out("SNAPSHOT\t%s\n", snap.ID)
	out("CLUSTER\t%s\n", snap.ClusterID)
	out("NAME\t%s\n", snap.Name)
	out("AT\t%s\n", snap.At.Format(time.RFC3339))
	out("OBJECT COUNT\t%d\n", len(snap.Objects))
	out("STREAMS\t%d\n", len(snap.Streams))
	out("COMPLETE\t%v\n", snap.Complete)
	if len(snap.Missing) > 0 {
		out("MISSING STREAMS\n")
		for _, m := range snap.Missing {
			out("  %s\n", m)
		}
	}
	if snap.Warning != "" {
		warn("%s\n", snap.Warning)
	}
}
