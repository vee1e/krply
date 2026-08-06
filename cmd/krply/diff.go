package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	queryv1 "github.com/krply/krply/api/query/v1"
	"github.com/krply/krply/internal/materialize"
)

var (
	diffSince     string
	diffUntil     string
	diffNamespace string
	diffClusterID string
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "show semantic field changes between two times",
	Args:  cobra.NoArgs,
	RunE:  runDiff,
}

func init() {
	rootCmd.AddCommand(diffCmd)
	f := diffCmd.Flags()
	f.StringVar(&diffSince, "since", "", "start of the diff window (required; RFC3339 or duration like 30m)")
	f.StringVar(&diffUntil, "until", "", "end of the diff window (required; RFC3339 or duration like 30m)")
	f.StringVar(&diffNamespace, "namespace", "", "restrict the diff to one namespace")
	f.StringVar(&diffClusterID, "cluster-id", "", "cluster to diff (default: the first recorded cluster)")
}

func runDiff(cmd *cobra.Command, args []string) error {
	now := time.Now()
	since, err := parseTimeFlag(diffSince, now)
	if err != nil {
		return err
	}
	until, err := parseTimeFlag(diffUntil, now)
	if err != nil {
		return err
	}
	if since.IsZero() || until.IsZero() {
		return errors.New("diff requires both --since and --until")
	}

	if serverURL != "" {
		return diffServer(cmd.Context(), since, until)
	}

	store, err := openStore(storePath)
	if err != nil {
		return err
	}
	defer closeStore(store)

	clusterID := diffClusterID
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

	mat := materialize.NewMaterializer(store)
	res, err := mat.Diff(cmd.Context(), clusterID, diffNamespace, since, until)
	if err != nil {
		return err
	}
	printDiffHeader(clusterID, since, until)
	printDiffChanges(res.Changes)
	if res.HasGaps {
		warn("coverage incomplete; diff may be partial: %s\n", res.Warning)
	}
	return nil
}

func diffServer(ctx context.Context, since, until time.Time) error {
	c := newAPIClient(serverURL)
	q := url.Values{}
	if diffClusterID != "" {
		q.Set("cluster_id", diffClusterID)
	}
	if diffNamespace != "" {
		q.Set("namespace", diffNamespace)
	}
	q.Set("since", since.Format(time.RFC3339))
	q.Set("until", until.Format(time.RFC3339))

	var res queryv1.DiffResult
	if err := c.get(withQuery("/v1/diff", q), &res); err != nil {
		return err
	}
	printDiffHeader(res.ClusterID, since, until)
	printQueryDiffChanges(res.Changed)
	if res.HasGaps {
		warn("coverage incomplete; diff may be partial: %s\n", res.Warning)
	}
	return nil
}

func printDiffHeader(clusterID string, since, until time.Time) {
	scope := "(all namespaces)"
	if diffNamespace != "" {
		scope = diffNamespace
	}
	out("CLUSTER\t%s\n", clusterID)
	out("SCOPE\t%s\n", scope)
	out("WINDOW\t%s .. %s\n", since.Format(time.RFC3339), until.Format(time.RFC3339))
}

func printDiffChanges(changes []materialize.ObjectDiff) {
	out("CHANGED\t%d objects\n", len(changes))
	for _, od := range changes {
		printOneObjectDiff(od.Kind, od.Namespace, od.Name, od.Changes)
	}
}

func printQueryDiffChanges(changes []queryv1.ObjectDiff) {
	out("CHANGED\t%d objects\n", len(changes))
	for _, od := range changes {
		out("%s %s\n", od.Kind, objKey(od.Namespace, od.Name))
		if len(od.Changes) == 0 {
			continue
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		for _, c := range od.Changes {
			writeFieldChange(w, c.Path, c.Before, c.After, c.Added, c.Removed)
		}
		w.Flush()
	}
}

func printOneObjectDiff(kind, namespace, name string, changes []materialize.FieldChange) {
	if len(changes) == 1 && changes[0].Path == "" {
		switch {
		case changes[0].Added:
			out("%s %s\t+ object added\n", kind, objKey(namespace, name))
		case changes[0].Removed:
			out("%s %s\t- object removed\n", kind, objKey(namespace, name))
		default:
			out("%s %s\t(whole object changed)\n", kind, objKey(namespace, name))
		}
		return
	}
	out("%s %s\n", kind, objKey(namespace, name))
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	for _, c := range changes {
		writeFieldChange(w, c.Path, c.Before, c.After, c.Added, c.Removed)
	}
	w.Flush()
}

func writeFieldChange(w *tabwriter.Writer, path string, before, after any, added, removed bool) {
	switch {
	case added:
		fmt.Fprintf(w, "  +\t%s\n", path)
	case removed:
		fmt.Fprintf(w, "  -\t%s\n", path)
	default:
		fmt.Fprintf(w, "  \t%s\t%s -> %s\n", path, shortVal(before), shortVal(after))
	}
}

func objKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}
