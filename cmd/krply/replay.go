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
	"github.com/krply/krply/internal/replay"
)

var (
	replaySnapshotID    string
	replaySourceNS      string
	replayTargetNS      string
	replayTargetContext string
	replayAllowGaps     bool

	replayPlanID  string
	replayConfirm bool
)

var replayCmd = &cobra.Command{
	Use:   "replay",
	Short: "plan and apply replays of recorded state",
}

var replayPlanCmd = &cobra.Command{
	Use:   "plan",
	Short: "build a sanitized replay plan from a snapshot",
	Args:  cobra.NoArgs,
	RunE:  runReplayPlan,
}

var replayApplyCmd = &cobra.Command{
	Use:   "apply",
	Short: "dry-run and apply a replay plan (local, or via --server)",
	Args:  cobra.NoArgs,
	RunE:  runReplayApply,
}

func init() {
	rootCmd.AddCommand(replayCmd)
	replayCmd.AddCommand(replayPlanCmd)
	replayCmd.AddCommand(replayApplyCmd)

	f := replayPlanCmd.Flags()
	f.StringVar(&replaySnapshotID, "snapshot", "", "snapshot id to replay")
	f.StringVar(&replaySourceNS, "source-namespace", "", "source namespace to replay (empty = all)")
	f.StringVar(&replayTargetNS, "target-namespace", "", "target namespace to apply into")
	f.StringVar(&replayTargetContext, "target-context", "", "target kubeconfig context")
	f.BoolVar(&replayAllowGaps, "allow-gaps", false, "allow a plan with incomplete coverage")

	g := replayApplyCmd.Flags()
	g.StringVar(&replayPlanID, "plan-id", "", "plan id to apply (server mode)")
	g.StringVar(&replaySnapshotID, "snapshot", "", "snapshot id to re-plan and apply (local mode)")
	g.StringVar(&replaySourceNS, "source-namespace", "", "source namespace to replay (empty = all)")
	g.StringVar(&replayTargetNS, "target-namespace", "", "target namespace to apply into")
	g.StringVar(&replayTargetContext, "target-context", "", "target kubeconfig context")
	g.BoolVar(&replayAllowGaps, "allow-gaps", false, "allow a plan with incomplete coverage")
	g.BoolVar(&replayConfirm, "confirm", false, "confirm the apply")
}

func runReplayPlan(cmd *cobra.Command, args []string) error {
	if replaySnapshotID == "" {
		return errors.New("replay plan requires --snapshot")
	}
	if serverURL != "" {
		return replayPlanServer(cmd.Context())
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
	clusterID, err := firstClusterID(clusters)
	if err != nil {
		return err
	}

	mat := materialize.NewMaterializer(store)
	policy := replay.DefaultPolicy()
	policy.AllowGaps = replayAllowGaps

	planner := replay.NewPlanner(store, mat, policy)
	plan, err := planner.Plan(cmd.Context(), clusterID, replaySnapshotID, replaySourceNS, replayTargetNS)
	if err != nil {
		return err
	}
	printPlan(plan)
	return nil
}

func runReplayApply(cmd *cobra.Command, args []string) error {
	if !replayConfirm {
		return errors.New("refusing apply without --confirm")
	}
	if serverURL != "" {
		if replayPlanID == "" {
			return errors.New("replay apply --server requires --plan-id")
		}
		return replayApplyServer(cmd.Context())
	}
	return replayApplyLocal(cmd)
}

// replayApplyLocal replans the snapshot from the local journal, dry-runs it,
// and applies it directly. This makes the natural plan-then-apply flow work
// without a server.
func replayApplyLocal(cmd *cobra.Command) error {
	if replaySnapshotID == "" {
		return errors.New("replay apply (local) requires --snapshot")
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
	clusterID, err := firstClusterID(clusters)
	if err != nil {
		return err
	}

	mat := materialize.NewMaterializer(store)
	policy := replay.DefaultPolicy()
	policy.AllowGaps = replayAllowGaps
	planner := replay.NewPlanner(store, mat, policy)
	plan, err := planner.Plan(cmd.Context(), clusterID, replaySnapshotID, replaySourceNS, replayTargetNS)
	if err != nil {
		return err
	}
	printPlan(plan)

	dry, err := plan.DryRun(cmd.Context(), kubeconfig, replayTargetContext)
	if err != nil {
		return err
	}
	printDryRunResult(dry)
	if !dry.OK {
		return errors.New("refusing apply: dry run did not pass")
	}
	res, err := plan.Apply(cmd.Context(), kubeconfig, replayTargetContext, true)
	if err != nil {
		return err
	}
	out("applied %d objects\n", res.Applied)
	for _, e := range res.Errors {
		warn("apply error: %s/%s (%s): %s\n", e.Namespace, e.Name, e.Kind, e.Message)
	}
	for _, e := range res.Skipped {
		warn("skipped: %s/%s (%s): %s\n", e.Namespace, e.Name, e.Kind, e.Message)
	}
	return nil
}

func replayApplyServer(ctx context.Context) error {
	c := newAPIClient(serverURL)

	var dry queryv1.DryRunResult
	if err := c.post("/v1/replay-plans/"+url.PathEscape(replayPlanID)+"/dry-run", map[string]any{
		"plan_id":        replayPlanID,
		"target_context": replayTargetContext,
	}, &dry); err != nil {
		return err
	}
	out("dry run: applied=%d conflicts=%d errors=%d skipped=%d ok=%v\n", dry.Applied, len(dry.Conflicts), len(dry.Errors), len(dry.Skipped), dry.OK)
	for _, ci := range dry.Conflicts {
		warn("conflict: %s/%s (%s): %s\n", ci.Namespace, ci.Name, ci.Kind, ci.Message)
	}
	for _, e := range dry.Errors {
		warn("dry-run error: %s/%s (%s): %s\n", e.Namespace, e.Name, e.Kind, e.Message)
	}
	for _, e := range dry.Skipped {
		warn("dry-run skipped: %s/%s (%s): %s\n", e.Namespace, e.Name, e.Kind, e.Message)
	}
	if !dry.OK {
		return errors.New("refusing apply: dry run did not pass")
	}

	var run queryv1.ReplayRun
	if err := c.post("/v1/replay-runs", map[string]any{
		"plan_id":        replayPlanID,
		"target_context": replayTargetContext,
		"confirm":        true,
	}, &run); err != nil {
		return err
	}
	out("applied %d objects\n", run.Applied)
	for _, e := range run.Errors {
		warn("apply error: %s/%s (%s): %s\n", e.Namespace, e.Name, e.Kind, e.Message)
	}
	for _, e := range run.Skipped {
		warn("skipped: %s/%s (%s): %s\n", e.Namespace, e.Name, e.Kind, e.Message)
	}
	if run.Status != "" {
		out("status: %s\n", run.Status)
	}
	return nil
}

func printDryRunResult(d *replay.DryRunResult) {
	out("dry run: applied=%d conflicts=%d errors=%d skipped=%d ok=%v\n", d.Applied, len(d.Conflicts), len(d.Errors), len(d.Skipped), d.OK)
	for _, ci := range d.Conflicts {
		warn("conflict: %s/%s (%s): %s\n", ci.Namespace, ci.Name, ci.Kind, ci.Message)
	}
	for _, e := range d.Errors {
		warn("dry-run error: %s/%s (%s): %s\n", e.Namespace, e.Name, e.Kind, e.Message)
	}
	for _, e := range d.Skipped {
		warn("dry-run skipped: %s/%s (%s): %s\n", e.Namespace, e.Name, e.Kind, e.Message)
	}
}

func replayPlanServer(ctx context.Context) error {
	c := newAPIClient(serverURL)
	var plan queryv1.ReplayPlan
	if err := c.post("/v1/replay-plans", map[string]any{
		"snapshot_id":      replaySnapshotID,
		"source_namespace": replaySourceNS,
		"target_namespace": replayTargetNS,
		"target_context":   replayTargetContext,
		"allow_gaps":       replayAllowGaps,
	}, &plan); err != nil {
		return err
	}
	printQueryPlan(&plan)
	return nil
}

func printPlan(plan *replay.Plan) {
	out("PLAN ID\t%s\n", plan.ID)
	out("CLUSTER\t%s\n", plan.ClusterID)
	out("SNAPSHOT\t%s\n", plan.SnapshotID)
	out("SOURCE NAMESPACE\t%s\n", plan.SourceNamespace)
	out("TARGET NAMESPACE\t%s\n", plan.TargetNamespace)
	out("TARGET CONTEXT\t%s\n", plan.TargetContext)
	out("FIELD MANAGER\t%s\n", plan.FieldManager)
	out("COVERAGE\t%s\n", boolLabel(plan.CoverageComplete, "complete", "incomplete"))
	out("STATUS\t%s\n", plan.Status)

	out("OBJECTS\t%d\n", len(plan.Objects))
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ORDER\tNAMESPACE\tNAME\tKIND")
	for _, o := range plan.Objects {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", o.Order, o.Namespace, o.Name, o.Kind)
	}
	w.Flush()

	printPlanWarnings(plan.Warnings)
	printPlanExcluded(len(plan.Excluded), func(w2 *tabwriter.Writer) {
		for _, e := range plan.Excluded {
			fmt.Fprintf(w2, "%s\t%s\t%s\t%s\n", e.Namespace, e.Name, e.Kind, e.Reason)
		}
	})
}

func printQueryPlan(plan *queryv1.ReplayPlan) {
	out("PLAN ID\t%s\n", plan.ID)
	out("CLUSTER\t%s\n", plan.ClusterID)
	out("SNAPSHOT\t%s\n", plan.SnapshotID)
	out("SOURCE NAMESPACE\t%s\n", plan.SourceNamespace)
	out("TARGET NAMESPACE\t%s\n", plan.TargetNamespace)
	out("TARGET CONTEXT\t%s\n", plan.TargetContext)
	out("CREATED\t%s\n", plan.CreatedAt.Format(time.RFC3339))
	out("FIELD MANAGER\t%s\n", plan.FieldManager)
	out("COVERAGE\t%s\n", boolLabel(plan.CoverageComplete, "complete", "incomplete"))
	out("STATUS\t%s\n", plan.Status)

	out("OBJECTS\t%d\n", len(plan.Objects))
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ORDER\tNAMESPACE\tNAME\tKIND")
	for _, o := range plan.Objects {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", o.Order, o.Namespace, o.Name, o.Kind)
	}
	w.Flush()

	printPlanWarnings(plan.Warnings)
	printPlanExcluded(len(plan.Excluded), func(w2 *tabwriter.Writer) {
		for _, e := range plan.Excluded {
			fmt.Fprintf(w2, "%s\t%s\t%s\t%s\n", e.Namespace, e.Name, e.Kind, e.Reason)
		}
	})
}

func printPlanWarnings(warnings []string) {
	if len(warnings) == 0 {
		return
	}
	out("WARNINGS:\n")
	for _, ww := range warnings {
		out("  %s\n", ww)
	}
}

func printPlanExcluded(count int, emit func(w *tabwriter.Writer)) {
	if count == 0 {
		return
	}
	out("EXCLUDED:\n")
	w2 := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w2, "NAMESPACE\tNAME\tKIND\tREASON")
	emit(w2)
	w2.Flush()
}
