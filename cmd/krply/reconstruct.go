package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/materialize"
)

var (
	reconstructNamespace string
	reconstructAt        string
	reconstructKind      string
)

var reconstructCmd = &cobra.Command{
	Use:   "reconstruct <name>",
	Short: "print the JSON state of an object at a point in time",
	Args:  cobra.ExactArgs(1),
	RunE:  runReconstruct,
}

func init() {
	rootCmd.AddCommand(reconstructCmd)
	f := reconstructCmd.Flags()
	f.StringVar(&reconstructNamespace, "namespace", "", "namespace of the object (auto-detected when empty)")
	f.StringVar(&reconstructAt, "at", "", "point in time (RFC3339 or duration like 30m; required)")
	f.StringVar(&reconstructKind, "kind", "", "kind of the object (disambiguates multiple streams)")
}

func runReconstruct(cmd *cobra.Command, args []string) error {
	name := args[0]
	at, err := parseTimeFlag(reconstructAt, time.Now())
	if err != nil {
		return err
	}
	if at.IsZero() {
		return errors.New("reconstruct requires --at")
	}
	if serverURL != "" {
		return errors.New("reconstruct requires local mode; the krply-server API does not expose object payloads at a point in time")
	}

	store, err := openStore(storePath)
	if err != nil {
		return err
	}
	defer closeStore(store)

	namespace, err := resolveObjectNamespace(cmd.Context(), store, name, reconstructNamespace, reconstructKind)
	if err != nil {
		return err
	}

	streams, err := streamsForObject(cmd.Context(), store, namespace, reconstructKind)
	if err != nil {
		return err
	}
	if len(streams) == 0 {
		return fmt.Errorf("no stream found for object %s/%s", namespace, name)
	}
	stream := streams[0]

	mat := materialize.NewMaterializer(store)
	rec, err := mat.ObjectAt(cmd.Context(), stream.ClusterID, stream.StreamID, namespace, name, at)
	if err != nil {
		if errors.Is(err, materialize.ErrNoObjectAtTime) {
			return fmt.Errorf("no object %s/%s at %s", namespace, name, at.Format(time.RFC3339))
		}
		return err
	}
	printObjectRecord(rec)
	return nil
}

func printObjectRecord(rec *event.Record) {
	out("CLUSTER\t%s\n", rec.ClusterID)
	out("STREAM\t%s\n", rec.StreamID)
	out("NAMESPACE\t%s\n", rec.Resource.Namespace)
	out("NAME\t%s\n", rec.Resource.Name)
	out("KIND\t%s\n", rec.Resource.Kind)
	out("OBSERVED AT\t%s\n", rec.ObservedAt.Format(time.RFC3339))
	out("RESOURCE VERSION\t%s\n", rec.Resource.ResourceVersion)
	out("---\n")
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, rec.Object, "", "  "); err != nil {
		out("%s\n", string(rec.Object))
		return
	}
	out("%s\n", pretty.String())
}
