package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/krply/krply/internal/discovery"
	"github.com/krply/krply/internal/event"
	"github.com/krply/krply/internal/watch"
)

var (
	recordNamespace   string
	recordResources   []string
	recordSelector    string
	recordClusterID   string
	recordBookmarks   bool
	recordSendInitial bool
	recordAgent       string
)

var recordCmd = &cobra.Command{
	Use:   "record",
	Short: "record object history from a live cluster into the journal",
	Args:  cobra.NoArgs,
	RunE:  runRecord,
}

func init() {
	rootCmd.AddCommand(recordCmd)
	f := recordCmd.Flags()
	f.StringVar(&recordNamespace, "namespace", "default", "namespace to watch (empty = all namespaces)")
	f.StringSliceVar(&recordResources, "resource", nil, "resource to watch (repeatable; defaults to the MVP watch list)")
	f.StringVar(&recordSelector, "selector", "", "label selector applied to every stream")
	f.StringVar(&recordClusterID, "cluster-id", "", "cluster id (default: derived from the kubeconfig server URL)")
	f.BoolVar(&recordBookmarks, "bookmarks", false, "request bookmarks from the API server")
	f.BoolVar(&recordSendInitial, "send-initial", false, "emit synthetic baseline events from the initial list")
	f.StringVar(&recordAgent, "agent-name", "", "user-agent to present to the API server")
}

func runRecord(cmd *cobra.Command, args []string) error {
	store, err := openStore(storePath)
	if err != nil {
		return err
	}
	defer closeStore(store)

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clusterID := recordClusterID
	if clusterID == "" {
		clusterID, err = watch.ClusterID(ctx, kubeconfig, contextName)
		if err != nil {
			return fmt.Errorf("derive cluster id: %w (pass --cluster-id to override)", err)
		}
	}

	resources, err := resolveRecordResources(ctx, recordResources)
	if err != nil {
		return err
	}
	resources = applyRecordNamespace(resources, recordNamespace)

	collector, err := watch.NewCollector(watch.Config{
		KubeConfig:  kubeconfig,
		Context:     contextName,
		ClusterID:   clusterID,
		Resources:   resources,
		Selector:    recordSelector,
		Store:       store,
		Bookmarks:   recordBookmarks,
		SendInitial: recordSendInitial,
		AgentName:   recordAgent,
	})
	if err != nil {
		return err
	}

	for _, spec := range resources {
		sid := event.Stream{
			ClusterID: clusterID,
			Group:     spec.APIGroup,
			Version:   spec.Version,
			Resource:  spec.Resource,
			Namespace: spec.Namespace,
			Selector:  recordSelector,
		}.ID()
		out("starting stream %s\n", sid)
	}

	if err := collector.Run(ctx); err != nil {
		return err
	}
	if ctx.Err() != nil {
		out("recording stopped; coverage may be partial\n")
	}
	return nil
}

// clusterScopedKinds are resources that must never be namespaced, even when a
// --namespace flag is supplied.
var clusterScopedKinds = map[string]bool{
	"Namespace":                      true,
	"RuntimeClass":                   true,
	"ClusterRole":                    true,
	"ClusterRoleBinding":             true,
	"PersistentVolume":               true,
	"StorageClass":                   true,
	"CustomResourceDefinition":       true,
	"MutatingWebhookConfiguration":   true,
	"ValidatingWebhookConfiguration": true,
	"PriorityClass":                  true,
	"Node":                           true,
}

// applyRecordNamespace scopes each namespaced resource to the requested
// namespace. An empty namespace watches the collection cluster-wide.
func applyRecordNamespace(resources []discovery.ResourceSpec, namespace string) []discovery.ResourceSpec {
	out := make([]discovery.ResourceSpec, len(resources))
	for i, spec := range resources {
		out[i] = spec
		out[i].Namespace = ""
		if namespace != "" && !clusterScopedKinds[spec.Kind] {
			out[i].Namespace = namespace
		}
	}
	return out
}

// resolveRecordResources returns the MVP watch list when no --resource flag
// was given, otherwise resolves the user-provided list against the cluster's
// discovery API (well-known aliases resolve without contacting a cluster).
func resolveRecordResources(ctx context.Context, in []string) ([]discovery.ResourceSpec, error) {
	if len(in) == 0 {
		return discovery.DefaultResources(), nil
	}
	client, err := buildKubeClient()
	if err != nil {
		return nil, fmt.Errorf("resolve resource list: %w", err)
	}
	specs := make([]discovery.ResourceSpec, 0, len(in))
	for _, r := range in {
		specs = append(specs, discovery.ResourceSpec{Resource: r})
	}
	resolved, err := discovery.Resolve(ctx, client, specs)
	if err != nil {
		return nil, fmt.Errorf("resolve resource list: %w", err)
	}
	return resolved, nil
}

// buildKubeClient builds a typed client from the persistent kubeconfig
// flags, falling back to in-cluster configuration when no path is given.
func buildKubeClient() (kubernetes.Interface, error) {
	var (
		cfg *rest.Config
		err error
	)
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster config: %w (pass --kubeconfig to use an external cluster)", err)
		}
	} else {
		raw, err := clientcmd.LoadFromFile(kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig %q: %w", kubeconfig, err)
		}
		cc := clientcmd.NewNonInteractiveClientConfig(*raw, contextName, &clientcmd.ConfigOverrides{}, nil)
		cfg, err = cc.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("resolve config for context %q: %w", contextName, err)
		}
	}
	return kubernetes.NewForConfig(cfg)
}
