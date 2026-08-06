package watch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ClusterID derives a stable cluster identifier from the API server host in
// the given kubeconfig/context. The id is "cluster-" + first 8 hex chars of
// sha256(host) + "-" + a sanitized context name.
//
// An empty kubeconfigPath falls back to the in-cluster configuration.
func ClusterID(ctx context.Context, kubeconfigPath, contextName string) (string, error) {
	cfg, err := clientConfig(kubeconfigPath, contextName)
	if err != nil {
		return "", err
	}
	return clusterIDFromHost(cfg.Host, contextName), nil
}

func clusterIDFromHost(host, contextName string) string {
	sum := sha256.Sum256([]byte(host))
	return "cluster-" + hex.EncodeToString(sum[:])[:8] + "-" + sanitizeName(contextName)
}

func sanitizeName(name string) string {
	name = strings.ToLower(name)
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "default"
	}
	return out
}

// clientConfig loads a rest.Config from a kubeconfig path and context, or
// falls back to the in-cluster configuration when the path is empty.
func clientConfig(kubeconfigPath, contextName string) (*rest.Config, error) {
	if kubeconfigPath == "" {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster config: %w", err)
		}
		return cfg, nil
	}
	raw, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig %q: %w", kubeconfigPath, err)
	}
	cc := clientcmd.NewNonInteractiveClientConfig(*raw, contextName, &clientcmd.ConfigOverrides{}, nil)
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("resolve config for context %q: %w", contextName, err)
	}
	return cfg, nil
}
