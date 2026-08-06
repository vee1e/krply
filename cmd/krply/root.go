package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/krply/krply/internal/materialize"
	"github.com/krply/krply/internal/version"
)

var (
	storePath   string
	contextName string
	serverURL   string
	kubeconfig  string
)

var rootCmd = &cobra.Command{
	Use:           "krply",
	Short:         "gap-aware Kubernetes object history",
	Version:       version.String(),
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&storePath, "store", "krply.db", "path to the local SQLite journal (use :memory: for a throwaway)")
	rootCmd.PersistentFlags().StringVar(&contextName, "context", "", "kubeconfig context to use")
	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "", "URL of a krply-server (switches read commands to HTTP mode)")
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "path to a kubeconfig file")
}

// out prints formatted output to stdout.
func out(format string, args ...any) {
	fmt.Fprintf(os.Stdout, format, args...)
}

// warn prints a formatted warning to stderr.
func warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "krply: "+format, args...)
}

// parseTimeFlag accepts "now", a Go duration relative to now (for example
// "30m" or "2h"), or an RFC3339 timestamp. An empty string returns the zero
// time, meaning "no bound".
func parseTimeFlag(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if strings.EqualFold(s, "now") {
		return now, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return now.Add(-d), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q: want an RFC3339 timestamp or a duration like 30m", s)
	}
	return t, nil
}

// firstClusterID returns the first recorded cluster, or an error when none
// exist.
func firstClusterID(clusters []string) (string, error) {
	if len(clusters) == 0 {
		return "", errors.New("no clusters recorded")
	}
	return clusters[0], nil
}

// coverageLabel reduces stream health to the single COVERAGE column value.
func coverageLabel(hasGaps, degraded, available bool) string {
	switch {
	case hasGaps:
		return "GAP"
	case degraded || !available:
		return "DEGRADED"
	default:
		return "OK"
	}
}

func boolLabel(b bool, t, f string) string {
	if b {
		return t
	}
	return f
}

// nsLabel renders an empty namespace as "(all)".
func nsLabel(ns string) string {
	if ns == "" {
		return "(all)"
	}
	return ns
}

// shortVal renders a diff value for table output, truncating long strings.
func shortVal(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return truncate(x, 32)
	case map[string]any:
		return "{...}"
	case []any:
		return fmt.Sprintf("[%d]", len(x))
	default:
		return truncate(fmt.Sprintf("%v", x), 32)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

// summarizeChanges renders a materialize field-change list as a short line,
// suitable for a timeline summary column.
func summarizeChanges(changes []materialize.FieldChange) string {
	if len(changes) == 0 {
		return "no change"
	}
	parts := make([]string, 0, len(changes))
	for _, c := range changes {
		switch {
		case c.Added && c.Path == "":
			parts = append(parts, "added")
		case c.Removed && c.Path == "":
			parts = append(parts, "removed")
		case c.Added:
			parts = append(parts, "+ "+c.Path)
		case c.Removed:
			parts = append(parts, "- "+c.Path)
		default:
			parts = append(parts, fmt.Sprintf("%s %s -> %s", c.Path, shortVal(c.Before), shortVal(c.After)))
		}
	}
	if len(parts) > 6 {
		parts = parts[:6]
		parts = append(parts, "...")
	}
	return strings.Join(parts, "; ")
}
