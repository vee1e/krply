// Package version holds build metadata for krply.
package version

// Version is set at build time via -ldflags. It reports the git describe
// value when built through the Makefile, otherwise "dev".
var Version = "dev"

// Commit is the git commit hash when available.
var Commit = ""

// String returns the full human-readable version.
func String() string {
	if Commit != "" {
		return Version + " (" + Commit + ")"
	}
	return Version
}
