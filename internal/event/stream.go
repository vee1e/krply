package event

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

// Stream identifies one collection to watch. Resource versions and UIDs are
// only comparable within a single stream.
type Stream struct {
	ClusterID string
	Group     string
	Version   string
	Resource  string
	Namespace string
	Selector  string
}

// ID returns the stable stream identifier used across the journal.
// Format: cluster/group/version/resource/namespace/selector
// The selector is URL-escaped because label selectors legitimately contain '/'
// (prefix/name label keys) which would otherwise make the ID ambiguous.
func (s Stream) ID() string {
	var b strings.Builder
	b.WriteString(s.ClusterID)
	b.WriteByte('/')
	b.WriteString(s.Group)
	b.WriteByte('/')
	b.WriteString(s.Version)
	b.WriteByte('/')
	b.WriteString(s.Resource)
	b.WriteByte('/')
	b.WriteString(s.Namespace)
	b.WriteByte('/')
	b.WriteString(url.PathEscape(s.Selector))
	return b.String()
}

// StreamID parses a stream identifier produced by Stream.ID.
func StreamID(id string) (Stream, error) {
	parts := strings.Split(id, "/")
	if len(parts) != 6 {
		return Stream{}, fmt.Errorf("stream id %q: want 6 slash-separated parts", id)
	}
	selector, err := url.PathUnescape(parts[5])
	if err != nil {
		return Stream{}, fmt.Errorf("stream id %q: invalid selector: %w", id, err)
	}
	return Stream{
		ClusterID: parts[0],
		Group:     parts[1],
		Version:   parts[2],
		Resource:  parts[3],
		Namespace: parts[4],
		Selector:  selector,
	}, nil
}

// GVR returns group, version, and resource lowercased for client-go use.
func (s Stream) GVR() (string, string, string) {
	return strings.ToLower(s.Group), strings.ToLower(s.Version), strings.ToLower(s.Resource)
}

// EventID derives a deterministic deduplication key for a watch event.
// The same underlying API event must always produce the same key, so a
// duplicate delivery after a reconnect is idempotent. For live events
// observedAt is zero and excluded. Collector-generated synthetic baselines
// pass a non-zero observedAt so each relist produces a distinct key: an
// unchanged object re-listed after a gap is a new observation, not a
// duplicate delivery, and must survive deduplication.
func EventID(stream Stream, resource ResourceRef, watchType WatchType, observedAt int64) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\x00%s", stream.ID(), resource.Namespace, resource.Name, resource.UID, resource.ResourceVersion, string(watchType))
	if observedAt != 0 {
		fmt.Fprintf(h, "\x00%d", observedAt)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ObjectHash computes a fast equality hash over the raw object payload.
func ObjectHash(object []byte) string {
	h := sha256.Sum256(object)
	return hex.EncodeToString(h[:])
}
