package event

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	b.WriteString(s.Selector)
	return b.String()
}

// StreamID parses a stream identifier produced by Stream.ID.
func StreamID(id string) (Stream, error) {
	parts := strings.Split(id, "/")
	if len(parts) != 6 {
		return Stream{}, fmt.Errorf("stream id %q: want 6 slash-separated parts", id)
	}
	return Stream{
		ClusterID: parts[0],
		Group:     parts[1],
		Version:   parts[2],
		Resource:  parts[3],
		Namespace: parts[4],
		Selector:  parts[5],
	}, nil
}

// GVR returns group, version, and resource lowercased for client-go use.
func (s Stream) GVR() (string, string, string) {
	return strings.ToLower(s.Group), strings.ToLower(s.Version), strings.ToLower(s.Resource)
}

// EventID derives a deterministic deduplication key for a watch event.
// The same underlying API event must always produce the same key, so a
// duplicate delivery after a reconnect is idempotent.
func EventID(stream Stream, resource ResourceRef, watchType WatchType, observedAt int64) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s\x00%s", stream.ID(), resource.Namespace, resource.Name, resource.UID, resource.ResourceVersion, string(watchType))
	// observedAt is deliberately excluded: resource version ordering is the
	// dedup domain, not wall-clock observation time.
	_ = observedAt
	return hex.EncodeToString(h.Sum(nil))
}

// ObjectHash computes a fast equality hash over the raw object payload.
func ObjectHash(object []byte) string {
	h := sha256.Sum256(object)
	return hex.EncodeToString(h[:])
}
