package event

import "testing"

func TestStreamIDSelectorRoundTrip(t *testing.T) {
	// Label selectors legitimately contain '/' (prefix/name label keys). The
	// ID must round-trip through StreamID without loss or ambiguity.
	stream := Stream{
		ClusterID: "cluster-abc",
		Group:     "apps",
		Version:   "v1",
		Resource:  "deployments",
		Namespace: "default",
		Selector:  "app.kubernetes.io/name=nginx,env=prod",
	}
	id := stream.ID()
	parsed, err := StreamID(id)
	if err != nil {
		t.Fatalf("StreamID(%q): %v", id, err)
	}
	if parsed != stream {
		t.Fatalf("StreamID round-trip = %+v, want %+v", parsed, stream)
	}
}

func TestStreamIDSelectorAmbiguity(t *testing.T) {
	// {Namespace: e, Selector: f/g} must not collide with {Namespace: e/f,
	// Selector: g}: the selector's slash is escaped, so the two IDs differ.
	a := Stream{ClusterID: "c", Group: "g", Version: "v", Resource: "r", Namespace: "e", Selector: "f/g"}
	b := Stream{ClusterID: "c", Group: "g", Version: "v", Resource: "r", Namespace: "e/f", Selector: "g"}
	if a.ID() == b.ID() {
		t.Fatalf("streams with slash in selector collided: %q", a.ID())
	}
	if _, err := StreamID(a.ID()); err != nil {
		t.Fatalf("StreamID(%q): %v", a.ID(), err)
	}
}

func TestStreamIDNoSelector(t *testing.T) {
	stream := Stream{ClusterID: "c", Group: "", Version: "v1", Resource: "pods", Namespace: "default"}
	id := stream.ID()
	parsed, err := StreamID(id)
	if err != nil {
		t.Fatalf("StreamID(%q): %v", id, err)
	}
	if parsed != stream {
		t.Fatalf("round-trip = %+v, want %+v", parsed, stream)
	}
}

func TestEventIDLiveVsSynthetic(t *testing.T) {
	stream := Stream{ClusterID: "c", Group: "", Version: "v1", Resource: "pods", Namespace: "default"}
	ref := ResourceRef{Namespace: "default", Name: "p", UID: "u", ResourceVersion: "100"}
	// Live events (observedAt 0) dedup to the same key.
	if EventID(stream, ref, WatchAdded, 0) != EventID(stream, ref, WatchAdded, 0) {
		t.Fatal("live event_id not deterministic")
	}
	// Synthetic relist events (non-zero observedAt) differ from live events and
	// from each other, so an unchanged re-listed object survives dedup.
	if EventID(stream, ref, WatchAdded, 0) == EventID(stream, ref, WatchAdded, 1) {
		t.Fatal("synthetic event_id must differ from the live key")
	}
	if EventID(stream, ref, WatchAdded, 1) == EventID(stream, ref, WatchAdded, 2) {
		t.Fatal("synthetic event_ids must differ across relists")
	}
}
