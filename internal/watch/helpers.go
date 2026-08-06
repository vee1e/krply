package watch

import (
	"net/http"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krply/krply/internal/discovery"
	"github.com/krply/krply/internal/event"
)

// streamIDFor builds the event.Stream for a resource spec and its stable ID.
// The selector is filled in by the collector because it is a Config-level
// property, not a per-spec one.
func streamIDFor(spec discovery.ResourceSpec, clusterID string) (event.Stream, string) {
	s := event.Stream{
		ClusterID: clusterID,
		Group:     spec.APIGroup,
		Version:   spec.Version,
		Resource:  spec.Resource,
		Namespace: spec.Namespace,
	}
	return s, s.ID()
}

// gvrFor converts a spec into a GroupVersionResource for the dynamic client.
func gvrFor(spec discovery.ResourceSpec) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    spec.APIGroup,
		Version:  spec.Version,
		Resource: strings.ToLower(spec.Resource),
	}
}

// refFromObject normalizes an unstructured object into a journal ResourceRef.
func refFromObject(spec discovery.ResourceSpec, obj *unstructured.Unstructured) event.ResourceRef {
	ref := event.ResourceRef{
		Group:           spec.APIGroup,
		Version:         spec.Version,
		Kind:            spec.Kind,
		Namespace:       obj.GetNamespace(),
		Name:            obj.GetName(),
		UID:             string(obj.GetUID()),
		ResourceVersion: obj.GetResourceVersion(),
	}
	if ref.Namespace == "" && spec.Namespace != "" {
		ref.Namespace = spec.Namespace
	}
	return ref
}

// objRV extracts a resource version from a watch event object (used for
// bookmarks).
func objRV(obj runtime.Object) string {
	switch o := obj.(type) {
	case *unstructured.Unstructured:
		return o.GetResourceVersion()
	case *metav1.PartialObjectMetadata:
		return o.ResourceVersion
	}
	return ""
}

// statusOf converts a watch error payload into a metav1.Status.
func statusOf(obj runtime.Object) *metav1.Status {
	if st, ok := obj.(*metav1.Status); ok {
		return st
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		st := &metav1.Status{Status: metav1.StatusFailure}
		if code, found, _ := unstructured.NestedInt64(u.Object, "code"); found {
			st.Code = int32(code)
		}
		if msg, found, _ := unstructured.NestedString(u.Object, "message"); found {
			st.Message = msg
		}
		return st
	}
	return nil
}

// errorCode returns the HTTP status code of a watch error payload, or 0.
func errorCode(obj runtime.Object) int {
	if st := statusOf(obj); st != nil {
		return int(st.Code)
	}
	return 0
}

const goneCode = http.StatusGone // 410
