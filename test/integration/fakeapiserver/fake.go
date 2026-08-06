//go:build integration || e2e

// Package fakeapiserver implements a small in-process Kubernetes API server
// that supports list and watch for one core v1 resource (configmaps) so the
// real watch collector can run against it over the network.
package fakeapiserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type objState struct {
	name string
	ns   string
	obj  map[string]any
}

type eventEntry struct {
	Type string
	RV   string
	Obj  map[string]any
}

// Server is a fake Kubernetes apiserver. It is safe for concurrent use.
type Server struct {
	URL        string
	HTTPServer *httptest.Server

	mu            sync.Mutex
	objects       map[string]*objState
	events        []eventEntry
	rvCounter     int
	forceGap      bool
	requiredToken string
	bookmarkEvery int
	broadcast     chan struct{}
}

// NewFake builds a fake apiserver seeded with the given configmaps
// (name to JSON body). The initial resource version is "100".
func NewFake(initial map[string]string) (*Server, error) {
	s := &Server{
		objects:   map[string]*objState{},
		rvCounter: 100,
		broadcast: make(chan struct{}),
	}
	for name, body := range initial {
		var obj map[string]any
		if err := json.Unmarshal([]byte(body), &obj); err != nil {
			return nil, fmt.Errorf("fakeapiserver: initial configmap %q: %w", name, err)
		}
		ns, obj := normalize(name, obj, "100")
		s.objects[ns+"/"+name] = &objState{name: name, ns: ns, obj: obj}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/configmaps", s.serveNamespaced)
	mux.HandleFunc("GET /api/v1/configmaps", s.serveCluster)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	hts := httptest.NewServer(mux)
	s.URL = hts.URL
	s.HTTPServer = hts
	return s, nil
}

// RequireToken makes the fake reject any request whose bearer token does not
// match the given value with a 403. Requests without an Authorization header
// are always rejected with a 401.
func (s *Server) RequireToken(tok string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requiredToken = tok
}

// SetBookmarkEvery enables emitting a BOOKMARK after every n delivered watch
// events. Zero disables bookmarks.
func (s *Server) SetBookmarkEvery(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bookmarkEvery = n
}

// ForceGap marks every currently valid resource version as expired and bumps
// the collection resource version, simulating events that happened while a
// client was disconnected.
func (s *Server) ForceGap() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forceGap = true
	s.rvCounter++
}

// CloseCurrentWatch closes all active client connections, forcing the
// collector to reconnect from its last durable resource version.
func (s *Server) CloseCurrentWatch() {
	s.HTTPServer.CloseClientConnections()
}

// Close shuts the fake apiserver down.
func (s *Server) Close() {
	s.HTTPServer.Close()
}

// AddOrUpdate creates or modifies a configmap. An empty rv assigns the next
// resource version from the server counter; a non-empty rv is used directly.
func (s *Server) AddOrUpdate(name string, obj map[string]any, rv string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rv == "" {
		s.rvCounter++
		rv = strconv.Itoa(s.rvCounter)
	} else if n := parseRV(rv); n > s.rvCounter {
		s.rvCounter = n
	}
	ns, obj := normalize(name, obj, rv)
	key := ns + "/" + name
	typ := "ADDED"
	if _, ok := s.objects[key]; ok {
		typ = "MODIFIED"
	}
	s.objects[key] = &objState{name: name, ns: ns, obj: obj}
	s.appendEventLocked(typ, rv, obj)
}

// Delete removes a configmap by name across all namespaces.
func (s *Server) Delete(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, o := range s.objects {
		if o.name != name {
			continue
		}
		s.rvCounter++
		rv := strconv.Itoa(s.rvCounter)
		del := cloneMap(o.obj)
		m, _ := del["metadata"].(map[string]any)
		if m == nil {
			m = map[string]any{}
			del["metadata"] = m
		}
		m["resourceVersion"] = rv
		s.appendEventLocked("DELETED", rv, del)
		delete(s.objects, key)
		return
	}
}

func (s *Server) serveNamespaced(w http.ResponseWriter, r *http.Request) {
	s.serveConfigMaps(w, r, r.PathValue("ns"))
}

func (s *Server) serveCluster(w http.ResponseWriter, r *http.Request) {
	s.serveConfigMaps(w, r, "")
}

func (s *Server) serveConfigMaps(w http.ResponseWriter, r *http.Request, ns string) {
	if !s.authorize(w, r) {
		return
	}
	q := r.URL.Query()
	if q.Get("watch") == "true" {
		s.serveWatch(w, r, ns, q.Get("resourceVersion"), q.Get("allowWatchBookmarks") == "true")
		return
	}
	s.serveList(w, r, ns)
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request) bool {
	tok := ""
	if h := r.Header.Get("Authorization"); h != "" {
		tok = strings.TrimPrefix(h, "Bearer ")
	}
	if tok == "" {
		writeStatus(w, http.StatusUnauthorized, metav1.StatusReasonUnauthorized, "missing Authorization header")
		return false
	}
	s.mu.Lock()
	required := s.requiredToken
	s.mu.Unlock()
	if required != "" && tok != required {
		writeStatus(w, http.StatusForbidden, metav1.StatusReasonForbidden, "the provided token does not match the required token")
		return false
	}
	return true
}

func (s *Server) serveList(w http.ResponseWriter, r *http.Request, ns string) {
	s.mu.Lock()
	if s.forceGap {
		if rv := r.URL.Query().Get("resourceVersion"); rv != "" && parseRV(rv) < s.rvCounter {
			s.forceGap = false
			s.mu.Unlock()
			writeStatus(w, http.StatusGone, metav1.StatusReasonExpired, "resourceVersion="+rv+" is expired")
			return
		}
	}
	current := strconv.Itoa(s.rvCounter)
	items := make([]map[string]any, 0, len(s.objects))
	for _, o := range s.objects {
		if ns != "" && o.ns != ns {
			continue
		}
		items = append(items, o.obj)
	}
	s.mu.Unlock()
	sort.Slice(items, func(i, j int) bool {
		ni, nj := objNS(items[i]), objNS(items[j])
		if ni != nj {
			return ni < nj
		}
		return objName(items[i]) < objName(items[j])
	})
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMapList",
		"metadata":   map[string]any{"resourceVersion": current},
		"items":      items,
	})
}

func (s *Server) serveWatch(w http.ResponseWriter, r *http.Request, ns, rv string, bookmarks bool) {
	s.mu.Lock()
	if s.forceGap && rv != "" && parseRV(rv) < s.rvCounter {
		s.forceGap = false
		s.mu.Unlock()
		s.streamGone(w, "resourceVersion="+rv+" is expired")
		return
	}
	every := s.bookmarkEvery
	s.mu.Unlock()

	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fl.Flush()

	s.mu.Lock()
	startIdx := len(s.events)
	if rv != "" {
		if i := s.firstEventAfterLocked(rv); i >= 0 {
			startIdx = i
		}
	}
	s.mu.Unlock()

	idx := startIdx
	delivered := 0
	for {
		s.mu.Lock()
		if idx < len(s.events) {
			ev := s.events[idx]
			idx++
			s.mu.Unlock()
			if err := writeJSONLine(w, fl, map[string]any{"type": ev.Type, "object": ev.Obj}); err != nil {
				return
			}
			delivered++
			if bookmarks && every > 0 && delivered%every == 0 {
				if err := s.writeBookmark(w, fl); err != nil {
					return
				}
			}
			continue
		}
		ch := s.broadcast
		s.mu.Unlock()
		select {
		case <-ch:
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) writeBookmark(w http.ResponseWriter, fl http.Flusher) error {
	s.mu.Lock()
	rv := strconv.Itoa(s.rvCounter)
	s.mu.Unlock()
	return writeJSONLine(w, fl, map[string]any{
		"type": "BOOKMARK",
		"object": map[string]any{
			"kind":       "ConfigMap",
			"apiVersion": "v1",
			"metadata":   map[string]any{"resourceVersion": rv},
		},
	})
}

func (s *Server) streamGone(w http.ResponseWriter, msg string) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fl.Flush()
	writeJSONLine(w, fl, map[string]any{
		"type": "ERROR",
		"object": metav1.Status{
			TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
			Status:   metav1.StatusFailure,
			Message:  msg,
			Reason:   metav1.StatusReasonExpired,
			Code:     http.StatusGone,
		},
	})
}

func (s *Server) appendEventLocked(typ, rv string, obj map[string]any) {
	s.events = append(s.events, eventEntry{Type: typ, RV: rv, Obj: obj})
	close(s.broadcast)
	s.broadcast = make(chan struct{})
}

func (s *Server) firstEventAfterLocked(rv string) int {
	want := parseRV(rv)
	for i := range s.events {
		if parseRV(s.events[i].RV) > want {
			return i
		}
	}
	return len(s.events)
}

func normalize(name string, obj map[string]any, rv string) (string, map[string]any) {
	ns := "default"
	m, _ := obj["metadata"].(map[string]any)
	if m == nil {
		m = map[string]any{}
		obj["metadata"] = m
	}
	if n, _ := m["namespace"].(string); n != "" {
		ns = n
	}
	m["name"] = name
	m["namespace"] = ns
	m["resourceVersion"] = rv
	if _, ok := m["uid"]; !ok {
		m["uid"] = "uid-" + name
	}
	if _, ok := obj["kind"]; !ok {
		obj["kind"] = "ConfigMap"
	}
	if _, ok := obj["apiVersion"]; !ok {
		obj["apiVersion"] = "v1"
	}
	return ns, obj
}

func writeStatus(w http.ResponseWriter, code int32, reason metav1.StatusReason, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(int(code))
	json.NewEncoder(w).Encode(metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure,
		Message:  msg,
		Reason:   reason,
		Code:     code,
	})
}

func writeJSONLine(w http.ResponseWriter, fl http.Flusher, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := w.Write(append(data, '\n')); err != nil {
		return err
	}
	fl.Flush()
	return nil
}

func parseRV(rv string) int {
	if rv == "" {
		return 0
	}
	n, err := strconv.Atoi(rv)
	if err != nil {
		return 0
	}
	return n
}

func objName(obj map[string]any) string {
	m, _ := obj["metadata"].(map[string]any)
	name, _ := m["name"].(string)
	return name
}

func objNS(obj map[string]any) string {
	m, _ := obj["metadata"].(map[string]any)
	ns, _ := m["namespace"].(string)
	return ns
}

func cloneMap(m map[string]any) map[string]any {
	b, err := json.Marshal(m)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}
