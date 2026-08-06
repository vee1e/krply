//go:build integration || e2e

package fakeapiserver

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

type watchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

func doRaw(t *testing.T, method, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

func startWatch(t *testing.T, s *Server, rv string, bookmarks bool) (*http.Response, *bufio.Reader) {
	t.Helper()
	url := s.URL + "/api/v1/namespaces/default/configmaps?watch=true"
	if rv != "" {
		url += "&resourceVersion=" + rv
	}
	if bookmarks {
		url += "&allowWatchBookmarks=true"
	}
	resp := doRaw(t, http.MethodGet, url)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("watch start status = %d, body = %s", resp.StatusCode, body)
	}
	return resp, bufio.NewReader(resp.Body)
}

func nextEvent(t *testing.T, r *bufio.Reader) watchEvent {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read watch line: %v", err)
	}
	var ev watchEvent
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		t.Fatalf("decode watch line %q: %v", line, err)
	}
	return ev
}

func cm(name string) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"name": name},
		"data":     map[string]any{"app": name},
	}
}

func TestListReturnsConfigMapList(t *testing.T) {
	initial := map[string]string{
		"cm-b": `{"metadata":{"name":"cm-b"},"data":{"k":"v"}}`,
		"cm-a": `{"metadata":{"name":"cm-a"},"data":{"k":"v"}}`,
	}
	s, err := NewFake(initial)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer s.Close()

	resp := doRaw(t, http.MethodGet, s.URL+"/api/v1/namespaces/default/configmaps")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("list status = %d, body = %s", resp.StatusCode, body)
	}

	var list struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Items []map[string]any `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if list.APIVersion != "v1" || list.Kind != "ConfigMapList" {
		t.Fatalf("list apiVersion/kind = %q/%q", list.APIVersion, list.Kind)
	}
	if list.Metadata.ResourceVersion != "100" {
		t.Fatalf("list resourceVersion = %q, want 100", list.Metadata.ResourceVersion)
	}
	if len(list.Items) != 2 {
		t.Fatalf("list items = %d, want 2", len(list.Items))
	}
	if got := objName(list.Items[0]); got != "cm-a" {
		t.Fatalf("first item = %q, want cm-a (sorted)", got)
	}
}

func TestWatchReceivesAddedAfterAdd(t *testing.T) {
	s, err := NewFake(nil)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer s.Close()

	resp, r := startWatch(t, s, "100", false)
	defer resp.Body.Close()

	s.AddOrUpdate("cm-a", cm("cm-a"), "")

	ev := nextEvent(t, r)
	if ev.Type != "ADDED" {
		t.Fatalf("first event type = %q, want ADDED", ev.Type)
	}
	var obj map[string]any
	if err := json.Unmarshal(ev.Object, &obj); err != nil {
		t.Fatalf("decode event object: %v", err)
	}
	if objName(obj) != "cm-a" {
		t.Fatalf("event object name = %q, want cm-a", objName(obj))
	}
	if m, _ := obj["metadata"].(map[string]any); m["resourceVersion"] != "101" {
		t.Fatalf("event object resourceVersion = %v, want 101", m["resourceVersion"])
	}
}

func TestWatchDeliversBookmark(t *testing.T) {
	s, err := NewFake(nil)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer s.Close()
	s.SetBookmarkEvery(1)

	resp, r := startWatch(t, s, "100", true)
	defer resp.Body.Close()

	s.AddOrUpdate("cm-a", cm("cm-a"), "")

	if ev := nextEvent(t, r); ev.Type != "ADDED" {
		t.Fatalf("first event type = %q, want ADDED", ev.Type)
	}
	ev := nextEvent(t, r)
	if ev.Type != "BOOKMARK" {
		t.Fatalf("second event type = %q, want BOOKMARK", ev.Type)
	}
	var obj map[string]any
	if err := json.Unmarshal(ev.Object, &obj); err != nil {
		t.Fatalf("decode bookmark object: %v", err)
	}
	m, _ := obj["metadata"].(map[string]any)
	if m["resourceVersion"] == "" {
		t.Fatal("bookmark missing resourceVersion")
	}
}

func TestForceGapExpiresOldResourceVersion(t *testing.T) {
	s, err := NewFake(nil)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer s.Close()

	s.AddOrUpdate("cm-a", cm("cm-a"), "")
	s.ForceGap()

	resp, r := startWatch(t, s, "100", false)
	defer resp.Body.Close()

	ev := nextEvent(t, r)
	if ev.Type != "ERROR" {
		t.Fatalf("event type = %q, want ERROR", ev.Type)
	}
	var status struct {
		Kind   string `json:"kind"`
		Status string `json:"status"`
		Reason string `json:"reason"`
		Code   int    `json:"code"`
	}
	if err := json.Unmarshal(ev.Object, &status); err != nil {
		t.Fatalf("decode status object: %v", err)
	}
	if status.Kind != "Status" || status.Status != "Failure" || status.Reason != "Expired" || status.Code != http.StatusGone {
		t.Fatalf("status object = %+v, want Status/Failure/Expired/410", status)
	}
}

func TestForceGapLeavesCurrentResourceVersionWatchable(t *testing.T) {
	s, err := NewFake(nil)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer s.Close()

	s.AddOrUpdate("cm-a", cm("cm-a"), "")
	s.ForceGap()

	resp, r := startWatch(t, s, "102", false)
	defer resp.Body.Close()

	s.AddOrUpdate("cm-b", cm("cm-b"), "")
	ev := nextEvent(t, r)
	if ev.Type != "ADDED" {
		t.Fatalf("event type = %q, want ADDED", ev.Type)
	}
	var obj map[string]any
	if err := json.Unmarshal(ev.Object, &obj); err != nil {
		t.Fatalf("decode event object: %v", err)
	}
	if objName(obj) != "cm-b" {
		t.Fatalf("event object name = %q, want cm-b", objName(obj))
	}
}

func TestWatchRequiresAuthorizationHeader(t *testing.T) {
	s, err := NewFake(nil)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer s.Close()

	for _, path := range []string{
		"/api/v1/namespaces/default/configmaps",
		"/api/v1/configmaps",
		"/api/v1/namespaces/default/configmaps?watch=true&resourceVersion=100",
	} {
		req, err := http.NewRequest(http.MethodGet, s.URL+path, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s status = %d, want 401; body = %s", path, resp.StatusCode, body)
		}
	}
}

func TestWatchRejectsWrongRequiredToken(t *testing.T) {
	s, err := NewFake(nil)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer s.Close()
	s.RequireToken("super-secret")

	req, _ := http.NewRequest(http.MethodGet, s.URL+"/api/v1/configmaps", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 403; body = %s", resp.StatusCode, body)
	}
}

func TestClusterWideListIncludesAllNamespaces(t *testing.T) {
	s, err := NewFake(nil)
	if err != nil {
		t.Fatalf("NewFake: %v", err)
	}
	defer s.Close()

	other := map[string]any{
		"metadata": map[string]any{"name": "cm-other", "namespace": "kube-system"},
	}
	s.AddOrUpdate("cm-other", other, "")
	s.AddOrUpdate("cm-a", cm("cm-a"), "")

	resp := doRaw(t, http.MethodGet, s.URL+"/api/v1/configmaps")
	defer resp.Body.Close()
	var list struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("cluster-wide items = %d, want 2", len(list.Items))
	}
}
