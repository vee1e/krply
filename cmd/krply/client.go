package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// apiClient is a minimal JSON client for the krply-server HTTP API.
type apiClient struct {
	base string
	hc   *http.Client
}

func newAPIClient(base string) *apiClient {
	return &apiClient{
		base: strings.TrimRight(base, "/"),
		hc:   &http.Client{Timeout: 60 * time.Second},
	}
}

// get issues a GET request and decodes the JSON response into out.
func (c *apiClient) get(path string, out any) error {
	return c.do(http.MethodGet, path, nil, out)
}

// post issues a POST request with a JSON body and decodes the response.
func (c *apiClient) post(path string, body, out any) error {
	return c.do(http.MethodPost, path, body, out)
}

func (c *apiClient) do(method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("krply-server %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("krply-server %s %s: read response: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("krply-server %s %s: %s: %s", method, path, resp.Status, errorBody(data))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("krply-server %s %s: decode response: %w", method, path, err)
		}
	}
	return nil
}

// errorBody extracts a server-provided {"error":...} message from a failed
// response body, falling back to the raw body.
func errorBody(data []byte) string {
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &e); err == nil && e.Error != "" {
		return e.Error
	}
	body := strings.TrimSpace(string(data))
	if body == "" {
		return "no detail in response body"
	}
	return body
}

// withQuery appends url-encoded query parameters to a path.
func withQuery(path string, q url.Values) string {
	if len(q) == 0 {
		return path
	}
	return path + "?" + q.Encode()
}
