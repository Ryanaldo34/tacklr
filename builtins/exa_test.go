package builtins

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestClient_Search_success posts the request and parses results.
func TestClient_Search_success(t *testing.T) {
	var gotBody map[string]any
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" || r.Method != http.MethodPost {
			t.Fatalf("path/method = %s %s", r.Method, r.URL.Path)
		}
		gotKey = r.Header.Get("x-api-key")
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &gotBody); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"requestId":"req1",
			"results":[{
				"title":"Example",
				"url":"https://example.com",
				"highlights":["snippet one"]
			}]
		}`))
	}))
	t.Cleanup(srv.Close)

	c := NewExa("test-key")
	c.BaseURL = srv.URL
	c.HTTPClient = srv.Client()

	resp, err := c.Search(context.Background(), SearchRequest{
		Query:      "what is exa",
		Type:       "auto",
		NumResults: 5,
		Contents: &ContentsOptions{
			Highlights: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotKey != "test-key" {
		t.Fatalf("api key header = %q", gotKey)
	}
	if gotBody["query"] != "what is exa" {
		t.Fatalf("body = %#v", gotBody)
	}
	contents, _ := gotBody["contents"].(map[string]any)
	if contents["highlights"] != true {
		t.Fatalf("contents = %#v", contents)
	}
	if len(resp.Results) != 1 || resp.Results[0].Title != "Example" {
		t.Fatalf("resp = %+v", resp)
	}
	if len(resp.Results[0].Highlights) != 1 {
		t.Fatal("highlights")
	}
}

// TestClient_Search_httpError surfaces status and body.
func TestClient_Search_httpError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid api key"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewExa("bad")
	c.BaseURL = srv.URL
	c.HTTPClient = srv.Client()

	_, err := c.Search(context.Background(), SearchRequest{Query: "q"})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v", err)
	}
	if !errors.Is(err, ErrService) {
		t.Fatalf("401 must be service failed: %v", err)
	}
}

// TestClient_Search_validation rejects empty query and empty key.
func TestClient_Search_validation(t *testing.T) {
	c := NewExa("")
	if _, err := c.Search(context.Background(), SearchRequest{Query: "q"}); err == nil {
		t.Fatal("want missing key")
	}
	c = NewExa("k")
	if _, err := c.Search(context.Background(), SearchRequest{}); err == nil {
		t.Fatal("want missing query")
	}
}

// TestClient_Contents_success posts urls and parses page text.
func TestClient_Contents_success(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/contents" || r.Method != http.MethodPost {
			t.Fatalf("path/method = %s %s", r.Method, r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &gotBody); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"requestId":"c1",
			"results":[{
				"title":"Paper",
				"url":"https://arxiv.org/abs/1",
				"text":"abstract body"
			}],
			"statuses":[{"id":"https://arxiv.org/abs/1","status":"success","source":"cached"}]
		}`))
	}))
	t.Cleanup(srv.Close)

	c := NewExa("test-key")
	c.BaseURL = srv.URL
	c.HTTPClient = srv.Client()

	resp, err := c.Contents(context.Background(), ContentsRequest{
		URLs: []string{"https://arxiv.org/abs/1"},
		Text: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	urls, _ := gotBody["urls"].([]any)
	if len(urls) != 1 {
		t.Fatalf("body = %#v", gotBody)
	}
	if gotBody["text"] != true {
		t.Fatalf("text field = %#v", gotBody["text"])
	}
	if len(resp.Results) != 1 || resp.Results[0].Text != "abstract body" {
		t.Fatalf("resp = %+v", resp)
	}
	if len(resp.Statuses) != 1 || resp.Statuses[0].Status != "success" {
		t.Fatalf("statuses = %+v", resp.Statuses)
	}
}

// TestClient_Contents_validation rejects empty urls and mixed urls+ids.
func TestClient_Contents_validation(t *testing.T) {
	c := NewExa("k")
	if _, err := c.Contents(context.Background(), ContentsRequest{}); err == nil {
		t.Fatal("want missing urls")
	}
	if _, err := c.Contents(context.Background(), ContentsRequest{
		URLs: []string{"https://a.com"},
		IDs:  []string{"id1"},
	}); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("want urls-or-ids: %v", err)
	}
	// Nil client / missing key.
	var nilC *Exa
	if _, err := nilC.Contents(context.Background(), ContentsRequest{URLs: []string{"https://a.com"}}); err == nil {
		t.Fatal("want nil client error")
	}
	if _, err := NewExa("").Contents(context.Background(), ContentsRequest{IDs: []string{"id1"}}); err == nil {
		t.Fatal("want missing key")
	}
}

// TestClient_Contents_byIDs posts ids-only request (no urls).
func TestClient_Contents_byIDs(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"title":"T","url":"https://x","text":"body"}]}`))
	}))
	t.Cleanup(srv.Close)
	c := NewExa("k")
	c.BaseURL = srv.URL
	c.HTTPClient = srv.Client()
	resp, err := c.Contents(context.Background(), ContentsRequest{
		IDs:  []string{"doc-1"},
		Text: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotBody["urls"] != nil {
		t.Fatalf("urls should be omitted: %#v", gotBody)
	}
	ids, _ := gotBody["ids"].([]any)
	if len(ids) != 1 {
		t.Fatalf("ids = %#v", gotBody["ids"])
	}
	if len(resp.Results) != 1 || resp.Results[0].Text != "body" {
		t.Fatalf("%+v", resp)
	}
}

// TestClient_Contents_httpErrorAndBadJSON covers non-2xx and decode failure.
func TestClient_Contents_httpErrorAndBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"nope"}`))
	}))
	t.Cleanup(srv.Close)
	c := NewExa("k")
	c.BaseURL = srv.URL
	c.HTTPClient = srv.Client()
	if _, err := c.Contents(context.Background(), ContentsRequest{URLs: []string{"https://a.com"}}); err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("err = %v", err)
	}

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not-json`))
	}))
	t.Cleanup(srv2.Close)
	c.BaseURL = srv2.URL
	c.HTTPClient = srv2.Client()
	if _, err := c.Contents(context.Background(), ContentsRequest{URLs: []string{"https://a.com"}}); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("err = %v", err)
	}
}

// TestClient_postJSON_edges covers empty base URL, nil HTTP client, empty error body,
// truncated error body, and nil out decode skip.
func TestClient_postJSON_edges(t *testing.T) {
	// Empty error body uses status text.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	c := NewExa("k")
	c.BaseURL = srv.URL
	c.HTTPClient = nil // use default client with srv — need srv.Client for host
	c.HTTPClient = srv.Client()
	if _, err := c.Search(context.Background(), SearchRequest{Query: "q"}); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("empty body: %v", err)
	}

	// Long error body truncated with ellipsis.
	long := strings.Repeat("x", 600)
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(long))
	}))
	t.Cleanup(srv2.Close)
	c.BaseURL = srv2.URL
	c.HTTPClient = srv2.Client()
	err := func() error {
		_, e := c.Search(context.Background(), SearchRequest{Query: "q"})
		return e
	}()
	if err == nil || !strings.Contains(err.Error(), "…") {
		t.Fatalf("truncate: %v", err)
	}

	// Empty BaseURL falls back to default (will fail dial unless we set client transport).
	// Cover default base + success with custom transport via BaseURL "" and absolute? Client concatenates base+path.
	// Use a server and set BaseURL to "" but override HTTPClient to rewrite — simpler: set BaseURL "" and use RoundTripper.
	// Just hit empty BaseURL path with a Transport that returns OK for any URL.
	c3 := NewExa("k")
	c3.BaseURL = ""
	c3.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(r.URL.String(), "https://api.exa.ai/") {
			t.Fatalf("default base: %s", r.URL)
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`{"results":[]}`)),
			Header:     make(http.Header),
		}, nil
	})}
	if _, err := c3.Search(context.Background(), SearchRequest{Query: "q"}); err != nil {
		t.Fatal(err)
	}
}

func TestExaPostJSON_remainingBranches(t *testing.T) {
	c := NewExa("k")
	if err := c.postJSON(t.Context(), "/search", make(chan int), nil); err == nil || !strings.Contains(err.Error(), "marshal") {
		t.Fatalf("marshal: %v", err)
	}
	c.BaseURL = "://bad"
	if err := c.postJSON(t.Context(), "/search", SearchRequest{Query: "q"}, nil); err == nil || !strings.Contains(err.Error(), "build request") {
		t.Fatalf("url: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	t.Cleanup(srv.Close)
	c.BaseURL = srv.URL
	c.HTTPClient = nil
	if err := c.postJSON(t.Context(), "/", SearchRequest{Query: "q"}, nil); err != nil {
		t.Fatalf("nil client / empty op: %v", err)
	}

	c.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: errReadCloser{}, Header: make(http.Header)}, nil
	})}
	if err := c.postJSON(t.Context(), "/search", SearchRequest{Query: "q"}, &SearchResponse{}); err == nil || !strings.Contains(err.Error(), "read body") {
		t.Fatalf("read body: %v", err)
	}
}

type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) { return 0, errors.New("read fail") }
func (errReadCloser) Close() error             { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestClient_Search_contextCancel fails the in-flight request.
func TestClient_Search_contextCancel(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewExa("k")
	c.BaseURL = srv.URL
	c.HTTPClient = &http.Client{Timeout: 5 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	_, err := c.Search(ctx, SearchRequest{Query: "q"})
	if err == nil {
		t.Fatal("want cancel error")
	}
}
