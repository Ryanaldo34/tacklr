package exa_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/internal/exa"
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

	c := exa.NewClient("test-key")
	c.BaseURL = srv.URL
	c.HTTPClient = srv.Client()

	resp, err := c.Search(context.Background(), exa.SearchRequest{
		Query:      "what is exa",
		Type:       "auto",
		NumResults: 5,
		Contents: &exa.ContentsOptions{
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

	c := exa.NewClient("bad")
	c.BaseURL = srv.URL
	c.HTTPClient = srv.Client()

	_, err := c.Search(context.Background(), exa.SearchRequest{Query: "q"})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v", err)
	}
}

// TestClient_Search_validation rejects empty query and empty key.
func TestClient_Search_validation(t *testing.T) {
	c := exa.NewClient("")
	if _, err := c.Search(context.Background(), exa.SearchRequest{Query: "q"}); err == nil {
		t.Fatal("want missing key")
	}
	c = exa.NewClient("k")
	if _, err := c.Search(context.Background(), exa.SearchRequest{}); err == nil {
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

	c := exa.NewClient("test-key")
	c.BaseURL = srv.URL
	c.HTTPClient = srv.Client()

	resp, err := c.Contents(context.Background(), exa.ContentsRequest{
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
	c := exa.NewClient("k")
	if _, err := c.Contents(context.Background(), exa.ContentsRequest{}); err == nil {
		t.Fatal("want missing urls")
	}
	if _, err := c.Contents(context.Background(), exa.ContentsRequest{
		URLs: []string{"https://a.com"},
		IDs:  []string{"id1"},
	}); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("want urls-or-ids: %v", err)
	}
}

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

	c := exa.NewClient("k")
	c.BaseURL = srv.URL
	c.HTTPClient = &http.Client{Timeout: 5 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	_, err := c.Search(ctx, exa.SearchRequest{Query: "q"})
	if err == nil {
		t.Fatal("want cancel error")
	}
}
