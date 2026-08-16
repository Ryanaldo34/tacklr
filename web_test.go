package tacklr

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/exa"
	"github.com/ryanaldo34/tacklr/stores"
)

func newExaTestClient(t *testing.T, handler http.HandlerFunc) *exa.Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	client := exa.NewClient("k")
	client.BaseURL = srv.URL
	client.HTTPClient = srv.Client()
	return client
}

func TestWebSearchTool_invokeAgainstServer(t *testing.T) {
	client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		w.Header().Set("Content-Type", "application/json")
		q, _ := body["query"].(string)
		switch q {
		case "capital of France":
			_, _ = w.Write([]byte(`{"results":[{"title":"Paris","url":"https://example.com/paris","highlights":["Paris is the capital",""],"text":"Paris is the capital of France.","summary":"Capital city","author":"Ed","publishedDate":"2024-01-01"}],"output":{"content":"Paris is the capital."}}`))
		case "empty-results":
			_, _ = w.Write([]byte(`{"results":[]}`))
		case "untitled":
			_, _ = w.Write([]byte(`{"results":[{"url":"https://example.com/x","highlights":["hit"]}],"output":{"content":{"answer":"ok"}}}`))
		default:
			_, _ = w.Write([]byte(`{"results":[{"title":"Hit","url":"https://example.com"}]}`))
		}
	})
	tool := newWebSearchTool(client)
	ctx := context.Background()

	res, err := tool.invoke(ctx, `{
		"query":"capital of France",
		"num_results":20,
		"type":"fast",
		"content_mode":"both",
		"max_text_characters":20000,
		"user_location":"US",
		"system_prompt":"prefer primary sources",
		"max_age_hours":24
	}`, nopRuntime())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "Paris") || !strings.Contains(res.output, "capital") {
		t.Fatal(res.output)
	}

	res, err = tool.invoke(ctx, `{"query":"empty-results"}`, nopRuntime())
	if err != nil || !strings.Contains(res.output, "No results found") {
		t.Fatalf("empty: %q err=%v", res.output, err)
	}
	res, err = tool.invoke(ctx, `{"query":"untitled","content_mode":"highlights"}`, nopRuntime())
	if err != nil || !strings.Contains(res.output, "(untitled)") || !strings.Contains(res.output, `"answer"`) {
		t.Fatalf("untitled/synth: %q err=%v", res.output, err)
	}
	if _, err := tool.invoke(ctx, `{"query":"  "}`, nil); err == nil || !strings.Contains(err.Error(), "query is required") {
		t.Fatalf("empty query: %v", err)
	}
	if _, err := tool.invoke(ctx, `{"query":"q","type":"nope"}`, nil); err == nil {
		t.Fatal("invalid type")
	}
	if _, err := tool.invoke(ctx, `{"query":"q","category":"nope"}`, nil); err == nil {
		t.Fatal("invalid category")
	}
	if _, err := tool.invoke(ctx, `{"query":"q","category":"company","exclude_domains":["x.com"]}`, nil); err == nil {
		t.Fatal("company exclude")
	}
	if _, err := tool.invoke(ctx, `{"query":"q","category":"people","start_published_date":"2024-01-01T00:00:00Z"}`, nil); err == nil {
		t.Fatal("people dates")
	}
	if _, err := tool.invoke(ctx, `{"query":"q","content_mode":"raw"}`, nil); err == nil {
		t.Fatal("invalid mode")
	}
	res, err = tool.invoke(ctx, `{"query":"q","type":"text","content_mode":"text","category":"news"}`, nopRuntime())
	if err == nil && !strings.Contains(res.output, "Hit") {
		// type "text" is invalid — already covered; news+text mode
	}
	res, err = tool.invoke(ctx, `{"query":"news-q","type":"auto","content_mode":"text","category":"news"}`, nopRuntime())
	if err != nil || !strings.Contains(res.output, "Hit") {
		t.Fatalf("text mode: %q err=%v", res.output, err)
	}

	h := mustNewAgent(t, ctx, AgentOptions{
		Store: stores.NewInMemoryStore(), Model: &mockStrategy{}, ExaAPIKey: "from-opts",
	})
	t.Cleanup(h.Close)
	if h.findTool("web_search", "") == nil || h.findTool("web_fetch", "") == nil {
		t.Fatal("ExaAPIKey should install web tools")
	}
}

func TestRunWebFetch_endToEnd(t *testing.T) {
	client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/contents" {
			http.NotFound(w, r)
			return
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		w.Header().Set("Content-Type", "application/json")
		urls, _ := body["urls"].([]any)
		if len(urls) == 1 && urls[0] == "https://example.gov/missing" {
			_, _ = w.Write([]byte(`{"results":[],"statuses":[{"id":"https://example.gov/missing","status":"error","error":{"tag":"not_found"}}]}`))
			return
		}
		if len(urls) == 0 {
			_, _ = w.Write([]byte(`{"results":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"title":"City Code","url":"https://example.gov/code","text":"Section 1.2 setbacks"}]}`))
	})
	ctx := context.Background()

	got, err := runWebFetch(ctx, client, webFetchArgs{URLs: []string{"https://example.gov/code"}}, nopRuntime())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "City Code") || !strings.Contains(got, "setbacks") {
		t.Fatal(got)
	}

	got, err = runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"  ", "ftp://x", "example.gov/code", "https://example.gov/code", "not a host"},
	}, nopRuntime())
	if err != nil || !strings.Contains(got, "City Code") {
		t.Fatalf("normalize: %q err=%v", got, err)
	}

	got, err = runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"https://example.gov/missing"}, ContentMode: "highlights", HighlightQuery: "setbacks",
	}, nopRuntime())
	if err != nil || !strings.Contains(got, "error") || !strings.Contains(got, "not_found") {
		t.Fatalf("status: %q err=%v", got, err)
	}

	if _, err := runWebFetch(ctx, client, webFetchArgs{}, nil); err == nil {
		t.Fatal("no urls")
	}
	if _, err := runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"https://a", "https://b", "https://c", "https://d", "https://e", "https://f"},
	}, nopRuntime()); err == nil {
		t.Fatal("too many urls")
	}
	if _, err := runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"https://example.gov/code"}, ContentMode: "raw",
	}, nopRuntime()); err == nil {
		t.Fatal("invalid mode")
	}
	got, err = runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"https://example.gov/code"}, ContentMode: "both", MaxTextCharacters: 20000,
	}, nopRuntime())
	if err != nil || !strings.Contains(got, "City Code") {
		t.Fatalf("both: %q err=%v", got, err)
	}
}
