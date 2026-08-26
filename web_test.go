package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/exa"
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
	if _, err := tool.invoke(ctx, `{"query":"  "}`, nopRuntime()); err == nil || !strings.Contains(err.Error(), "query is required") {
		t.Fatalf("empty query: %v", err)
	}
	if _, err := tool.invoke(ctx, `{"query":"q","type":"nope"}`, nopRuntime()); err == nil {
		t.Fatal("invalid type")
	}
	if _, err := tool.invoke(ctx, `{"query":"q","category":"nope"}`, nopRuntime()); err == nil {
		t.Fatal("invalid category")
	}
	res, err = tool.invoke(ctx, `{"query":"q","category":"company","exclude_domains":["x.com"]}`, nopRuntime())
	if err != nil || !strings.Contains(res.output, "Hit") || !strings.Contains(res.output, "Dropped exclude_domains") {
		t.Fatalf("company exclude coerced: %q err=%v", res.output, err)
	}
	res, err = tool.invoke(ctx, `{"query":"q","category":"people","start_published_date":"2024-01-01T00:00:00Z"}`, nopRuntime())
	if err != nil || !strings.Contains(res.output, "Hit") || !strings.Contains(res.output, "Dropped published-date") {
		t.Fatalf("people dates coerced: %q err=%v", res.output, err)
	}
	if _, err := tool.invoke(ctx, `{"query":"q","content_mode":"raw"}`, nopRuntime()); err == nil {
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

	res, err = tool.invoke(ctx, `{"query":"ACS API","category":"publication","include_domains":["census.gov"]}`, nopRuntime())
	if err != nil || !strings.Contains(res.output, "Hit") || !strings.Contains(res.output, "Dropped category=publication") {
		t.Fatalf("publication+domain coerced: %q err=%v", res.output, err)
	}
	res, err = tool.invoke(ctx, `{"query":"site:census.gov ACS 5-year API"}`, nopRuntime())
	if err != nil || !strings.Contains(res.output, "Hit") || !strings.Contains(res.output, "Moved site:") {
		t.Fatalf("site: lift: %q err=%v", res.output, err)
	}

	h := mustNewTurnManager(t, AgentOptions{Model: &mockStrategy{}, ExaAPIKey: "from-opts"})
	t.Cleanup(h.Close)
	if h.findTool("web_search", "") == nil || h.findTool("web_fetch", "") == nil {
		t.Fatal("ExaAPIKey should install web tools")
	}
}

func TestWebSearchTool_httpStatusCorrectionAndFailed(t *testing.T) {
	ctx := context.Background()

	fixable := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad query filters"}`))
	})
	_, err := newWebSearchTool(fixable).invoke(ctx, `{"query":"census ACS"}`, nopRuntime())
	if err == nil || !errors.Is(err, ErrCorrection) || !strings.Contains(err.Error(), "Simplify filters") {
		t.Fatalf("400: %v", err)
	}

	limited := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	})
	_, err = newWebSearchTool(limited).invoke(ctx, `{"query":"census ACS"}`, nopRuntime())
	if err == nil || !errors.Is(err, ErrFailed) || errors.Is(err, ErrCorrection) || !strings.Contains(err.Error(), "search provider failed") {
		t.Fatalf("429: %v", err)
	}
	var st *exa.StatusError
	if !errors.As(err, &st) || st.Status != http.StatusTooManyRequests {
		t.Fatalf("429 cause: %v", err)
	}
}

func TestWebSearch_schemaHidesExaKnobs(t *testing.T) {
	search := newWebSearchTool(exa.NewClient("k")).AsJson()
	params, _ := search["parameters"].(map[string]any)
	props, _ := params["properties"].(map[string]any)
	for _, hidden := range []string{"type", "category", "exclude_domains", "start_published_date", "end_published_date", "content_mode", "max_text_characters", "system_prompt", "user_location", "max_age_hours"} {
		if _, ok := props[hidden]; ok {
			t.Fatalf("web_search schema still exposes %s", hidden)
		}
	}
	for _, want := range []string{"query", "include_domains", "num_results"} {
		if _, ok := props[want]; !ok {
			t.Fatalf("web_search schema missing %s", want)
		}
	}
	fetch := newWebFetchTool(exa.NewClient("k")).AsJson()
	fparams, _ := fetch["parameters"].(map[string]any)
	fprops, _ := fparams["properties"].(map[string]any)
	if _, ok := fprops["content_mode"]; ok {
		t.Fatal("web_fetch schema still exposes content_mode")
	}
	if _, ok := fprops["urls"]; !ok {
		t.Fatal("web_fetch schema missing urls")
	}
}

func TestWebSearch_retriesEmptyFilteredSearch(t *testing.T) {
	var n int
	client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		n++
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		w.Header().Set("Content-Type", "application/json")
		cat, _ := body["category"].(string)
		if cat == "publication" {
			_, _ = w.Write([]byte(`{"results":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"title":"Census ACS","url":"https://www.census.gov/acs"}]}`))
	})
	res, err := newWebSearchTool(client).invoke(context.Background(),
		`{"query":"ACS 5-year demographics","category":"publication"}`, nopRuntime())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("searches = %d, want 2 (empty then retry)", n)
	}
	if !strings.Contains(res.output, "Census ACS") || !strings.Contains(res.output, "No hits with those filters") {
		t.Fatalf("retry output: %s", res.output)
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
