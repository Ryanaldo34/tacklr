package web

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

func newExaTestClient(t *testing.T, handler http.HandlerFunc) *Exa {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	client := NewExa("k")
	client.BaseURL = srv.URL
	client.HTTPClient = srv.Client()
	return client
}

// TestWebSearch_searchesAndCoercesFilters: web_search formats Exa's results,
// rejects arguments it cannot send, and drops or moves filters Exa does not
// take, with a note that says so.
func TestWebSearch_searchesAndCoercesFilters(t *testing.T) {
	t.Parallel()
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
		case "census.gov":
			_, _ = w.Write([]byte(`{"results":[{"title":"Census","url":"https://census.gov","highlights":["` + strings.Repeat("h", 900) + `"]}]}`))
		case "untitled":
			_, _ = w.Write([]byte(`{"results":[{"url":"https://example.com/x","highlights":["hit"]}],"output":{"content":{"answer":"ok"}}}`))
		default:
			_, _ = w.Write([]byte(`{"results":[{"title":"Hit","url":"https://example.com"}]}`))
		}
	})
	ctx := context.Background()
	age := 24

	got, err := runWebSearch(ctx, client, webSearchArgs{
		Query: "capital of France", NumResults: 20, Type: "fast", ContentMode: "both",
		MaxTextCharacters: 20000, UserLocation: "US", SystemPrompt: "prefer primary sources", MaxAgeHours: &age,
	})
	if err != nil || !strings.Contains(got, "Paris") || !strings.Contains(got, "capital") {
		t.Fatalf("search: %q err=%v", got, err)
	}

	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "empty-results"})
	if err != nil || !strings.Contains(got, "No results found") {
		t.Fatalf("empty: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "untitled", ContentMode: "highlights"})
	if err != nil || !strings.Contains(got, "(untitled)") || !strings.Contains(got, `"answer"`) {
		t.Fatalf("untitled/synth: %q err=%v", got, err)
	}
	if _, err := runWebSearch(ctx, client, webSearchArgs{Query: "  "}); err == nil || !strings.Contains(err.Error(), "query is required") {
		t.Fatalf("empty query: %v", err)
	}
	if _, err := runWebSearch(ctx, client, webSearchArgs{Query: "q", Type: "nope"}); err == nil {
		t.Fatal("invalid type")
	}
	if _, err := runWebSearch(ctx, client, webSearchArgs{Query: "q", Category: "nope"}); err == nil {
		t.Fatal("invalid category")
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "q", Category: "company", ExcludeDomains: []string{"x.com"}})
	if err != nil || !strings.Contains(got, "Hit") || !strings.Contains(got, "Dropped exclude_domains") {
		t.Fatalf("company exclude coerced: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "q", Category: "people", StartPublishedDate: "2024-01-01T00:00:00Z"})
	if err != nil || !strings.Contains(got, "Hit") || !strings.Contains(got, "Dropped published-date") {
		t.Fatalf("people dates coerced: %q err=%v", got, err)
	}
	if _, err := runWebSearch(ctx, client, webSearchArgs{Query: "q", ContentMode: "raw"}); err == nil {
		t.Fatal("invalid mode")
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "news-q", Type: "auto", ContentMode: "text", Category: "news"})
	if err != nil || !strings.Contains(got, "Hit") {
		t.Fatalf("text mode: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "ACS API", Category: "publication", IncludeDomains: []string{"census.gov"}})
	if err != nil || !strings.Contains(got, "Hit") || !strings.Contains(got, "Dropped category=publication") {
		t.Fatalf("publication+domain coerced: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "site:census.gov ACS 5-year API"})
	if err != nil || !strings.Contains(got, "Hit") || !strings.Contains(got, "Moved site:") {
		t.Fatalf("site: lift: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "site:census.gov"})
	if err != nil || !strings.Contains(got, "Query: census.gov") || !strings.Contains(got, strings.Repeat("h", 800)+"…") {
		t.Fatalf("site-only query searches the host, with a long highlight cut: %q err=%v", got, err)
	}
}

// TestWebSearch_retriesOnTheOpenWebWhenTheFiltersFail: a filtered search that
// finds nothing, or that Exa rejects for its filters, runs again without the
// filters; a retry that fails is the tool's failure.
func TestWebSearch_retriesOnTheOpenWebWhenTheFiltersFail(t *testing.T) {
	t.Parallel()
	hit := `{"results":[{"title":"Open web hit","url":"https://example.com"}]}`
	for name, tc := range map[string]struct {
		args        webSearchArgs
		first       func(http.ResponseWriter)
		retryStatus int // 0 answers the retry with a hit
		want        string
	}{
		"no hits with a category": {
			webSearchArgs{Query: "ACS 5-year demographics", Category: "publication"},
			func(w http.ResponseWriter) { _, _ = w.Write([]byte(`{"results":[]}`)) }, 0, "No hits with those filters",
		},
		"a domain filter exa rejects": {
			webSearchArgs{Query: "paper", IncludeDomains: []string{"arxiv.org"}},
			func(w http.ResponseWriter) { http.Error(w, "UNSUPPORTED_PUBLICATION", http.StatusBadRequest) }, 0, "retried on the open web",
		},
		"a retry that fails": {
			webSearchArgs{Query: "q", Category: "news"},
			func(w http.ResponseWriter) { _, _ = w.Write([]byte(`{"results":[]}`)) }, http.StatusBadGateway, "search provider failed",
		},
	} {
		var requests []map[string]any
		client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			requests = append(requests, body)
			switch {
			case len(requests) == 1:
				tc.first(w)
			case tc.retryStatus != 0:
				w.WriteHeader(tc.retryStatus)
			default:
				_, _ = w.Write([]byte(hit))
			}
		})

		got, err := runWebSearch(t.Context(), client, tc.args)

		if len(requests) != 2 || requests[1]["category"] != nil || requests[1]["includeDomains"] != nil {
			t.Errorf("%s: requests = %v, want the search, then one without filters", name, requests)
		}
		if tc.retryStatus != 0 {
			if !errors.Is(err, tacklr.ErrFailed) || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s = %v, want ErrFailed naming %q", name, err, tc.want)
			}
			continue
		}
		if err != nil || !strings.Contains(got, "Open web hit") || !strings.Contains(got, tc.want) {
			t.Errorf("%s = %q %v, want the open-web hit and %q", name, got, err, tc.want)
		}
	}
}

// TestWebSearch_mapsProviderFailuresToToolErrors: a query Exa rejects asks
// the model for a correction, an expired key is ErrAuthExpired, a provider
// that is down or limits the rate is ErrFailed, and any other failure keeps
// its own error.
func TestWebSearch_mapsProviderFailuresToToolErrors(t *testing.T) {
	t.Parallel()
	answering := func(status int, body string) *Exa {
		return newExaTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		})
	}
	down := answering(http.StatusOK, "")
	down.BaseURL = "http://127.0.0.1:1"

	for name, tc := range map[string]struct {
		client *Exa
		is     error
		want   string
	}{
		"a query exa rejects":             {answering(http.StatusBadRequest, `{"error":"bad query filters"}`), tacklr.ErrCorrection, "Simplify filters"},
		"a domain filter on publications": {answering(http.StatusBadRequest, "not supported for category=publication"), tacklr.ErrCorrection, "category=publication cannot filter by domain"},
		"an expired key":                  {answering(http.StatusUnauthorized, "no"), tacklr.ErrAuthExpired, "credentials expired"},
		"a rate limit":                    {answering(http.StatusTooManyRequests, `{"error":"rate limited"}`), ErrService, "search provider failed"},
		"a provider that does not answer": {down, tacklr.ErrFailed, "connection refused"},
		"a response that is not json":     {answering(http.StatusOK, "not-json"), nil, "decode response"},
		"a client with no key":            {NewExa(""), nil, "API key is required"},
	} {
		_, err := runWebSearch(t.Context(), tc.client, webSearchArgs{Query: "census ACS"})

		if err == nil || (tc.is != nil && !errors.Is(err, tc.is)) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s = %v, want %v naming %q", name, err, tc.is, tc.want)
		}
	}
}

// TestWebFetch_readsKnownURLs: web_fetch reads the URLs it can normalize,
// asks Exa for text, highlights or both, reports a URL Exa could not read,
// and rejects arguments it cannot send.
func TestWebFetch_readsKnownURLs(t *testing.T) {
	t.Parallel()
	var highlights any
	client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/contents" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		highlights = body["highlights"]
		w.Header().Set("Content-Type", "application/json")
		urls, _ := body["urls"].([]any)
		if len(urls) == 1 && urls[0] == "https://example.gov/missing" {
			_, _ = w.Write([]byte(`{"results":[],"statuses":[{"id":"https://example.gov/missing","status":"error","error":{"tag":"not_found"}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"title":"City Code","url":"https://example.gov/code","text":"Section 1.2 setbacks"}]}`))
	})
	ctx := t.Context()
	fetch := func(args webFetchArgs) string {
		t.Helper()
		got, err := runWebFetch(ctx, client, args)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	text := fetch(webFetchArgs{URLs: []string{"https://example.gov/code"}})
	normalized := fetch(webFetchArgs{URLs: []string{"  ", "ftp://x", "example.gov/code", "https://example.gov/code", "not a host"}})
	missing := fetch(webFetchArgs{URLs: []string{"https://example.gov/missing"}, ContentMode: "highlights", HighlightQuery: "setbacks"})
	fetch(webFetchArgs{URLs: []string{"https://example.gov/code"}, ContentMode: "highlights"})
	anyHighlights := highlights
	fetch(webFetchArgs{URLs: []string{"https://example.gov/code"}, ContentMode: "both"})
	textAndAnyHighlights := highlights
	fetch(webFetchArgs{URLs: []string{"https://example.gov/code"}, ContentMode: "both", MaxTextCharacters: 20000, HighlightQuery: "setbacks"})
	focused, _ := highlights.(map[string]any)
	_, noURLs := runWebFetch(ctx, client, webFetchArgs{})
	_, tooMany := runWebFetch(ctx, client, webFetchArgs{URLs: []string{"https://a", "https://b", "https://c", "https://d", "https://e", "https://f"}})
	_, badMode := runWebFetch(ctx, client, webFetchArgs{URLs: []string{"https://example.gov/code"}, ContentMode: "raw"})
	_, noKey := runWebFetch(ctx, NewExa(""), webFetchArgs{URLs: []string{"https://example.gov/code"}})
	down := newExaTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) })
	_, downErr := runWebFetch(ctx, down, webFetchArgs{URLs: []string{"https://example.com"}})

	if !strings.Contains(text, "City Code") || !strings.Contains(text, "setbacks") || !strings.Contains(normalized, "URLs requested: 1") {
		t.Errorf("fetch = %q, normalized = %q, want the page, and one URL left after normalizing", text, normalized)
	}
	if !strings.Contains(missing, "error (not_found)") {
		t.Errorf("fetch a missing page = %q, want its status", missing)
	}
	if anyHighlights != true || textAndAnyHighlights != true || !maps.Equal(focused, map[string]any{"query": "setbacks"}) {
		t.Errorf("highlights asked for = %v, %v, then %v, want any highlights twice, then highlights on the query", anyHighlights, textAndAnyHighlights, focused)
	}
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"no urls":          {noURLs, "at least one"},
		"too many urls":    {tooMany, "at most 5"},
		"an unknown mode":  {badMode, "invalid content_mode"},
		"no key":           {noKey, "API key is required"},
		"a provider error": {downErr, "search provider failed"},
	} {
		if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s = %v, want %q", name, tc.err, tc.want)
		}
	}
	if !errors.Is(downErr, tacklr.ErrFailed) {
		t.Errorf("fetch from a provider that fails = %v, want ErrFailed", downErr)
	}
}

// TestWebConstructors_offerOnlyTheAgentsArgumentsAndPanicWithoutAClient:
// each tool's schema holds only the arguments the agent sets (the harness
// sets the Exa options), and each constructor panics without an Exa client.
func TestWebConstructors_offerOnlyTheAgentsArgumentsAndPanicWithoutAClient(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		build func(*Exa) *tacklr.Tool
		args  []string
	}{
		"web_search": {WebSearch, []string{"include_domains", "num_results", "query"}},
		"web_fetch":  {WebFetch, []string{"highlight_query", "urls"}},
	} {
		panicked := func() (p bool) {
			defer func() { p = recover() != nil }()
			tc.build(nil)
			return false
		}()
		tool := tc.build(NewExa("k"))
		props, _ := tool.Parameters()["properties"].(map[string]any)

		if got := slices.Sorted(maps.Keys(props)); tool.Name() != name || !slices.Equal(got, tc.args) || !panicked {
			t.Errorf("%s: tool %q with arguments %v, panicked without a client %v, want %v and a panic", name, tool.Name(), got, panicked, tc.args)
		}
	}
}

// TestWebTools_runInAHarnessAndReportProgress: web_search and web_fetch run as
// harness tools: each reports its progress on the call's stream and returns
// what Exa answered.
func TestWebTools_runInAHarnessAndReportProgress(t *testing.T) {
	t.Parallel()
	client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/contents" {
			_, _ = w.Write([]byte(`{"results":[{"title":"City Code","url":"https://example.gov/code","text":"Section 1.2 setbacks"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"title":"Paris","url":"https://example.com/paris","text":"Paris is the capital of France."}]}`))
	})
	h, err := tacklr.NewHarness(t.Context(), tacklr.BindTurn(tacklr.AgentOptions{
		Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192, Tools: []*tacklr.Tool{WebSearch(client), WebFetch(client)},
	}, "web", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	run := func(name, args string) (string, []string) {
		var mu sync.Mutex
		var updates []string
		out, stop := tacklr.PipeEvents(func(ev session.Event) {
			if ev.Type == session.EventToolUpdate {
				mu.Lock()
				defer mu.Unlock()
				updates = append(updates, ev.Content)
			}
		})
		c, err := h.RunCall(t.Context(), tacklr.NewCall(message.ToolCall{ID: name, CallID: name, Name: name, Arguments: args}, 0), out)
		stop()
		if err != nil {
			t.Fatal(err)
		}
		return c.Body.Text(), updates
	}

	searched, searchUpdates := run("web_search", `{"query":"capital of France"}`)
	fetched, fetchUpdates := run("web_fetch", `{"urls":["https://example.gov/code"]}`)

	if !strings.Contains(searched, "Paris is the capital") || !slices.Equal(searchUpdates, []string{"Searching the web…"}) {
		t.Errorf("web_search = %q with updates %q, want Exa's result and one search update", searched, searchUpdates)
	}
	if !strings.Contains(fetched, "Section 1.2 setbacks") || !slices.Equal(fetchUpdates, []string{"Fetching page content…"}) {
		t.Errorf("web_fetch = %q with updates %q, want the page and one fetch update", fetched, fetchUpdates)
	}
}
