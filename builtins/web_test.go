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

	"github.com/ryanaldo34/tacklr"
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

func TestWebSearch_searchesAndCoercesFilters(t *testing.T) {
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
	ctx := context.Background()
	age := 24

	got, err := runWebSearch(ctx, client, webSearchArgs{
		Query: "capital of France", NumResults: 20, Type: "fast", ContentMode: "both",
		MaxTextCharacters: 20000, UserLocation: "US", SystemPrompt: "prefer primary sources", MaxAgeHours: &age,
	}, stubRuntime{})
	if err != nil || !strings.Contains(got, "Paris") || !strings.Contains(got, "capital") {
		t.Fatalf("search: %q err=%v", got, err)
	}

	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "empty-results"}, stubRuntime{})
	if err != nil || !strings.Contains(got, "No results found") {
		t.Fatalf("empty: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "untitled", ContentMode: "highlights"}, stubRuntime{})
	if err != nil || !strings.Contains(got, "(untitled)") || !strings.Contains(got, `"answer"`) {
		t.Fatalf("untitled/synth: %q err=%v", got, err)
	}
	if _, err := runWebSearch(ctx, client, webSearchArgs{Query: "  "}, stubRuntime{}); err == nil || !strings.Contains(err.Error(), "query is required") {
		t.Fatalf("empty query: %v", err)
	}
	if _, err := runWebSearch(ctx, client, webSearchArgs{Query: "q", Type: "nope"}, stubRuntime{}); err == nil {
		t.Fatal("invalid type")
	}
	if _, err := runWebSearch(ctx, client, webSearchArgs{Query: "q", Category: "nope"}, stubRuntime{}); err == nil {
		t.Fatal("invalid category")
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "q", Category: "company", ExcludeDomains: []string{"x.com"}}, stubRuntime{})
	if err != nil || !strings.Contains(got, "Hit") || !strings.Contains(got, "Dropped exclude_domains") {
		t.Fatalf("company exclude coerced: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "q", Category: "people", StartPublishedDate: "2024-01-01T00:00:00Z"}, stubRuntime{})
	if err != nil || !strings.Contains(got, "Hit") || !strings.Contains(got, "Dropped published-date") {
		t.Fatalf("people dates coerced: %q err=%v", got, err)
	}
	if _, err := runWebSearch(ctx, client, webSearchArgs{Query: "q", ContentMode: "raw"}, stubRuntime{}); err == nil {
		t.Fatal("invalid mode")
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "news-q", Type: "auto", ContentMode: "text", Category: "news"}, stubRuntime{})
	if err != nil || !strings.Contains(got, "Hit") {
		t.Fatalf("text mode: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "ACS API", Category: "publication", IncludeDomains: []string{"census.gov"}}, stubRuntime{})
	if err != nil || !strings.Contains(got, "Hit") || !strings.Contains(got, "Dropped category=publication") {
		t.Fatalf("publication+domain coerced: %q err=%v", got, err)
	}
	got, err = runWebSearch(ctx, client, webSearchArgs{Query: "site:census.gov ACS 5-year API"}, stubRuntime{})
	if err != nil || !strings.Contains(got, "Hit") || !strings.Contains(got, "Moved site:") {
		t.Fatalf("site: lift: %q err=%v", got, err)
	}
	if WebSearch(NewExa("k")).Name() != "web_search" || WebFetch(NewExa("k")).Name() != "web_fetch" {
		t.Fatal("web constructors must name the tools")
	}
}

func TestWebSearch_httpStatusCorrectionAndFailed(t *testing.T) {
	ctx := context.Background()

	fixable := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad query filters"}`))
	})
	_, err := runWebSearch(ctx, fixable, webSearchArgs{Query: "census ACS"}, stubRuntime{})
	if err == nil || !errors.Is(err, tacklr.ErrCorrection) || !strings.Contains(err.Error(), "Simplify filters") {
		t.Fatalf("400: %v", err)
	}

	limited := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	})
	_, err = runWebSearch(ctx, limited, webSearchArgs{Query: "census ACS"}, stubRuntime{})
	if err == nil || !errors.Is(err, tacklr.ErrFailed) || errors.Is(err, tacklr.ErrCorrection) || !strings.Contains(err.Error(), "search provider failed") {
		t.Fatalf("429: %v", err)
	}
	var st *StatusError
	if !errors.As(err, &st) || st.Status != http.StatusTooManyRequests {
		t.Fatalf("429 cause: %v", err)
	}

	pub := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`UNSUPPORTED_PUBLICATION`))
	})
	_, err = runWebSearch(ctx, pub, webSearchArgs{Query: "paper"}, stubRuntime{})
	if err == nil || !errors.Is(err, tacklr.ErrCorrection) || !strings.Contains(err.Error(), "category=publication") {
		t.Fatalf("publication: %v", err)
	}
}

func TestWebSearch_schemaHidesExaKnobs(t *testing.T) {
	search := WebSearch(NewExa("k")).AsJson()
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
	fetch := WebFetch(NewExa("k")).AsJson()
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
	got, err := runWebSearch(context.Background(), client, webSearchArgs{Query: "ACS 5-year demographics", Category: "publication"}, stubRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("searches = %d, want 2 (empty then retry)", n)
	}
	if !strings.Contains(got, "Census ACS") || !strings.Contains(got, "No hits with those filters") {
		t.Fatalf("retry output: %s", got)
	}
}

func TestWebFetch_readsKnownURLs(t *testing.T) {
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

	got, err := runWebFetch(ctx, client, webFetchArgs{URLs: []string{"https://example.gov/code"}}, stubRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "City Code") || !strings.Contains(got, "setbacks") {
		t.Fatal(got)
	}

	got, err = runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"  ", "ftp://x", "example.gov/code", "https://example.gov/code", "not a host"},
	}, stubRuntime{})
	if err != nil || !strings.Contains(got, "City Code") {
		t.Fatalf("normalize: %q err=%v", got, err)
	}

	got, err = runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"https://example.gov/missing"}, ContentMode: "highlights", HighlightQuery: "setbacks",
	}, stubRuntime{})
	if err != nil || !strings.Contains(got, "error") || !strings.Contains(got, "not_found") {
		t.Fatalf("status: %q err=%v", got, err)
	}

	if _, err := runWebFetch(ctx, client, webFetchArgs{}, nil); err == nil {
		t.Fatal("no urls")
	}
	if _, err := runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"https://a", "https://b", "https://c", "https://d", "https://e", "https://f"},
	}, stubRuntime{}); err == nil {
		t.Fatal("too many urls")
	}
	if _, err := runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"https://example.gov/code"}, ContentMode: "raw",
	}, stubRuntime{}); err == nil {
		t.Fatal("invalid mode")
	}
	got, err = runWebFetch(ctx, client, webFetchArgs{
		URLs: []string{"https://example.gov/code"}, ContentMode: "both", MaxTextCharacters: 20000,
	}, stubRuntime{})
	if err != nil || !strings.Contains(got, "City Code") {
		t.Fatalf("both: %q err=%v", got, err)
	}
}

func TestWebConstructors_panicWithoutClient(t *testing.T) {
	for _, fn := range []func(){
		func() { WebSearch(nil) },
		func() { WebFetch(nil) },
	} {
		var panicked bool
		func() {
			defer func() { panicked = recover() != nil }()
			fn()
		}()
		if !panicked {
			t.Fatal("nil Exa constructor did not panic")
		}
	}
}

func TestMapExaErr_andStatusErrorEdges(t *testing.T) {
	if err := mapExaErr("web_search", nil); err != nil {
		t.Fatal(err)
	}
	var st *StatusError
	if st.Error() != "exa: status error" || st.Unwrap() != nil || st.QueryFixable() || st.PublicationDomain() {
		t.Fatalf("nil status: %+v", st)
	}
	if !(&StatusError{Status: 400, Body: "not supported for category=publication"}).PublicationDomain() {
		t.Fatal("publication body")
	}
	if err := mapExaErr("web_search", ErrService); err == nil || !errors.Is(err, tacklr.ErrFailed) {
		t.Fatalf("service: %v", err)
	}
	if err := mapExaErr("web_search", &StatusError{Status: 401, Body: "no"}); err == nil || !errors.Is(err, tacklr.ErrAuthExpired) {
		t.Fatalf("401: %v", err)
	}
	if err := mapExaErr("web_search", errors.New("plain")); err == nil || err.Error() != "plain" {
		t.Fatalf("plain: %v", err)
	}
}

func TestWebSearch_siteOnlyQueryAndTruncation(t *testing.T) {
	query, hosts := liftSiteOperators(`site:census.gov`)
	if query != "census.gov" || len(hosts) != 1 {
		t.Fatalf("site-only = %q %v", query, hosts)
	}
	got := formatWebSearchResult("q", "", nil, nil)
	if !strings.Contains(got, "No results found") {
		t.Fatal(got)
	}
	if !strings.HasSuffix(truncateRunes(strings.Repeat("a", 10), 3), "…") {
		t.Fatal("truncate")
	}
	if formatSynthesisContent(json.RawMessage(`not-json`)) == "" {
		t.Fatal("raw synthesis")
	}
	if !strings.Contains(formatSynthesisContent(json.RawMessage(`"quoted"`)), "quoted") {
		t.Fatal("string synthesis")
	}
	if !strings.Contains(formatWebSearchResult("q", "auto", &SearchResponse{}, []string{"note"}), "Adjusted: note") {
		t.Fatal("notes")
	}
	if !strings.Contains(formatWebSearchResult("q", "", &SearchResponse{Results: []SearchResult{{Title: "T"}}}, nil), "Mode: auto") {
		t.Fatal("default mode")
	}
}

func TestWebSearch_retriesProviderConflictThenSucceeds(t *testing.T) {
	var n int
	client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		n++
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`UNSUPPORTED_PUBLICATION`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"title":"Open","url":"https://example.com"}]}`))
	})
	got, err := runWebSearch(t.Context(), client, webSearchArgs{Query: "paper", IncludeDomains: []string{"arxiv.org"}}, stubRuntime{})
	if err != nil || n != 2 || !strings.Contains(got, "Open") || !strings.Contains(got, "retried on the open web") {
		t.Fatalf("retry: n=%d out=%q err=%v", n, got, err)
	}

	var fails int
	failRetry := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		fails++
		if fails == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[]}`))
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	})
	if _, err := runWebSearch(t.Context(), failRetry, webSearchArgs{Query: "q", Category: "news"}, stubRuntime{}); err == nil || !errors.Is(err, tacklr.ErrFailed) {
		t.Fatalf("retry fail: %v", err)
	}
}

func TestWebFetch_highlightsAndProviderFailure(t *testing.T) {
	req, err := buildExaContentsRequest(webFetchArgs{URLs: []string{"https://example.com"}, ContentMode: "highlights"})
	if err != nil || req.Highlights != true {
		t.Fatalf("highlights default: %+v err=%v", req, err)
	}
	both, err := buildExaContentsRequest(webFetchArgs{URLs: []string{"https://example.com"}, ContentMode: "both", HighlightQuery: "setbacks"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := both.Highlights.(HighlightsOptions); !ok {
		t.Fatalf("both highlights: %#v", both.Highlights)
	}
	fail := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})
	if _, err := runWebFetch(t.Context(), fail, webFetchArgs{URLs: []string{"https://example.com"}}, stubRuntime{}); err == nil || !errors.Is(err, tacklr.ErrFailed) {
		t.Fatalf("fetch fail: %v", err)
	}
}
