package web

import (
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
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

// TestCoerceSearch_rejectsValuesExaDoesNotKnow: an empty query, or a type,
// category or content mode Exa does not know, fails before any request.
func TestCoerceSearch_rejectsValuesExaDoesNotKnow(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		args webSearchArgs
		want string
	}{
		"a blank query":       {webSearchArgs{Query: "  "}, "query is required"},
		"an unknown type":     {webSearchArgs{Query: "q", Type: "nope"}, "invalid type"},
		"an unknown category": {webSearchArgs{Query: "q", Category: "nope"}, "invalid category"},
		"an unknown mode":     {webSearchArgs{Query: "q", ContentMode: "raw"}, "invalid content_mode"},
	} {
		_, _, err := coerceSearch(tc.args)

		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s = %v, want %q", name, err, tc.want)
		}
	}
}

// TestCoerceSearch_dropsOrMovesFiltersExaRejectsWithANote: a filter Exa
// rejects in combination is dropped, and a site: operator moves into
// include_domains, each with a note saying so.
func TestCoerceSearch_dropsOrMovesFiltersExaRejectsWithANote(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		args webSearchArgs
		want func(SearchRequest) bool
		note string
	}{
		"exclude_domains on companies": {
			webSearchArgs{Query: "q", Category: "company", ExcludeDomains: []string{"x.com"}},
			func(r SearchRequest) bool { return r.Category == "company" && r.ExcludeDomains == nil },
			"Dropped exclude_domains",
		},
		"dates on people": {
			webSearchArgs{Query: "q", Category: "people", StartPublishedDate: "2024-01-01T00:00:00Z", EndPublishedDate: "2025-01-01T00:00:00Z"},
			func(r SearchRequest) bool { return r.StartPublishedDate == "" && r.EndPublishedDate == "" },
			"Dropped published-date",
		},
		"publications filtered by domain": {
			webSearchArgs{Query: "ACS API", Category: "publication", IncludeDomains: []string{"census.gov"}},
			func(r SearchRequest) bool {
				return r.Category == "" && slices.Equal(r.IncludeDomains, []string{"census.gov"})
			},
			"Dropped category=publication",
		},
		"a site: operator": {
			webSearchArgs{Query: "site:https://census.gov/ ACS 5-year API"},
			func(r SearchRequest) bool {
				return r.Query == "ACS 5-year API" && slices.Equal(r.IncludeDomains, []string{"census.gov"})
			},
			"Moved site:",
		},
		"a query of only a site: operator": {
			webSearchArgs{Query: "site:census.gov"},
			func(r SearchRequest) bool {
				return r.Query == "census.gov" && slices.Equal(r.IncludeDomains, []string{"census.gov"})
			},
			"Moved site:",
		},
	} {
		req, notes, err := coerceSearch(tc.args)

		if err != nil || !tc.want(req) || !strings.Contains(strings.Join(notes, " "), tc.note) {
			t.Errorf("%s = %+v notes %q err %v, want the filter coerced and %q", name, req, notes, err, tc.note)
		}
	}
}

// TestCoerceSearch_fillsDefaultsAndCapsLimits: an unset type, count and mode
// take their defaults, and a count or text length above the limit is capped.
func TestCoerceSearch_fillsDefaultsAndCapsLimits(t *testing.T) {
	t.Parallel()
	age := 24
	for name, tc := range map[string]struct {
		args webSearchArgs
		want SearchRequest
	}{
		"only a query": {
			webSearchArgs{Query: " q "},
			SearchRequest{Query: "q", Type: "auto", NumResults: webSearchDefaultResults, Contents: &ContentsOptions{Highlights: true}},
		},
		"limits above the caps": {
			webSearchArgs{Query: "q", Type: "fast", NumResults: 20, ContentMode: "both", MaxTextCharacters: 20000, MaxAgeHours: &age, UserLocation: " US ", SystemPrompt: " cite "},
			SearchRequest{Query: "q", Type: "fast", NumResults: webSearchMaxResults, UserLocation: "US", SystemPrompt: "cite", Contents: &ContentsOptions{
				Highlights: true, Text: TextOptions{MaxCharacters: webSearchMaxTextCap, Verbosity: "compact"}, MaxAgeHours: &age,
			}},
		},
		"text only": {
			webSearchArgs{Query: "q", Category: "news", ContentMode: "text"},
			SearchRequest{Query: "q", Type: "auto", NumResults: webSearchDefaultResults, Category: "news", Contents: &ContentsOptions{
				Text: TextOptions{MaxCharacters: webSearchDefaultTextCap, Verbosity: "compact"},
			}},
		},
	} {
		got, _, err := coerceSearch(tc.args)
		got.IncludeDomains, got.ExcludeDomains = nil, nil

		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s = %+v %v, want %+v", name, got, err, tc.want)
		}
	}
}

// TestWebSearch_formatsWhatExaFinds: web_search lists each result with what
// Exa returned for it, and Exa's synthesized answer, or says it found
// nothing.
func TestWebSearch_formatsWhatExaFinds(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		answer string
		want   []string
	}{
		"a full result and a text answer": {
			`{"results":[{"title":"Paris","url":"https://example.com/paris","highlights":["Paris is the capital",""],"text":"Paris is the capital of France.","summary":"Capital city","author":"Ed","publishedDate":"2024-01-01"}],"output":{"content":"Paris it is."}}`,
			[]string{"## 1. Paris", "https://example.com/paris", "Published: 2024-01-01 | Author: Ed", "Paris is the capital", "Summary: Capital city", "Text: Paris is the capital of France.", "Paris it is."},
		},
		"an untitled result with a long highlight and a structured answer": {
			`{"results":[{"url":"https://example.com/x","highlights":["` + strings.Repeat("h", 900) + `"]}],"output":{"content":{"answer":"ok"}}}`,
			[]string{"(untitled)", strings.Repeat("h", 800) + "…", `"answer": "ok"`},
		},
		"no results": {`{"results":[]}`, []string{"No results found"}},
	} {
		client := newExaTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(tc.answer))
		})

		got, err := runWebSearch(t.Context(), client, webSearchArgs{Query: "capital of France"})

		for _, want := range tc.want {
			if err != nil || !strings.Contains(got, want) {
				t.Errorf("%s = %q %v, want %q", name, got, err, want)
			}
		}
	}
}

// TestWebSearch_retriesOnTheOpenWebWhenTheFiltersFail: a filtered search that
// finds nothing, or that Exa rejects for its filters, runs again without the
// filters; a retry that fails is the tool's failure.
func TestWebSearch_retriesOnTheOpenWebWhenTheFiltersFail(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		args        webSearchArgs
		filtered    func(http.ResponseWriter)
		retryStatus int
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
		client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			var req SearchRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			switch {
			case hasSearchFilters(req):
				tc.filtered(w)
			case tc.retryStatus != 0:
				w.WriteHeader(tc.retryStatus)
			default:
				_, _ = w.Write([]byte(`{"results":[{"title":"Open web hit","url":"https://example.com"}]}`))
			}
		})

		got, err := runWebSearch(t.Context(), client, tc.args)

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

// TestWebSearch_failsWithoutARequestOnArgumentsItCannotSend: arguments
// coerceSearch rejects fail before the client is used.
func TestWebSearch_failsWithoutARequestOnArgumentsItCannotSend(t *testing.T) {
	t.Parallel()

	_, err := runWebSearch(t.Context(), nil, webSearchArgs{Query: " "})

	if err == nil || !strings.Contains(err.Error(), "query is required") {
		t.Errorf("search with a blank query = %v, want query is required", err)
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

// TestBuildExaContentsRequest_asksForWhatTheModeNames: web_fetch keeps the
// http(s) URLs it can normalize, once each, and asks Exa for text,
// highlights, or both, with highlights on the query when one is given.
func TestBuildExaContentsRequest_asksForWhatTheModeNames(t *testing.T) {
	t.Parallel()
	page := []string{"https://example.gov/code"}
	defaultText := TextOptions{MaxCharacters: webFetchDefaultTextCap, Verbosity: "compact"}
	for name, tc := range map[string]struct {
		args webFetchArgs
		want ContentsRequest
	}{
		"urls to normalize": {
			webFetchArgs{URLs: []string{"  ", "ftp://x", "example.gov/code", "https://example.gov/code", "not a host"}},
			ContentsRequest{URLs: page, Text: defaultText},
		},
		"highlights": {
			webFetchArgs{URLs: page, ContentMode: "highlights"},
			ContentsRequest{URLs: page, Highlights: true},
		},
		"highlights on a query": {
			webFetchArgs{URLs: page, ContentMode: "highlights", HighlightQuery: " setbacks "},
			ContentsRequest{URLs: page, Highlights: HighlightsOptions{Query: "setbacks"}},
		},
		"both": {
			webFetchArgs{URLs: page, ContentMode: "both"},
			ContentsRequest{URLs: page, Text: defaultText, Highlights: true},
		},
		"both on a query with text above the cap": {
			webFetchArgs{URLs: page, ContentMode: "both", MaxTextCharacters: 20000, HighlightQuery: "setbacks"},
			ContentsRequest{URLs: page, Text: TextOptions{MaxCharacters: webFetchMaxTextCap, Verbosity: "compact"}, Highlights: HighlightsOptions{Query: "setbacks"}},
		},
	} {
		got, err := buildExaContentsRequest(tc.args)

		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s = %+v %v, want %+v", name, got, err, tc.want)
		}
	}
}

// TestWebFetch_readsPagesAndReportsOnesExaCouldNotRead: web_fetch returns
// each page Exa read, and the status of a URL it could not read.
func TestWebFetch_readsPagesAndReportsOnesExaCouldNotRead(t *testing.T) {
	t.Parallel()
	client := newExaTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var req ContentsRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if req.URLs[0] == "https://example.gov/missing" {
			_, _ = w.Write([]byte(`{"results":[],"statuses":[{"id":"https://example.gov/missing","status":"error","error":{"tag":"not_found"}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"title":"City Code","url":"https://example.gov/code","text":"Section 1.2 setbacks"}]}`))
	})

	page, pageErr := runWebFetch(t.Context(), client, webFetchArgs{URLs: []string{"https://example.gov/code"}})
	missing, missingErr := runWebFetch(t.Context(), client, webFetchArgs{URLs: []string{"https://example.gov/missing"}})

	if pageErr != nil || !strings.Contains(page, "City Code") || !strings.Contains(page, "Section 1.2 setbacks") {
		t.Errorf("fetch = %q %v, want the page", page, pageErr)
	}
	if missingErr != nil || !strings.Contains(missing, "error (not_found)") || !strings.Contains(missing, "No content returned") {
		t.Errorf("fetch a missing page = %q %v, want its status and no content", missing, missingErr)
	}
}

// TestWebFetch_failsOnArgumentsItCannotSendAndOnProviderErrors: no URLs, too
// many, an unknown mode or a client with no key fail before a request, and
// a provider that fails is ErrFailed.
func TestWebFetch_failsOnArgumentsItCannotSendAndOnProviderErrors(t *testing.T) {
	t.Parallel()
	page := []string{"https://example.gov/code"}
	down := newExaTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) })
	for name, tc := range map[string]struct {
		client *Exa
		args   webFetchArgs
		is     error
		want   string
	}{
		"no urls":          {down, webFetchArgs{}, nil, "at least one"},
		"too many urls":    {down, webFetchArgs{URLs: []string{"https://a", "https://b", "https://c", "https://d", "https://e", "https://f"}}, nil, "at most 5"},
		"an unknown mode":  {down, webFetchArgs{URLs: page, ContentMode: "raw"}, nil, "invalid content_mode"},
		"no key":           {NewExa(""), webFetchArgs{URLs: page}, nil, "API key is required"},
		"a provider error": {down, webFetchArgs{URLs: page}, tacklr.ErrFailed, "search provider failed"},
	} {
		_, err := runWebFetch(t.Context(), tc.client, tc.args)

		if err == nil || (tc.is != nil && !errors.Is(err, tc.is)) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s = %v, want %v naming %q", name, err, tc.is, tc.want)
		}
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
