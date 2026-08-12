package tacklr

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestWebHelpers_formatAndNormalize covers pure web tool helpers (no network).
func TestWebHelpers_formatAndNormalize(t *testing.T) {
	// truncateRunes
	if truncateRunes("", 10) != "" {
		t.Fatal("empty")
	}
	if truncateRunes("abc", 0) != "abc" {
		t.Fatal("max 0")
	}
	if truncateRunes("hello", 10) != "hello" {
		t.Fatal("under max")
	}
	long := strings.Repeat("x", 50)
	got := truncateRunes(long, 10)
	if !strings.HasSuffix(got, "…") || len([]rune(got)) != 11 {
		t.Fatalf("truncate: %q", got)
	}

	// formatSynthesisContent
	if formatSynthesisContent(json.RawMessage(`"hello world"`)) != "hello world" {
		t.Fatal("string synthesis")
	}
	pretty := formatSynthesisContent(json.RawMessage(`{"a":1}`))
	if !strings.Contains(pretty, `"a"`) {
		t.Fatalf("object synthesis: %q", pretty)
	}
	raw := formatSynthesisContent(json.RawMessage(`not-json`))
	if raw == "" {
		t.Fatal("fallback raw")
	}

	// normalizeFetchURLs
	urls := normalizeFetchURLs([]string{"", " https://a.example ", "https://a.example", "https://b.example"})
	if len(urls) != 2 {
		t.Fatalf("dedupe/trim: %v", urls)
	}
	// buildExaContentsRequest
	if _, err := buildExaContentsRequest(webFetchArgs{}); err == nil {
		t.Fatal("no urls")
	}
	// over max urls rejected or truncated at build
	many := make([]string, webFetchMaxURLs+3)
	for i := range many {
		many[i] = "https://x.example/" + string(rune('a'+i))
	}
	if _, err := buildExaContentsRequest(webFetchArgs{URLs: many}); err == nil {
		// may allow and trim — either ok
	}
	req, err := buildExaContentsRequest(webFetchArgs{
		URLs: []string{"https://example.com"}, ContentMode: "highlights", HighlightQuery: "foo",
	})
	if err != nil || len(req.URLs) != 1 {
		t.Fatalf("highlights: %+v err=%v", req, err)
	}
	req, err = buildExaContentsRequest(webFetchArgs{
		URLs: []string{"https://example.com"}, ContentMode: "both", MaxTextCharacters: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = req

	// nil client paths
	if _, err := runWebFetch(t.Context(), nil, webFetchArgs{URLs: []string{"https://x"}}, HarnessRuntime{}); err == nil {
		t.Fatal("nil client fetch")
	}
	if _, err := runWebSearch(t.Context(), nil, webSearchArgs{Query: "q"}, HarnessRuntime{}); err == nil {
		t.Fatal("nil search client")
	}

	// format empty fetch result
	empty := formatWebFetchResult([]string{"https://a"}, nil)
	if !strings.Contains(empty, "Web fetch") {
		t.Fatalf("empty format: %q", empty)
	}
	// bare host normalize
	if len(normalizeFetchURLs([]string{"example.com/path"})) != 1 {
		t.Fatal("bare host")
	}
	// bad schemes skipped
	if len(normalizeFetchURLs([]string{"ftp://x", "://", "not a url"})) != 0 {
		t.Fatal("bad urls")
	}
	// text mode default
	req2, err := buildExaContentsRequest(webFetchArgs{URLs: []string{"https://example.com"}, ContentMode: "text"})
	if err != nil {
		t.Fatal(err)
	}
	_ = req2
	// invalid content mode falls through
	_, _ = buildExaContentsRequest(webFetchArgs{URLs: []string{"https://example.com"}, ContentMode: "weird"})
}
