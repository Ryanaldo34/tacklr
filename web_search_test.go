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
	"github.com/ryanaldo34/tacklr/streaming"
)

// TestWebSearchTool_strictSchemaRequired ensures every property is in required
// (OpenAI / DeepSeek strict function tools).
func TestWebSearchTool_strictSchemaRequired(t *testing.T) {
	tool := newWebSearchTool(exa.NewClient("k"))
	def := tool.AsJson()
	params, _ := def["parameters"].(map[string]any)
	props, _ := params["properties"].(map[string]any)
	reqRaw, ok := params["required"]
	if !ok {
		t.Fatal("missing required array")
	}
	req, ok := reqRaw.([]string)
	if !ok {
		t.Fatalf("required type %T", reqRaw)
	}
	for name := range props {
		found := false
		for _, r := range req {
			if r == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("property %q missing from required %v", name, req)
		}
	}
	// Optional category should be nullable, not omitted from required.
	cat, _ := props["category"].(map[string]any)
	if _, isStr := cat["type"].(string); isStr {
		t.Fatalf("category should be nullable union, got type=%v", cat["type"])
	}
}

// TestBuildExaSearchRequest_defaultsAndMapping is the arg → wire request outcome.
func TestBuildExaSearchRequest_defaultsAndMapping(t *testing.T) {
	req, err := buildExaSearchRequest(webSearchArgs{Query: "  latest Fed rate  "})
	if err != nil {
		t.Fatal(err)
	}
	if req.Query != "latest Fed rate" || req.Type != "auto" || req.NumResults != 8 {
		t.Fatalf("%+v", req)
	}
	if req.Contents == nil || req.Contents.Highlights != true {
		t.Fatalf("default highlights: %+v", req.Contents)
	}

	req, err = buildExaSearchRequest(webSearchArgs{
		Query:             "transformer architecture",
		Type:              "deep",
		NumResults:        100, // clamp
		ContentMode:       "both",
		MaxTextCharacters: 500,
		IncludeDomains:    []string{"arxiv.org"},
		SystemPrompt:      "prefer primary sources",
	})
	if err != nil {
		t.Fatal(err)
	}
	if req.NumResults != 10 || req.Type != "deep" {
		t.Fatalf("%+v", req)
	}
	if req.Contents.Highlights != true {
		t.Fatal("both should set highlights")
	}
	text, ok := req.Contents.Text.(exa.TextOptions)
	if !ok || text.MaxCharacters != 500 {
		t.Fatalf("text opts = %#v", req.Contents.Text)
	}
	if len(req.IncludeDomains) != 1 || req.SystemPrompt == "" {
		t.Fatalf("%+v", req)
	}
}

// TestBuildExaSearchRequest_validation covers required fields and category constraints.
func TestBuildExaSearchRequest_validation(t *testing.T) {
	if _, err := buildExaSearchRequest(webSearchArgs{}); err == nil {
		t.Fatal("empty query")
	}
	if _, err := buildExaSearchRequest(webSearchArgs{Query: "q", Type: "nope"}); err == nil {
		t.Fatal("bad type")
	}
	if _, err := buildExaSearchRequest(webSearchArgs{Query: "q", ContentMode: "raw"}); err == nil {
		t.Fatal("bad mode")
	}
	if _, err := buildExaSearchRequest(webSearchArgs{
		Query: "q", Category: "company", ExcludeDomains: []string{"x.com"},
	}); err == nil || !strings.Contains(err.Error(), "exclude_domains") {
		t.Fatalf("company+exclude: %v", err)
	}
	if _, err := buildExaSearchRequest(webSearchArgs{
		Query: "q", Category: "people", StartPublishedDate: "2024-01-01T00:00:00Z",
	}); err == nil || !strings.Contains(err.Error(), "published date") {
		t.Fatalf("people+date: %v", err)
	}
}

// TestFormatWebSearchResult_compactMarkdown structures results for the context window.
func TestFormatWebSearchResult_compactMarkdown(t *testing.T) {
	out := formatWebSearchResult("q", "auto", &exa.SearchResponse{
		Results: []exa.SearchResult{
			{
				Title:         "Fed holds rates",
				URL:           "https://example.com/fed",
				PublishedDate: "2024-01-01",
				Author:        "Reporter",
				Highlights:    []string{"rate remains 5.25%"},
				Text:          "long body",
			},
		},
		Output: &exa.SynthesisOutput{Content: json.RawMessage(`"short answer"`)},
	})
	for _, want := range []string{
		"# Web search results",
		"Fed holds rates",
		"https://example.com/fed",
		"rate remains 5.25%",
		"Synthesized output",
		"short answer",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}

	empty := formatWebSearchResult("nothing", "auto", &exa.SearchResponse{})
	if !strings.Contains(empty, "No results found") {
		t.Fatal(empty)
	}
}

// TestWebSearchTool_invokeAgainstServer is the end-to-end tool invoke outcome.
func TestWebSearchTool_invokeAgainstServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		if body["query"] != "capital of France" {
			t.Fatalf("query = %#v", body["query"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"title":"Paris","url":"https://example.com/paris","highlights":["Paris is the capital"]}]}`))
	}))
	t.Cleanup(srv.Close)

	client := exa.NewClient("k")
	client.BaseURL = srv.URL
	client.HTTPClient = srv.Client()
	tool := newWebSearchTool(client)

	got, err := tool.Invoke(context.Background(), `{"query":"capital of France"}`, HarnessRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "Paris") || !strings.Contains(got, "capital") {
		t.Fatal(got)
	}
}

// TestInjectBuiltinTools_webSearchGatedOnAPIKey registers web tools only with a key.
func TestInjectBuiltinTools_webSearchGatedOnAPIKey(t *testing.T) {
	t.Setenv("EXA_API_KEY", "")
	hOff := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
	})
	if hOff.findTool("web_search", "") != nil {
		t.Fatal("expected no web_search without key")
	}

	hOn := NewAgent(context.Background(), AgentOptions{
		Config:    Config{MaxWindowSize: 1024},
		Model:     &mockStrategy{},
		ExaAPIKey: "from-options",
	})
	if hOn.findTool("web_search", "") == nil {
		t.Fatal("expected web_search from options key")
	}
	if hOn.findTool("web_fetch", "") == nil {
		t.Fatal("expected web_fetch from options key")
	}
	// Tool schemas carry usage guidance — system prompt does not.
	if strings.Contains(hOn.constructSystemPrompt(), "web_search") ||
		strings.Contains(hOn.constructSystemPrompt(), "web_fetch") {
		t.Fatal("system prompt must not document web tools")
	}

	t.Setenv("EXA_API_KEY", "from-env")
	hEnv := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
	})
	if hEnv.findTool("web_search", "") == nil || hEnv.findTool("web_fetch", "") == nil {
		t.Fatal("expected web tools from env")
	}
}

// TestResolveExaAPIKey_optionsWinOverEnv prefers AgentOptions.
func TestResolveExaAPIKey_optionsWinOverEnv(t *testing.T) {
	t.Setenv("EXA_API_KEY", "env-key")
	if got := resolveExaAPIKey("opt-key"); got != "opt-key" {
		t.Fatal(got)
	}
	if got := resolveExaAPIKey(""); got != "env-key" {
		t.Fatal(got)
	}
	t.Setenv("EXA_API_KEY", "")
	if got := resolveExaAPIKey(""); got != "" {
		t.Fatal(got)
	}
}

// Ensure streaming category constant is used (compile-time link for tools).
var _ = streaming.ToolCategorySearch
