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
)

// TestWebFetchTool_strictSchemaRequired ensures every property is in required.
func TestWebFetchTool_strictSchemaRequired(t *testing.T) {
	tool := newWebFetchTool(exa.NewClient("k"))
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
	if _, ok := props["urls"]; !ok {
		t.Fatal("urls property required")
	}
}

// TestBuildExaContentsRequest_defaultsAndValidation covers arg → wire request.
func TestBuildExaContentsRequest_defaultsAndValidation(t *testing.T) {
	if _, err := buildExaContentsRequest(webFetchArgs{}); err == nil {
		t.Fatal("empty urls")
	}
	if _, err := buildExaContentsRequest(webFetchArgs{
		URLs: []string{"https://a.com", "https://b.com", "https://c.com", "https://d.com", "https://e.com", "https://f.com"},
	}); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("max urls: %v", err)
	}
	if _, err := buildExaContentsRequest(webFetchArgs{
		URLs: []string{"https://a.com"}, ContentMode: "raw",
	}); err == nil {
		t.Fatal("bad mode")
	}

	req, err := buildExaContentsRequest(webFetchArgs{
		URLs: []string{"  example.com/page  ", "https://example.com/page", "ftp://bad"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Dedup + https default + drop non-http(s).
	if len(req.URLs) != 1 || req.URLs[0] != "https://example.com/page" {
		t.Fatalf("urls = %#v", req.URLs)
	}
	text, ok := req.Text.(exa.TextOptions)
	if !ok || text.MaxCharacters != webFetchDefaultTextCap {
		t.Fatalf("default text = %#v", req.Text)
	}

	req, err = buildExaContentsRequest(webFetchArgs{
		URLs:           []string{"https://a.com"},
		ContentMode:    "highlights",
		HighlightQuery: "zoning setbacks",
	})
	if err != nil {
		t.Fatal(err)
	}
	if req.Text != nil {
		t.Fatalf("highlights should not set text: %#v", req.Text)
	}
	hq, ok := req.Highlights.(exa.HighlightsOptions)
	if !ok || hq.Query != "zoning setbacks" {
		t.Fatalf("highlights = %#v", req.Highlights)
	}
}

// TestFormatWebFetchResult_includesErrorsAndText structures tool output.
func TestFormatWebFetchResult_includesErrorsAndText(t *testing.T) {
	out := formatWebFetchResult([]string{"https://a.com", "https://b.com"}, &exa.ContentsResponse{
		Results: []exa.SearchResult{
			{Title: "Good", URL: "https://a.com", Text: "body text here"},
		},
		Statuses: []exa.ContentsURLStatus{
			{ID: "https://b.com", Status: "error", Error: &struct {
				Tag            string `json:"tag,omitempty"`
				HTTPStatusCode *int   `json:"httpStatusCode,omitempty"`
			}{Tag: "CRAWL_NOT_FOUND"}},
		},
	})
	for _, want := range []string{
		"# Web fetch results",
		"Good",
		"body text here",
		"CRAWL_NOT_FOUND",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in %s", want, out)
		}
	}
}

// TestRunWebFetch_endToEnd hits a fake /contents endpoint.
func TestRunWebFetch_endToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/contents" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"title":"City Code","url":"https://example.gov/code","text":"Section 1.2 setbacks"}]}`))
	}))
	t.Cleanup(srv.Close)

	client := exa.NewClient("k")
	client.BaseURL = srv.URL
	client.HTTPClient = srv.Client()

	got, err := runWebFetch(context.Background(), client, webFetchArgs{
		URLs: []string{"https://example.gov/code"},
	}, HarnessRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "City Code") || !strings.Contains(got, "setbacks") {
		t.Fatal(got)
	}
}

// TestInjectBuiltinTools_webToolsGatedOnAPIKey registers search + fetch with a key.
func TestInjectBuiltinTools_webToolsGatedOnAPIKey(t *testing.T) {
	t.Setenv("EXA_API_KEY", "")
	hOff := NewAgent(context.Background(), AgentOptions{
		Config: Config{MaxWindowSize: 1024},
		Model:  &mockStrategy{},
	})
	if hOff.findTool("web_search", "") != nil || hOff.findTool("web_fetch", "") != nil {
		t.Fatal("expected no web tools without key")
	}
	// Tool usage lives in schemas only — system prompt must not document them.
	if strings.Contains(hOff.constructSystemPrompt(), "web_search") ||
		strings.Contains(hOff.constructSystemPrompt(), "web_fetch") {
		t.Fatal("system prompt must not mention web tools")
	}

	hOn := NewAgent(context.Background(), AgentOptions{
		Config:    Config{MaxWindowSize: 1024},
		Model:     &mockStrategy{},
		ExaAPIKey: "from-options",
	})
	if hOn.findTool("web_search", "") == nil {
		t.Fatal("expected web_search")
	}
	if hOn.findTool("web_fetch", "") == nil {
		t.Fatal("expected web_fetch")
	}
	prompt := hOn.constructSystemPrompt()
	if strings.Contains(prompt, "web_search") || strings.Contains(prompt, "web_fetch") ||
		strings.Contains(prompt, "### Web search") {
		t.Fatalf("system prompt must not document web tools, got snippet containing web_*:\n%s", prompt)
	}
}
