package tacklr

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Two harnesses built from the same inputs, as on two workers, send the model
// the same request prefix: the same tools in the same order and the same
// system prompt, so the provider's prompt cache holds across a rebuild.
func TestNewHarness_twoBuildsSendTheSameRequestPrefix(t *testing.T) {
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "svc", Version: "v0.0.1"}, nil)
	for _, name := range []string{"zeta", "alpha"} {
		mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: name, Description: name},
			func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
				return &mcpsdk.CallToolResult{}, struct{}{}, nil
			})
	}
	hs := httptest.NewServer(mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil))
	t.Cleanup(hs.Close)
	opts := AgentOptions{Model: &scriptedModel{}, MaxWindowSize: 8192,
		MCPConfigs:   []session.MCPServer{{Name: "svc", Type: session.MCPHTTP, URL: hs.URL}},
		SkillsLoader: skillPack(t, "research", "Research carefully", "Verify claims."),
		Specialists:  []*Specialist{{Name: "writer", Model: &scriptedModel{}}, {Name: "analyst", Model: &scriptedModel{}}}}
	one, two := mustNewHarness(t, opts), mustNewHarness(t, opts)
	t.Cleanup(two.Close)

	a, b := one.request(nil), two.request(nil)

	tools := ToolsAsJson(a.Tools)
	if tools != ToolsAsJson(b.Tools) || a.SystemPrompt != b.SystemPrompt {
		t.Fatal("two builds from the same inputs sent different request prefixes")
	}
	if i, j := strings.Index(tools, `"svc__alpha"`), strings.Index(tools, `"svc__zeta"`); i < 0 || j < i {
		t.Fatalf("MCP tools must be in the tool list in name order, got %s", tools)
	}
}
