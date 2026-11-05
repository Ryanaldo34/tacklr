package tacklr_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/skills"
)

// TestNewHarness_refusesAnInvalidConfiguration: each broken option fails
// construction with an error that names it, and a valid configuration
// without a window takes the model's window.
func TestNewHarness_refusesAnInvalidConfiguration(t *testing.T) {
	t.Parallel()
	model := testkit.HTTPModel(t, nil)
	noNamespace, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	broken := t.TempDir()
	if err := os.Mkdir(filepath.Join(broken, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	remote := func(name, url string) session.MCPServer {
		return session.MCPServer{Name: name, Type: session.MCPHTTP, URL: url}
	}
	vaulted := remote("remote", "https://example.test")
	vaulted.CredentialRef = "vault://remote"
	cases := map[string]struct {
		opts tacklr.AgentOptions
		want string
	}{
		"no model":                               {tacklr.AgentOptions{}, "Model is required"},
		"a model that reports no window":         {tacklr.AgentOptions{Model: model}, "context window"},
		"an allowed tool that is not registered": {tacklr.AgentOptions{Model: model, AllowTools: []string{"nope"}}, `"nope" is not a registered tool`},
		"a nil tool":                             {tacklr.AgentOptions{Model: model, Tools: []*tacklr.Tool{nil}}, "Tools[0] is nil"},
		"a brain without a search namespace":     {tacklr.AgentOptions{Model: model, Brain: noNamespace}, "SearchNamespace is required"},
		"an invalid MCP transport":               {tacklr.AgentOptions{Model: model, MCP: mcp.Client{}, MCPConfigs: []session.MCPServer{{Name: "stdio"}}}, "stdio"},
		"MCP servers without a client":           {tacklr.AgentOptions{Model: model, MCPConfigs: []session.MCPServer{remote("remote", "https://example.test")}}, "MCP is required"},
		"a duplicate MCP server": {tacklr.AgentOptions{Model: model, MCP: mcp.Client{}, MCPConfigs: []session.MCPServer{
			remote("remote", "https://example.test/a"), remote("remote", "https://example.test/b"),
		}}, "duplicate MCP server"},
		"a credential ref without a resolver": {tacklr.AgentOptions{Model: model, MCP: mcp.Client{}, MCPConfigs: []session.MCPServer{vaulted}}, "credential resolver is required"},
		"a nil specialist":                    {tacklr.AgentOptions{Model: model, Specialists: []*tacklr.Specialist{nil}}, "must not be nil"},
		"a specialist without a name":         {tacklr.AgentOptions{Model: model, Specialists: []*tacklr.Specialist{{Model: model}}}, "Name is required"},
		"a specialist without a model":        {tacklr.AgentOptions{Model: model, Specialists: []*tacklr.Specialist{{Name: "w"}}}, "Model is required"},
		"a duplicate specialist":              {tacklr.AgentOptions{Model: model, Specialists: []*tacklr.Specialist{{Name: "w", Model: model}, {Name: "w", Model: model}}}, "duplicate Specialist"},
		"a nil nested specialist": {tacklr.AgentOptions{Model: model, Specialists: []*tacklr.Specialist{
			{Name: "lead", Model: model, Specialists: []*tacklr.Specialist{nil}},
		}}, "specialist lead"},
		"a skill directory without SKILL.md": {tacklr.AgentOptions{Model: model, SkillsLoader: skills.Loader{Root: broken}}, "initialize skills"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if tc.opts.Model != nil && name != "a model that reports no window" {
				tc.opts.MaxWindowSize = 8192
			}

			_, err := tacklr.NewHarness(t.Context(), tc.opts)

			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("construct = %v, want an error naming %q", err, tc.want)
			}
		})
	}
	valid := tacklr.AgentOptions{Model: openai.New(nil).WithModel("o3")}
	if err := valid.Validate(); err != nil || valid.MaxWindowSize != 200000 {
		t.Fatalf("validate = %v with window %d, want the model's window", err, valid.MaxWindowSize)
	}
}

// expiredVault is the host's credential store: each reference it holds
// has expired.
type expiredVault struct{}

func (expiredVault) ResolveMCP(context.Context, string) (session.MCPCredentials, error) {
	return session.MCPCredentials{}, errors.New("expired")
}

// skillPack is a local skills directory with one skill.
func skillPack(t *testing.T, name, description, instructions string) skills.Loader {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, name), 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: " + name + "\ndescription: " + description + "\n---\n\n" + instructions + "\n"
	if err := os.WriteFile(filepath.Join(root, name, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return skills.Loader{Root: root}
}

// TestNewHarness_twoBuildsSendTheSameRequestPrefix: two harnesses built
// from the same inputs, as on two workers, send the model the same tools
// in the same order and the same system prompt, so the provider's prompt
// cache holds across a rebuild. The tools are the reachable MCP servers'
// in name order (a server whose credentials do not resolve is skipped),
// and the prompt carries the skills catalog, the specialists in name order,
// the knowledge store and the host's instructions. read_skill reads a
// skill's body.
func TestNewHarness_twoBuildsSendTheSameRequestPrefix(t *testing.T) {
	t.Parallel()
	srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "svc", Version: "v0.0.1"}, nil)
	for _, name := range []string{"zeta", "alpha"} {
		mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: name, Description: name},
			func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
				return &mcpsdk.CallToolResult{}, struct{}{}, nil
			})
	}
	hs := httptest.NewServer(mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return srv }, nil))
	t.Cleanup(hs.Close)
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	model, provider := scripted(t)
	opts := tacklr.AgentOptions{
		Model: model, MaxWindowSize: 8192, SystemPrompt: "be brief",
		MCP: mcp.Client{},
		MCPConfigs: []session.MCPServer{
			{Name: "svc", Type: session.MCPHTTP, URL: hs.URL},
			{Name: "vault", Type: session.MCPHTTP, URL: hs.URL, CredentialRef: "vault://svc"},
		},
		MCPCredentialResolver: expiredVault{},
		SkillsLoader:          skillPack(t, "research", "Research carefully", "Verify claims."),
		Brain:                 eng, SearchNamespace: storetest.Namespace(t, "org", "acme"),
		Specialists: []*tacklr.Specialist{{Name: "writer", Description: "Writes.", Model: model}, {Name: "analyst", Model: model}},
	}
	var skill []string
	for range 2 {
		h := newHarness(t, "prefix", opts)
		prompt(t, h, "go")
		if _, err := h.Infer(t.Context(), newStream(t).out); err != nil {
			t.Fatal(err)
		}
		skill = append(skill, result(t, h, call("s1", "read_skill", `{"name":"research"}`)), result(t, h, call("s2", "read_skill", `{"name":"missing"}`)))
	}

	sent := provider.sent()
	one, two := sent[0], sent[1]
	if !slices.Equal(one.toolNames(), two.toolNames()) || one.msgs[0].Content != two.msgs[0].Content {
		t.Fatal("two builds from the same inputs sent different request prefixes")
	}
	mcpTools := slices.DeleteFunc(one.toolNames(), func(n string) bool { return !strings.Contains(n, "__") })
	if !slices.Equal(mcpTools, []string{"svc__alpha", "svc__zeta"}) {
		t.Fatalf("MCP tools = %v, want the reachable server's tools in name order", mcpTools)
	}
	system := one.msgs[0].Content
	analyst, writer := strings.Index(system, " - analyst\n"), strings.Index(system, " - writer: Writes.\n")
	if !containsAll(system, "- research: Research carefully", "search the knowledge store", "be brief") || analyst < 0 || writer < analyst {
		t.Fatalf("system prompt = %s, want the skills, the specialists in name order, the knowledge store and the host's instructions", system)
	}
	if !strings.Contains(skill[0], "Verify claims.") || !strings.Contains(skill[1], `unknown skill "missing"`) {
		t.Fatalf("read_skill = %q, want the body, then the unknown name", skill[:2])
	}
}
