package mcp

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// stdioServerEnv makes the test binary run as a stdio MCP server instead of
// its tests. Its one tool, env, returns the environment the server sees.
const stdioServerEnv = "TACKLR_MCP_TEST_STDIO_SERVER"

func TestMain(m *testing.M) {
	if os.Getenv(stdioServerEnv) == "1" {
		srv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "stdio", Version: "v0.0.1"}, nil)
		mcpsdk.AddTool(srv, &mcpsdk.Tool{Name: "env"},
			func(context.Context, *mcpsdk.CallToolRequest, struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
				return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: strings.Join(os.Environ(), "\n")}}}, struct{}{}, nil
			})
		if err := srv.Run(context.Background(), &mcpsdk.StdioTransport{}); err != nil {
			os.Exit(1)
		}
		return
	}
	os.Exit(m.Run())
}

// TestConnect_givesAStdioServerOnlyTheEnvironmentItsPolicyAllows: a stdio
// server gets the default host variables, the host variables it names and
// its own variables, with each malformed name left out. A server that
// inherits the host environment gets all of it, plus its own variables.
func TestConnect_givesAStdioServerOnlyTheEnvironmentItsPolicyAllows(t *testing.T) {
	t.Setenv("TACKLR_ALLOWED", "allowed")
	t.Setenv("TACKLR_SECRET", "secret")
	server := session.MCPEnv{Name: stdioServerEnv, Value: "1"}
	tools, cleanup := Client{}.Connect(t.Context(), []session.MCPServer{
		{Name: "allowlisted", Command: os.Args[0], HostEnv: []string{"TACKLR_ALLOWED", "", "BAD=NAME"},
			Env: []session.MCPEnv{{Name: "", Value: "x"}, {Name: "A=B", Value: "x"}, {Name: "OWN", Value: "1"}, server}},
		{Name: "trusted", Type: session.MCPStdio, Command: os.Args[0], InheritHostEnv: true,
			Env: []session.MCPEnv{{Name: "B=C", Value: "x"}, {Name: "OWN", Value: "2"}, server}},
	})
	t.Cleanup(cleanup)
	envOf := map[string][]string{}
	for _, tool := range tools {
		out, err := tool.Call(t.Context(), map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		envOf[tool.Server] = strings.Split(out, "\n")
	}
	want := []string{"OWN=1", "TACKLR_ALLOWED=allowed", stdioServerEnv + "=1"}
	for _, name := range defaultMCPHostEnvironment {
		if value, ok := os.LookupEnv(name); ok {
			want = append(want, name+"="+value)
		}
	}
	slices.Sort(want)

	if got := envOf["allowlisted"]; !slices.Equal(got, want) {
		t.Errorf("allowlisted server environment = %v, want %v", got, want)
	}
	if got := envOf["trusted"]; !slices.Contains(got, "TACKLR_SECRET=secret") || !slices.Contains(got, "OWN=2") || !slices.Contains(got, "PATH="+os.Getenv("PATH")) {
		t.Errorf("trusted server environment = %v, want the host's and its own", got)
	}
}
