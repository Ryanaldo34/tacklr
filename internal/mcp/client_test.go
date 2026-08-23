package mcpruntime

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	mcpjsonrpc "github.com/modelcontextprotocol/go-sdk/jsonrpc"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr/mcp"
)

func TestInterpretErrorJSONRPC(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "test"}}

	rpcErr := &mcpjsonrpc.Error{
		Code:    -32602,
		Message: "invalid params",
		Data:    []byte(`{"detail":"missing field"}`),
	}
	wrapped := fmt.Errorf("calling %q: %w", "tools/call", rpcErr)

	err := c.interpretError("call tool \"foo\"", wrapped)
	if !errors.As(err, &rpcErr) {
		t.Errorf("expected errors.As to find *jsonrpc.Error, got %v", err)
	}
	if !strings.Contains(err.Error(), "JSON-RPC error -32602") {
		t.Errorf("expected error to mention JSON-RPC code, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "invalid params") {
		t.Errorf("expected error to mention message, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), `{"detail":"missing field"}`) {
		t.Errorf("expected error to include data, got %q", err.Error())
	}
}

func TestInterpretErrorJSONRPCNoData(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "test"}}

	rpcErr := &mcpjsonrpc.Error{Code: -32601, Message: "method not found"}
	wrapped := fmt.Errorf("calling %q: %w", "tools/call", rpcErr)

	err := c.interpretError("list tools", wrapped)
	if !strings.Contains(err.Error(), "JSON-RPC error -32601") {
		t.Errorf("expected error to mention JSON-RPC code, got %q", err.Error())
	}
	if strings.Contains(err.Error(), "data:") {
		t.Errorf("expected no data suffix when Data is empty, got %q", err.Error())
	}
}

func TestInterpretErrorHTTPForbidden(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "google"}}

	httpErr := &httpError{Status: http.StatusForbidden, Body: "Request had insufficient authentication scopes."}
	wrapped := fmt.Errorf("mcp server %q: call tool %q: %w", "google", "gmail", httpErr)

	err := c.interpretError("call tool \"gmail\"", wrapped)

	var got *httpError
	if !errors.As(err, &got) {
		t.Errorf("expected errors.As to find *httpError, got %v", err)
	}
	if !strings.Contains(err.Error(), "HTTP error (status 403)") {
		t.Errorf("expected HTTP status mention, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "insufficient authentication scopes") {
		t.Errorf("expected response body in error, got %q", err.Error())
	}
}

func TestInterpretErrorHTTPNonForbidden(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "test"}}

	httpErr := &httpError{Status: http.StatusInternalServerError, Body: "server exploded"}
	wrapped := fmt.Errorf("call failed: %w", httpErr)

	err := c.interpretError("call tool \"foo\"", wrapped)
	if !strings.Contains(err.Error(), "HTTP error (status 500)") {
		t.Errorf("expected HTTP status mention, got %q", err.Error())
	}
}

func TestInterpretErrorSessionMissing(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "test"}}

	err := c.interpretError("list tools", fmt.Errorf("calling %q: %w", "tools/list", mcpsdk.ErrSessionMissing))
	if !errors.Is(err, mcpsdk.ErrSessionMissing) {
		t.Errorf("expected errors.Is ErrSessionMissing, got %v", err)
	}
	if !strings.Contains(err.Error(), "session missing") {
		t.Errorf("expected session missing mention, got %q", err.Error())
	}
}

func TestInterpretErrorConnectionClosed(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "test"}}

	err := c.interpretError("call tool \"foo\"", fmt.Errorf("calling %q: %w", "tools/call", mcpsdk.ErrConnectionClosed))
	if !errors.Is(err, mcpsdk.ErrConnectionClosed) {
		t.Errorf("expected errors.Is ErrConnectionClosed, got %v", err)
	}
	if !strings.Contains(err.Error(), "connection closed") {
		t.Errorf("expected connection closed mention, got %q", err.Error())
	}
}

func TestInterpretErrorFallback(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "test"}}

	orig := errors.New("something weird")
	err := c.interpretError("call tool \"foo\"", orig)
	if !strings.Contains(err.Error(), "something weird") {
		t.Errorf("expected original error message preserved, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "mcp server \"test\"") {
		t.Errorf("expected server name in fallback, got %q", err.Error())
	}
}

func TestInterpretErrorConnect(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "gmail"}}

	httpErr := &httpError{Status: http.StatusForbidden, Body: "insufficient authentication scopes"}
	wrapped := fmt.Errorf("connect: %w", httpErr)

	err := c.interpretError("connect", wrapped)
	if !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected operation 'connect' in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "HTTP error (status 403)") {
		t.Errorf("expected HTTP status mention for connect 403, got %q", err.Error())
	}
}

func TestBuildTransportStdio(t *testing.T) {
	t.Setenv("TACKLR_HOST_SECRET", "must-not-leak")
	c := &client{config: mcp.MCPConfig{
		Name:    "fs",
		Command: "npx",
		Args:    []string{"-y", "@modelcontextprotocol/server-filesystem"},
		Env:     []mcp.EnvVariable{{Name: "API_KEY", Value: "secret"}},
	}}
	transport, err := c.buildTransport()
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	ct, ok := transport.(*mcpsdk.CommandTransport)
	if !ok {
		t.Fatalf("expected *CommandTransport, got %T", transport)
	}
	if ct.Command.Args[0] != "npx" {
		t.Errorf("command = %q, want npx", ct.Command.Args[0])
	}
	if len(ct.Command.Args) != 3 || ct.Command.Args[1] != "-y" {
		t.Errorf("command args = %v, want [npx -y @modelcontextprotocol/server-filesystem]", ct.Command.Args)
	}
	found := false
	leaked := false
	for _, env := range ct.Command.Env {
		if env == "API_KEY=secret" {
			found = true
		}
		if env == "TACKLR_HOST_SECRET=must-not-leak" {
			leaked = true
		}
	}
	if !found {
		t.Errorf("expected API_KEY=secret in command env, got %v", ct.Command.Env)
	}
	if leaked {
		t.Fatalf("stdio MCP inherited host secret: %v", ct.Command.Env)
	}
}

func TestBuildTransportStdio_hostEnvironmentRequiresExplicitPolicy(t *testing.T) {
	t.Setenv("TACKLR_ALLOWED", "allowed")
	t.Setenv("TACKLR_SECRET", "secret")

	allowlisted := &client{config: mcp.MCPConfig{
		Name:    "allowlisted",
		Command: "server",
		HostEnv: []string{"TACKLR_ALLOWED"},
	}}
	transport, err := allowlisted.buildTransport()
	if err != nil {
		t.Fatal(err)
	}
	allowlistedCommand := transport.(*mcpsdk.CommandTransport).Command
	if !slices.Contains(allowlistedCommand.Env, "TACKLR_ALLOWED=allowed") ||
		slices.Contains(allowlistedCommand.Env, "TACKLR_SECRET=secret") {
		t.Fatalf("allowlisted environment = %v", allowlistedCommand.Env)
	}

	trusted := &client{config: mcp.MCPConfig{
		Name:           "trusted",
		Command:        "server",
		InheritHostEnv: true,
	}}
	transport, err = trusted.buildTransport()
	if err != nil {
		t.Fatal(err)
	}
	if env := transport.(*mcpsdk.CommandTransport).Command.Env; !slices.Contains(env, "TACKLR_SECRET=secret") {
		t.Fatalf("trusted environment omitted host value: %v", env)
	}
}

func TestBuildTransportStdioDefaultWhenTypeEmpty(t *testing.T) {
	for _, typ := range []string{"", mcp.TransportStdio} {
		c := &client{config: mcp.MCPConfig{Name: "fs", Type: typ, Command: "server"}}
		transport, err := c.buildTransport()
		if err != nil {
			t.Fatalf("buildTransport(type=%q): %v", typ, err)
		}
		if _, ok := transport.(*mcpsdk.CommandTransport); !ok {
			t.Errorf("type=%q: expected *CommandTransport, got %T", typ, transport)
		}
	}
}

func TestAppendExplicitEnvironment_skipsMalformedEntries(t *testing.T) {
	out := appendExplicitEnvironment(
		[]string{"PLAIN", "A=1"},
		[]mcp.EnvVariable{{Name: "", Value: "x"}, {Name: "B=C", Value: "x"}, {Name: "OK", Value: "2"}},
	)
	if !slices.Contains(out, "A=1") || !slices.Contains(out, "OK=2") {
		t.Fatalf("out = %v", out)
	}
	for _, e := range out {
		if e == "PLAIN" || strings.HasPrefix(e, "B=C=") || e == "=x" {
			t.Fatalf("malformed leaked: %v", out)
		}
	}
}

func TestCommandEnvironment_skipsInvalidNames(t *testing.T) {
	t.Setenv("GOOD", "yes")
	out := commandEnvironment(mcp.MCPConfig{
		HostEnv: []string{"", "BAD=NAME", "GOOD"},
		Env:     []mcp.EnvVariable{{Name: "", Value: "x"}, {Name: "A=B", Value: "x"}, {Name: "OK", Value: "1"}},
	})
	if !slices.Contains(out, "GOOD=yes") || !slices.Contains(out, "OK=1") {
		t.Fatalf("out = %v", out)
	}
	for _, e := range out {
		if strings.HasPrefix(e, "BAD=NAME") || e == "=x" || strings.HasPrefix(e, "A=B=") {
			t.Fatalf("invalid name leaked: %v", out)
		}
	}
}

func TestBuildTransportStdioRequiresCommand(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "fs"}}
	_, err := c.buildTransport()
	if err == nil {
		t.Fatal("expected error for missing command")
	}
	if !strings.Contains(err.Error(), "command is required") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "command is required")
	}
}

func TestBuildTransportHTTP(t *testing.T) {
	c := &client{config: mcp.MCPConfig{
		Name:    "api",
		Type:    mcp.TransportHTTP,
		URL:     "https://api.example.com/mcp",
		Headers: []mcp.HTTPHeader{{Name: "Authorization", Value: "Bearer tok"}},
	}}
	transport, err := c.buildTransport()
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	st, ok := transport.(*mcpsdk.StreamableClientTransport)
	if !ok {
		t.Fatalf("expected *StreamableClientTransport, got %T", transport)
	}
	if st.Endpoint != "https://api.example.com/mcp" {
		t.Errorf("endpoint = %q, want https://api.example.com/mcp", st.Endpoint)
	}
	if st.HTTPClient == nil {
		t.Error("expected non-nil HTTPClient")
	}
}

func TestBuildTransportSSE(t *testing.T) {
	c := &client{config: mcp.MCPConfig{
		Name: "events",
		Type: mcp.TransportSSE,
		URL:  "https://events.example.com/mcp",
	}}
	transport, err := c.buildTransport()
	if err != nil {
		t.Fatalf("buildTransport: %v", err)
	}
	st, ok := transport.(*mcpsdk.SSEClientTransport)
	if !ok {
		t.Fatalf("expected *SSEClientTransport, got %T", transport)
	}
	if st.Endpoint != "https://events.example.com/mcp" {
		t.Errorf("endpoint = %q, want https://events.example.com/mcp", st.Endpoint)
	}
}

func TestBuildTransportHTTPRequiresURL(t *testing.T) {
	for _, typ := range []string{mcp.TransportHTTP, mcp.TransportSSE} {
		c := &client{config: mcp.MCPConfig{Name: "api", Type: typ}}
		_, err := c.buildTransport()
		if err == nil {
			t.Fatalf("type=%q: expected error for missing url", typ)
		}
		if !strings.Contains(err.Error(), "url is required") {
			t.Errorf("type=%q: error = %q, want to contain %q", typ, err.Error(), "url is required")
		}
	}
}

func TestBuildTransportUnsupported(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "weird", Type: "grpc"}}
	_, err := c.buildTransport()
	if !errors.Is(err, errTransportNotSupported) {
		t.Errorf("expected errTransportNotSupported, got %v", err)
	}
}

func TestCloseIgnores405(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "drive"}}
	err := c.close()
	if err != nil {
		t.Errorf("expected nil error when session is nil, got %v", err)
	}
}

func TestInterpretErrorPreservesUnwrap(t *testing.T) {
	c := &client{config: mcp.MCPConfig{Name: "test"}}

	rpcErr := &mcpjsonrpc.Error{Code: -32603, Message: "internal error"}
	wrapped := fmt.Errorf("calling %q: %w", "tools/call", rpcErr)

	err := c.interpretError("call tool \"foo\"", wrapped)
	var got *mcpjsonrpc.Error
	if !errors.As(err, &got) {
		t.Errorf("expected errors.As to find original JSON-RPC error, got %v", err)
	}
	if got.Code != -32603 {
		t.Errorf("expected code -32603, got %d", got.Code)
	}
}
