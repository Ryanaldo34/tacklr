package mcp

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/session"

	mcpjsonrpc "github.com/modelcontextprotocol/go-sdk/jsonrpc"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestInterpretError_surfacesRPCHTTPAndSessionCauses(t *testing.T) {
	c := &client{config: session.MCPServer{Name: "test"}}
	rpcWithData := &mcpjsonrpc.Error{Code: -32602, Message: "invalid params", Data: []byte(`{"detail":"missing field"}`)}
	rpcNoData := &mcpjsonrpc.Error{Code: -32601, Message: "method not found"}
	forbidden := &httpError{Status: http.StatusForbidden, Body: "insufficient authentication scopes"}
	internal := &httpError{Status: http.StatusInternalServerError, Body: "server exploded"}

	cases := []struct {
		name   string
		op     string
		in     error
		want   []string
		unwant string
		is     error
	}{
		{
			name: "jsonrpc with data",
			op:   `call tool "foo"`,
			in:   fmt.Errorf("calling %q: %w", "tools/call", rpcWithData),
			want: []string{"JSON-RPC error -32602", "invalid params", `{"detail":"missing field"}`},
		},
		{
			name:   "jsonrpc without data",
			op:     "list tools",
			in:     fmt.Errorf("calling %q: %w", "tools/call", rpcNoData),
			want:   []string{"JSON-RPC error -32601"},
			unwant: "data:",
		},
		{
			name: "http 403",
			op:   `call tool "gmail"`,
			in:   fmt.Errorf("mcp server %q: call tool %q: %w", "google", "gmail", forbidden),
			want: []string{"HTTP error (status 403)", "insufficient authentication scopes"},
		},
		{
			name: "http 500",
			op:   `call tool "foo"`,
			in:   fmt.Errorf("call failed: %w", internal),
			want: []string{"HTTP error (status 500)"},
		},
		{
			name: "session missing",
			op:   "list tools",
			in:   fmt.Errorf("calling %q: %w", "tools/list", mcpsdk.ErrSessionMissing),
			want: []string{"session missing"},
			is:   mcpsdk.ErrSessionMissing,
		},
		{
			name: "connection closed",
			op:   `call tool "foo"`,
			in:   fmt.Errorf("calling %q: %w", "tools/call", mcpsdk.ErrConnectionClosed),
			want: []string{"connection closed"},
			is:   mcpsdk.ErrConnectionClosed,
		},
		{
			name: "fallback",
			op:   `call tool "foo"`,
			in:   errors.New("something weird"),
			want: []string{"something weird", `mcp server "test"`},
		},
		{
			name: "connect 403",
			op:   "connect",
			in:   fmt.Errorf("connect: %w", forbidden),
			want: []string{"connect", "HTTP error (status 403)"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.interpretError(tc.op, tc.in)
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %q, want %q", err, want)
				}
			}
			if tc.unwant != "" && strings.Contains(err.Error(), tc.unwant) {
				t.Fatalf("error = %q, did not want %q", err, tc.unwant)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Fatalf("unwrap = %v, want %v", err, tc.is)
			}
		})
	}
}

func TestBuildTransportStdio_hostEnvironmentRequiresExplicitPolicy(t *testing.T) {
	t.Setenv("TACKLR_ALLOWED", "allowed")
	t.Setenv("TACKLR_SECRET", "secret")

	allowlisted := &client{config: session.MCPServer{
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

	trusted := &client{config: session.MCPServer{
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

func TestAppendExplicitEnvironment_skipsMalformedEntries(t *testing.T) {
	out := appendExplicitEnvironment(
		[]string{"PLAIN", "A=1"},
		[]session.MCPEnv{{Name: "", Value: "x"}, {Name: "B=C", Value: "x"}, {Name: "OK", Value: "2"}},
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
	out := commandEnvironment(session.MCPServer{
		HostEnv: []string{"", "BAD=NAME", "GOOD"},
		Env:     []session.MCPEnv{{Name: "", Value: "x"}, {Name: "A=B", Value: "x"}, {Name: "OK", Value: "1"}},
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

// TestBuildTransport_needsWhatEachTransportConnectsWith: stdio needs a
// command, HTTP and SSE need a URL, an SSE server gets an SSE transport at
// its URL, and an unknown type is not supported.
func TestBuildTransport_needsWhatEachTransportConnectsWith(t *testing.T) {
	for name, tc := range map[string]struct {
		config session.MCPServer
		want   string
	}{
		"stdio without a command": {session.MCPServer{Name: "fs"}, "command is required"},
		"http without a url":      {session.MCPServer{Name: "api", Type: session.MCPHTTP}, "url is required"},
		"sse without a url":       {session.MCPServer{Name: "api", Type: session.MCPSSE}, "url is required"},
		"an unknown type":         {session.MCPServer{Name: "weird", Type: "grpc"}, errTransportNotSupported.Error()},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := (&client{config: tc.config}).buildTransport()

			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("buildTransport = %v, want %q", err, tc.want)
			}
		})
	}

	transport, err := (&client{config: session.MCPServer{Name: "events", Type: session.MCPSSE, URL: "https://events.example.com/mcp"}}).buildTransport()

	if st, ok := transport.(*mcpsdk.SSEClientTransport); err != nil || !ok || st.Endpoint != "https://events.example.com/mcp" {
		t.Fatalf("sse transport = %T %v, want an SSE transport at its URL", transport, err)
	}
}

// A client whose connect failed has no session, and closing it is clean.
func TestClose_aClientThatNeverConnectedClosesCleanly(t *testing.T) {
	c := &client{config: session.MCPServer{Name: "drive"}}
	err := c.close()
	if err != nil {
		t.Errorf("expected nil error when session is nil, got %v", err)
	}
}
