package mcp

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	mcpjsonrpc "github.com/modelcontextprotocol/go-sdk/jsonrpc"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestInterpretErrorJSONRPC(t *testing.T) {
	c := &Client{config: MCPConfig{Name: "test"}}

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
	c := &Client{config: MCPConfig{Name: "test"}}

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
	c := &Client{config: MCPConfig{Name: "google", RequiredScopes: []string{"openid", "mail"}}}

	httpErr := &MCPHTTPError{Status: http.StatusForbidden, Body: "Request had insufficient authentication scopes."}
	wrapped := fmt.Errorf("mcp server %q: call tool %q: %w", "google", "gmail", httpErr)

	err := c.interpretError("call tool \"gmail\"", wrapped)

	var got *MCPHTTPError
	if !errors.As(err, &got) {
		t.Errorf("expected errors.As to find *MCPHTTPError, got %v", err)
	}
	if !strings.Contains(err.Error(), "request forbidden (HTTP 403)") {
		t.Errorf("expected forbidden mention, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "re-authenticate at /auth/google") {
		t.Errorf("expected re-auth hint, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "openid") || !strings.Contains(err.Error(), "mail") {
		t.Errorf("expected required scopes in error, got %q", err.Error())
	}
}

func TestInterpretErrorHTTPNonForbidden(t *testing.T) {
	c := &Client{config: MCPConfig{Name: "test"}}

	httpErr := &MCPHTTPError{Status: http.StatusInternalServerError, Body: "server exploded"}
	wrapped := fmt.Errorf("call failed: %w", httpErr)

	err := c.interpretError("call tool \"foo\"", wrapped)
	if !strings.Contains(err.Error(), "HTTP error (status 500)") {
		t.Errorf("expected HTTP status mention, got %q", err.Error())
	}
	if strings.Contains(err.Error(), "re-authenticate") {
		t.Errorf("did not expect re-auth hint for status 500, got %q", err.Error())
	}
}

func TestInterpretErrorSessionMissing(t *testing.T) {
	c := &Client{config: MCPConfig{Name: "test"}}

	err := c.interpretError("list tools", fmt.Errorf("calling %q: %w", "tools/list", mcpsdk.ErrSessionMissing))
	if !errors.Is(err, mcpsdk.ErrSessionMissing) {
		t.Errorf("expected errors.Is ErrSessionMissing, got %v", err)
	}
	if !strings.Contains(err.Error(), "session missing") {
		t.Errorf("expected session missing mention, got %q", err.Error())
	}
}

func TestInterpretErrorConnectionClosed(t *testing.T) {
	c := &Client{config: MCPConfig{Name: "test"}}

	err := c.interpretError("call tool \"foo\"", fmt.Errorf("calling %q: %w", "tools/call", mcpsdk.ErrConnectionClosed))
	if !errors.Is(err, mcpsdk.ErrConnectionClosed) {
		t.Errorf("expected errors.Is ErrConnectionClosed, got %v", err)
	}
	if !strings.Contains(err.Error(), "connection closed") {
		t.Errorf("expected connection closed mention, got %q", err.Error())
	}
}

func TestInterpretErrorFallback(t *testing.T) {
	c := &Client{config: MCPConfig{Name: "test"}}

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
	c := &Client{config: MCPConfig{Name: "gmail", RequiredScopes: []string{"mail"}}}

	httpErr := &MCPHTTPError{Status: http.StatusForbidden, Body: "insufficient authentication scopes"}
	wrapped := fmt.Errorf("connect: %w", httpErr)

	err := c.interpretError("connect", wrapped)
	if !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected operation 'connect' in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "re-authenticate at /auth/google") {
		t.Errorf("expected re-auth hint for connect 403, got %q", err.Error())
	}
}

func TestCloseIgnores405(t *testing.T) {
	c := &Client{config: MCPConfig{Name: "drive"}}
	err := c.Close()
	if err != nil {
		t.Errorf("expected nil error when session is nil, got %v", err)
	}
}

func TestInterpretErrorPreservesUnwrap(t *testing.T) {
	c := &Client{config: MCPConfig{Name: "test"}}

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
