package mcpruntime

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ryanaldo34/tacklr/mcp"
)

// fakeSession implements toolSession for pagination, structured content, and close.
type fakeSession struct {
	pages      [][]*mcpsdk.Tool
	pageIdx    int
	listErr    error
	callResult *mcpsdk.CallToolResult
	callErr    error
	closeErr   error
	closed     bool
}

func (f *fakeSession) ListTools(ctx context.Context, params *mcpsdk.ListToolsParams) (*mcpsdk.ListToolsResult, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.pageIdx >= len(f.pages) {
		return &mcpsdk.ListToolsResult{}, nil
	}
	tools := f.pages[f.pageIdx]
	f.pageIdx++
	res := &mcpsdk.ListToolsResult{Tools: tools}
	if f.pageIdx < len(f.pages) {
		res.NextCursor = "next"
	}
	return res, nil
}

func (f *fakeSession) CallTool(ctx context.Context, params *mcpsdk.CallToolParams) (*mcpsdk.CallToolResult, error) {
	if f.callErr != nil {
		return nil, f.callErr
	}
	return f.callResult, nil
}

func (f *fakeSession) Close() error {
	f.closed = true
	return f.closeErr
}

func TestListTools_paginatesUntilEmptyCursor(t *testing.T) {
	c := &client{
		config: mcp.MCPConfig{Name: "p"},
		session: &fakeSession{
			pages: [][]*mcpsdk.Tool{
				{{Name: "a", Description: "A"}},
				{{Name: "b", Title: "Bee"}}, // description empty → Title used in DiscoverAllTools, not listTools
			},
		},
	}
	tools, err := c.listTools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 || tools[0].Name != "a" || tools[1].Name != "b" {
		t.Fatalf("tools = %+v", tools)
	}
}

func TestCallTool_structuredContentWhenNoText(t *testing.T) {
	c := &client{
		config: mcp.MCPConfig{Name: "p"},
		session: &fakeSession{
			callResult: &mcpsdk.CallToolResult{
				StructuredContent: map[string]any{"k": "v"},
			},
		},
	}
	out, err := c.callTool(context.Background(), "t", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"k"`) || !strings.Contains(out, `"v"`) {
		t.Fatalf("out = %q", out)
	}
}

func TestClose_405TreatedAsSuccess(t *testing.T) {
	c := &client{
		config: mcp.MCPConfig{Name: "p"},
		session: &fakeSession{
			closeErr: &httpError{Status: http.StatusMethodNotAllowed, Body: "no delete"},
		},
	}
	if err := c.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestClose_otherErrorPropagates(t *testing.T) {
	c := &client{
		config: mcp.MCPConfig{Name: "p"},
		session: &fakeSession{
			closeErr: errors.New("boom"),
		},
	}
	err := c.close()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
}

func TestListTools_sessionError(t *testing.T) {
	c := &client{
		config:  mcp.MCPConfig{Name: "p"},
		session: &fakeSession{listErr: errors.New("list down")},
	}
	if _, err := c.listTools(context.Background()); err == nil || !strings.Contains(err.Error(), "list down") {
		t.Fatalf("err = %v", err)
	}
}

func TestCallTool_sessionErrorAndMarshalFail(t *testing.T) {
	c := &client{
		config:  mcp.MCPConfig{Name: "p"},
		session: &fakeSession{callErr: errors.New("call down")},
	}
	if _, err := c.callTool(context.Background(), "t", nil); err == nil || !strings.Contains(err.Error(), "call down") {
		t.Fatalf("err = %v", err)
	}
	// StructuredContent that cannot marshal.
	c.session = &fakeSession{
		callResult: &mcpsdk.CallToolResult{
			StructuredContent: make(chan int),
		},
	}
	if _, err := c.callTool(context.Background(), "t", nil); err == nil || !strings.Contains(err.Error(), "marshal") {
		t.Fatalf("marshal err = %v", err)
	}
}

func TestConnect_transportBuildError(t *testing.T) {
	c := newClient(mcp.MCPConfig{Name: "x", Type: mcp.TransportHTTP}) // no URL
	if err := c.connect(context.Background()); err == nil || !strings.Contains(err.Error(), "url is required") {
		t.Fatalf("err = %v", err)
	}
}

func TestRegisterClientTools_listErrorTitleAndSchema(t *testing.T) {
	// list error
	c := &client{
		config:  mcp.MCPConfig{Name: "p"},
		session: &fakeSession{listErr: errors.New("no list")},
	}
	if err := registerClientTools(context.Background(), c, "ns", func(string, string, string, map[string]any, ToolHandler) {}); err == nil {
		t.Fatal("expected list error")
	}

	// Title fallback + non-map schema
	var gotDesc string
	var gotSchema map[string]any
	c.session = &fakeSession{
		pages: [][]*mcpsdk.Tool{
			{{Name: "t1", Title: "FromTitle", InputSchema: "not-a-map"}},
		},
	}
	err := registerClientTools(context.Background(), c, "ns", func(name, desc, ns string, schema map[string]any, h ToolHandler) {
		gotDesc = desc
		gotSchema = schema
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotDesc != "FromTitle" {
		t.Fatalf("desc = %q", gotDesc)
	}
	if gotSchema != nil {
		t.Fatalf("schema = %#v, want nil", gotSchema)
	}
}

func TestDiscoverAllTools_listFailAndCleanupCloseError(t *testing.T) {
	prev := connectClient
	t.Cleanup(func() { connectClient = prev })

	// Connect "succeeds" with a session that fails ListTools.
	connectClient = func(ctx context.Context, c *client) error {
		c.session = &fakeSession{listErr: errors.New("list fail")}
		return nil
	}
	cleanup := DiscoverAllTools(context.Background(), []mcp.MCPConfig{
		{Name: "s", Type: mcp.TransportHTTP, URL: "http://example.invalid"},
	}, func(string, string, string, map[string]any, ToolHandler) {})
	cleanup() // no clients retained

	// Cleanup close-error warn path.
	connectClient = func(ctx context.Context, c *client) error {
		c.session = &fakeSession{
			pages:    [][]*mcpsdk.Tool{{{Name: "t", Description: "d"}}},
			closeErr: errors.New("close fail"),
		}
		return nil
	}
	cleanup = DiscoverAllTools(context.Background(), []mcp.MCPConfig{
		{Name: "s2", Type: mcp.TransportHTTP, URL: "http://example.invalid"},
	}, func(string, string, string, map[string]any, ToolHandler) {})
	cleanup() // hits closeClients warn
	// also call closeClients directly for clarity
	closeClients([]*client{{
		config:  mcp.MCPConfig{Name: "x"},
		session: &fakeSession{closeErr: errors.New("close fail")},
	}})
}
