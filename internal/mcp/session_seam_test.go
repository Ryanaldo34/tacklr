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
	callResult *mcpsdk.CallToolResult
	callErr    error
	closeErr   error
	closed     bool
}

func (f *fakeSession) ListTools(ctx context.Context, params *mcpsdk.ListToolsParams) (*mcpsdk.ListToolsResult, error) {
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
