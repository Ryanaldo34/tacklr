package mcpruntime

import (
	"fmt"
	"io"
	"net/http"

	"github.com/ryanaldo34/tacklr/mcp"
)

// httpError conveys a non-2xx HTTP response from an MCP server.
type httpError struct {
	Status int
	Body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("mcp HTTP error (status %d): %s", e.Status, e.Body)
}

// headerTransport injects configured headers and captures non-2xx bodies.
type headerTransport struct {
	base    http.RoundTripper
	headers []mcp.HTTPHeader
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for _, h := range t.headers {
		req.Header.Set(h.Name, h.Value)
	}

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 32*1024))
		resp.Body.Close()
		return nil, &httpError{Status: resp.StatusCode, Body: string(body)}
	}

	return resp, nil
}

func buildHTTPClient(cfg mcp.MCPConfig) *http.Client {
	return &http.Client{
		Transport: &headerTransport{
			base:    http.DefaultTransport,
			headers: cfg.Headers,
		},
	}
}
