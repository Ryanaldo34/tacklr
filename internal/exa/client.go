package exa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultBaseURL = "https://api.exa.ai"
	defaultTimeout = 45 * time.Second
	maxErrorBody   = 512
)

// Client calls Exa’s Search API.
type Client struct {
	APIKey     string
	BaseURL    string
	HTTPClient *http.Client
}

// NewClient builds a client for the given API key.
func NewClient(apiKey string) *Client {
	return &Client{
		APIKey:  strings.TrimSpace(apiKey),
		BaseURL: defaultBaseURL,
		HTTPClient: &http.Client{
			Timeout: defaultTimeout,
		},
	}
}

// Search performs POST /search. req.Query must be non-empty.
func (c *Client) Search(ctx context.Context, req SearchRequest) (*SearchResponse, error) {
	if c == nil {
		return nil, fmt.Errorf("exa: client is nil")
	}
	if strings.TrimSpace(c.APIKey) == "" {
		return nil, fmt.Errorf("exa: API key is required")
	}
	if strings.TrimSpace(req.Query) == "" {
		return nil, fmt.Errorf("exa: query is required")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("exa: marshal request: %w", err)
	}

	base := c.BaseURL
	if base == "" {
		base = defaultBaseURL
	}
	base = strings.TrimRight(base, "/")

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/search", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("exa: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", c.APIKey)
	httpReq.Header.Set("Accept", "application/json")

	hc := c.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: defaultTimeout}
	}

	resp, err := hc.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("exa search: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20)) // 8 MiB safety cap
	if err != nil {
		return nil, fmt.Errorf("exa search: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(raw))
		if len(msg) > maxErrorBody {
			msg = msg[:maxErrorBody] + "…"
		}
		if msg == "" {
			msg = resp.Status
		}
		return nil, fmt.Errorf("exa search: status %d: %s", resp.StatusCode, msg)
	}

	var out SearchResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("exa search: decode response: %w", err)
	}
	return &out, nil
}
