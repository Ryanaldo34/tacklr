package exa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// ErrService is a host/network/auth/rate-limit failure (not a model-fixable query).
var ErrService = errors.New("service failed")

// StatusError is an Exa HTTP status. QueryFixable is a 4xx the model can correct
// (not 401/403/429). Those plus 5xx unwrap as ErrService.
type StatusError struct {
	Op     string
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	if e == nil {
		return "exa: status error"
	}
	return fmt.Sprintf("exa %s: status %d: %s", e.Op, e.Status, e.Body)
}

func (e *StatusError) Unwrap() error {
	if e != nil && (e.Status == 401 || e.Status == 403 || e.Status == 429 || e.Status >= 500) {
		return ErrService
	}
	return nil
}

func (e *StatusError) QueryFixable() bool {
	return e != nil && e.Status >= 400 && e.Status < 500 && e.Status != 401 && e.Status != 403 && e.Status != 429
}

func (e *StatusError) PublicationDomain() bool {
	if e == nil {
		return false
	}
	low := strings.ToLower(e.Body)
	return strings.Contains(e.Body, "UNSUPPORTED_PUBLICATION") ||
		strings.Contains(low, "not supported for category=publication")
}

// Client calls Exa’s Search and Contents APIs.
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
	if err := c.requireKey(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Query) == "" {
		return nil, fmt.Errorf("exa: query is required")
	}
	var out SearchResponse
	if err := c.postJSON(ctx, "/search", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Contents performs POST /contents for known URLs (or prior result ids).
// Provide at least one of URLs or IDs.
func (c *Client) Contents(ctx context.Context, req ContentsRequest) (*ContentsResponse, error) {
	if err := c.requireKey(); err != nil {
		return nil, err
	}
	if len(req.URLs) == 0 && len(req.IDs) == 0 {
		return nil, fmt.Errorf("exa: urls or ids is required")
	}
	if len(req.URLs) > 0 && len(req.IDs) > 0 {
		return nil, fmt.Errorf("exa: provide urls or ids, not both")
	}
	var out ContentsResponse
	if err := c.postJSON(ctx, "/contents", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) requireKey() error {
	if c == nil {
		return fmt.Errorf("exa: client is nil")
	}
	if strings.TrimSpace(c.APIKey) == "" {
		return fmt.Errorf("exa: API key is required")
	}
	return nil
}

func (c *Client) postJSON(ctx context.Context, path string, req any, out any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("exa: marshal request: %w", err)
	}

	base := c.BaseURL
	if base == "" {
		base = defaultBaseURL
	}
	base = strings.TrimRight(base, "/")

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("exa: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", c.APIKey)
	httpReq.Header.Set("Accept", "application/json")

	hc := c.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: defaultTimeout}
	}

	op := strings.TrimPrefix(path, "/")
	if op == "" {
		op = "request"
	}

	resp, err := hc.Do(httpReq)
	if err != nil {
		return fmt.Errorf("exa %s: %w", op, errors.Join(ErrService, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
		if err != nil {
			return fmt.Errorf("exa %s: read body: %w", op, errors.Join(ErrService, err))
		}
		msg := strings.TrimSpace(string(raw))
		if len(msg) > maxErrorBody {
			msg = msg[:maxErrorBody] + "…"
		}
		if msg == "" {
			msg = resp.Status
		}
		return &StatusError{Op: op, Status: resp.StatusCode, Body: msg}
	}

	limited := io.LimitReader(resp.Body, 8<<20) // 8 MiB safety cap
	if out == nil {
		_, _ = io.Copy(io.Discard, limited)
		return nil
	}
	if err := json.NewDecoder(limited).Decode(out); err != nil {
		return fmt.Errorf("exa %s: decode response: %w", op, err)
	}
	return nil
}
