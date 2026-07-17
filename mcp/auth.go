package mcp

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// MCPHTTPError conveys a non-2xx HTTP response from an MCP server. It is
// returned by the transport wrapper so callers can use errors.As to extract
// the status code and response body without parsing err.Error().
type MCPHTTPError struct {
	Status int
	Body   string
}

func (e *MCPHTTPError) Error() string {
	return fmt.Sprintf("mcp HTTP error (status %d): %s", e.Status, e.Body)
}

// headerTransport wraps an http.RoundTripper and injects custom headers plus
// an optional bearer token into every outgoing request. For non-2xx
// responses, it captures the status code and a limited amount of the response
// body so the MCP client can surface structured HTTP errors.
type headerTransport struct {
	base    http.RoundTripper
	token   string
	headers map[string]string
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		// Limit body capture to avoid buffering huge error pages.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 32*1024))
		resp.Body.Close()
		return nil, &MCPHTTPError{Status: resp.StatusCode, Body: string(body)}
	}

	return resp, nil
}

// buildHTTPClient creates an *http.Client configured with the auth strategy
// and custom headers specified in the MCPConfig.
//
// Auth precedence:
//  1. Bearer token (AuthToken) — injected via a custom transport.
//  2. OAuth client credentials flow (OAuthURL + client ID/secret) — uses
//     golang.org/x/oauth2 for automatic token acquisition and refresh.
//  3. No auth — only custom headers are applied (if any).
func buildHTTPClient(ctx context.Context, cfg MCPConfig) (*http.Client, error) {
	base := http.DefaultTransport

	if cfg.AuthRequired && cfg.AuthToken == "" && cfg.OAuthURL == "" {
		return nil, fmt.Errorf("mcp server %q: authRequired is true but no AuthToken or OAuthURL provided: %w", cfg.Name, ErrMCPAuthRequired)
	}

	if cfg.OAuthURL != "" {
		oauthCfg := clientcredentials.Config{
			TokenURL:     cfg.OAuthURL,
			ClientID:     cfg.OAuthClientID,
			ClientSecret: cfg.OAuthClientSecret,
			Scopes:       cfg.OAuthScopes,
		}
		tokenSource := oauthCfg.TokenSource(ctx)
		transport := &headerTransport{
			base:    base,
			headers: cfg.Headers,
		}
		return &http.Client{
			Transport: &oauth2.Transport{
				Source: tokenSource,
				Base:   transport,
			},
		}, nil
	}

	transport := &headerTransport{
		base:    base,
		token:   cfg.AuthToken,
		headers: cfg.Headers,
	}
	return &http.Client{Transport: transport}, nil
}
