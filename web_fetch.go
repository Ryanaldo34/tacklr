package tacklr

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/internal/exa"
	"github.com/ryanaldo34/tacklr/streaming"
)

const (
	webFetchMaxURLs        = 5
	webFetchToolTimeout    = 60 * time.Second
	webFetchDefaultTextCap = webSearchDefaultTextCap
	webFetchMaxTextCap     = webSearchMaxTextCap
)

// webFetchArgs is the agent-facing surface for reading known URLs via Exa /contents.
type webFetchArgs struct {
	URLs []string `json:"urls" desc:"One or more full page URLs to read (1-5). Prefer https. Use after web_search or when the user/citation already gave a URL. Do not use for open-ended discovery."`

	ContentMode string `json:"content_mode,omitempty" enum:"text,highlights,both" desc:"What to extract. text (default) = page body for careful reading. highlights = short excerpts (optionally guided by highlight_query). both = highlights plus capped text."`

	MaxTextCharacters int `json:"max_text_characters,omitempty" desc:"When content_mode is text or both, cap characters per page (default 4000, max 10000)."`

	HighlightQuery string `json:"highlight_query,omitempty" desc:"Optional focus phrase when content_mode is highlights or both (e.g. what to extract from a long page)."`
}

const webFetchToolDescription = `Fetch and extract content from known web page URLs (Exa contents API).

Use this when you already have specific URLs (from web_search results, the user, or a citation) and need the page body or targeted excerpts. Do not use web_fetch for open-ended discovery — use web_search first.

Default: content_mode text with a character cap for token efficiency. Prefer a small urls list (1-3) over fetching many pages at once.`

func newWebFetchTool(client *exa.Client) *Tool {
	return NewTool(ToolConfig{
		Name:        "web_fetch",
		DisplayName: "Web Fetch",
		Description: webFetchToolDescription,
		Category:    streaming.ToolCategorySearch,
		Access:      ToolReadAccess,
		Timeout:     webFetchToolTimeout,
		Handler: func(ctx context.Context, args webFetchArgs, runtime HarnessRuntime) (string, error) {
			return runWebFetch(ctx, client, args, runtime)
		},
	})
}

func runWebFetch(ctx context.Context, client *exa.Client, args webFetchArgs, runtime HarnessRuntime) (string, error) {
	if client == nil {
		return "", fmt.Errorf("web_fetch: Exa client is not configured")
	}
	req, err := buildExaContentsRequest(args)
	if err != nil {
		return "", err
	}
	runtime.EmitUpdate("Fetching page content…")
	resp, err := client.Contents(ctx, req)
	if err != nil {
		return "", err
	}
	return formatWebFetchResult(req.URLs, resp), nil
}

func buildExaContentsRequest(args webFetchArgs) (exa.ContentsRequest, error) {
	urls := normalizeFetchURLs(args.URLs)
	if len(urls) == 0 {
		return exa.ContentsRequest{}, fmt.Errorf("web_fetch: at least one url is required")
	}
	if len(urls) > webFetchMaxURLs {
		return exa.ContentsRequest{}, fmt.Errorf("web_fetch: at most %d urls per call", webFetchMaxURLs)
	}

	mode := strings.TrimSpace(args.ContentMode)
	if mode == "" {
		mode = "text"
	}
	switch mode {
	case "text", "highlights", "both":
	default:
		return exa.ContentsRequest{}, fmt.Errorf("web_fetch: invalid content_mode %q (use text, highlights, both)", mode)
	}

	textCap := args.MaxTextCharacters
	if textCap <= 0 {
		textCap = webFetchDefaultTextCap
	}
	if textCap > webFetchMaxTextCap {
		textCap = webFetchMaxTextCap
	}

	req := exa.ContentsRequest{URLs: urls}
	hq := strings.TrimSpace(args.HighlightQuery)

	switch mode {
	case "text":
		req.Text = exa.TextOptions{MaxCharacters: textCap, Verbosity: "compact"}
	case "highlights":
		if hq != "" {
			req.Highlights = exa.HighlightsOptions{Query: hq}
		} else {
			req.Highlights = true
		}
	case "both":
		req.Text = exa.TextOptions{MaxCharacters: textCap, Verbosity: "compact"}
		if hq != "" {
			req.Highlights = exa.HighlightsOptions{Query: hq}
		} else {
			req.Highlights = true
		}
	}
	return req, nil
}

func normalizeFetchURLs(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		u := strings.TrimSpace(raw)
		if u == "" {
			continue
		}
		// Accept bare hosts by assuming https (agents sometimes omit scheme).
		if !strings.Contains(u, "://") {
			u = "https://" + u
		}
		parsed, err := url.Parse(u)
		if err != nil || parsed.Host == "" {
			continue
		}
		scheme := strings.ToLower(parsed.Scheme)
		if scheme != "http" && scheme != "https" {
			continue
		}
		// Prefer https in the normalized form when the agent omitted scheme.
		key := parsed.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func formatWebFetchResult(requested []string, resp *exa.ContentsResponse) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Web fetch results\nURLs requested: %d\n", len(requested))

	if resp != nil {
		for _, st := range resp.Statuses {
			if strings.EqualFold(st.Status, "error") {
				detail := st.Status
				if st.Error != nil && st.Error.Tag != "" {
					detail = st.Error.Tag
				}
				fmt.Fprintf(&b, "\n## Status: %s\n- Outcome: error (%s)\n", st.ID, detail)
			}
		}
	}

	if resp == nil || len(resp.Results) == 0 {
		b.WriteString("\nNo content returned. Check the URLs are publicly reachable, or search again for a better link.")
		return strings.TrimRight(b.String(), "\n")
	}

	fmt.Fprintf(&b, "Pages: %d\n", len(resp.Results))
	// Reuse the same per-result layout as search (title/url/highlights/text).
	b.WriteString(formatExaResults(resp.Results, 4000))
	return strings.TrimRight(b.String(), "\n")
}
