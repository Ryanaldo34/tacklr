package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/internal/exa"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Web tools (Exa): web_search for discovery, web_fetch for known URLs.

const (
	envExaAPIKey            = "EXA_API_KEY"
	webSearchDefaultResults = 8
	webSearchMaxResults     = 10
	webSearchDefaultTextCap = 4000
	webSearchMaxTextCap     = 10000
	webSearchToolTimeout    = 60 * time.Second
)

// webSearchArgs is the agent-facing surface for the Exa-backed web_search tool.
// Descriptions here are what the model sees in the tool schema — keep them
// general guidance, not host allowlists.
type webSearchArgs struct {
	Query string `json:"query" desc:"Natural-language search query. Write a full sentence or question with the entities, place, and constraints you care about (not keyword soup). Put site constraints in include_domains, not as site: in the query."`

	Type string `json:"type,omitempty" enum:"auto,fast,instant,deep-lite,deep,deep-reasoning" desc:"Search mode. Prefer auto. fast/instant = lower latency. deep-lite/deep/deep-reasoning = multi-step research (slower, richer)."`

	NumResults int `json:"num_results,omitempty" desc:"How many results to return (1-10). Default 8. Prefer a sharper query over maxing this out."`

	Category string `json:"category,omitempty" enum:"company,people,news,publication,personal site,financial report" desc:"Optional specialized index — omit for general web search (most tasks). company=company profiles; people=person profiles; news=news articles; publication=scholarly papers/preprints/journals only (not general web or government pages); personal site=personal sites/blogs; financial report=filings and financial reports. company and people do not support exclude_domains or published-date filters. If category and include_domains conflict, omit category and search the open web."`

	IncludeDomains []string `json:"include_domains,omitempty" desc:"Only these hostnames, path prefixes (exa.ai/blog), or wildcards (*.substack.com). Use instead of site: in the query. Works best without category, or with a category whose index actually includes those hosts."`
	ExcludeDomains []string `json:"exclude_domains,omitempty" desc:"Drop these domains/paths. Not valid with category company or people."`

	StartPublishedDate string `json:"start_published_date,omitempty" desc:"ISO-8601 lower bound on published date (e.g. 2024-01-01T00:00:00Z). Not valid with company/people."`
	EndPublishedDate   string `json:"end_published_date,omitempty" desc:"ISO-8601 upper bound on published date."`

	MaxAgeHours *int `json:"max_age_hours,omitempty" desc:"Content freshness in hours. Omit for Exa default. 0 = always livecrawl; -1 = cache only; positive = use cache if younger than N hours."`

	ContentMode string `json:"content_mode,omitempty" enum:"highlights,text,both" desc:"What to extract per result. Prefer highlights (default) for multi-step agents. text = fuller page text. both = highlights plus capped text."`

	MaxTextCharacters int `json:"max_text_characters,omitempty" desc:"When content_mode is text or both, cap full text characters per result (default 4000, max 10000)."`

	SystemPrompt string `json:"system_prompt,omitempty" desc:"Optional guidance for synthesis/planning (e.g. prefer primary sources). Most useful with deep* types; leave empty otherwise."`

	UserLocation string `json:"user_location,omitempty" desc:"Two-letter ISO country code for geo-biased results (e.g. US)."`
}

// resolveExaAPIKey returns the API key from options (if set) or EXA_API_KEY env.
func resolveExaAPIKey(optsKey string) string {
	if k := strings.TrimSpace(optsKey); k != "" {
		return k
	}
	return strings.TrimSpace(os.Getenv(envExaAPIKey))
}

// Tool description is the primary guidance surface (not the system prompt).
const webSearchToolDescription = `Search the live web (Exa) and return compact results for research.

Prefer natural-language queries: full sentences or questions that name the subject, place, and what you need — not bare keywords or site: operators.

Default (most tasks): set query only; leave category unset; content_mode highlights. Use include_domains only when you must stay on specific hosts. For a full page body once you have a URL, use web_fetch instead of re-searching.

category selects a specialized index, not a topic tag — omit it unless you specifically need that index:
- company / people — profiles (no exclude_domains or published-date filters)
- news — news articles
- publication — scholarly papers and journals only, not general or government web pages
- personal site — personal sites/blogs
- financial report — filings and financial reports
If a call fails because filters conflict, retry with a clearer query and omit category (and/or domain filters) rather than stacking more constraints.`

func newWebSearchTool(client *exa.Client) *Tool {
	return NewTool(ToolConfig{
		Name:        "web_search",
		DisplayName: "Web Search",
		Description: webSearchToolDescription,
		Category:    streaming.ToolCategorySearch,
		Access:      ToolReadAccess,
		Timeout:     webSearchToolTimeout,
		Handler: func(ctx context.Context, args webSearchArgs, runtime HarnessRuntime) (string, error) {
			return runWebSearch(ctx, client, args, runtime)
		},
	})
}

func runWebSearch(ctx context.Context, client *exa.Client, args webSearchArgs, runtime HarnessRuntime) (string, error) {
	if client == nil {
		return "", fmt.Errorf("web_search: Exa client is not configured")
	}
	req, err := buildExaSearchRequest(args)
	if err != nil {
		return "", err
	}
	runtime.EmitUpdate("Searching the web…")
	resp, err := client.Search(ctx, req)
	if err != nil {
		return "", err
	}
	return formatWebSearchResult(req.Query, req.Type, resp), nil
}

func buildExaSearchRequest(args webSearchArgs) (exa.SearchRequest, error) {
	query := strings.TrimSpace(args.Query)
	if query == "" {
		return exa.SearchRequest{}, fmt.Errorf("web_search: query is required")
	}

	searchType := strings.TrimSpace(args.Type)
	if searchType == "" {
		searchType = "auto"
	}
	switch searchType {
	case "auto", "fast", "instant", "deep-lite", "deep", "deep-reasoning":
	default:
		return exa.SearchRequest{}, fmt.Errorf("web_search: invalid type %q (use auto, fast, instant, deep-lite, deep, deep-reasoning)", searchType)
	}

	category := strings.TrimSpace(args.Category)
	if category != "" {
		switch category {
		case "company", "people", "news", "publication", "personal site", "financial report":
		default:
			return exa.SearchRequest{}, fmt.Errorf("web_search: invalid category %q", category)
		}
	}

	// Exa rejects exclude_domains / published dates for company and people.
	if category == "company" || category == "people" {
		if len(args.ExcludeDomains) > 0 {
			return exa.SearchRequest{}, fmt.Errorf("web_search: exclude_domains is not supported with category %q", category)
		}
		if strings.TrimSpace(args.StartPublishedDate) != "" || strings.TrimSpace(args.EndPublishedDate) != "" {
			return exa.SearchRequest{}, fmt.Errorf("web_search: published date filters are not supported with category %q", category)
		}
	}

	n := args.NumResults
	if n <= 0 {
		n = webSearchDefaultResults
	}
	if n > webSearchMaxResults {
		n = webSearchMaxResults
	}

	mode := strings.TrimSpace(args.ContentMode)
	if mode == "" {
		mode = "highlights"
	}
	switch mode {
	case "highlights", "text", "both":
	default:
		return exa.SearchRequest{}, fmt.Errorf("web_search: invalid content_mode %q (use highlights, text, both)", mode)
	}

	textCap := args.MaxTextCharacters
	if textCap <= 0 {
		textCap = webSearchDefaultTextCap
	}
	if textCap > webSearchMaxTextCap {
		textCap = webSearchMaxTextCap
	}

	contents := &exa.ContentsOptions{}
	switch mode {
	case "highlights":
		contents.Highlights = true
	case "text":
		contents.Text = exa.TextOptions{MaxCharacters: textCap, Verbosity: "compact"}
	case "both":
		contents.Highlights = true
		contents.Text = exa.TextOptions{MaxCharacters: textCap, Verbosity: "compact"}
	}
	if args.MaxAgeHours != nil {
		contents.MaxAgeHours = args.MaxAgeHours
	}

	return exa.SearchRequest{
		Query:              query,
		Type:               searchType,
		NumResults:         n,
		Category:           category,
		IncludeDomains:     args.IncludeDomains,
		ExcludeDomains:     args.ExcludeDomains,
		StartPublishedDate: strings.TrimSpace(args.StartPublishedDate),
		EndPublishedDate:   strings.TrimSpace(args.EndPublishedDate),
		UserLocation:       strings.TrimSpace(args.UserLocation),
		SystemPrompt:       strings.TrimSpace(args.SystemPrompt),
		Contents:           contents,
	}, nil
}

func formatWebSearchResult(query, searchType string, resp *exa.SearchResponse) string {
	if resp == nil || len(resp.Results) == 0 {
		return fmt.Sprintf("# Web search results\nQuery: %s\n\nNo results found. Try a more specific natural-language query, different domains, or another category.", query)
	}
	if searchType == "" {
		searchType = "auto"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "# Web search results\nQuery: %s\nMode: %s | Results: %d\n", query, searchType, len(resp.Results))
	b.WriteString(formatExaResults(resp.Results, 2000))

	if resp.Output != nil && len(resp.Output.Content) > 0 {
		b.WriteString("\n## Synthesized output\n")
		b.WriteString(formatSynthesisContent(resp.Output.Content))
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

// formatExaResults writes shared title/url/highlights/text blocks for search and fetch.
func formatExaResults(results []exa.SearchResult, textCap int) string {
	if textCap <= 0 {
		textCap = 2000
	}
	var b strings.Builder
	for i, r := range results {
		title := strings.TrimSpace(r.Title)
		if title == "" {
			title = "(untitled)"
		}
		fmt.Fprintf(&b, "\n## %d. %s\n", i+1, title)
		if r.URL != "" {
			fmt.Fprintf(&b, "- URL: %s\n", r.URL)
		}
		meta := make([]string, 0, 2)
		if r.PublishedDate != "" {
			meta = append(meta, "Published: "+r.PublishedDate)
		}
		if r.Author != "" {
			meta = append(meta, "Author: "+r.Author)
		}
		if len(meta) > 0 {
			fmt.Fprintf(&b, "- %s\n", strings.Join(meta, " | "))
		}
		if len(r.Highlights) > 0 {
			b.WriteString("- Highlights:\n")
			for _, h := range r.Highlights {
				h = strings.TrimSpace(h)
				if h == "" {
					continue
				}
				fmt.Fprintf(&b, "  - %s\n", truncateRunes(h, 800))
			}
		}
		if s := strings.TrimSpace(r.Summary); s != "" {
			fmt.Fprintf(&b, "- Summary: %s\n", truncateRunes(s, 1200))
		}
		if t := strings.TrimSpace(r.Text); t != "" {
			fmt.Fprintf(&b, "- Text: %s\n", truncateRunes(t, textCap))
		}
	}
	return b.String()
}

func formatSynthesisContent(raw json.RawMessage) string {
	// Prefer pretty JSON object/array; otherwise unquote string.
	var asAny any
	if err := json.Unmarshal(raw, &asAny); err == nil {
		switch v := asAny.(type) {
		case string:
			return truncateRunes(v, 4000)
		default:
			pretty, err := json.MarshalIndent(v, "", "  ")
			if err == nil {
				return truncateRunes(string(pretty), 4000)
			}
		}
	}
	return truncateRunes(string(raw), 4000)
}

func truncateRunes(s string, maxLen int) string {
	if maxLen <= 0 || s == "" {
		return s
	}
	r := []rune(s)
	if len(r) <= maxLen {
		return s
	}
	return string(r[:maxLen]) + "…"
}

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
