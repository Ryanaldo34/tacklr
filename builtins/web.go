package builtins

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr"
)

// Web tools (Exa): web_search for discovery, web_fetch for known URLs.

const (
	webSearchDefaultResults = 8
	webSearchMaxResults     = 10
	webSearchDefaultTextCap = 4000
	webSearchMaxTextCap     = 10000
	webSearchToolTimeout    = 60 * time.Second
)

// webSearchArgs is the agent-facing surface for the Exa-backed web_search tool.
// Only query / include_domains / num_results are in the model schema. Exa
// type, category, dates, and content mode are harness-owned (docs: type=auto,
// highlights, includeDomains instead of site:).
type webSearchArgs struct {
	Query string `json:"query" desc:"Natural-language question or sentence naming the subject, place, and what you need. Do not use site: or keyword soup."`

	IncludeDomains []string `json:"include_domains,omitempty" desc:"Optional hostnames or path prefixes to restrict results (census.gov, learn.microsoft.com). Omit for the open web."`

	NumResults int `json:"num_results,omitempty" desc:"How many results to return (1-10). Default 8."`

	Type               string   `json:"type,omitempty" schema:"-"`
	Category           string   `json:"category,omitempty" schema:"-"`
	ExcludeDomains     []string `json:"exclude_domains,omitempty" schema:"-"`
	StartPublishedDate string   `json:"start_published_date,omitempty" schema:"-"`
	EndPublishedDate   string   `json:"end_published_date,omitempty" schema:"-"`
	MaxAgeHours        *int     `json:"max_age_hours,omitempty" schema:"-"`
	ContentMode        string   `json:"content_mode,omitempty" schema:"-"`
	MaxTextCharacters  int      `json:"max_text_characters,omitempty" schema:"-"`
	SystemPrompt       string   `json:"system_prompt,omitempty" schema:"-"`
	UserLocation       string   `json:"user_location,omitempty" schema:"-"`
}

// Tool description is the primary guidance surface (not the system prompt).
const webSearchToolDescription = `Search the live web and return compact result excerpts.

Pass a natural-language question. Optionally set include_domains to stay on specific hosts (census.gov, bls.gov). Do not use site: in the query.

Default: query only. For a known page URL, use web_fetch instead of searching again.`

func WebSearch(client *Exa) *tacklr.Tool {
	if client == nil {
		panic("builtins: web_search requires an Exa client")
	}
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "web_search",
		DisplayName: "Search: {query}",
		Description: webSearchToolDescription,
		Category:    tacklr.ToolCategorySearch,
		Access:      tacklr.ToolReadAccess,
		Timeout:     webSearchToolTimeout,
		Handler: func(ctx context.Context, args webSearchArgs, runtime tacklr.HarnessRuntime) (string, error) {
			return runWebSearch(ctx, client, args, runtime)
		},
	})
}

var siteOperator = regexp.MustCompile(`(?i)\bsite:([^\s]+)`)

type searchPrep struct {
	req   SearchRequest
	notes []string
}

func runWebSearch(ctx context.Context, client *Exa, args webSearchArgs, runtime tacklr.HarnessRuntime) (string, error) {
	prep, err := buildExaSearchRequest(args)
	if err != nil {
		return "", err
	}
	runtime.EmitUpdate("Searching the web…")
	resp, err := client.Search(ctx, prep.req)
	if retryExaConflict(err) {
		prep.notes = append(prep.notes, "Provider rejected those filters; retried on the open web.")
		prep.req.Category = ""
		prep.req.IncludeDomains = nil
		prep.req.ExcludeDomains = nil
		resp, err = client.Search(ctx, prep.req)
	}
	if err != nil {
		return "", mapExaErr("web_search", err)
	}
	if emptySearch(resp) && hasSearchFilters(prep.req) {
		prep.notes = append(prep.notes, "No hits with those filters; retried on the open web.")
		prep.req.Category = ""
		prep.req.IncludeDomains = nil
		prep.req.ExcludeDomains = nil
		retry, rerr := client.Search(ctx, prep.req)
		if rerr != nil {
			return "", mapExaErr("web_search", rerr)
		}
		resp = retry
	}
	return formatWebSearchResult(prep.req.Query, prep.req.Type, resp, prep.notes), nil
}

func retryExaConflict(err error) bool {
	var st *StatusError
	if errors.As(err, &st) {
		return st.PublicationDomain() || st.QueryFixable()
	}
	return false
}

func mapExaErr(name string, err error) error {
	if err == nil {
		return nil
	}
	var st *StatusError
	if errors.As(err, &st) {
		if st.PublicationDomain() {
			return tacklr.Correction(err, name+": category=publication cannot filter by domain. Omit category, or drop include_domains/exclude_domains, then search again")
		}
		if st.QueryFixable() {
			return tacklr.Correction(err, name+": the search provider rejected that request. Simplify filters (omit category or domain lists) and retry")
		}
		return fmt.Errorf("%s: the search provider failed: %w", name, errors.Join(tacklr.ErrFailed, err))
	}
	if errors.Is(err, ErrService) {
		return fmt.Errorf("%s: the search provider failed: %w", name, errors.Join(tacklr.ErrFailed, err))
	}
	return err
}

func emptySearch(resp *SearchResponse) bool {
	return resp == nil || len(resp.Results) == 0
}

func hasSearchFilters(req SearchRequest) bool {
	return req.Category != "" || len(req.IncludeDomains) > 0 || len(req.ExcludeDomains) > 0
}

func liftSiteOperators(q string) (query string, hosts []string) {
	for _, m := range siteOperator.FindAllStringSubmatch(q, -1) {
		host := strings.Trim(m[1], `"'`)
		host = strings.TrimPrefix(host, "https://")
		host = strings.TrimPrefix(host, "http://")
		host = strings.TrimSuffix(host, "/")
		if host != "" {
			hosts = append(hosts, host)
		}
	}
	query = strings.TrimSpace(siteOperator.ReplaceAllString(q, " "))
	query = strings.Join(strings.Fields(query), " ")
	if query == "" && len(hosts) > 0 {
		query = hosts[0]
	}
	return query, hosts
}

func buildExaSearchRequest(args webSearchArgs) (searchPrep, error) {
	query, sites := liftSiteOperators(strings.TrimSpace(args.Query))
	if query == "" {
		return searchPrep{}, fmt.Errorf("web_search: query is required. Pass a natural-language question or sentence")
	}

	searchType := strings.TrimSpace(args.Type)
	if searchType == "" {
		searchType = "auto"
	}
	switch searchType {
	case "auto", "fast", "instant", "deep-lite", "deep", "deep-reasoning":
	default:
		return searchPrep{}, fmt.Errorf("web_search: invalid type %q (use auto, fast, instant, deep-lite, deep, deep-reasoning)", searchType)
	}

	var notes []string
	include := append([]string{}, args.IncludeDomains...)
	exclude := append([]string{}, args.ExcludeDomains...)
	if len(sites) > 0 {
		include = append(include, sites...)
		notes = append(notes, "Moved site: from the query into include_domains.")
	}

	category := strings.TrimSpace(args.Category)
	if category != "" {
		switch category {
		case "company", "people", "news", "publication", "personal site", "financial report":
		default:
			return searchPrep{}, fmt.Errorf("web_search: invalid category %q (use company, people, news, publication, personal site, financial report — or omit category)", category)
		}
	}

	if category == "publication" && (len(include) > 0 || len(exclude) > 0) {
		notes = append(notes, "Dropped category=publication because it cannot filter by domain.")
		category = ""
	}
	start := strings.TrimSpace(args.StartPublishedDate)
	end := strings.TrimSpace(args.EndPublishedDate)
	if category == "company" || category == "people" {
		if len(exclude) > 0 {
			notes = append(notes, "Dropped exclude_domains (not valid with category "+category+").")
			exclude = nil
		}
		if start != "" || end != "" {
			notes = append(notes, "Dropped published-date filters (not valid with category "+category+").")
			start, end = "", ""
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
		return searchPrep{}, fmt.Errorf("web_search: invalid content_mode %q (use highlights, text, both)", mode)
	}

	textCap := args.MaxTextCharacters
	if textCap <= 0 {
		textCap = webSearchDefaultTextCap
	}
	if textCap > webSearchMaxTextCap {
		textCap = webSearchMaxTextCap
	}

	contents := &ContentsOptions{}
	switch mode {
	case "highlights":
		contents.Highlights = true
	case "text":
		contents.Text = TextOptions{MaxCharacters: textCap, Verbosity: "compact"}
	case "both":
		contents.Highlights = true
		contents.Text = TextOptions{MaxCharacters: textCap, Verbosity: "compact"}
	}
	if args.MaxAgeHours != nil {
		contents.MaxAgeHours = args.MaxAgeHours
	}

	return searchPrep{
		req: SearchRequest{
			Query:              query,
			Type:               searchType,
			NumResults:         n,
			Category:           category,
			IncludeDomains:     include,
			ExcludeDomains:     exclude,
			StartPublishedDate: start,
			EndPublishedDate:   end,
			UserLocation:       strings.TrimSpace(args.UserLocation),
			SystemPrompt:       strings.TrimSpace(args.SystemPrompt),
			Contents:           contents,
		},
		notes: notes,
	}, nil
}

func formatWebSearchResult(query, searchType string, resp *SearchResponse, notes []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Web search results\nQuery: %s\n", query)
	if len(notes) > 0 {
		fmt.Fprintf(&b, "Adjusted: %s\n", strings.Join(notes, " "))
	}
	if resp == nil || len(resp.Results) == 0 {
		b.WriteString("\nNo results found. Try a more specific natural-language query, or fetch a known URL with web_fetch.")
		return b.String()
	}
	if searchType == "" {
		searchType = "auto"
	}
	fmt.Fprintf(&b, "Mode: %s | Results: %d\n", searchType, len(resp.Results))
	b.WriteString(formatExaResults(resp.Results, 2000))

	if resp.Output != nil && len(resp.Output.Content) > 0 {
		b.WriteString("\n## Synthesized output\n")
		b.WriteString(formatSynthesisContent(resp.Output.Content))
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

// formatExaResults writes shared title/url/highlights/text blocks for search and fetch.
func formatExaResults(results []SearchResult, textCap int) string {
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
	URLs []string `json:"urls" desc:"One or more https page URLs to read (1-5). Use after web_search or when a citation already gave a URL."`

	HighlightQuery string `json:"highlight_query,omitempty" desc:"Optional focus phrase to extract from long pages."`

	ContentMode       string `json:"content_mode,omitempty" schema:"-"`
	MaxTextCharacters int    `json:"max_text_characters,omitempty" schema:"-"`
}

const webFetchToolDescription = `Read the body of known web page URLs.

Use when you already have URLs from web_search, the user, or a citation. Do not use for open-ended discovery — search first. Prefer 1–3 URLs per call.`

func WebFetch(client *Exa) *tacklr.Tool {
	if client == nil {
		panic("builtins: web_fetch requires an Exa client")
	}
	return tacklr.NewTool(tacklr.ToolConfig{
		Name:        "web_fetch",
		DisplayName: "Web Fetch",
		Description: webFetchToolDescription,
		Category:    tacklr.ToolCategoryFetch,
		Access:      tacklr.ToolReadAccess,
		Timeout:     webFetchToolTimeout,
		Handler: func(ctx context.Context, args webFetchArgs, runtime tacklr.HarnessRuntime) (string, error) {
			return runWebFetch(ctx, client, args, runtime)
		},
	})
}

func runWebFetch(ctx context.Context, client *Exa, args webFetchArgs, runtime tacklr.HarnessRuntime) (string, error) {
	req, err := buildExaContentsRequest(args)
	if err != nil {
		return "", err
	}
	runtime.EmitUpdate("Fetching page content…")
	resp, err := client.Contents(ctx, req)
	if err != nil {
		return "", mapExaErr("web_fetch", err)
	}
	return formatWebFetchResult(req.URLs, resp), nil
}

func buildExaContentsRequest(args webFetchArgs) (ContentsRequest, error) {
	urls := normalizeFetchURLs(args.URLs)
	if len(urls) == 0 {
		return ContentsRequest{}, fmt.Errorf("web_fetch: at least one http(s) url is required")
	}
	if len(urls) > webFetchMaxURLs {
		return ContentsRequest{}, fmt.Errorf("web_fetch: at most %d urls per call; fetch the most relevant pages first", webFetchMaxURLs)
	}

	mode := strings.TrimSpace(args.ContentMode)
	if mode == "" {
		mode = "text"
	}
	switch mode {
	case "text", "highlights", "both":
	default:
		return ContentsRequest{}, fmt.Errorf("web_fetch: invalid content_mode %q (use text, highlights, both)", mode)
	}

	textCap := args.MaxTextCharacters
	if textCap <= 0 {
		textCap = webFetchDefaultTextCap
	}
	if textCap > webFetchMaxTextCap {
		textCap = webFetchMaxTextCap
	}

	req := ContentsRequest{URLs: urls}
	hq := strings.TrimSpace(args.HighlightQuery)

	switch mode {
	case "text":
		req.Text = TextOptions{MaxCharacters: textCap, Verbosity: "compact"}
	case "highlights":
		if hq != "" {
			req.Highlights = HighlightsOptions{Query: hq}
		} else {
			req.Highlights = true
		}
	case "both":
		req.Text = TextOptions{MaxCharacters: textCap, Verbosity: "compact"}
		if hq != "" {
			req.Highlights = HighlightsOptions{Query: hq}
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

func formatWebFetchResult(requested []string, resp *ContentsResponse) string {
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
