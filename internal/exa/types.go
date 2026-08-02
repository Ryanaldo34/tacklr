// Package exa is a minimal REST client for Exa Search (https://api.exa.ai).
// No third-party SDK — stdlib HTTP only.
package exa

import "encoding/json"

// SearchRequest is the subset of POST /search we use for agent tools.
// JSON names match Exa’s camelCase wire format.
type SearchRequest struct {
	Query              string           `json:"query"`
	Type               string           `json:"type,omitempty"`
	NumResults         int              `json:"numResults,omitempty"`
	Category           string           `json:"category,omitempty"`
	IncludeDomains     []string         `json:"includeDomains,omitempty"`
	ExcludeDomains     []string         `json:"excludeDomains,omitempty"`
	StartPublishedDate string           `json:"startPublishedDate,omitempty"`
	EndPublishedDate   string           `json:"endPublishedDate,omitempty"`
	UserLocation       string           `json:"userLocation,omitempty"`
	SystemPrompt       string           `json:"systemPrompt,omitempty"`
	Contents           *ContentsOptions `json:"contents,omitempty"`
}

// ContentsOptions controls extraction and freshness on each result.
type ContentsOptions struct {
	Highlights  any  `json:"highlights,omitempty"` // bool or HighlightsOptions
	Text        any  `json:"text,omitempty"`       // bool or TextOptions
	MaxAgeHours *int `json:"maxAgeHours,omitempty"`
}

// HighlightsOptions steers highlight extraction.
type HighlightsOptions struct {
	Query         string `json:"query,omitempty"`
	MaxCharacters int    `json:"maxCharacters,omitempty"`
}

// TextOptions steers full-page text extraction.
type TextOptions struct {
	MaxCharacters int    `json:"maxCharacters,omitempty"`
	Verbosity     string `json:"verbosity,omitempty"` // compact | standard | full
}

// SearchResponse is the JSON body for a successful non-streaming /search call.
type SearchResponse struct {
	RequestID string           `json:"requestId"`
	Results   []SearchResult   `json:"results"`
	Output    *SynthesisOutput `json:"output,omitempty"`
}

// SearchResult is one hit from Exa.
type SearchResult struct {
	Title         string   `json:"title"`
	URL           string   `json:"url"`
	PublishedDate string   `json:"publishedDate,omitempty"`
	Author        string   `json:"author,omitempty"`
	ID            string   `json:"id,omitempty"`
	Text          string   `json:"text,omitempty"`
	Highlights    []string `json:"highlights,omitempty"`
	Summary       string   `json:"summary,omitempty"`
}

// SynthesisOutput holds optional deep-search / schema synthesis.
// Content may be a string or a JSON object depending on outputSchema.
type SynthesisOutput struct {
	Content json.RawMessage `json:"content,omitempty"`
}

// ContentsRequest is POST /contents (known URLs or prior result ids).
// Provide urls or ids, not both. Content options are top-level (not nested).
type ContentsRequest struct {
	URLs        []string `json:"urls,omitempty"`
	IDs         []string `json:"ids,omitempty"`
	Text        any      `json:"text,omitempty"`       // bool or TextOptions
	Highlights  any      `json:"highlights,omitempty"` // bool or HighlightsOptions
	MaxAgeHours *int     `json:"maxAgeHours,omitempty"`
}

// ContentsResponse is the body for a successful POST /contents call.
type ContentsResponse struct {
	RequestID string              `json:"requestId"`
	Results   []SearchResult      `json:"results"`
	Statuses  []ContentsURLStatus `json:"statuses,omitempty"`
}

// ContentsURLStatus reports per-URL fetch outcome from /contents.
type ContentsURLStatus struct {
	ID     string `json:"id"`
	Status string `json:"status"` // success | error
	Source string `json:"source,omitempty"`
	Error  *struct {
		Tag            string `json:"tag,omitempty"`
		HTTPStatusCode *int   `json:"httpStatusCode,omitempty"`
	} `json:"error,omitempty"`
}
