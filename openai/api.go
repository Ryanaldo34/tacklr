package openai

import (
	"encoding/json"
	"fmt"
)

// APIStatusError conveys an HTTP error response from an upstream LLM API.
// Use errors.As to extract structured status/body details from a wrapped chain.
type APIStatusError struct {
	Status int
	Body   string
	Code   string
}

func (e *APIStatusError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("api error (status %d): %s (code: %s)", e.Status, e.Body, e.Code)
	}
	return fmt.Sprintf("api error (status %d): %s", e.Status, e.Body)
}

type apiErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Type    string `json:"type"`
}

type incompleteDetail struct {
	Reason string `json:"reason"`
}

// extractErrorMessage attempts to parse the OpenAI error payload from a non-200 response body.
func extractErrorMessage(body []byte) string {
	var errResp struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error.Message != "" {
		return errResp.Error.Message
	}
	return string(body)
}

type countTokensRequest struct {
	Model        string          `json:"model"`
	Input        json.RawMessage `json:"input"`
	Instructions *string         `json:"instructions,omitempty"`
	Tools        json.RawMessage `json:"tools,omitempty"`
	Text         *textFormat     `json:"text,omitempty"`
}

type responsesRequest struct {
	Model        string           `json:"model"`
	Input        json.RawMessage  `json:"input"`
	Instructions *string          `json:"instructions,omitempty"`
	Tools        json.RawMessage  `json:"tools,omitempty"`
	Stream       bool             `json:"stream,omitempty"`
	Reasoning    *reasoningDetail `json:"reasoning,omitempty"`
	Text         *textFormat      `json:"text,omitempty"`
}

type textFormat struct {
	Format *jsonSchemaFormat `json:"format,omitempty"`
}

type jsonSchemaFormat struct {
	Type   string         `json:"type"`
	Name   string         `json:"name"`
	Schema map[string]any `json:"schema"`
	Strict bool           `json:"strict"`
}

type reasoningDetail struct {
	Effort string `json:"effort,omitempty"`
}

type easyInputRequest struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type functionCallInputRequest struct {
	Type      string `json:"type"`
	ID        string `json:"id,omitempty"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type functionCallOutputRequest struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

var modelContextLimits = map[string]int{
	"gpt-5.5":          1000000,
	"gpt-5.4":          1000000,
	"gpt-5.4-mini":     400000,
	"gpt-5.4-nano":     400000,
	"o1-preview":       200000,
	"o1-pro":           200000,
	"o3":               200000,
	"o3-mini":          200000,
	"o3-pro":           200000,
	"o3-deep-research": 200000,
}
