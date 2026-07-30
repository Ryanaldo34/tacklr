package inference

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr"
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

// ClassifyProviderFailure maps an HTTP status + body to a typed error.
// High-confidence refusal / max-token signals wrap tacklr stop-reason sentinels;
// everything else is *APIStatusError (unmapped → protocol internal error).
func ClassifyProviderFailure(status int, body []byte) error {
	msg := extractErrorMessage(body)
	code, errType := parseAPIErrorMeta(body)
	apiErr := &APIStatusError{Status: status, Body: msg, Code: code}
	lower := strings.ToLower(msg + " " + code + " " + errType)

	if isRefusalSignal(lower) {
		return tacklr.WrapStopReason(tacklr.ErrModelRefused, apiErr)
	}
	if isMaxTokensSignal(lower) {
		return tacklr.WrapStopReason(tacklr.ErrMaxTokens, apiErr)
	}
	return apiErr
}

// ClassifyIncompleteReason maps Responses API incomplete_details.reason (or
// similar) to a stop-reason sentinel when known.
func ClassifyIncompleteReason(reason string) error {
	r := strings.ToLower(strings.TrimSpace(reason))
	if r == "" {
		return nil
	}
	if isMaxTokensSignal(r) || r == "max_output_tokens" || r == "max_tokens" {
		return tacklr.WrapStopReason(tacklr.ErrMaxTokens, fmt.Errorf("incomplete: %s", reason))
	}
	if isRefusalSignal(r) || r == "content_filter" {
		return tacklr.WrapStopReason(tacklr.ErrModelRefused, fmt.Errorf("incomplete: %s", reason))
	}
	return fmt.Errorf("response incomplete: %s", reason)
}

func parseAPIErrorMeta(body []byte) (code, errType string) {
	var errResp struct {
		Error apiErrorDetail `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil {
		return errResp.Error.Code, errResp.Error.Type
	}
	return "", ""
}

func isRefusalSignal(lower string) bool {
	return strings.Contains(lower, "content_filter") ||
		strings.Contains(lower, "content filter") ||
		strings.Contains(lower, "refusal") ||
		strings.Contains(lower, "refused") ||
		errTypeIs(lower, "invalid_prompt") // some providers use this for blocked prompts
}

func isMaxTokensSignal(lower string) bool {
	return strings.Contains(lower, "max_output_tokens") ||
		strings.Contains(lower, "max_tokens") ||
		strings.Contains(lower, "maximum context length") ||
		strings.Contains(lower, "context_length_exceeded") ||
		strings.Contains(lower, "context length") ||
		strings.Contains(lower, "token limit") ||
		strings.Contains(lower, "finish_reason") && strings.Contains(lower, "length")
}

func errTypeIs(lower, want string) bool {
	return strings.Contains(lower, want)
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

// reasoningDetail is the Responses API reasoning config.
// Azure OpenAI / Foundry and OpenAI stream shareable thought text via
// response.reasoning_summary_text.delta when Summary is set (e.g. "auto").
type reasoningDetail struct {
	Effort  string `json:"effort,omitempty"`
	Summary string `json:"summary,omitempty"`
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
