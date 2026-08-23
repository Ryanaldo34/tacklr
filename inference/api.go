package inference

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ryanaldo34/tacklr"
)

var _ tacklr.ProviderStatus = (*APIStatusError)(nil)

// APIStatusError conveys an HTTP error response from an upstream LLM API.
// Use errors.As to extract structured status/body details from a wrapped chain.
type APIStatusError struct {
	Status int
	Body   string
	Code   string
}

func (e *APIStatusError) Error() string {
	if e == nil {
		return "provider error"
	}
	msg := strings.TrimSpace(e.Body)
	if msg == "" {
		msg = "no error body"
	}
	if e.Code != "" {
		return fmt.Sprintf("provider HTTP %d (%s): %s", e.Status, e.Code, msg)
	}
	return fmt.Sprintf("provider HTTP %d: %s", e.Status, msg)
}

// ProviderHTTPStatus implements tacklr.ProviderStatus.
func (e *APIStatusError) ProviderHTTPStatus() int {
	if e == nil {
		return 0
	}
	return e.Status
}

// ProviderErrorCode implements tacklr.ProviderStatus.
func (e *APIStatusError) ProviderErrorCode() string {
	if e == nil {
		return ""
	}
	return e.Code
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

// classifyProviderFailure maps an HTTP status + body to a typed error.
// High-confidence refusal / max-token signals wrap tacklr stop-reason sentinels;
// everything else is *APIStatusError.
func classifyProviderFailure(status int, body []byte) error {
	msg := extractErrorMessage(body)
	code, errType := parseAPIErrorMeta(body)
	return classifyAPIStatus(&APIStatusError{Status: status, Body: msg, Code: code}, errType)
}

// classifyAPIStatus applies refusal/max-token signals to a structured provider error.
func classifyAPIStatus(apiErr *APIStatusError, errType string) error {
	if apiErr == nil {
		return nil
	}
	lower := strings.ToLower(apiErr.Body + " " + apiErr.Code + " " + errType)
	if isRefusalSignal(lower) {
		return fmt.Errorf("%w: %w", tacklr.ErrModelRefused, apiErr)
	}
	if isMaxTokensSignal(lower) {
		return fmt.Errorf("%w: %w", tacklr.ErrMaxTokens, apiErr)
	}
	return apiErr
}

// classifyIncompleteReason maps Responses API incomplete_details.reason (or
// similar) to a stop-reason sentinel when known.
func classifyIncompleteReason(reason string) error {
	r := strings.ToLower(strings.TrimSpace(reason))
	if r == "" {
		return nil
	}
	if isMaxTokensSignal(r) || r == "max_output_tokens" || r == "max_tokens" {
		return fmt.Errorf("%w: response incomplete (%s)", tacklr.ErrMaxTokens, reason)
	}
	if isRefusalSignal(r) || r == "content_filter" {
		return fmt.Errorf("%w: response incomplete (%s)", tacklr.ErrModelRefused, reason)
	}
	return fmt.Errorf("response incomplete (%s)", reason)
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
		strings.Contains(lower, "invalid_prompt")
}

func isMaxTokensSignal(lower string) bool {
	return strings.Contains(lower, "max_output_tokens") ||
		strings.Contains(lower, "max_tokens") ||
		strings.Contains(lower, "maximum context length") ||
		strings.Contains(lower, "context_length_exceeded") ||
		strings.Contains(lower, "context length") ||
		strings.Contains(lower, "token limit") ||
		(strings.Contains(lower, "finish_reason") && strings.Contains(lower, "length"))
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
	// Include asks the provider for extra output fields. reasoning.encrypted_content
	// is required to replay reasoning items statelessly (OpenAI ZDR / Azure store=false).
	Include []string `json:"include,omitempty"`
	// MaxOutputTokens caps completion size (reasoning + visible text). Omit when 0.
	// Azure often ends streams as response.incomplete with empty details when this is too low.
	MaxOutputTokens int `json:"max_output_tokens,omitempty"`
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

// easyInputRequest is a plain string content message (text-only turns).
type easyInputRequest struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// multiInputRequest is a user message with typed content parts (vision / files).
type multiInputRequest struct {
	Role    string `json:"role"`
	Content []any  `json:"content"`
}

// Responses API content parts (array form of message content).
type inputTextPart struct {
	Type string `json:"type"` // input_text
	Text string `json:"text"`
}

// inputImagePart: image_url is a string data/https URL (not a nested object).
type inputImagePart struct {
	Type     string `json:"type"` // input_image
	ImageURL string `json:"image_url"`
	Detail   string `json:"detail,omitempty"`
}

type inputFilePart struct {
	Type     string `json:"type"` // input_file
	Filename string `json:"filename,omitempty"`
	FileData string `json:"file_data,omitempty"`
	FileID   string `json:"file_id,omitempty"`
}

type functionCallInputRequest struct {
	Type string `json:"type"`
	// Pairing uses call_id. Provider item ids (fc_…) are output identifiers and
	// are not re-submitted as input (Responses multi-turn tool history).
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	// Lifecycle status for completed tool turns in multi-turn input.
	Status string `json:"status"`
}

type functionCallOutputRequest struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
	Status string `json:"status"`
}

// reasoningInputRequest is a Responses API reasoning item for multi-turn history.
// OpenAI / Azure Foundry require `summary` on input reasoning items (even when
// empty). Omitting it yields missing_required_parameter on models like GPT Luna.
// Do not send output-only fields such as status — unknown_parameter on input.
//
// `id` is a store lookup unless encrypted_content is present. Stateless clients
// (Azure default store=false, OpenAI ZDR) must send encrypted_content with the
// original item id; an id alone is "Item with id 'rs_…' not found".
type reasoningInputRequest struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
	// EncryptedContent is the provider ciphertext from include=reasoning.encrypted_content.
	EncryptedContent string `json:"encrypted_content,omitempty"`
	// Summary is required on the wire; never omit (use empty slice, not null).
	Summary []reasoningSummaryPart `json:"summary"`
}

type reasoningSummaryPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
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
