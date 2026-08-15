package tacklr

import (
	"context"
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Shared roles, stream event types, errors, and re-exports for harness hosts.

const (
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleReasoning MessageRole = "reasoning"
	RoleSystem    MessageRole = "system"
	RoleDeveloper MessageRole = "developer"
	RoleTool      MessageRole = "tool"

	StatusInProgress ItemStatus = "in_progress"
	StatusCompleted  ItemStatus = "completed"
	StatusIncomplete ItemStatus = "incomplete"

	ContentTypeOutputText = "output_text"
	ContentTypeInputText  = "input_text"
	ContentTypeInputImage = "input_image"
	ContentTypeInputFile  = "input_file"
	ContentTypeRefusal    = "refusal"

	StreamEventMessage      StreamEventType = "message"
	StreamEventReasoning    StreamEventType = "reasoning"
	StreamEventFunctionCall StreamEventType = "function_call"
	StreamEventToolResult   StreamEventType = "tool_result"
	StreamEventComplete     StreamEventType = "complete"
	StreamEventError        StreamEventType = "error"
	StreamEventInterrupt    StreamEventType = "yield"
)

type (
	MessageRole      = streaming.MessageRole
	ItemStatus       = streaming.ItemStatus
	ContentPart      = streaming.ContentPart
	ImageURL         = streaming.ImageURL
	FileData         = streaming.FileData
	Annotation       = streaming.Annotation
	URLAnnotation    = streaming.URLAnnotation
	ToolCall         = streaming.ToolCall
	StreamEventType  = streaming.StreamEventType
	StreamEvent      = streaming.StreamEvent
	LLMResponseChunk = streaming.LLMResponseChunk
	Message          = streaming.Message
)

// Coarse categories for errors.Is. Wrap with a specific message at the
// call site (fmt.Errorf("…: %w", ErrNotFound)) instead of adding a new sentinel
// per situation.
var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid")
	ErrFailed   = errors.New("failed")
)

// classified keeps a stable Error() string while unwrapping to a category.
type classified struct {
	cat error
	msg string
}

func (e classified) Error() string { return e.msg }
func (e classified) Unwrap() error { return e.cat }

func classify(cat error, msg string) error { return classified{cat: cat, msg: msg} }

var (
	ErrModelRefused         = errors.New("model refused")
	ErrMaxTokens            = errors.New("max tokens reached")
	ErrMaxTurnRequests      = errors.New("max turn model requests exceeded")
	ErrApiKeyNotSet         = classify(ErrInvalid, "api key not set")
	ErrModelNotSet          = classify(ErrInvalid, "model not set")
	ErrUnknownModel         = classify(ErrNotFound, "unknown model")
	ErrToolNotFound         = classify(ErrNotFound, "tool not found")
	ErrToolTimeout          = classify(ErrFailed, "tool timed out")
	ErrToolPermissionDenied = classify(ErrFailed, "tool permission denied")
	// ErrModelAfterTools is a model failure after a successful tool batch.
	// Tools completed; the next model request failed.
	ErrModelAfterTools = classify(ErrFailed, "model request failed after tools completed")
)

// WrapStopReason attaches cause under a stop-reason sentinel for errors.Is.
// Returns kind when cause is nil.
func WrapStopReason(kind, cause error) error {
	if cause == nil {
		return kind
	}
	return fmt.Errorf("%w: %w", kind, cause)
}

// ProviderStatus supplies HTTP status and error code from a provider error.
// Optional on InferenceStrategy errors for model-span attributes.
type ProviderStatus interface {
	ProviderHTTPStatus() int
	ProviderErrorCode() string
}

// InferenceStrategy is the model provider interface used by the harness.
type InferenceStrategy interface {
	WithApiKey(string) InferenceStrategy
	WithModel(string) InferenceStrategy
	WithURL(string) InferenceStrategy
	WithReasoningLevel(string) InferenceStrategy
	WithStructuredOutput(any) InferenceStrategy
	SetSystemPrompt(string)
	Invoke(context.Context, []*Message, []*Tool) (chan LLMResponseChunk, error)
	CountTokens(context.Context, []*Message, []*Tool) (int, error)
	CompressContextWindow() error
	MaxContextWindow() (int, error)
	// SupportsMIME reports whether the currently selected model accepts the
	// given MIME type as user input. Empty and text/* are always true.
	// Probe representatives for ads (e.g. image/png); do not enumerate all types.
	SupportsMIME(mimeType string) bool
}

// UnsupportedMIMEs returns mimes for which s.SupportsMIME is false (first-seen order).
func UnsupportedMIMEs(s InferenceStrategy, mimes []string) []string {
	if s == nil || len(mimes) == 0 {
		return nil
	}
	var bad []string
	seen := make(map[string]struct{}, len(mimes))
	for _, m := range mimes {
		m = streaming.NormalizeMIME(m)
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		if !s.SupportsMIME(m) {
			bad = append(bad, m)
		}
	}
	return bad
}

// AgentWatchDog records optional turn telemetry (thinking, tools, tokens).
type AgentWatchDog interface {
	RecordThinking(*Message) error
	RecordOutput(*Message) error
	RecordError(error) error
	RecordTokens(int, int) error
	RecordToolCalls(*Message) error
	RecordToolResult(*Message) error
}

// HarnessRuntime is the tool-facing API for handlers and interceptors:
// EmitUpdate, StateGet, StateSet, StateDelete, RaiseInterrupt, Store,
// and CurrentToolCallID. Turn lifecycle helpers live in internal/session.
type HarnessRuntime = session.Runtime

// Todo is one plan list item (also used in plan_update stream payloads).
type Todo = streaming.Todo

// Interrupt types re-exported for tool authors.
type (
	Interrupt               = interrupt.Interrupt
	PayloadValidator        = interrupt.PayloadValidator
	UserChoice              = interrupt.UserChoice
	UserSelectionInterrupt  = interrupt.UserSelectionInterrupt
	ToolPermissionInterrupt = interrupt.ToolPermissionInterrupt
	PermissionOption        = interrupt.PermissionOption
)

var (
	ErrInterruptNotFound     = interrupt.ErrInterruptNotFound
	ErrInvalidPayload        = interrupt.ErrInvalidPayload
	DefaultPermissionOptions = interrupt.DefaultPermissionOptions
)

const (
	PermissionAllowOnce    = interrupt.PermissionAllowOnce
	PermissionAllowAlways  = interrupt.PermissionAllowAlways
	PermissionRejectOnce   = interrupt.PermissionRejectOnce
	PermissionRejectAlways = interrupt.PermissionRejectAlways
)

// RegisterInterrupt registers a custom interrupt factory for session rehydrate.
func RegisterInterrupt(factory func() Interrupt) {
	interrupt.Register(factory)
}
