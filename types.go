package tacklr

import (
	"context"
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/streaming"
)

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

var (
	ErrModelRefused         = errors.New("model refused")
	ErrMaxTokens            = errors.New("max tokens reached")
	ErrMaxTurnRequests      = errors.New("max turn model requests exceeded")
	ErrApiKeyNotSet         = errors.New("api key not set")
	ErrModelNotSet          = errors.New("model not set")
	ErrUnknownModel         = errors.New("unknown model")
	ErrToolNotFound         = errors.New("tool not found")
	ErrToolTimeout          = errors.New("tool timed out")
	ErrToolPermissionDenied = errors.New("tool permission denied")
)

// WrapStopReason wraps a cause under a terminal stop-reason sentinel so
// protocols can use errors.Is while preserving provider detail in the chain.
// If cause is nil, kind is returned as-is.
func WrapStopReason(kind, cause error) error {
	if kind == nil {
		return cause
	}
	if cause == nil {
		return kind
	}
	return fmt.Errorf("%w: %w", kind, cause)
}

// and test files can reference it without the control package prefix.
type Response struct {
	Status            ItemStatus
	Messages          []*Message
	IncompleteDetails string
}

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
}

type AgentWatchDog interface {
	RecordThinking(*Message) error
	RecordOutput(*Message) error
	RecordError(error) error
	RecordTokens(int, int) error
	RecordToolCalls(*Message) error
	RecordToolResult(*Message) error
}
