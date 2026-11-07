package tacklr

import (
	"cmp"
	"errors"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"
)

type TodoStatus string

const (
	TodoStatusPending    TodoStatus = "pending"
	TodoStatusCompleted  TodoStatus = "completed"
	TodoStatusInProgress TodoStatus = "in_progress"
)

// Err is the chunk's failure: its Error, or for an error chunk without
// one, its Content; nil for any other chunk.
func (c LLMResponseChunk) Err() error {
	if c.Error != nil {
		return c.Error
	}
	if c.Type == session.EventError {
		return errors.New(cmp.Or(c.Content, "model stream failed"))
	}
	return nil
}

// LLMResponseChunk is the streaming unit emitted by a Model's
// Invoke call. Provider parse only — not client-facing wire.
type LLMResponseChunk struct {
	TurnId     string
	MessageId  string
	ToolCalls  []message.ToolCall
	Type       session.EventType
	Content    string
	IsComplete bool
	// Error is set on terminal provider failures (Type == session.EventError).
	// Harness copies it onto session.Event.Error so protocols can errors.Is
	// stop-reason sentinels (refusal, max_tokens, …).
	Error error

	// Token usage when the provider reports it (typically on session.EventComplete
	// after response.completed). Zero means unknown / not reported.
	InputTokens      int
	OutputTokens     int
	ReasoningTokens  int
	CachedTokens     int
	CacheWriteTokens int

	// EncryptedContent is the provider's opaque data for a reasoning item,
	// which only the provider client reads. It is copied onto the Message so
	// a later request can replay the item without provider-side storage.
	EncryptedContent string
}

// Todo is one item in an agent plan list (create_plan / plan_update stream data).
type Todo struct {
	Title       string     `json:"title" desc:"Todo title. Must be unique in the list."`
	Status      TodoStatus `json:"status" desc:"pending, in_progress, or completed."`
	Description string     `json:"description" desc:"Objective, expected outcomes, and acceptance criteria."`
}
