package stores

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ryanaldo34/tacklr/streaming"
)

type PendingToolCall struct {
	ToolCall        *streaming.ToolCall
	InterruptActive bool
}

// sessionState is harness-owned durable agent state (not wire-protocol envelopes).
type sessionState struct {
	// Version 2 stores framework modules as typed JSON sections and isolates
	// arbitrary host state. Zero/one denotes the legacy RuntimeState shape.
	Version   int                        `json:"version,omitempty"`
	UserState map[string]json.RawMessage `json:"userState,omitempty"`
	Modules   map[string]json.RawMessage `json:"modules,omitempty"`

	PendingToolCalls map[string]PendingToolCall `json:"pendingToolCalls"`
	// InterruptToRequester is read-only legacy (old checkpoints keyed wire
	// interrupt ids separately from tool call ids). New saves omit it.
	InterruptToRequester map[string]string `json:"interruptToRequester,omitempty"`
	RuntimeState         map[string]any    `json:"runtimeState,omitempty"`
	PendingInterrupts    []byte            `json:"pendingInterrupts,omitempty"`
	ResolvedInterrupts   []byte            `json:"resolvedInterrupts,omitempty"`
	// SearchContext is an opaque brain.SearchContext export (JSON bytes).
	// Owned by the harness, not SessionManager.
	SearchContext []byte `json:"searchContext,omitempty"`
}

// CheckpointVersion is the current typed session checkpoint schema.
const CheckpointVersion = 2

// SessionCheckpoint is the agent harness checkpoint blob.
// Wire protocols (ACP, …) must not store protocol envelopes here — use a
// ProtocolWireStore (or equivalent) owned by the protocol.
type SessionCheckpoint struct {
	ContextWindow []*streaming.Message `json:"contextWindow"`
	State         sessionState         `json:"state"`
}

func NewCheckpoint(contextWindow []*streaming.Message, pendingToolCalls map[string]PendingToolCall, runtimeState map[string]any, pendingInterrupts, resolvedInterrupts any) (*SessionCheckpoint, error) {
	if err := streaming.ValidateMessages(contextWindow); err != nil {
		return nil, fmt.Errorf("invalid context window: %w", err)
	}
	var pendingJSON, resolvedJSON []byte
	var err error

	if pendingInterrupts != nil {
		pendingJSON, err = json.Marshal(pendingInterrupts)
		if err != nil {
			return nil, fmt.Errorf("marshal pending interrupts: %w", err)
		}
	}
	if resolvedInterrupts != nil {
		resolvedJSON, err = json.Marshal(resolvedInterrupts)
		if err != nil {
			return nil, fmt.Errorf("marshal resolved interrupts: %w", err)
		}
	}

	return &SessionCheckpoint{
		ContextWindow: contextWindow,
		State: sessionState{
			PendingToolCalls:   pendingToolCalls,
			RuntimeState:       runtimeState,
			PendingInterrupts:  pendingJSON,
			ResolvedInterrupts: resolvedJSON,
		},
	}, nil
}

// NewTypedCheckpoint builds the current checkpoint schema. modules contain
// framework-owned typed JSON; userState contains host-owned arbitrary JSON.
func NewTypedCheckpoint(
	contextWindow []*streaming.Message,
	pendingToolCalls map[string]PendingToolCall,
	userState, modules map[string]json.RawMessage,
	pendingInterrupts, resolvedInterrupts any,
) (*SessionCheckpoint, error) {
	if err := streaming.ValidateMessages(contextWindow); err != nil {
		return nil, fmt.Errorf("invalid context window: %w", err)
	}
	pendingJSON, err := marshalOptional(pendingInterrupts)
	if err != nil {
		return nil, fmt.Errorf("marshal pending interrupts: %w", err)
	}
	resolvedJSON, err := marshalOptional(resolvedInterrupts)
	if err != nil {
		return nil, fmt.Errorf("marshal resolved interrupts: %w", err)
	}
	return &SessionCheckpoint{
		ContextWindow: contextWindow,
		State: sessionState{
			Version:            CheckpointVersion,
			UserState:          cloneRawMap(userState),
			Modules:            cloneRawMap(modules),
			PendingToolCalls:   pendingToolCalls,
			PendingInterrupts:  pendingJSON,
			ResolvedInterrupts: resolvedJSON,
		},
	}, nil
}

func marshalOptional(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return json.Marshal(value)
}

func cloneRawMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		out[key] = append(json.RawMessage(nil), value...)
	}
	return out
}

// BaseStore is the minimal persistence interface for agent harness session state.
//
// Implementations included in this package:
//   - InMemoryStore — sessions live in memory, lost on restart.
//   - PostgresStore — sessions persisted via PostgreSQL/pgx.
//
// Users may provide their own implementation (e.g. Redis, SQLite, custom DB).
// Wire-protocol session envelopes are not part of this API.
type BaseStore interface {
	SaveSession(context.Context, string, SessionCheckpoint) error
	LoadSession(context.Context, string) (SessionCheckpoint, error)
}

// ErrSessionNotFound is returned by LoadSession when the requested session
// does not exist.
var ErrSessionNotFound = errors.New("session not found")
