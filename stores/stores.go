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

type sessionState struct {
	PendingToolCalls     map[string]PendingToolCall `json:"pendingToolCalls"`
	InterruptToRequester map[string]string          `json:"interruptToRequester"`
	RuntimeState         map[string]any             `json:"runtimeState"`
	PendingInterrupts    []byte                     `json:"pendingInterrupts,omitempty"`
	ResolvedInterrupts   []byte                     `json:"resolvedInterrupts,omitempty"`
	// SearchContext is an opaque brain.SearchContext export (JSON bytes).
	// Owned by the harness, not SessionManager.
	SearchContext []byte `json:"searchContext,omitempty"`
}

type SessionCheckpoint struct {
	ContextWindow []*streaming.Message `json:"contextWindow"`
	State         sessionState         `json:"state"`
}

func NewCheckpoint(contextWindow []*streaming.Message, pendingToolCalls map[string]PendingToolCall, interruptToRequester map[string]string, runtimeState map[string]any, pendingInterrupts, resolvedInterrupts any) (*SessionCheckpoint, error) {
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
			PendingToolCalls:     pendingToolCalls,
			InterruptToRequester: interruptToRequester,
			RuntimeState:         runtimeState,
			PendingInterrupts:    pendingJSON,
			ResolvedInterrupts:   resolvedJSON,
		},
	}, nil
}

// BaseStore is the minimal persistence interface for agent session state.
//
// Implementations included in this package:
//   - InMemoryStore — sessions live in memory, lost on restart.
//   - PostgresStore — sessions persisted via PostgreSQL/pgx.
//
// Users may provide their own implementation (e.g. Redis, SQLite, custom DB).
type BaseStore interface {
	SaveSession(context.Context, string, SessionCheckpoint) error
	LoadSession(context.Context, string) (SessionCheckpoint, error)
}

// ErrSessionNotFound is returned by LoadSession when the requested session
// does not exist.
var ErrSessionNotFound = errors.New("session not found")
