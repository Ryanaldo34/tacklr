package tacklr

import (
	"encoding/json"
	"fmt"
)

// PendingToolCall is a parked or in-flight tool call in a checkpoint.
type PendingToolCall struct {
	ToolCall        *ToolCall `json:"toolCall,omitempty"`
	InterruptActive bool      `json:"interruptActive,omitempty"`
}

// sessionState is harness-owned durable agent state (not wire-protocol envelopes).
type sessionState struct {
	// Version identifies the typed module schema.
	Version   int                        `json:"version,omitempty"`
	UserState map[string]json.RawMessage `json:"userState,omitempty"`
	Modules   map[string]json.RawMessage `json:"modules,omitempty"`

	PendingToolCalls   map[string]PendingToolCall `json:"pendingToolCalls"`
	PendingInterrupts  []byte                     `json:"pendingInterrupts,omitempty"`
	ResolvedInterrupts []byte                     `json:"resolvedInterrupts,omitempty"`
}

// CheckpointVersion is the current typed session checkpoint schema.
const CheckpointVersion = 2

// SessionCheckpoint is the agent harness checkpoint blob.
// Wire protocols must not store protocol envelopes here — use a
// ProtocolWireStore (or equivalent) owned by the protocol.
// Harness-owned module/interrupt bytes are opaque to store implementations.
type SessionCheckpoint struct {
	ContextWindow []*Message `json:"contextWindow"`
	state         sessionState
}

type checkpointJSON struct {
	ContextWindow []*Message   `json:"contextWindow"`
	State         sessionState `json:"state"`
}

func (c SessionCheckpoint) MarshalJSON() ([]byte, error) {
	return json.Marshal(checkpointJSON{ContextWindow: c.ContextWindow, State: c.state})
}

func (c *SessionCheckpoint) UnmarshalJSON(data []byte) error {
	var raw checkpointJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	c.ContextWindow = raw.ContextWindow
	c.state = raw.State
	return nil
}

func (c SessionCheckpoint) Version() int { return c.state.Version }
func (c SessionCheckpoint) UserState() map[string]json.RawMessage {
	return c.state.UserState
}
func (c SessionCheckpoint) Modules() map[string]json.RawMessage { return c.state.Modules }
func (c SessionCheckpoint) PendingToolCalls() map[string]PendingToolCall {
	return c.state.PendingToolCalls
}
func (c SessionCheckpoint) PendingInterrupts() []byte  { return c.state.PendingInterrupts }
func (c SessionCheckpoint) ResolvedInterrupts() []byte { return c.state.ResolvedInterrupts }

// WithVersion returns a copy with the schema version set. Tests use this to
// exercise apply reject paths.
func (c SessionCheckpoint) WithVersion(v int) SessionCheckpoint {
	c.state.Version = v
	return c
}

// WithModule returns a copy with one harness module blob replaced.
func (c SessionCheckpoint) WithModule(name string, raw json.RawMessage) SessionCheckpoint {
	mods := cloneRawMap(c.state.Modules)
	if mods == nil {
		mods = map[string]json.RawMessage{}
	}
	mods[name] = raw
	c.state.Modules = mods
	return c
}

// WithUserStateKey returns a copy with one user-state blob replaced.
func (c SessionCheckpoint) WithUserStateKey(key string, raw json.RawMessage) SessionCheckpoint {
	us := cloneRawMap(c.state.UserState)
	if us == nil {
		us = map[string]json.RawMessage{}
	}
	us[key] = raw
	c.state.UserState = us
	return c
}

// NewCheckpoint builds the current checkpoint schema. modules contain
// framework-owned typed JSON; userState contains host-owned arbitrary JSON.
func NewCheckpoint(
	contextWindow []*Message,
	pendingToolCalls map[string]PendingToolCall,
	userState, modules map[string]json.RawMessage,
	pendingInterrupts, resolvedInterrupts any,
) (*SessionCheckpoint, error) {
	if err := ValidateMessages(contextWindow); err != nil {
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
		state: sessionState{
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
