package session

import (
	"fmt"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Checkpointer builds and applies SessionCheckpoint blobs. It does not own
// live session data — SessionManager does. It does not own persistence I/O —
// stores.BaseStore does.
//
// Capture/Apply are pure over their inputs so tests can assert wire format
// without a real store.
type Checkpointer struct{}

// NewCheckpointer returns a Checkpointer.
func NewCheckpointer() Checkpointer {
	return Checkpointer{}
}

// Capture assembles a durable checkpoint from the message window, session
// manager (user state + plan + interrupts), and harness park maps.
func (Checkpointer) Capture(
	window []*streaming.Message,
	sm *SessionManager,
	pendingToolCalls map[string]stores.PendingToolCall,
) (*stores.SessionCheckpoint, error) {
	if sm == nil {
		return nil, fmt.Errorf("checkpointer: session manager is nil")
	}
	if err := streaming.ValidateMessages(window); err != nil {
		return nil, fmt.Errorf("checkpointer: invalid context window: %w", err)
	}
	userState, modules, pending, resolved, err := sm.snapshotCheckpoint()
	if err != nil {
		return nil, err
	}
	cp, err := stores.NewCheckpoint(window, pendingToolCalls, userState, modules, pending, resolved)
	if err != nil {
		return nil, err
	}
	return cp, nil
}

// AppliedCheckpoint is harness-side state restored from a store blob
// (everything not owned by SessionManager).
type AppliedCheckpoint struct {
	Window           []*streaming.Message
	PendingToolCalls map[string]stores.PendingToolCall
}

// Apply loads SessionManager state from the checkpoint and returns maps the
// harness must reattach (window, pending tools, interrupt routing).
func (Checkpointer) Apply(cp stores.SessionCheckpoint, sm *SessionManager) (AppliedCheckpoint, error) {
	if sm == nil {
		return AppliedCheckpoint{}, fmt.Errorf("checkpointer: session manager is nil")
	}
	if err := streaming.ValidateMessages(cp.ContextWindow); err != nil {
		return AppliedCheckpoint{}, fmt.Errorf("checkpointer: invalid context window: %w", err)
	}
	pendingInterrupts, resolvedInterrupts, err := decodeInterruptMaps(cp.PendingInterrupts(), cp.ResolvedInterrupts())
	if err != nil {
		return AppliedCheckpoint{}, err
	}
	if cp.Version() != stores.CheckpointVersion {
		return AppliedCheckpoint{}, fmt.Errorf("checkpointer: unsupported checkpoint version %d", cp.Version())
	}
	if err := sm.applyCheckpoint(cp.UserState(), cp.Modules()); err != nil {
		return AppliedCheckpoint{}, err
	}
	sm.replaceInterrupts(pendingInterrupts, resolvedInterrupts)
	ptc := cp.PendingToolCalls()
	if ptc == nil {
		ptc = make(map[string]stores.PendingToolCall)
	}
	return AppliedCheckpoint{
		Window:           cp.ContextWindow,
		PendingToolCalls: ptc,
	}, nil
}
