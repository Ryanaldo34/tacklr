package tacklr

import (
	"fmt"
)

// captureCheckpoint assembles a durable checkpoint from the message window,
// session manager (user state + plan + interrupts), and harness park maps.
// Persistence I/O is durable.SnapshotStore.
func captureCheckpoint(
	window []*Message,
	sm *sessionManager,
	pendingToolCalls map[string]PendingToolCall,
) (*SessionCheckpoint, error) {
	if sm == nil {
		return nil, fmt.Errorf("checkpointer: session manager is nil")
	}
	if err := ValidateMessages(window); err != nil {
		return nil, fmt.Errorf("checkpointer: invalid context window: %w", err)
	}
	userState, modules, pending, resolved, err := sm.snapshotCheckpoint()
	if err != nil {
		return nil, err
	}
	cp, err := NewCheckpoint(window, pendingToolCalls, userState, modules, pending, resolved)
	if err != nil {
		return nil, err
	}
	return cp, nil
}

// appliedCheckpoint is harness-side state restored from a store blob
// (everything not owned by sessionManager).
type appliedCheckpoint struct {
	Window           []*Message
	PendingToolCalls map[string]PendingToolCall
}

// applyCheckpoint loads sessionManager state from the checkpoint and returns
// maps the harness must reattach (window, pending tools, interrupt routing).
func applyCheckpoint(cp SessionCheckpoint, sm *sessionManager) (appliedCheckpoint, error) {
	if sm == nil {
		return appliedCheckpoint{}, fmt.Errorf("checkpointer: session manager is nil")
	}
	if err := ValidateMessages(cp.ContextWindow); err != nil {
		return appliedCheckpoint{}, fmt.Errorf("checkpointer: invalid context window: %w", err)
	}
	pendingInterrupts, resolvedInterrupts, err := decodeInterruptMaps(cp.PendingInterrupts(), cp.ResolvedInterrupts())
	if err != nil {
		return appliedCheckpoint{}, err
	}
	if cp.Version() != CheckpointVersion {
		return appliedCheckpoint{}, fmt.Errorf("checkpointer: unsupported checkpoint version %d", cp.Version())
	}
	if err := sm.applyCheckpoint(cp.UserState(), cp.Modules()); err != nil {
		return appliedCheckpoint{}, err
	}
	sm.replaceInterrupts(pendingInterrupts, resolvedInterrupts)
	ptc := cp.PendingToolCalls()
	if ptc == nil {
		ptc = make(map[string]PendingToolCall)
	}
	return appliedCheckpoint{
		Window:           cp.ContextWindow,
		PendingToolCalls: ptc,
	}, nil
}
