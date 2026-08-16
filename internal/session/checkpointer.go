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
	interruptToRequester map[string]string,
) (*stores.SessionCheckpoint, error) {
	if sm == nil {
		return nil, fmt.Errorf("checkpointer: session manager is nil")
	}
	runtimeState, pending, resolved := sm.SnapshotDurable()
	cp, err := stores.NewCheckpoint(window, pendingToolCalls, interruptToRequester, runtimeState, pending, resolved)
	if err != nil {
		return nil, err
	}
	if sm.Search != nil {
		raw, err := sm.Search.Export()
		if err != nil {
			return nil, fmt.Errorf("checkpointer: export search context: %w", err)
		}
		cp.State.SearchContext = raw
	}
	return cp, nil
}

// AppliedCheckpoint is harness-side state restored from a store blob
// (everything not owned by SessionManager).
type AppliedCheckpoint struct {
	Window               []*streaming.Message
	PendingToolCalls     map[string]stores.PendingToolCall
	InterruptToRequester map[string]string
}

// Apply loads SessionManager state from the checkpoint and returns maps the
// harness must reattach (window, pending tools, interrupt routing).
func (Checkpointer) Apply(cp stores.SessionCheckpoint, sm *SessionManager) (AppliedCheckpoint, error) {
	if sm == nil {
		return AppliedCheckpoint{}, fmt.Errorf("checkpointer: session manager is nil")
	}
	sm.LoadUserAndPlanState(cp.State.RuntimeState)
	if len(cp.State.SearchContext) > 0 {
		if err := sm.Search.Restore(cp.State.SearchContext); err != nil {
			return AppliedCheckpoint{}, fmt.Errorf("checkpointer: restore search context: %w", err)
		}
	}
	if err := sm.LoadInterruptsJSON(cp.State.PendingInterrupts, cp.State.ResolvedInterrupts); err != nil {
		return AppliedCheckpoint{}, err
	}
	ptc := cp.State.PendingToolCalls
	if ptc == nil {
		ptc = make(map[string]stores.PendingToolCall)
	}
	itr := cp.State.InterruptToRequester
	if itr == nil {
		itr = make(map[string]string)
	}
	return AppliedCheckpoint{
		Window:               cp.ContextWindow,
		PendingToolCalls:     ptc,
		InterruptToRequester: itr,
	}, nil
}
