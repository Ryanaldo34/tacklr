package tacklr

import (
	"fmt"
	"slices"
	"sync"

	"github.com/ryanaldo34/tacklr/control"
	"github.com/ryanaldo34/tacklr/streaming"
)

// ContextPolicy controls when and how conversation windows are reshaped under pressure.
// Used by ModelTasks.Absorb, not by ContextManager itself.
// Small value type — pass by value.
type ContextPolicy struct {
	// PressureRatio is the fraction of MaxSize at which Absorb starts collapsing
	// (e.g. 0.85 means compress when estimated tokens exceed 85% of max).
	PressureRatio float64
	// CompressFraction is the heuristic used when estimating how much of the
	// window to summarize (message fraction and token-diff seed).
	CompressFraction float64
	// StreamFitSummary, when true, Absorb returns model summary chunks for the
	// harness to stream to the client (current product default).
	StreamFitSummary bool
}

// DefaultContextPolicy matches historical harness behavior.
func DefaultContextPolicy() ContextPolicy {
	return ContextPolicy{
		PressureRatio:    0.85,
		CompressFraction: 0.25,
		StreamFitSummary: true,
	}
}

// ContextManager owns the conversation message list and pure structural transforms.
// It does not call inference or count tokens; ModelTasks performs model work and
// applies results via Replace / InstallPlanDocument.
//
// Implementations must be safe for concurrent Snapshot during checkpoint while
// another Run on the same harness Absorbs/Replaces after interrupt resume.
type ContextManager interface {
	// Messages returns a snapshot of the live window (safe to retain after return).
	Messages() []*Message
	// Snapshot returns a shallow copy of the message pointer slice for checkpointing.
	// Message values are shared (pointers), not deep-cloned.
	Snapshot() []*Message
	// Restore copies window into internal storage (safe for external/session slices).
	Restore(window []*Message)
	// Replace takes ownership of window without copying the slice.
	// Caller must not reuse the slice after Replace.
	Replace(window []*Message)

	// Add appends a message without pressure fitting (streamed assistant/reasoning).
	Add(msg *Message)

	// InstallPlanDocument prunes to [window[0], plan document].
	InstallPlanDocument(planRaw string) error
}

// ModelContextManager is the default ContextManager implementation.
// Name is historical; it does not use a model.
type ModelContextManager struct {
	mu     sync.RWMutex
	window []*Message
}

// NewModelContextManager returns an empty default ContextManager.
func NewModelContextManager() *ModelContextManager {
	return &ModelContextManager{}
}

func (m *ModelContextManager) Messages() []*Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.window) == 0 {
		return nil
	}
	return slices.Clone(m.window)
}

func (m *ModelContextManager) Snapshot() []*Message {
	return m.Messages()
}

func (m *ModelContextManager) Restore(window []*Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(window) == 0 {
		m.window = nil
		return
	}
	// Copy so session/test callers keep independent slice headers.
	m.window = slices.Clone(window)
}

func (m *ModelContextManager) Replace(window []*Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Ownership transfer: Absorb/Handoff already allocated the new slice.
	m.window = window
}

func (m *ModelContextManager) Add(msg *Message) {
	if msg == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.window = append(m.window, msg)
}

func (m *ModelContextManager) InstallPlanDocument(planRaw string) error {
	if planRaw == "" {
		return fmt.Errorf("install plan document: no plan document")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.window) == 0 || m.window[0] == nil {
		return fmt.Errorf("install plan document: empty window")
	}
	// Keep the existing user message pointer; only drop the rest.
	m.window = []*Message{m.window[0], buildPlanDocumentMessage(planRaw)}
	return nil
}

// protectedPrefixLen returns how many leading messages Absorb must keep:
// [0] user request; [1] plan document when present.
func protectedPrefixLen(window []*Message) int {
	if len(window) == 0 {
		return 0
	}
	if len(window) > 1 && isPlanDocument(window[1]) {
		return 2
	}
	return 1
}

// continuePlanNudge is appended after handoff when todos remain so the next
// Turn keeps tool-calling instead of ending the turn.
const continuePlanNudge = `The plan still has incomplete todos. Continue executing now: work the in-progress todo (or the next pending one), call tools as needed, and do not stop for user confirmation. Do not restate the handoff; act on the next todo.`

func planHasOpenTodos(plan []control.Todo) bool {
	for i := range plan {
		if plan[i].Status != streaming.TodoStatusCompleted {
			return true
		}
	}
	return false
}
