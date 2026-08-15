package tacklr

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/streaming"
)

// ContextPolicy controls window compress under pressure (used by ModelTasks.Absorb).
type ContextPolicy struct {
	// PressureRatio is the max-size fraction that triggers compress (for example 0.85).
	PressureRatio float64
	// CompressFraction seeds how much of the window to summarize.
	CompressFraction float64
	// StreamFitSummary streams compress summary chunks to the client when true.
	StreamFitSummary bool
}

// DefaultContextPolicy is the product default pressure and compress settings.
func DefaultContextPolicy() ContextPolicy {
	return ContextPolicy{
		PressureRatio:    0.85,
		CompressFraction: 0.25,
		StreamFitSummary: true,
	}
}

// ContextManager owns the conversation window structure only (no inference).
// ModelTasks does model work and applies results with Replace or InstallPlanDocument.
// Snapshot must be safe while another path Absorbs or Replaces after resume.
type ContextManager interface {
	// Messages returns a retainable snapshot of the live window.
	Messages() []*Message
	// Snapshot is for checkpointing (shallow copy of message pointers).
	Snapshot() []*Message
	// Restore copies window into storage (caller keeps its slice).
	Restore(window []*Message)
	// Replace takes ownership of window; do not reuse the slice after.
	Replace(window []*Message)
	// Add appends without pressure fitting (streamed assistant/reasoning).
	Add(msg *Message)
	// InstallPlanDocument sets the window to [user, plan document].
	InstallPlanDocument(planRaw string) error
}

// ModelContextManager is the default ContextManager (name is historical).
type ModelContextManager struct {
	mu     sync.RWMutex
	window []*Message
}

// NewModelContextManager returns an empty ContextManager.
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
	m.window = slices.Clone(window)
}

func (m *ModelContextManager) Replace(window []*Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.window = window
}

func (m *ModelContextManager) Add(msg *Message) {
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
	m.window = []*Message{m.window[0], buildPlanDocumentMessage(planRaw)}
	return nil
}

// protectedPrefixLen is the Absorb keep-prefix: [0] user; [1] plan document if present.
func protectedPrefixLen(window []*Message) int {
	if len(window) > 1 && isPlanDocument(window[1]) {
		return 2
	}
	return 1
}

// continuePlanNudge is appended after handoff when todos remain so the next
// Turn keeps tool-calling instead of ending the turn.
const continuePlanNudge = `The plan still has incomplete todos. Continue executing now: work the in-progress todo (or the next pending one), call tools as needed, and do not stop for user confirmation. Do not restate the handoff; act on the next todo.`

func planHasOpenTodos(plan []Todo) bool {
	for i := range plan {
		if plan[i].Status != streaming.TodoStatusCompleted {
			return true
		}
	}
	return false
}

// planDocumentPrefix identifies durable plan messages so Absorb can protect them.
const planDocumentPrefix = "PROJECT PLAN\n────────────\n"

func isPlanDocument(m *Message) bool {
	return m != nil && strings.HasPrefix(m.Content, planDocumentPrefix)
}

func rawPlanFromDocumentMessage(m *Message) string {
	if m == nil {
		return ""
	}
	return strings.TrimPrefix(m.Content, planDocumentPrefix)
}

func buildPlanDocumentMessage(raw string) *Message {
	return &Message{
		Role:    RoleDeveloper,
		Content: planDocumentPrefix + raw,
	}
}
