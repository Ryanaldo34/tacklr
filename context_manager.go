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

// Validate checks non-zero context policy overrides.
func (p ContextPolicy) Validate() error {
	if p.PressureRatio < 0 || p.PressureRatio > 1 {
		return fmt.Errorf("tacklr: ContextPolicy.PressureRatio must be zero or in (0, 1]")
	}
	if p.CompressFraction < 0 || p.CompressFraction > 1 {
		return fmt.Errorf("tacklr: ContextPolicy.CompressFraction must be zero or in (0, 1]")
	}
	return nil
}

// DefaultContextPolicy is the product default pressure and compress settings.
func DefaultContextPolicy() ContextPolicy {
	return ContextPolicy{
		PressureRatio:    0.85,
		CompressFraction: 0.25,
		StreamFitSummary: true,
	}
}

// contextManager owns the conversation window structure only (no inference).
// modelTasks does model work and applies results with Replace or InstallPlanDocument.
// Messages must be safe while another path Absorbs or Replaces after resume.
type contextManager interface {
	// Messages returns a retainable snapshot of the live window (also used for checkpoints).
	Messages() []*Message
	// Restore copies window into storage (caller keeps its slice).
	Restore(window []*Message)
	// Replace takes ownership of window; do not reuse the slice after.
	Replace(window []*Message)
	// Add appends without pressure fitting (streamed assistant/reasoning).
	Add(msg *Message)
	// InstallPlanDocument sets the window to [user, plan document].
	InstallPlanDocument(planRaw string) error
}

// modelContextManager is the default contextManager (name is historical).
type modelContextManager struct {
	mu     sync.RWMutex
	window []*Message
}

func newModelContextManager() *modelContextManager {
	return &modelContextManager{}
}

func (m *modelContextManager) Messages() []*Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.window) == 0 {
		return nil
	}
	return slices.Clone(m.window)
}

func (m *modelContextManager) Restore(window []*Message) {
	assertValidContextWindow(window)
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(window) == 0 {
		m.window = nil
		return
	}
	m.window = slices.Clone(window)
}

func (m *modelContextManager) Replace(window []*Message) {
	assertValidContextWindow(window)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.window = window
}

func (m *modelContextManager) Add(msg *Message) {
	if err := streaming.ValidateMessages([]*Message{msg}); err != nil {
		panic("tacklr: invalid context message: " + err.Error())
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.window = append(m.window, msg)
}

func assertValidContextWindow(window []*Message) {
	if err := streaming.ValidateMessages(window); err != nil {
		panic("tacklr: invalid context window: " + err.Error())
	}
}

func (m *modelContextManager) InstallPlanDocument(planRaw string) error {
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
