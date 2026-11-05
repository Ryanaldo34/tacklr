package tacklr

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/message"
)

// pressureRatio is the fraction of MaxWindowSize a commit may fill before
// the window collapses into one summary. A collapse leaves the window small,
// so the exact value matters little.
const pressureRatio = 0.85

// modelContextManager holds the context window: committed rounds only.
// Harness commits rounds into it; window effects replace it.
type modelContextManager struct {
	mu     sync.RWMutex
	window []*message.Message
	// usage is the provider's count of the last request on this window. A
	// Replace or a Restore forgets it: it counted messages that are gone.
	usage requestUsage
}

// requestUsage is a provider's input token count for one model request,
// and how many window messages that request carried. Zero is no count.
type requestUsage struct {
	tokens, messages int
}

// Usage is the provider's count of the last request on the window.
func (m *modelContextManager) Usage() requestUsage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.usage
}

// Counted notes the provider's count of a request on the window as it is.
func (m *modelContextManager) Counted(tokens int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage = requestUsage{tokens: tokens, messages: len(m.window)}
}

func newModelContextManager() *modelContextManager {
	return &modelContextManager{}
}

// Messages returns a retainable snapshot of the window (also used for checkpoints).
func (m *modelContextManager) Messages() []*message.Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.window) == 0 {
		return nil
	}
	return slices.Clone(m.window)
}

// Restore copies window into storage (caller keeps its slice).
func (m *modelContextManager) Restore(window []*message.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage = requestUsage{}
	if len(window) == 0 {
		m.window = nil
		return
	}
	m.window = slices.Clone(window)
}

// Replace takes ownership of window; do not reuse the slice after.
func (m *modelContextManager) Replace(window []*message.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.window = window
	m.usage = requestUsage{}
}

// Add appends a round. The usage stays: the request it counted is the
// window's start.
func (m *modelContextManager) Add(round ...*message.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.window = append(m.window, round...)
}

// InstallPlanDocument sets the window to [user, plan document].
func (m *modelContextManager) InstallPlanDocument(planRaw string) error {
	if planRaw == "" {
		return fmt.Errorf("install plan document: no plan document")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.window = []*message.Message{m.window[0], buildPlanDocumentMessage(planRaw)}
	m.usage = requestUsage{}
	return nil
}

// protectedPrefixLen is what a collapse keeps: the first user message, and
// the plan document after it.
func protectedPrefixLen(window []*message.Message) int {
	if len(window) > 1 && IsPlanDocument(window[1]) {
		return 2
	}
	return min(len(window), 1)
}

// continuePlanNudge is appended after handoff when todos remain so the next
// Turn keeps tool-calling instead of ending the turn.
const continuePlanNudge = `The plan still has incomplete todos. Continue executing now: work the in-progress todo (or the next pending one), call tools as needed, and do not stop for user confirmation. Do not restate the handoff; act on the next todo.`

func planHasOpenTodos(plan []Todo) bool {
	for i := range plan {
		if plan[i].Status != TodoStatusCompleted {
			return true
		}
	}
	return false
}

// planDocumentPrefix identifies durable plan messages so compression keeps them.
const planDocumentPrefix = "PROJECT PLAN\n────────────\n"

// IsPlanDocument reports the durable PROJECT PLAN developer message.
func IsPlanDocument(m *message.Message) bool {
	return m != nil && strings.HasPrefix(m.Content, planDocumentPrefix)
}

func buildPlanDocumentMessage(raw string) *message.Message {
	return &message.Message{
		Role:    message.RoleDeveloper,
		Content: planDocumentPrefix + raw,
	}
}

// contextSummaryPrefix heads the summary a collapse writes.
const contextSummaryPrefix = "CONVERSATION SUMMARY\n────────────\n"
