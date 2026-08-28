package tacklr

import (
	"strings"
	"sync"
)

// planStore holds the plan document and todo list for Adaptive Case Management.
// It is a sessionManager module — not exposed on HarnessRuntime.
// After newPlanStore / newSessionManager the receiver is never nil.
type planStore struct {
	mu              sync.RWMutex
	todos           []Todo
	document        string
	documentUpdated bool
	todosUpdated    bool
}

// newPlanStore returns an empty plan store.
func newPlanStore() *planStore {
	return &planStore{}
}

// HasActive reports whether a todo list is present (write-lock unlock condition).
func (p *planStore) HasActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.todos) > 0
}

// Get returns a shallow copy of the current todos, or nil if no plan was ever set.
// An empty non-nil slice means an explicit empty plan (e.g. after deleting all todos).
func (p *planStore) Get() []Todo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.todos == nil {
		return nil
	}
	cp := make([]Todo, len(p.todos))
	copy(cp, p.todos)
	return cp
}

// Set replaces the todo list. The harness emits plan_update after ConsumeTodosUpdated.
// Pass nil to clear the plan entirely; pass a non-nil empty slice for an empty plan.
func (p *planStore) Set(todos []Todo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.todosUpdated = true
	if todos == nil {
		p.todos = nil
		return
	}
	cp := make([]Todo, len(todos))
	copy(cp, todos)
	p.todos = cp
}

// ConsumeTodosUpdated returns the current todos when Set ran since the last
// consume, and clears the flag. The harness streams plan_update from this.
func (p *planStore) ConsumeTodosUpdated() ([]Todo, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.todosUpdated {
		return nil, false
	}
	p.todosUpdated = false
	if p.todos == nil {
		return nil, true
	}
	cp := make([]Todo, len(p.todos))
	copy(cp, p.todos)
	return cp, true
}

// Document returns the plaintext project plan draft.
func (p *planStore) Document() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.document
}

// SetDocument stores the plaintext project plan draft.
// Marks the document updated only when replacing an existing draft with different
// text (edits), not on the initial install from create_plan.
func (p *planStore) SetDocument(plan string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prev := p.document
	p.document = plan
	if prev != "" && strings.TrimSpace(prev) != strings.TrimSpace(plan) {
		p.documentUpdated = true
	}
}

// ConsumeDocumentUpdated returns whether the plan document was updated since the
// last consume, and clears the flag.
func (p *planStore) ConsumeDocumentUpdated() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.documentUpdated {
		return false
	}
	p.documentUpdated = false
	return true
}
