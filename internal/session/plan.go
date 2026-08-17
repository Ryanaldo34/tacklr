package session

import (
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/streaming"
)

// Reserved checkpoint keys for SessionManager modules (blocked on StateGet/Set).
const (
	planStateKey            = "_plan"
	planDocumentStateKey    = "_plan_document"
	planDocumentUpdatedKey  = "_plan_document_updated"
	searchNamespaceStateKey = "_search_namespace"
)

func init() {
	reserveStateKeys(
		planStateKey,
		planDocumentStateKey,
		planDocumentUpdatedKey,
		searchNamespaceStateKey,
	)
}

// PlanStore holds the plan document and todo list for Adaptive Case Management.
// It is a SessionManager module — not exposed on HarnessRuntime.
// After NewPlanStore / NewSessionManager the receiver is never nil.
type PlanStore struct {
	mu              sync.RWMutex
	todos           []streaming.Todo
	document        string
	documentUpdated bool
	todosUpdated    bool
}

// NewPlanStore returns an empty plan store.
func NewPlanStore() *PlanStore {
	return &PlanStore{}
}

// HasActive reports whether a todo list is present (write-lock unlock condition).
func (p *PlanStore) HasActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.todos) > 0
}

// Get returns a shallow copy of the current todos, or nil if no plan was ever set.
// An empty non-nil slice means an explicit empty plan (e.g. after deleting all todos).
func (p *PlanStore) Get() []streaming.Todo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.todos == nil {
		return nil
	}
	cp := make([]streaming.Todo, len(p.todos))
	copy(cp, p.todos)
	return cp
}

// Set replaces the todo list. The harness emits plan_update after ConsumeTodosUpdated.
// Pass nil to clear the plan entirely; pass a non-nil empty slice for an empty plan.
func (p *PlanStore) Set(todos []streaming.Todo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.todosUpdated = true
	if todos == nil {
		p.todos = nil
		return
	}
	cp := make([]streaming.Todo, len(todos))
	copy(cp, todos)
	p.todos = cp
}

// ConsumeTodosUpdated returns the current todos when Set ran since the last
// consume, and clears the flag. The harness streams plan_update from this.
func (p *PlanStore) ConsumeTodosUpdated() ([]streaming.Todo, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.todosUpdated {
		return nil, false
	}
	p.todosUpdated = false
	if p.todos == nil {
		return nil, true
	}
	cp := make([]streaming.Todo, len(p.todos))
	copy(cp, p.todos)
	return cp, true
}

// Document returns the plaintext project plan draft.
func (p *PlanStore) Document() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.document
}

// SetDocument stores the plaintext project plan draft.
// Marks the document updated only when replacing an existing draft with different
// text (edits), not on the initial install from create_plan.
func (p *PlanStore) SetDocument(plan string) {
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
func (p *PlanStore) ConsumeDocumentUpdated() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.documentUpdated {
		return false
	}
	p.documentUpdated = false
	return true
}

// ExportInto writes plan fields into a runtime-state map for session checkpoints.
// Overwrites reserved keys with the current PlanStore contents.
func (p *PlanStore) ExportInto(state map[string]any) {
	if state == nil {
		return
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.todos == nil {
		delete(state, planStateKey)
	} else {
		cp := make([]streaming.Todo, len(p.todos))
		copy(cp, p.todos)
		state[planStateKey] = cp
	}
	if p.document == "" {
		delete(state, planDocumentStateKey)
	} else {
		state[planDocumentStateKey] = p.document
	}
	if p.documentUpdated {
		state[planDocumentUpdatedKey] = true
	} else {
		delete(state, planDocumentUpdatedKey)
	}
}
