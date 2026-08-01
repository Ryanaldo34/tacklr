package control

import (
	"encoding/json"
	"strings"
	"sync"
)

// Reserved keys in the checkpoint RuntimeState map for the plan module.
// Live plan data is on PlanStore (via SessionManager); user StateGet/StateSet block these keys.
const (
	planStateKey           = "_plan"
	planDocumentStateKey   = "_plan_document"
	planDocumentUpdatedKey = "_plan_document_updated"
)

// IsReservedRuntimeStateKey reports keys owned by SessionManager plan export.
// User tools must not read or write these via StateGet/StateSet.
func IsReservedRuntimeStateKey(key string) bool {
	switch key {
	case planStateKey, planDocumentStateKey, planDocumentUpdatedKey:
		return true
	default:
		return false
	}
}

// PlanStore holds the plan document and todo list for Adaptive Case Management.
// It is a SessionManager module — not exposed on HarnessRuntime.
type PlanStore struct {
	mu              sync.RWMutex
	todos           []Todo
	document        string
	documentUpdated bool
}

// NewPlanStore returns an empty plan store.
func NewPlanStore() *PlanStore {
	return &PlanStore{}
}

// HasActive reports whether a todo list is present (write-lock unlock condition).
func (p *PlanStore) HasActive() bool {
	if p == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.todos) > 0
}

// Get returns a shallow copy of the current todos, or nil if no plan was ever set.
// An empty non-nil slice means an explicit empty plan (e.g. after deleting all todos).
func (p *PlanStore) Get() []Todo {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.todos == nil {
		return nil
	}
	cp := make([]Todo, len(p.todos))
	copy(cp, p.todos)
	return cp
}

// Set replaces the todo list (caller should emit StreamEventPlanUpdate separately).
// Pass nil to clear the plan entirely; pass a non-nil empty slice for an empty plan.
func (p *PlanStore) Set(todos []Todo) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if todos == nil {
		p.todos = nil
		return
	}
	cp := make([]Todo, len(todos))
	copy(cp, todos)
	p.todos = cp
}

// Document returns the plaintext project plan draft.
func (p *PlanStore) Document() string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.document
}

// SetDocument stores the plaintext project plan draft.
// Marks the document updated only when replacing an existing draft with different
// text (edits), not on the initial install from create_plan.
func (p *PlanStore) SetDocument(plan string) {
	if p == nil {
		return
	}
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
	if p == nil {
		return false
	}
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
	if p == nil {
		delete(state, planStateKey)
		delete(state, planDocumentStateKey)
		delete(state, planDocumentUpdatedKey)
		return
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.todos == nil {
		delete(state, planStateKey)
	} else {
		cp := make([]Todo, len(p.todos))
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

// LoadFromState hydrates the store from checkpoint RuntimeState (including
// JSON-rehydrated []any / map shapes). Safe to call with nil state.
func (p *PlanStore) LoadFromState(state map[string]any) {
	if p == nil || state == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.todos = nil
	p.document = ""
	p.documentUpdated = false

	if v, ok := state[planStateKey]; ok && v != nil {
		if plan, ok := v.([]Todo); ok {
			cp := make([]Todo, len(plan))
			copy(cp, plan)
			p.todos = cp
		} else {
			// Checkpoint reload: rehydrate from JSON-compatible types.
			b, err := json.Marshal(v)
			if err == nil {
				var plan []Todo
				if err := json.Unmarshal(b, &plan); err == nil {
					p.todos = plan
				}
			}
		}
	}
	if s, ok := state[planDocumentStateKey].(string); ok {
		p.document = s
	}
	if b, ok := state[planDocumentUpdatedKey].(bool); ok {
		p.documentUpdated = b
	}
}

// StripPlanKeys removes reserved plan keys from a runtime state map so they are
// not exposed via user-facing StateGet after load.
func StripPlanKeys(state map[string]any) {
	if state == nil {
		return
	}
	delete(state, planStateKey)
	delete(state, planDocumentStateKey)
	delete(state, planDocumentUpdatedKey)
}
