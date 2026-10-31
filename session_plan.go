package tacklr

import (
	"slices"
	"sync"
)

// planStore holds the plan document and todo list for Adaptive Case Management.
// It is a sessionManager module — not exposed on HarnessRuntime.
// After newPlanStore / newSessionManager the receiver is never nil.
type planStore struct {
	mu           sync.RWMutex
	todos        []Todo
	document     string
	todosUpdated bool
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

// Change applies fn to the plan atomically. fn gets a copy of the todos
// (nil when no plan exists) and the document, and returns the new ones; when
// it fails the plan stays as it was. Tool calls of one reply run at once and
// change the plan only through Change, so no change is lost. The harness
// emits plan_update after ConsumeTodosUpdated.
func (p *planStore) Change(fn func(todos []Todo, doc string) ([]Todo, string, error)) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	todos, doc, err := fn(slices.Clone(p.todos), p.document)
	if err != nil {
		return err
	}
	p.todos, p.document, p.todosUpdated = todos, doc, true
	return nil
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

// snapshot is the todos and the document, for the checkpoint.
func (p *planStore) snapshot() ([]Todo, string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slices.Clone(p.todos), p.document
}

// restore replaces the plan with a checkpoint's.
func (p *planStore) restore(todos []Todo, document string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.todos, p.document, p.todosUpdated = todos, document, false
}
