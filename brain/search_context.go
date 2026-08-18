package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

// SearchContext is the single retrieval session surface for one agent thread:
// host namespace isolation + the active ResultSet for continue.
type SearchContext struct {
	mu        sync.Mutex
	namespace *uuid.UUID
	current   *ResultSet
}

// searchContextExport is the checkpoint envelope (namespace + optional result set).
type searchContextExport struct {
	Namespace *uuid.UUID `json:"namespace,omitempty"`
	ResultSet *ResultSet `json:"result_set,omitempty"`
}

// NewSearchContext returns an empty search context.
func NewSearchContext() *SearchContext {
	return &SearchContext{}
}

// Scope returns the retrieval Scope for engine calls.
func (c *SearchContext) Scope() Scope {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.namespace == nil {
		return Scope{}
	}
	cp := *c.namespace
	return Scope{Namespace: &cp}
}

// SetNamespace sets host retrieval isolation.
func (c *SearchContext) SetNamespace(id uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := id
	c.namespace = &cp
}

// ClearNamespace clears retrieval isolation.
func (c *SearchContext) ClearNamespace() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespace = nil
}

// Namespace returns the host-set search namespace, if any.
func (c *SearchContext) Namespace() (uuid.UUID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.namespace == nil {
		return uuid.UUID{}, false
	}
	return *c.namespace, true
}

// Put implements ResultSetStore: stores set as the sole active ResultSet.
func (c *SearchContext) Put(_ context.Context, set ResultSet) error {
	if set.ID == uuid.Nil {
		return ErrResultSetIDRequired
	}
	cp := cloneResultSet(set)
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = &cp
	return nil
}

// Get implements ResultSetStore.
func (c *SearchContext) Get(_ context.Context, id uuid.UUID) (ResultSet, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil || c.current.ID != id {
		return ResultSet{}, fmt.Errorf("%w: %s", ErrResultSetNotFound, id)
	}
	return cloneResultSet(*c.current), nil
}

func cloneResultSet(set ResultSet) ResultSet {
	cp := set
	cp.ObjectIDs = slices.Clone(set.ObjectIDs)
	if len(set.Relations) > 0 {
		cp.Relations = maps.Clone(set.Relations)
	}
	return cp
}

// Export serializes namespace + active ResultSet for session checkpoints.
func (c *SearchContext) Export() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.namespace == nil && c.current == nil {
		return nil, nil
	}
	env := searchContextExport{}
	if c.namespace != nil {
		cp := *c.namespace
		env.Namespace = &cp
	}
	if c.current != nil {
		rs := cloneResultSet(*c.current)
		env.ResultSet = &rs
	}
	b, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("brain: export search context: %w", err)
	}
	return b, nil
}

// Restore loads a prior Export. Empty/nil clears the context.
func (c *SearchContext) Restore(raw []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(raw) == 0 {
		c.namespace = nil
		c.current = nil
		return nil
	}
	var env searchContextExport
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("brain: restore search context: %w", err)
	}
	c.namespace = env.Namespace
	c.current = env.ResultSet
	if c.current != nil && c.current.ID == uuid.Nil {
		c.current = nil
	}
	return nil
}
