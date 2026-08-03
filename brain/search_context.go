package brain

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// SearchContext owns the current ResultSet for one agent thread.
// Put replaces any prior set (continue on the old id fails).
// Export/Restore support harness session checkpoints.
type SearchContext struct {
	mu      sync.Mutex
	current *ResultSet
}

// NewSearchContext returns an empty search context.
func NewSearchContext() *SearchContext {
	return &SearchContext{}
}

// Put implements ResultSetStore: stores set as the sole active ResultSet.
func (c *SearchContext) Put(_ context.Context, set ResultSet) error {
	if set.ID == uuid.Nil {
		return fmt.Errorf("brain: result set id is required")
	}
	cp := set
	if set.ObjectIDs != nil {
		cp.ObjectIDs = append([]uuid.UUID(nil), set.ObjectIDs...)
	}
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
	out := *c.current
	if c.current.ObjectIDs != nil {
		out.ObjectIDs = append([]uuid.UUID(nil), c.current.ObjectIDs...)
	}
	return out, nil
}

// Export serializes the current ResultSet for session checkpoints.
func (c *SearchContext) Export() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil {
		return nil, nil
	}
	b, err := json.Marshal(c.current)
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
		c.current = nil
		return nil
	}
	var set ResultSet
	if err := json.Unmarshal(raw, &set); err != nil {
		return fmt.Errorf("brain: restore search context: %w", err)
	}
	if set.ID == uuid.Nil {
		c.current = nil
		return nil
	}
	c.current = &set
	return nil
}
