package brain

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// SearchContext is the retrieval state of one agent thread: the host
// namespace, the caller's principals, and the ranked rest of the last call
// of each tool, which a call with more continues. Principals are not
// exported: the host sets them every turn, so a revoked group membership
// applies on the next turn.
type SearchContext struct {
	mu         sync.Mutex
	namespace  Namespace
	principals []string
	rests      map[string]Rest
}

// searchContextExport is the harness checkpoint envelope.
type searchContextExport struct {
	Namespace Namespace       `json:"namespace,omitempty"`
	Rests     map[string]Rest `json:"rests,omitempty"`
}

// NewSearchContext returns an empty search context.
func NewSearchContext() *SearchContext {
	return &SearchContext{}
}

// Scope returns the retrieval Scope for engine calls.
func (c *SearchContext) Scope() Scope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Scope{Namespace: c.namespace.Clone(), Principals: slices.Clone(c.principals)}
}

// SetPrincipals sets the caller identities checked against object Readers.
func (c *SearchContext) SetPrincipals(principals []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.principals = slices.Clone(principals)
}

// SetNamespace sets host retrieval isolation.
func (c *SearchContext) SetNamespace(ns Namespace) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespace = ns.Clone()
}

// ClearNamespace clears retrieval isolation.
func (c *SearchContext) ClearNamespace() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespace = nil
}

// Namespace returns the host-set search namespace, if any.
func (c *SearchContext) Namespace() (Namespace, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.namespace.Empty() {
		return nil, false
	}
	return c.namespace.Clone(), true
}

// SetRest keeps rest as the ranked remainder of the last call named key.
func (c *SearchContext) SetRest(key string, rest Rest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rests == nil {
		c.rests = map[string]Rest{}
	}
	c.rests[key] = rest
}

// Rest returns the ranked remainder of the last call named key.
func (c *SearchContext) Rest(key string) Rest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rests[key]
}

// Export serializes the namespace and the rests into the harness checkpoint.
func (c *SearchContext) Export() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.namespace.Empty() && len(c.rests) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(searchContextExport{Namespace: c.namespace.Clone(), Rests: maps.Clone(c.rests)})
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
		c.namespace, c.rests = nil, nil
		return nil
	}
	var env searchContextExport
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("brain: restore search context: %w", err)
	}
	c.namespace, c.rests = env.Namespace, env.Rests
	return nil
}
