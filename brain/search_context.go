package brain

import (
	"maps"
	"slices"
	"sync"
)

// SearchContext is the retrieval state of one agent thread: the host
// namespace, the caller's principals, and the ranked rest of the last call
// of each tool, which a call with more continues. The namespace and the
// principals are host configuration, set every turn (a revoked group
// membership applies on the next turn); only the rests are session state.
type SearchContext struct {
	mu         sync.Mutex
	namespace  Namespace
	principals []string
	rests      map[string]Rest
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

// Rests is each call's ranked remainder, for the harness checkpoint.
func (c *SearchContext) Rests() map[string]Rest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.rests)
}

// SetRests replaces the rests with ones from a checkpoint.
func (c *SearchContext) SetRests(rests map[string]Rest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rests = maps.Clone(rests)
}
