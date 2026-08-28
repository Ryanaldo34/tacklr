package durable

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/vfs"
)

// AgentSpec is the immutable agent definition. Runtime injects SessionID and
// MountSession per turn. Protocol-neutral: no server.Protocol types.
type AgentSpec struct {
	Name string
	// Options is the canonical agent definition. SessionID and MountSession
	// must be empty; the runtime injects those per turn.
	Options tacklr.AgentOptions
	// OpenVFS builds the turn tree (typically vfs.Tree). Nil means no VFS.
	OpenVFS vfs.OpenVFS
}

// Catalog is the agent lookup table. Hosts construct it and pass it to
// inprocess.New or temporal.New. There is no backend plugin registry.
type Catalog interface {
	Lookup(agentID string) (AgentSpec, bool)
	DefaultID() string
	IDs() []string
}

// MemoryCatalog is an in-process Catalog.
type MemoryCatalog struct {
	mu        sync.RWMutex
	defaultID string
	agents    map[string]AgentSpec
}

// NewCatalog returns an empty catalog. defaultID may be empty.
func NewCatalog(defaultID string) *MemoryCatalog {
	return &MemoryCatalog{
		defaultID: defaultID,
		agents:    make(map[string]AgentSpec),
	}
}

// Register adds or replaces an agent. Panics on invalid spec (host misconfig).
func (c *MemoryCatalog) Register(agentID string, spec AgentSpec) {
	if c == nil {
		panic("durable: nil Catalog")
	}
	if strings.TrimSpace(agentID) == "" {
		panic("durable: agent id is required")
	}
	if spec.Options.SessionID != "" || spec.Options.MountSession != nil {
		panic("durable: AgentSpec.Options cannot set SessionID or MountSession; Runtime injects those per turn")
	}
	if err := spec.Options.Validate(); err != nil {
		panic(fmt.Sprintf("durable: register agent %q: %v", agentID, err))
	}
	c.mu.Lock()
	c.agents[agentID] = spec
	c.mu.Unlock()
}

// Lookup implements Catalog.
func (c *MemoryCatalog) Lookup(agentID string) (AgentSpec, bool) {
	if c == nil {
		return AgentSpec{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if agentID == "" {
		agentID = c.defaultID
	}
	spec, ok := c.agents[agentID]
	return spec, ok
}

// DefaultID implements Catalog.
func (c *MemoryCatalog) DefaultID() string {
	if c == nil {
		return ""
	}
	return c.defaultID
}

// IDs implements Catalog.
func (c *MemoryCatalog) IDs() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Sorted(maps.Keys(c.agents))
}

var _ Catalog = (*MemoryCatalog)(nil)
