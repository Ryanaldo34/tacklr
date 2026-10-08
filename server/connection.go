package server

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// Connection is one WebSocket. Harness sessions live on session.Runtime.
type Connection struct {
	ID     string
	Writer MessageWriter

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	security Context
}

func (c *Connection) SecurityContext() Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.security
}

func (c *Connection) SetSecurityContext(securityContext Context) {
	c.mu.Lock()
	c.security = securityContext
	c.mu.Unlock()
}

// ConnectionRegistry tracks live ACP WebSockets by Acp-Connection-Id.
type ConnectionRegistry struct {
	mu   sync.Mutex
	byID map[string]*Connection
}

// NewConnectionRegistry returns an empty registry.
func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{byID: make(map[string]*Connection)}
}

// Create registers a new connection. Writer may be filled in after Accept.
func (r *ConnectionRegistry) Create(writer MessageWriter) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{
		ID:     uuid.NewString(),
		Writer: writer,
		ctx:    ctx,
		cancel: cancel,
	}
	r.mu.Lock()
	r.byID[c.ID] = c
	r.mu.Unlock()
	return c
}

// Get returns the connection for id, or nil.
func (r *ConnectionRegistry) Get(id string) *Connection {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byID[id]
}

// Remove deletes the connection and cancels its context.
func (r *ConnectionRegistry) Remove(id string) {
	r.mu.Lock()
	c := r.byID[id]
	delete(r.byID, id)
	r.mu.Unlock()
	if c != nil {
		c.cancel()
	}
}

// Context is cancelled when the connection is removed.
func (c *Connection) Context() context.Context {
	return c.ctx
}
