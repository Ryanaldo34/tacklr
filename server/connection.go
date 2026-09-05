package server

import (
	"context"
	"sync"

	"github.com/google/uuid"

	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
)

// HeaderAcpConnectionID is set on the WebSocket 101 response.
const HeaderAcpConnectionID = "Acp-Connection-Id"

// Connection is one WebSocket. Harness sessions live on durable.Runtime.
type Connection struct {
	ID     string
	Bridge *ClientBridge
	Writer MessageWriter

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	security tacklrsecurity.Context
}

func (c *Connection) securityContext() tacklrsecurity.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.security
}

func (c *Connection) setSecurityContext(securityContext tacklrsecurity.Context) {
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

// Create registers a new connection. bridge/writer may be filled in after Accept.
func (r *ConnectionRegistry) Create(bridge *ClientBridge, writer MessageWriter) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{
		ID:     uuid.NewString(),
		Bridge: bridge,
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
