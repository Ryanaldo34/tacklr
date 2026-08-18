package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var errConnectionNotInitialized = errors.New("connection closed before initialize")

// ClientCapabilities captures client features from initialize.
type ClientCapabilities struct {
	ElicitationForm bool
	ElicitationURL  bool
	VFSTokenRefresh bool
}

// ParseClientCapabilities extracts elicitation mode support and Tacklr VFS
// token refresh from initialize params.
func ParseClientCapabilities(params json.RawMessage) ClientCapabilities {
	var p struct {
		ClientCapabilities *struct {
			Elicitation *struct {
				Form json.RawMessage `json:"form"`
				URL  json.RawMessage `json:"url"`
			} `json:"elicitation"`
			Meta *struct {
				Tacklr *struct {
					VFS *struct {
						TokenRefresh bool `json:"tokenRefresh"`
					} `json:"vfs"`
				} `json:"tacklr"`
			} `json:"_meta"`
		} `json:"clientCapabilities"`
	}
	if len(params) == 0 || json.Unmarshal(params, &p) != nil || p.ClientCapabilities == nil {
		return ClientCapabilities{}
	}
	var caps ClientCapabilities
	if el := p.ClientCapabilities.Elicitation; el != nil {
		// Mode is supported only when the field is explicitly present and non-null.
		caps.ElicitationForm = el.Form != nil && string(el.Form) != "null"
		caps.ElicitationURL = el.URL != nil && string(el.URL) != "null"
	}
	if p.ClientCapabilities.Meta != nil && p.ClientCapabilities.Meta.Tacklr != nil && p.ClientCapabilities.Meta.Tacklr.VFS != nil {
		caps.VFSTokenRefresh = p.ClientCapabilities.Meta.Tacklr.VFS.TokenRefresh
	}
	return caps
}

type rpcWaiter struct {
	ch chan rpcOutcome
}

type rpcOutcome struct {
	result json.RawMessage
	err    error
}

// ClientBridge sends JSON-RPC requests to the Client and demuxes responses by id.
// Safe for concurrent Call from tool/turn goroutines; one bridge per connection.
type ClientBridge struct {
	w    MessageWriter
	mu   sync.Mutex
	seq  atomic.Int64
	wait map[string]*rpcWaiter
	// Caps is protected by mu; use GetCaps/SetCaps from concurrent stdio handlers.
	Caps ClientCapabilities
	// initialized is closed once initialize has run on this connection.
	initialized     chan struct{}
	initializedOnce sync.Once
	// closed is closed when the connection is torn down (stdio EOF, etc.).
	closed     chan struct{}
	closedOnce sync.Once
}

// NewClientBridge creates a bridge that writes requests through w.
func NewClientBridge(w MessageWriter) *ClientBridge {
	return &ClientBridge{
		w:           w,
		wait:        make(map[string]*rpcWaiter),
		initialized: make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

// MarkInitialized records that initialize completed on this connection.
func (b *ClientBridge) MarkInitialized() {
	if b == nil {
		return
	}
	b.initializedOnce.Do(func() { close(b.initialized) })
}

// Close unblocks WaitInitialized when the connection ends without initialize.
func (b *ClientBridge) Close() {
	if b == nil {
		return
	}
	b.closedOnce.Do(func() { close(b.closed) })
}

// WaitInitialized blocks until initialize has run, the connection closes, or ctx is done.
// If initialize already completed, that wins even when closed/ctx are also ready
// (stdio EOF closes the bridge while in-flight prompt handlers still wait).
func (b *ClientBridge) WaitInitialized(ctx context.Context) error {
	if b == nil {
		return nil
	}
	select {
	case <-b.initialized:
		return nil
	case <-b.closed:
		select {
		case <-b.initialized:
			return nil
		default:
			return errConnectionNotInitialized
		}
	case <-ctx.Done():
		select {
		case <-b.initialized:
			return nil
		default:
			return ctx.Err()
		}
	}
}

// GetCaps returns a snapshot of client capabilities (safe for concurrent use).
func (b *ClientBridge) GetCaps() ClientCapabilities {
	if b == nil {
		return ClientCapabilities{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Caps
}

// SetCaps stores client capabilities (safe for concurrent use).
func (b *ClientBridge) SetCaps(c ClientCapabilities) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Caps = c
}

// Call sends a JSON-RPC request and waits for the matching response or ctx cancel.
func (b *ClientBridge) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if b == nil || b.w == nil {
		return nil, fmt.Errorf("client rpc: no bridge")
	}
	idNum := b.seq.Add(1)
	idRaw, _ := json.Marshal(idNum)
	idKey := string(idRaw)

	waiter := &rpcWaiter{ch: make(chan rpcOutcome, 1)}
	b.mu.Lock()
	b.wait[idKey] = waiter
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.wait, idKey)
		b.mu.Unlock()
	}()

	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      idNum,
		"method":  method,
		"params":  params,
	}
	frame, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("client rpc marshal: %w", err)
	}
	if err := b.w.WriteFrame(frame); err != nil {
		return nil, fmt.Errorf("client rpc write: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case out := <-waiter.ch:
		return out.result, out.err
	}
}

// TryCompleteResponse returns true if body is a JSON-RPC response that completed a waiter.
func (b *ClientBridge) TryCompleteResponse(body []byte) bool {
	if b == nil {
		return false
	}
	var env struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return false
	}
	// Requests/notifications have a method; responses do not.
	if env.Method != "" || len(env.ID) == 0 || string(env.ID) == "null" {
		return false
	}
	b.mu.Lock()
	waiter, ok := b.wait[string(env.ID)]
	b.mu.Unlock()
	if !ok {
		return false
	}
	var out rpcOutcome
	if env.Error != nil {
		out.err = fmt.Errorf("client rpc error %d: %s", env.Error.Code, env.Error.Message)
	} else {
		out.result = env.Result
	}
	select {
	case waiter.ch <- out:
	default:
	}
	return true
}
