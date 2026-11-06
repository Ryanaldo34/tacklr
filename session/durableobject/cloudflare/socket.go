//go:build js && wasm

package cloudflare

import (
	"context"
	"io"
	"sync"
	"syscall/js"
)

// socket is a server.Socket over the server side of a WebSocketPair that
// a connection object accepted. JavaScript calls its listeners, which must
// not block, so a message goes into a queue that Read takes from.
type socket struct {
	ws js.Value

	mu     sync.Mutex
	queue  [][]byte
	closed bool
	// ready has a value while the queue or closed changed since the last
	// Read looked.
	ready chan struct{}

	onMessage, onClose js.Func
}

func newSocket(ws js.Value) *socket {
	s := &socket{ws: ws, ready: make(chan struct{}, 1)}
	s.onMessage = js.FuncOf(func(_ js.Value, args []js.Value) any {
		data := args[0].Get("data")
		var frame []byte
		if data.Type() == js.TypeString {
			frame = []byte(data.String())
		} else {
			frame = bytesToGo(jsUint8Array.New(data))
		}
		s.mu.Lock()
		s.queue = append(s.queue, frame)
		s.mu.Unlock()
		s.signal()
		return nil
	})
	s.onClose = js.FuncOf(func(js.Value, []js.Value) any {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.signal()
		return nil
	})
	ws.Call("addEventListener", "message", s.onMessage)
	ws.Call("addEventListener", "close", s.onClose)
	ws.Call("addEventListener", "error", s.onClose)
	return s
}

func (s *socket) signal() {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// Read implements server.Socket: io.EOF once the peer closed.
func (s *socket) Read(ctx context.Context) ([]byte, error) {
	for {
		s.mu.Lock()
		switch {
		case len(s.queue) > 0:
			frame := s.queue[0]
			s.queue = s.queue[1:]
			s.mu.Unlock()
			return frame, nil
		case s.closed:
			s.mu.Unlock()
			return nil, io.EOF
		}
		s.mu.Unlock()
		select {
		case <-s.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Write implements server.Socket: a text message.
func (s *socket) Write(_ context.Context, frame []byte) error {
	_, err := try(func() js.Value { return s.ws.Call("send", string(frame)) })
	return err
}

// close closes the socket, once its connection ended, and releases the
// listeners.
func (s *socket) close() {
	_, _ = try(func() js.Value { return s.ws.Call("close", 1000, "") })
	s.ws.Call("removeEventListener", "message", s.onMessage)
	s.ws.Call("removeEventListener", "close", s.onClose)
	s.ws.Call("removeEventListener", "error", s.onClose)
	s.onMessage.Release()
	s.onClose.Release()
}
