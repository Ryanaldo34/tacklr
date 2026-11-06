package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/ryanaldo34/tacklr/session"
)

// Socket is one connection of text frames, both ways: a WebSocket that
// net/http accepted, or one that a platform holds (on Cloudflare, the
// server side of a WebSocketPair).
type Socket interface {
	// Read is the next frame from the peer. An error ends the connection.
	Read(ctx context.Context) ([]byte, error)
	// Write sends one frame to the peer.
	Write(ctx context.Context, frame []byte) error
}

// Connection is one connection that a Server admitted (Connect): its
// protocol and its caller. Serve runs it on a socket.
type Connection struct {
	proto Protocol
	env   Env
}

// Serve runs the connection on sock until a read from sock fails or ctx
// ends. It reads the frames in order and gives each to the protocol's
// Handler, and it runs one subscription for each follow of the Handler.
// The sessions' turns go on after the connection ends: a new connection
// can load a session and follow it again.
func (c *Connection) Serve(ctx context.Context, sock Socket) {
	ctx, cancel := context.WithCancel(ctx)
	p := &peer{ctx: ctx, sock: sock, client: c.env.Client}
	p.h = c.proto.Open(c.env, p)
	for {
		frame, err := sock.Read(ctx)
		if err != nil {
			break
		}
		if len(frame) == 0 {
			continue
		}
		p.mu.Lock()
		p.h.Frame(ctx, frame)
		p.mu.Unlock()
	}
	cancel()
	p.wg.Wait()
}

// peer is the Peer of a connection that a process holds: frames go to its
// socket, and a goroutine for each follow delivers its events.
type peer struct {
	ctx    context.Context
	sock   Socket
	client session.Client
	h      Handler

	// mu serializes the calls to h.
	mu sync.Mutex
	wg sync.WaitGroup
}

func (p *peer) Send(ctx context.Context, frame []byte) error { return p.sock.Write(ctx, frame) }

// Follow subscribes before it returns, so the events that a prompt sent
// after it makes are not missed.
func (p *peer) Follow(_ context.Context, id session.ID, after session.Seq) (func(), error) {
	ctx, stop := context.WithCancel(p.ctx)
	sub, err := p.client.Subscribe(ctx, id, after)
	if err != nil {
		stop()
		return nil, err
	}
	p.wg.Add(1)
	go p.pump(ctx, id, sub)
	return stop, nil
}

// pump delivers the events of one follow until it is stopped or its
// stream ends.
func (p *peer) pump(ctx context.Context, id session.ID, sub session.Subscription) {
	defer p.wg.Done()
	defer func() { _ = sub.Close() }()
	streamed := map[string]int{}
	for {
		var ev session.Event
		ok := true
		select {
		case <-ctx.Done():
			return
		case ev, ok = <-sub.Events():
		}
		if !ok {
			cause := ErrStreamEnded
			if err := sub.Err(); err != nil {
				cause = fmt.Errorf("%w: %w", ErrStreamEnded, err)
			}
			ev = session.Event{Type: session.EventError, Error: cause}
		}
		p.mu.Lock()
		// A follow its Handler stopped while it waited delivers nothing
		// more.
		if ctx.Err() == nil {
			p.h.Deliver(p.ctx, id, noteStreamed(streamed, ev))
		}
		p.mu.Unlock()
		if !ok {
			return
		}
	}
}
