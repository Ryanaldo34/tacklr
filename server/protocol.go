package server

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
)

// Protocol is a wire protocol over session.Client: the Server admits its
// connections at Path, and each connection gets a Handler from Open.
//
// A connection is event-driven. The host calls the Handler once for each
// frame from the peer and once for each event of a session the Handler
// follows, one call at a time. The Handler keeps no goroutine and does not
// wait for the peer: it sends its own requests and handles the answers as
// frames. So the same protocol runs on a host that holds the socket in a
// process (net/http, a Durable Object) and on one where the platform holds
// it and calls the host once for each frame.
//
// The ACP built-in is package server/acp.
type Protocol interface {
	// Path is where the protocol's connections open, such as "/acp".
	Path() string
	// Open is the protocol's side of a new connection.
	Open(env Env, peer Peer) Handler
}

// Env is what a connection serves with.
type Env struct {
	Client session.Client
	Agent  tacklr.AgentOptions
	// Workspace says the runtime mounts a workspace for the sessions
	// (Server.Workspace), so clients may bind backends to it.
	Workspace bool
	// Security is the host's authentication and authorization. Nil allows
	// every operation.
	Security *Service
	// Caller is the security context of the request that opened the
	// connection: its principal when the request carried a credential the
	// host accepted, else the zero Context.
	Caller Context
}

// Peer is a connection as its Handler sees it: the host sends the frames
// and delivers the events of the sessions the Handler follows.
type Peer interface {
	// Send writes one frame to the peer.
	Send(ctx context.Context, frame []byte) error
	// Follow delivers the events of session id after after to
	// Handler.Deliver, until the Handler calls stop or the connection
	// ends. stop does not wait. The Handler owns its follows: the host
	// keeps none it would have to match.
	Follow(ctx context.Context, id session.ID, after session.Seq) (stop func(), err error)
}

// Handler is a protocol's side of one connection. The host calls it one
// call at a time, so it needs no lock, and each call returns without
// waiting for the peer.
type Handler interface {
	// Frame handles one frame from the peer.
	Frame(ctx context.Context, frame []byte)
	// Deliver handles one event of session id, which the Handler follows.
	// When the events stop before the Handler stops the follow, the last
	// event is an EventError whose Error wraps ErrStreamEnded.
	Deliver(ctx context.Context, id session.ID, ev session.Event)
}

// ErrStreamEnded is why a follow ended before its Handler stopped it: the
// session's event stream broke, or the session closed.
var ErrStreamEnded = errors.New("server: the session's event stream ended")

// noteStreamed counts the text of each message a follow got as deltas,
// and sets a complete event's Streamed to what its reader already has.
// Subscribe sends a message's deltas in order without gaps.
func noteStreamed(streamed map[string]int, ev session.Event) session.Event {
	if ev.Type != session.EventMessage && ev.Type != session.EventReasoning {
		return ev
	}
	k := string(ev.Type) + ":" + ev.MessageID
	if ev.Delta {
		streamed[k] += len(ev.Content)
		return ev
	}
	ev.Streamed = min(streamed[k], len(ev.Content))
	delete(streamed, k)
	return ev
}
