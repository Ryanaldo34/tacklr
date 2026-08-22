package durable

import (
	"context"

	"github.com/ryanaldo34/tacklr/streaming"
)

// Runtime is the only session kernel API. Protocol handlers and hosts call it.
// Backends: in-process (goroutine wait loop) or Temporal (one workflow per session).
//
// Prompt and Resume signal the session; they do not return a harness.
// Subscribe yields StreamEvent values (message, tool, yield, error, complete).
//
// VFS credentials travel on Prompt / Resume / CreateSession.Mounts — not as
// separate kernel bind RPCs. Protocols map wire auth into AuthContext.
type Runtime interface {
	CreateSession(ctx context.Context, req CreateSession) (SessionID, error)
	Prompt(ctx context.Context, sessionID SessionID, msg Prompt) error
	Resume(ctx context.Context, sessionID SessionID, resume Resume) error
	Cancel(ctx context.Context, sessionID SessionID) error
	Close(ctx context.Context, sessionID SessionID) error
	// Head is the current EventLog offset. Protocol pumps pass it to Subscribe
	// to tail from now (skip events from prior turns).
	Head(ctx context.Context, sessionID SessionID) (Seq, error)
	Subscribe(ctx context.Context, sessionID SessionID, after Seq) (Subscription, error)
}

// Subscription is one consumer of a session EventLog.
type Subscription interface {
	Events() <-chan streaming.StreamEvent
	Close() error
}
