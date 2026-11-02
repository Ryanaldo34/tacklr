package session

import (
	"context"

	"github.com/ryanaldo34/tacklr"
)

// Runtime is the host session API. One backend runs one session per id and
// implements Driver for it. Package session owns the session loop.
// Open-round and HITL rules live in Turn.Run.
//
// Prompt and Resume deliver work to the session. They do not return a harness.
// Subscribe yields StreamEvent values (message, tool, yield, error, complete).
// VFS credentials travel on Prompt, Resume, and CreateSession.Mounts.
// Protocols map wire auth into AuthContext.
type Runtime interface {
	CreateSession(ctx context.Context, req CreateSession) (SessionID, error)
	// Close destroys the session and recursively stops children.
	Close(ctx context.Context, sessionID SessionID) error
	// Status is running, complete, failed, or unknown. A child waiting on HITL
	// stays running until that interrupt is resolved.
	Status(ctx context.Context, id SessionID) (SessionStatus, error)
	// Head is the current EventLog offset. Protocol pumps pass it to Subscribe
	// to tail from now (skip events from prior turns).
	Head(ctx context.Context, sessionID SessionID) (Seq, error)
	Subscribe(ctx context.Context, sessionID SessionID, after Seq) (Subscription, error)

	// Prompt while a turn is running or parked steers; it does not cancel
	// the in-flight model call. Cancel aborts that turn and stops child
	// sessions. The parent session stays open.
	Prompt(ctx context.Context, sessionID SessionID, msg Prompt) error
	Resume(ctx context.Context, sessionID SessionID, resume Resume) error
	Cancel(ctx context.Context, sessionID SessionID) error

	// Children lists child sessions in start order. The agent starts and
	// cancels them through tools. Hosts only read this list.
	Children(ctx context.Context, parent SessionID) ([]SessionID, error)
	Jobs(ctx context.Context, parent SessionID) ([]SessionStatus, error)
}

// Subscription is one consumer of a session EventLog.
type Subscription interface {
	Events() <-chan tacklr.StreamEvent
	Close() error
}
