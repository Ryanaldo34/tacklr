package session

import (
	"context"
)

// Runtime is the host session API. One backend runs one session per id and
// implements Driver for it. Package session owns the session loop.
// Open-round and HITL rules live in Turn.Run.
//
// Prompt and Resume deliver work to the session. They do not return a harness.
// Subscribe yields Event values (message, tool, yield, error, complete).
// VFS credentials travel on Prompt, Resume, and CreateSession.Mounts.
// Protocols map wire auth into AuthContext.
type Client interface {
	CreateSession(ctx context.Context, req CreateSession) (ID, error)
	// Close destroys the session and recursively stops children.
	Close(ctx context.Context, sessionID ID) error
	// Status is running, complete, failed, or unknown. A child waiting on HITL
	// stays running until that interrupt is resolved.
	Status(ctx context.Context, id ID) (Status, error)
	// Head is the current EventLog offset. Protocol pumps pass it to Subscribe
	// to tail from now (skip events from prior turns).
	Head(ctx context.Context, sessionID ID) (Seq, error)
	Subscribe(ctx context.Context, sessionID ID, after Seq) (Subscription, error)

	// Prompt while a turn is running or parked steers; it does not cancel
	// the in-flight model call. Cancel aborts that turn and stops child
	// sessions. The parent session stays open.
	Prompt(ctx context.Context, sessionID ID, msg Prompt) error
	Resume(ctx context.Context, sessionID ID, resume Resume) error
	Cancel(ctx context.Context, sessionID ID) error

	// Jobs is the status of each job the session runs (its id in
	// SessionStatus.ID), in start order. The agent starts and cancels jobs
	// through tools; hosts read them here.
	Jobs(ctx context.Context, parent ID) ([]Status, error)
}

// Subscription is one consumer of a session EventLog.
type Subscription interface {
	Events() <-chan Event
	// Err is why Events closed: nil when the subscriber closed it, else the
	// error that stopped delivery (a stream or decode failure, or a tool
	// result that could not be read back). Call it after Events closes.
	Err() error
	Close() error
}
