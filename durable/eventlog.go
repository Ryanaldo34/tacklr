package durable

import (
	"context"

	"github.com/ryanaldo34/tacklr"
)

// EventLog is the portable progress stream. Temporal implements it with
// Workflow Streams. In-process uses a memory channel. Topics are TopicEvents
// and TopicRetry (activity attempt > 1).
type EventLog interface {
	Append(ctx context.Context, sessionID SessionID, topic string, ev tacklr.StreamEvent) error
	Subscribe(ctx context.Context, sessionID SessionID, after Seq) (<-chan tacklr.StreamEvent, error)
	Head(ctx context.Context, sessionID SessionID) (Seq, error)
	CloseSession(ctx context.Context, sessionID SessionID) error
}
