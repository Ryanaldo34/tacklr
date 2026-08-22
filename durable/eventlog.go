package durable

import (
	"context"

	"github.com/ryanaldo34/tacklr/streaming"
)

// EventLog is the portable progress stream. Temporal implements it with
// Workflow Streams. In-process uses a memory channel. Topics are TopicEvents,
// TopicRetry (activity attempt > 1), and TopicClose (turn-slice terminator).
type EventLog interface {
	Append(ctx context.Context, sessionID SessionID, topic string, ev streaming.StreamEvent) error
	Subscribe(ctx context.Context, sessionID SessionID, after Seq) (<-chan streaming.StreamEvent, error)
	Head(ctx context.Context, sessionID SessionID) (Seq, error)
	CloseSession(ctx context.Context, sessionID SessionID) error
}
