package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/ryanaldo34/tacklr/streaming"
)

// Conn is one client connection (stdio session, or a logical HTTP request scope).
type Conn struct {
	Writer MessageWriter
	RPC    *ClientBridge
	Caps   ClientCapabilities
}

// ProtocolEnv is the domain + connection context passed into protocol handlers.
type ProtocolEnv struct {
	Registry *Registry
	Conn     *Conn
}

// StreamControl is the protocol's decision after observing one harness event.
type StreamControl struct {
	Frames        [][]byte
	ReplaceEvents <-chan streaming.StreamEvent
	Finished      bool
	Err           error
}

// HTTPRoute is one HTTP endpoint owned by a protocol.
type HTTPRoute struct {
	Method  string // e.g. "POST"
	Pattern string // e.g. "/" or "/resume"
	Handler func(env ProtocolEnv, w http.ResponseWriter, r *http.Request)
}

// Protocol is a complete wire façade over Registry.
// Transports only provide Conn I/O; protocols own methods, routes, and stream policy.
type Protocol interface {
	Name() string

	// HandleInbound processes one inbound message on a connection-oriented
	// transport (stdio NDJSON). Pure-HTTP protocols may return nil without work.
	HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error

	// HTTPRoutes returns routes to mount for ServeHTTP. Nil/empty is fine.
	HTTPRoutes() []HTTPRoute

	// OnStreamEvent handles one harness event during an active turn.
	OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl

	// OnStreamClosed is called when the event channel closes without Finished.
	// cancelled is true when the turn context was cancelled (session/cancel or parent ctx).
	OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error
}

// runTurnStream pumps a harness event stream through a protocol stream policy.
//
// Cancellation uses a single source of truth: stream.TurnContext() (the turn
// context created in RunTurn as a child of the request/connection ctx).
// session/cancel and connection teardown both cancel that context; producers
// (harness + registry forwarder) stop; this loop exits when Events closes or
// TurnContext is done, then writes one terminal result via OnStreamClosed.
func runTurnStream(
	ctx context.Context,
	env ProtocolEnv,
	proto Protocol,
	threadID string,
	stream *EventStream,
	reqID json.RawMessage,
) error {
	if stream == nil {
		return nil
	}

	// Prefer the turn context: it is cancelled by session/cancel and by parent ctx.
	turnCtx := stream.TurnContext()
	if turnCtx == nil {
		turnCtx = ctx
	}

	events := stream.Events
	finished := false

	writeFrames := func(frames [][]byte) error {
		if env.Conn == nil || env.Conn.Writer == nil || len(frames) == 0 {
			return nil
		}
		for _, f := range frames {
			if err := env.Conn.Writer.WriteFrame(f); err != nil {
				return err
			}
		}
		return nil
	}

	// discard remaining events until the channel is closed (producers exit via turnCtx).
	discardUntilClosed := func() {
		for range events {
		}
	}

	end := func(cancelled bool) error {
		if finished {
			if cancelled {
				return context.Canceled
			}
			return nil
		}
		finished = true
		// Ensure turn ctx is cancelled so any residual producers stop.
		stream.Cancel()
		if err := proto.OnStreamClosed(ctx, env, threadID, reqID, cancelled); err != nil && !cancelled {
			return err
		}
		if cancelled {
			return context.Canceled
		}
		return nil
	}

	for {
		select {
		case <-turnCtx.Done():
			// Stop encoding updates; wait for the event channel to close.
			discardUntilClosed()
			return end(true)

		case ev, ok := <-events:
			if !ok {
				// Channel closed: cancelled if turn ctx was cancelled, else natural/park end.
				return end(turnCtx.Err() != nil)
			}
			ctrl := proto.OnStreamEvent(ctx, env, threadID, stream, ev, reqID)
			if err := writeFrames(ctrl.Frames); err != nil {
				stream.Cancel()
				return err
			}
			if ctrl.Err != nil {
				stream.Cancel()
				return ctrl.Err
			}
			if ctrl.ReplaceEvents != nil {
				events = ctrl.ReplaceEvents
				continue
			}
			if ctrl.Finished {
				finished = true
				return nil
			}
		}
	}
}
