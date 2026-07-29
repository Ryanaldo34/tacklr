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
	OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error
}

// runTurnStream pumps a harness event stream through a protocol stream policy.
func runTurnStream(
	ctx context.Context,
	env ProtocolEnv,
	proto Protocol,
	threadID string,
	stream *EventStream,
	reqID json.RawMessage,
) error {
	events := stream.Events
	finished := false

	writeFrames := func(frames [][]byte) error {
		if env.Conn == nil || env.Conn.Writer == nil {
			return nil
		}
		for _, f := range frames {
			if err := env.Conn.Writer.WriteFrame(f); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			stream.Cancel()
			for {
				select {
				case ev, ok := <-events:
					if !ok {
						if !finished {
							_ = proto.OnStreamClosed(ctx, env, threadID, reqID, true)
						}
						return ctx.Err()
					}
					ctrl := proto.OnStreamEvent(ctx, env, threadID, stream, ev, reqID)
					_ = writeFrames(ctrl.Frames)
					if ctrl.Finished {
						return ctx.Err()
					}
					if ctrl.ReplaceEvents != nil {
						events = ctrl.ReplaceEvents
					}
				default:
					if !finished {
						_ = proto.OnStreamClosed(ctx, env, threadID, reqID, true)
					}
					return ctx.Err()
				}
			}
		case ev, ok := <-events:
			if !ok {
				if !finished {
					cancelled := env.Registry != nil && env.Registry.WasCancelled(threadID)
					if err := proto.OnStreamClosed(ctx, env, threadID, reqID, cancelled); err != nil {
						return err
					}
				}
				return nil
			}
			ctrl := proto.OnStreamEvent(ctx, env, threadID, stream, ev, reqID)
			if err := writeFrames(ctrl.Frames); err != nil {
				return err
			}
			if ctrl.Err != nil {
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

