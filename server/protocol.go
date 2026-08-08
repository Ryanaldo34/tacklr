package server

import (
	"context"
	"encoding/json"
	"errors"
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
	// Connections is the server-wide connection registry (WebSocket / Streamable HTTP).
	// Nil for pure stdio or tests that only use HandleInbound.
	Connections *ConnectionRegistry
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

// ErrWireSessionUnsupported is returned by Protocol session methods when the
// protocol has no wire-session lifecycle (e.g. simple SSE).
var ErrWireSessionUnsupported = errors.New("wire sessions not supported by this protocol")

// Protocol is a complete wire façade over Registry.
// Transports only provide Conn I/O; protocols own methods, routes, stream policy,
// and wire-session lifecycle (create/load/bind/close).
//
// Multi-protocol model (ACP, SSE, future A2A):
//   - Registry.RunTurn produces protocol-agnostic streaming.StreamEvent values.
//   - runTurnStream pumps those events through OnStreamEvent / OnStreamClosed.
//   - Each Protocol maps StreamEvent → its client wire and owns any wire session state.
//
// Adding a protocol should not require harness streaming changes.
type Protocol interface {
	Name() string

	// HandleInbound processes one inbound message on a connection-oriented
	// transport (stdio NDJSON). Pure-HTTP protocols may return nil without work.
	HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error

	// HTTPRoutes returns routes to mount for ServeHTTP. Nil/empty is fine.
	HTTPRoutes() []HTTPRoute

	// OnStreamEvent encodes one harness StreamEvent for the client connection.
	OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl

	// OnStreamClosed is called when the event channel closes without Finished.
	OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error

	// CreateSession allocates a wire session. params is protocol-defined JSON.
	// Stateless protocols return ErrWireSessionUnsupported.
	CreateSession(ctx context.Context, env ProtocolEnv, params json.RawMessage) (sessionID string, result any, err error)

	// LoadSession reattaches a wire session (memory or durable wire store).
	LoadSession(ctx context.Context, env ProtocolEnv, sessionID string, params json.RawMessage) (result any, err error)

	// BindTurn maps a wire session + turn body into a Registry TurnRequest.
	BindTurn(ctx context.Context, env ProtocolEnv, sessionID string, turnParams json.RawMessage) (TurnRequest, error)

	// CloseSession drops live wire binding and may cancel an in-flight turn.
	CloseSession(ctx context.Context, env ProtocolEnv, sessionID string) error
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
			// drain only; encoding already stopped
		}
	}

	end := func(cancelled bool) error {
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
				return nil
			}
		}
	}
}
