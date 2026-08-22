package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/ryanaldo34/tacklr/durable"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Conn is one client connection (stdio session, or a logical HTTP request scope).
type Conn struct {
	Writer   MessageWriter
	RPC      *ClientBridge
	Security *tacklrsecurity.Context

	setSecurity func(tacklrsecurity.Context)
}

func (c *Conn) establishSecurity(securityContext tacklrsecurity.Context) {
	if c == nil {
		return
	}
	c.Security = &securityContext
	if c.setSecurity != nil {
		c.setSecurity(securityContext)
	}
}

// ProtocolEnv is the domain + connection context passed into protocol handlers.
type ProtocolEnv struct {
	Runtime durable.Runtime
	Catalog durable.Catalog
	Conn    *Conn
	// Security is protocol-neutral. Protocol implementations translate their
	// wire authentication into this service and store the resulting Context on Conn.
	Security *tacklrsecurity.Service
	// Connections is the server-wide connection registry (WebSocket / Streamable HTTP).
	// Nil for pure stdio or tests that only use HandleInbound.
	Connections *ConnectionRegistry
}

// StreamControl is the protocol's decision after observing one harness event.
type StreamControl struct {
	Frames   [][]byte
	Resume   map[string][]byte
	Finished bool
	Err      error
}

// HTTPRoute is one HTTP endpoint owned by a protocol.
type HTTPRoute struct {
	Method               string // e.g. "POST"
	Pattern              string // e.g. "/" or "/resume"
	AllowUnauthenticated bool
	Handler              func(env ProtocolEnv, w http.ResponseWriter, r *http.Request)
}

// ErrWireSessionUnsupported is returned by Protocol session methods when the
// protocol has no wire-session lifecycle (e.g. simple SSE).
var ErrWireSessionUnsupported = errors.New("wire sessions not supported by this protocol")

// Protocol is a complete wire façade over durable.Runtime.
// Transports only provide Conn I/O; protocols own methods, routes, stream policy,
// and wire-session lifecycle (create/load/bind/close).
type Protocol interface {
	Name() string

	HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error
	HTTPRoutes() []HTTPRoute

	OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl
	OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error

	CreateSession(ctx context.Context, env ProtocolEnv, params json.RawMessage) (sessionID string, result any, err error)
	LoadSession(ctx context.Context, env ProtocolEnv, sessionID string, params json.RawMessage) (result any, err error)
	BindTurn(ctx context.Context, env ProtocolEnv, sessionID, method string, turnParams json.RawMessage) (TurnRequest, error)
	CloseSession(ctx context.Context, env ProtocolEnv, sessionID string) error
}

// runRuntimeTurn pumps Runtime.Subscribe through a protocol stream policy.
func runRuntimeTurn(
	ctx context.Context,
	env ProtocolEnv,
	proto Protocol,
	threadID string,
	reqID json.RawMessage,
	prompt PromptOrResume,
) error {
	if env.Runtime == nil {
		return errors.New("server: Runtime is required")
	}
	id := durable.SessionID(threadID)
	after := prompt.After
	if seq, err := env.Runtime.Head(ctx, id); err == nil {
		after = seq
	}

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

	if prompt.Resume != nil {
		if err := env.Runtime.Resume(ctx, id, *prompt.Resume); err != nil {
			return err
		}
	} else {
		if err := env.Runtime.Prompt(ctx, id, prompt.Prompt); err != nil {
			return err
		}
	}

	sub, err := env.Runtime.Subscribe(ctx, id, after)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Close() }()

	end := func(cancelled bool) error {
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
		case <-ctx.Done():
			_ = env.Runtime.Cancel(context.Background(), id)
			return end(true)
		case ev, ok := <-sub.Events():
			if !ok {
				return end(ctx.Err() != nil)
			}
			ctrl := proto.OnStreamEvent(ctx, env, threadID, ev, reqID)
			if err := writeFrames(ctrl.Frames); err != nil {
				_ = env.Runtime.Cancel(context.Background(), id)
				return err
			}
			if ctrl.Err != nil {
				_ = env.Runtime.Cancel(context.Background(), id)
				return ctrl.Err
			}
			if len(ctrl.Resume) > 0 {
				if err := env.Runtime.Resume(ctx, id, durable.Resume{
					Responses: ctrl.Resume,
					Auth:      prompt.Prompt.Auth,
				}); err != nil {
					return err
				}
				continue
			}
			if ctrl.Finished {
				return nil
			}
		}
	}
}

// PromptOrResume is one protocol turn against Runtime.
type PromptOrResume struct {
	Prompt durable.Prompt
	Resume *durable.Resume
	After  durable.Seq
}
