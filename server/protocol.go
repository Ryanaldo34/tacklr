package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/ryanaldo34/tacklr"

	"github.com/ryanaldo34/tacklr/durable"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
)

// Conn is one client connection (WebSocket session).
type Conn struct {
	Writer   MessageWriter
	RPC      *ClientBridge
	Security *tacklrsecurity.Context

	setSecurity func(tacklrsecurity.Context)
}

func (c *Conn) establishSecurity(securityContext tacklrsecurity.Context) {
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
	// Security is protocol-neutral. Implementations map wire credentials into
	// this service and store the resulting Context on Conn.
	Security *tacklrsecurity.Service
	// Connections is optional connection tracking (ACP WebSocket uses it).
	// Custom protocols may leave it unused.
	Connections *ConnectionRegistry
}

// StreamControl is the protocol's decision after observing one harness event.
type StreamControl struct {
	Frames   [][]byte
	Resume   map[string][]byte
	Finished bool
	Err      error
}

// HTTPRoute is one HTTP endpoint owned by a Protocol.
type HTTPRoute struct {
	Method               string // e.g. "POST"
	Pattern              string // e.g. "/acp"
	AllowUnauthenticated bool
	Handler              func(env ProtocolEnv, w http.ResponseWriter, r *http.Request)
}

// Protocol is the host extension point for streaming and delivery over durable.Runtime.
//
// ACP is the native implementation (NewACPProtocol). Hosts implement Protocol to
// define their own wire: HTTP/WebSocket routes, frame encoding, and HITL resume.
// The kernel does not import protocol types. Map wire auth into durable.AuthContext
// on Prompt/Resume; call RunTurn to pump Runtime.Subscribe through OnStreamEvent.
type Protocol interface {
	// HandleInbound decodes one connection-oriented body (WebSocket).
	// HTTP-route-only protocols may return nil.
	HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error
	HTTPRoutes() []HTTPRoute

	OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, ev tacklr.StreamEvent, reqID json.RawMessage) StreamControl
	OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error
}

// RunTurn pumps Runtime.Prompt or Resume, then Subscribe, through proto's stream policy.
func RunTurn(
	ctx context.Context,
	env ProtocolEnv,
	proto Protocol,
	threadID string,
	reqID json.RawMessage,
	prompt PromptOrResume,
) error {
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

	// Subscribe before Prompt/Resume so a fast-finishing turn cannot land in
	// the log before the pump is attached.
	sub, err := env.Runtime.Subscribe(ctx, id, after)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Close() }()

	if prompt.Resume != nil {
		if err := env.Runtime.Resume(ctx, id, *prompt.Resume); err != nil {
			return err
		}
	} else {
		if err := env.Runtime.Prompt(ctx, id, prompt.Prompt); err != nil {
			return err
		}
	}

	end := func(cancelled bool) error {
		_ = proto.OnStreamClosed(ctx, env, threadID, reqID, cancelled)
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
				if ctx.Err() != nil {
					return end(true)
				}
				return errors.New("server: turn stream ended without a result")
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
				return end(false)
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
