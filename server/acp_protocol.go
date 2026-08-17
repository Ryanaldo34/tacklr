package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"

	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
	"github.com/ryanaldo34/tacklr/streaming"
)

// acpProtocol implements Protocol for the Agent Client Protocol.
// Wire session state (cwd, mcp, config) lives here — not on Registry or BaseStore.
type acpProtocol struct {
	mu       sync.Mutex
	sessions map[string]*acpWireSession
	wire     ProtocolWireStore

	authMethods []ACPAuthMethod
	logout      bool
}

// ACPAuthMethod presents one host security scheme through the ACP v1 agent
// authentication flow. Scheme is the protocol-neutral Authenticator identifier.
type ACPAuthMethod struct {
	ID          string
	Name        string
	Description string
	Scheme      string
}

// NewACPProtocol returns an ACP protocol with optional durable wire store.
// Nil wire uses an in-memory ProtocolWireStore.
func NewACPProtocol(wire ProtocolWireStore) Protocol {
	return NewACPProtocolWithAuth(wire, nil, false)
}

// NewACPProtocolWithAuth configures the ACP v1 presentation for a generic host
// security service. It does not implement credential verification itself.
func NewACPProtocolWithAuth(wire ProtocolWireStore, methods []ACPAuthMethod, logout bool) Protocol {
	if wire == nil {
		wire = NewMemoryWireStore()
	}
	seen := make(map[string]struct{}, len(methods))
	for i := range methods {
		methods[i].ID = strings.TrimSpace(methods[i].ID)
		methods[i].Name = strings.TrimSpace(methods[i].Name)
		methods[i].Scheme = strings.TrimSpace(methods[i].Scheme)
		if methods[i].ID == "" || methods[i].Name == "" {
			panic("server: ACP auth method id and name are required")
		}
		if methods[i].Scheme == "" {
			methods[i].Scheme = methods[i].ID
		}
		if _, ok := seen[methods[i].ID]; ok {
			panic("server: duplicate ACP auth method " + methods[i].ID)
		}
		seen[methods[i].ID] = struct{}{}
	}
	return &acpProtocol{
		sessions:    make(map[string]*acpWireSession),
		wire:        wire,
		authMethods: append([]ACPAuthMethod(nil), methods...),
		logout:      logout,
	}
}

// ACPProtocol returns a new ACP protocol with an in-memory wire store.
// Each call is a fresh instance (own live map + wire store).
func ACPProtocol() Protocol { return NewACPProtocol(nil) }

// Built-in protocol aliases.
//
// ACP is a process-scoped default for simple apps (NewServer(reg, server.ACP)).
// Prefer NewACPProtocol(wire) when you need durable/shared wire state or test isolation.
// Tests that share a *Registry should use protocolForRegistry (via serveACPRaw) or
// acpTestServer — not this package-level value for multi-step session flows.
var (
	ACP Protocol = NewACPProtocol(NewMemoryWireStore())
	SSE Protocol = SSEProtocol()
)

func (*acpProtocol) Name() string { return "acp" }

func (p *acpProtocol) HTTPRoutes() []HTTPRoute {
	return []HTTPRoute{
		// ACP remote transport (RFD Streamable HTTP + WebSocket).
		{Method: http.MethodPost, Pattern: "/acp", AllowUnauthenticated: true, Handler: p.handleACPPost},
		{Method: http.MethodGet, Pattern: "/acp", AllowUnauthenticated: true, Handler: p.handleACPGet},
		{Method: http.MethodDelete, Pattern: "/acp", AllowUnauthenticated: true, Handler: p.handleACPDelete},
	}
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// handleACPWebSocket serves a full-duplex ACP JSON-RPC connection over WebSocket.
// Same lifecycle and ClientBridge demux as ServeStdio.
func (p *acpProtocol) handleACPWebSocket(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	// Register before Accept so Acp-Connection-Id is on the 101 response (RFD).
	// Bridge/writer are filled in after the socket is open.
	var acpConn *Connection
	if env.Connections != nil {
		acpConn = env.Connections.Create(nil, nil)
		if env.Conn != nil && env.Conn.Security != nil {
			acpConn.setSecurityContext(*env.Conn.Security)
		}
		w.Header().Set(HeaderAcpConnectionID, acpConn.ID)
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		if acpConn != nil {
			env.Connections.Remove(acpConn.ID)
		}
		slog.Warn("acp websocket accept failed", "error", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	mw := &jsonRPCWSMessageWriter{ctx: ctx, c: c}
	bridge := NewClientBridge(mw)
	if acpConn != nil {
		acpConn.Bridge = bridge
		acpConn.Writer = mw
		defer env.Connections.Remove(acpConn.ID)
	} else {
		acpConn = &Connection{ID: "local", Bridge: bridge, Writer: mw}
	}

	var wg sync.WaitGroup
	dispatch := func(body []byte) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			securityContext := acpConn.securityContext()
			reqConn := &Conn{
				Writer:      mw,
				RPC:         bridge,
				Security:    &securityContext,
				setSecurity: acpConn.setSecurityContext,
			}
			reqEnv := ProtocolEnv{
				Registry:    env.Registry,
				Conn:        reqConn,
				Security:    env.Security,
				Connections: env.Connections,
			}
			if err := p.HandleInbound(ctx, reqEnv, body); err != nil {
				slog.Debug("acp websocket inbound", "error", err, "connection_id", acpConn.ID)
			}
		}()
	}

	// Read loop: demux client RPC responses vs agent method requests (stdio twin).
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			break
		}
		if len(data) == 0 {
			continue
		}
		if bridge.TryCompleteResponse(data) {
			continue
		}
		dispatch(data)
	}
	wg.Wait()
}

func (p *acpProtocol) HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error {
	if err := ctx.Err(); err != nil {
		_ = env.Conn.Writer.WriteError(nil, err)
		return err
	}

	pr, err := validateACPRequest(body)
	if err != nil {
		// validateACPRequest always returns a nil *parsedRequest on error.
		slog.Debug("client error", "error", err)
		_ = env.Conn.Writer.WriteError(nil, err)
		return err
	}

	if pr.Notification {
		if pr.Method == "session/cancel" && pr.ThreadID != "" {
			if _, err := p.resolveOwnedWireSession(ctx, env, pr.ThreadID, actionSessionPrompt); err == nil {
				env.Registry.CancelSession(pr.ThreadID)
			}
		} else {
			slog.Debug("ignored notification", "method", pr.Method)
		}
		return nil
	}

	switch pr.Method {
	case "session/prompt", "session/resume":
		return p.handleSessionTurn(ctx, env, pr)
	case "initialize":
		if env.Conn != nil && env.Conn.RPC != nil {
			if len(pr.ClientCapsRaw) > 0 {
				env.Conn.RPC.SetCaps(ParseClientCapabilities(pr.ClientCapsRaw))
			}
			env.Conn.RPC.MarkInitialized()
		}
		return env.Conn.Writer.WriteResult(pr.ID, acpInitializeResultWithAuth(env.Registry, pr.ProtocolVersion, p.authMethods, p.logout))
	case "authenticate":
		if err := p.authenticate(ctx, env, pr.AuthMethodID); err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
	case "logout":
		if !p.logout {
			return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrMethodNotFound, "method not found"))
		}
		env.Conn.establishSecurity(tacklrsecurity.Context{})
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
	case "session/new":
		if err := p.requireAuthentication(env); err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		_, result, err := p.CreateSession(ctx, env, pr.Params)
		if err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, result)
	case "session/load":
		result, err := p.LoadSession(ctx, env, pr.ThreadID, pr.Params)
		if err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, result)
	case "session/set_config_option":
		result, err := p.setConfig(ctx, env, pr.ThreadID, pr.ConfigID, pr.ConfigValue)
		if err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, result)
	case "session/close":
		if err := p.CloseSession(ctx, env, pr.ThreadID); err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
	case "session/cancel":
		if _, err := p.resolveOwnedWireSession(ctx, env, pr.ThreadID, actionSessionPrompt); err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		env.Registry.CancelSession(pr.ThreadID)
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
	case methodVFSBind:
		return p.handleVFSBind(ctx, env, pr)
	case methodVFSRefresh:
		return p.handleVFSRefresh(ctx, env, pr)
	case methodVFSUnbind:
		return p.handleVFSUnbind(ctx, env, pr)
	default:
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrMethodNotFound, "method not found"))
	}
}

func (p *acpProtocol) authenticate(ctx context.Context, env ProtocolEnv, methodID string) error {
	var method *ACPAuthMethod
	for i := range p.authMethods {
		if p.authMethods[i].ID == methodID {
			method = &p.authMethods[i]
			break
		}
	}
	if method == nil {
		return clientErrorf(ErrInvalidRequest, "authentication method %q was not advertised", methodID)
	}
	if env.Security == nil {
		return clientErrorf(ErrAuthenticationRequired, "authentication required")
	}
	binding := tacklrsecurity.ChannelBinding{Kind: "acp"}
	if env.Conn != nil && env.Conn.Security != nil {
		binding = env.Conn.Security.Binding
		if binding.Kind == "" {
			binding.Kind = "acp"
		}
	}
	securityContext, err := env.Security.Authenticate(ctx, tacklrsecurity.Attempt{
		Scheme:  method.Scheme,
		Binding: binding,
	})
	if err != nil {
		return clientErrorf(ErrAuthenticationRequired, "authentication required")
	}
	env.Conn.establishSecurity(securityContext)
	return nil
}

func (p *acpProtocol) requireAuthentication(env ProtocolEnv) error {
	if len(p.authMethods) == 0 {
		return nil
	}
	if env.Conn != nil && env.Conn.Security != nil && env.Conn.Security.Authenticated() {
		return nil
	}
	return clientErrorf(ErrAuthenticationRequired, "authentication required")
}

func (p *acpProtocol) handleSessionTurn(ctx context.Context, env ProtocolEnv, pr *parsedRequest) error {
	if env.Conn != nil && env.Conn.RPC != nil {
		if err := env.Conn.RPC.WaitInitialized(ctx); err != nil {
			_ = env.Conn.Writer.WriteError(pr.ID, err)
			return err
		}
	}
	req, err := p.BindTurn(ctx, env, pr.ThreadID, pr.Method, pr.Params)
	if err != nil {
		_ = env.Conn.Writer.WriteError(pr.ID, err)
		return err
	}
	stream, err := env.Registry.RunTurn(ctx, req)
	if err != nil {
		if !IsClientError(err) {
			logTurnError(err, req.AgentID, req.ThreadID)
		}
		_ = env.Conn.Writer.WriteError(pr.ID, err)
		return err
	}
	defer func() {
		stream.Cancel()
		stream.Close()
	}()
	threadID := req.ThreadID
	if stream.SessionID() != "" {
		threadID = stream.SessionID()
	}
	err = runTurnStream(ctx, env, p, threadID, stream, pr.ID)
	if err != nil && !IsClientError(err) {
		slog.Debug("acp turn stream ended", "error", err, "thread_id", threadID)
	}
	return err
}

func (p *acpProtocol) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl {
	if ev.Type == streaming.StreamEventInterrupt && env.Conn != nil && env.Conn.RPC != nil {
		newEvents, err := resolveInterruptViaACP(ctx, env, threadID, stream, &ev)
		if err != nil {
			slog.Warn("acp interrupt resolution failed", "error", err, "thread_id", threadID)
			frames, _ := eventToAcpJsonRpc(threadID, &streaming.StreamEvent{
				Type:  streaming.StreamEventError,
				Error: err,
			})
			frames = injectReqID(frames, reqID, true)
			return StreamControl{Frames: frames, Finished: true, Err: err}
		}
		if newEvents != nil {
			return StreamControl{ReplaceEvents: newEvents}
		}
		// nil, nil → client cannot resolve mid-turn; park for OnStreamClosed.
	}

	if ev.Type == streaming.StreamEventComplete && len(reqID) > 0 && stream.Cancelled() {
		_ = env.Conn.Writer.WriteResult(reqID, acpPromptResult(stopReasonCancelled))
		return StreamControl{Finished: true}
	}

	frames, err := eventToAcpJsonRpc(threadID, &ev)
	if err != nil {
		return StreamControl{Err: fmt.Errorf("protocol encode: %w", err)}
	}
	terminal := ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventError
	frames = injectReqID(frames, reqID, terminal)
	return StreamControl{Frames: frames, Finished: terminal}
}

func (p *acpProtocol) OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	if len(reqID) == 0 || env.Conn == nil || env.Conn.Writer == nil {
		return nil
	}
	if cancelled {
		return env.Conn.Writer.WriteResult(reqID, acpPromptResult(stopReasonCancelled))
	}
	// Parked for user input without mid-turn client RPC resolution.
	return env.Conn.Writer.WriteError(reqID, clientErrorf(ErrInvalidRequest,
		"turn requires user input but client cannot resolve interrupts mid-turn"))
}

func injectReqID(frames [][]byte, reqID json.RawMessage, terminal bool) [][]byte {
	if !terminal || len(reqID) == 0 {
		return frames
	}
	out := make([][]byte, len(frames))
	for i, frame := range frames {
		var msg map[string]any
		if json.Unmarshal(frame, &msg) == nil {
			msg["id"] = reqID
			out[i], _ = json.Marshal(msg)
		} else {
			out[i] = frame
		}
	}
	return out
}

// resolveInterruptViaACP handles mid-turn interrupts over client RPC.
// Returns (nil, nil) when this interrupt kind cannot be resolved mid-turn
// (caller parks the turn). Returns (events, nil) on successful resume.
func resolveInterruptViaACP(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev *streaming.StreamEvent) (<-chan streaming.StreamEvent, error) {
	envl, err := ParseInterruptEnvelope(ev.Data)
	if err != nil {
		return nil, fmt.Errorf("parse interrupt envelope: %w", err)
	}
	kind := envl.Type
	if kind == "" {
		return nil, fmt.Errorf("interrupt envelope missing type")
	}
	switch kind {
	case "tool_permission":
		return resolvePermissionViaRequest(ctx, env, threadID, stream, ev)
	case "user_selection_choice":
		if !connElicitationForm(env.Conn) {
			return nil, nil
		}
		return resolveSelectionViaElicitation(ctx, env, threadID, stream, ev)
	default:
		return nil, fmt.Errorf("unsupported interrupt type %q", kind)
	}
}

// connElicitationForm reports form-mode elicitation support from the live RPC
// bridge. No snapshot: initialize writes caps on the bridge.
func connElicitationForm(c *Conn) bool {
	if c == nil || c.RPC == nil {
		return false
	}
	return c.RPC.GetCaps().ElicitationForm
}

func resolvePermissionViaRequest(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev *streaming.StreamEvent) (<-chan streaming.StreamEvent, error) {
	interruptID, perm, err := ParseToolPermissionFromInterruptData(ev.Data)
	if err != nil {
		return nil, fmt.Errorf("parse permission interrupt: %w", err)
	}
	params := PermissionToACPParams(threadID, ev.MessageID, perm)
	raw, err := env.Conn.RPC.Call(ctx, "session/request_permission", params)
	if err != nil {
		return nil, fmt.Errorf("session/request_permission: %w", err)
	}
	resolution, cancelled, err := RequestPermissionResultToPayload(raw)
	if err != nil {
		return nil, err
	}
	if cancelled {
		return nil, fmt.Errorf("permission request cancelled")
	}
	return stream.ResumeInterrupts(ctx, map[string][]byte{interruptID: resolution})
}

func resolveSelectionViaElicitation(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev *streaming.StreamEvent) (<-chan streaming.StreamEvent, error) {
	interruptID, opts, err := ParseUserSelectionFromInterruptData(ev.Data)
	if err != nil {
		return nil, fmt.Errorf("parse selection interrupt: %w", err)
	}
	question := stream.AskUserQuestion(ev.MessageID)
	params, err := SelectionToElicitationParams(threadID, ev.MessageID, question, opts)
	if err != nil {
		return nil, err
	}
	raw, err := env.Conn.RPC.Call(ctx, "elicitation/create", params)
	if err != nil {
		return nil, fmt.Errorf("elicitation/create: %w", err)
	}
	action, resolution, err := ElicitationResultToSelectionPayload(raw, opts)
	if err != nil {
		return nil, err
	}
	return resumeElicitation(ctx, stream, interruptID, action, resolution,
		fmt.Errorf("user declined to answer"),
		fmt.Errorf("user cancelled the prompt"),
	)
}

func resumeElicitation(ctx context.Context, stream *EventStream, interruptID, action string, resolution []byte, declined, cancelled error) (<-chan streaming.StreamEvent, error) {
	switch action {
	case "accept":
		return stream.ResumeInterrupts(ctx, map[string][]byte{interruptID: resolution})
	case "decline":
		return nil, declined
	default:
		return nil, cancelled
	}
}

// acpInitializeResult is the ACP initialize advertisement (wire shape).
//
// Prompt baseline (no capability bits): Text + ResourceLink are always accepted.
// Optional: image (model-gated), audio (off), embeddedContext (Resource text/blob).
func acpInitializeResult(r *Registry, clientProtocolVersion int) map[string]any {
	return acpInitializeResultWithAuth(r, clientProtocolVersion, nil, false)
}

func acpInitializeResultWithAuth(r *Registry, clientProtocolVersion int, methods []ACPAuthMethod, logout bool) map[string]any {
	image := false
	if r != nil {
		if m := r.AgentModel(""); m != nil {
			image = m.SupportsMIME("image/png")
		}
	}
	_ = clientProtocolVersion
	authMethods := make([]map[string]any, 0, len(methods))
	for _, method := range methods {
		item := map[string]any{
			"id":   method.ID,
			"name": method.Name,
		}
		if method.Description != "" {
			item["description"] = method.Description
		}
		authMethods = append(authMethods, item)
	}
	agentCapabilities := map[string]any{
		// Durable session/load against the registry store (survives restarts).
		"loadSession": true,
		"promptCapabilities": map[string]any{
			// image: ContentBlock::Image when the default agent model accepts vision.
			"image": image,
			// audio: not implemented.
			"audio": false,
			// embeddedContext: ContentBlock::Resource (text + PDF blob).
			// Text and ResourceLink need no capability flags (ACP baseline).
			"embeddedContext": true,
		},
		"mcpCapabilities": map[string]any{
			"http": true,
			"sse":  true,
		},
		"sessionCapabilities": map[string]any{
			"close": struct{}{},
		},
		"_meta": map[string]any{
			"tacklr": map[string]any{
				"vfs": acpVFSCapability(r),
			},
		},
	}
	if logout {
		agentCapabilities["auth"] = map[string]any{
			"logout": struct{}{},
		}
	}
	return map[string]any{
		"protocolVersion":   acpProtocolVersion,
		"agentCapabilities": agentCapabilities,
		"agentInfo": map[string]string{
			"name":    "tacklr",
			"title":   "Tacklr ACP",
			"version": "0.1.0",
		},
		"authMethods": authMethods,
		// Non-standard transport hint for operators (not part of ACP schema).
		"_meta": map[string]any{
			"tacklr": map[string]any{
				"transports": []string{"stdio", "websocket", "streamable_http"},
				"vfs":        acpVFSCapability(r),
			},
		},
	}
}

func acpVFSCapability(r *Registry) map[string]any {
	providers := []string{}
	if r != nil {
		if spec, ok := r.agents[r.defaultAgent]; ok && spec.FSRegistry != nil {
			providers = spec.FSRegistry.Profiles()
		}
	}
	return map[string]any{
		"credentials":  true,
		"providers":    providers,
		"tokenRefresh": true,
		"tokenExpiry":  true,
	}
}

func readHTTPBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, 16<<20))
}
