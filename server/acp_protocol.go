package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr/durable"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
	"github.com/ryanaldo34/tacklr/streaming"
)

// acpProtocol is the native Protocol implementation for the Agent Client Protocol.
// Wire session state (cwd, mcp, config) lives here — not on SnapshotStore.
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

// NewACPProtocol returns the native ACP Protocol. Nil wire uses an in-memory store.
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
// Same ClientBridge demux as Streamable HTTP.
func (p *acpProtocol) handleACPWebSocket(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	// Register before Accept so Acp-Connection-Id is on the 101 response (RFD).
	// Bridge/writer are filled in after the socket is open.
	acpConn := env.Connections.Create(nil, nil)
	if env.Conn != nil && env.Conn.Security != nil {
		acpConn.setSecurityContext(*env.Conn.Security)
	}
	w.Header().Set(HeaderAcpConnectionID, acpConn.ID)

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		env.Connections.Remove(acpConn.ID)
		slog.Warn("acp websocket accept failed", "error", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	mw := &jsonRPCWSMessageWriter{ctx: ctx, c: c}
	bridge := NewClientBridge(mw)
	acpConn.Bridge = bridge
	acpConn.Writer = mw
	defer env.Connections.Remove(acpConn.ID)

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
				Runtime:     env.Runtime,
				Catalog:     env.Catalog,
				Conn:        reqConn,
				Security:    env.Security,
				Connections: env.Connections,
			}
			if err := p.HandleInbound(ctx, reqEnv, body); err != nil {
				slog.Debug("acp websocket inbound", "error", err, "connection_id", acpConn.ID)
			}
		}()
	}

	// Read loop: demux client RPC responses vs agent method requests.
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
		writeWireError(env.Conn.Writer, nil, err)
		return err
	}

	pr, err := validateACPRequest(body)
	if err != nil {
		// validateACPRequest always returns a nil *parsedRequest on error.
		slog.Debug("client error", "error", err)
		writeWireError(env.Conn.Writer, nil, err)
		return err
	}

	if pr.Notification {
		if pr.Method == "session/cancel" && pr.ThreadID != "" {
			if _, err := p.resolveOwnedWireSession(ctx, env, pr.ThreadID, actionSessionPrompt); err == nil {
				_ = env.Runtime.Cancel(ctx, durable.SessionID(pr.ThreadID))
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
		return env.Conn.Writer.WriteResult(pr.ID, acpInitializeResultWithAuth(env.Catalog, pr.ProtocolVersion, p.authMethods, p.logout))
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
		_, result, err := p.createSession(ctx, env, pr)
		if err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, result)
	case "session/load":
		result, err := p.loadSession(ctx, env, pr)
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
		if err := p.closeSession(ctx, env, pr.ThreadID); err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
	case "session/cancel":
		if _, err := p.resolveOwnedWireSession(ctx, env, pr.ThreadID, actionSessionPrompt); err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		_ = env.Runtime.Cancel(ctx, durable.SessionID(pr.ThreadID))
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
		if errors.Is(err, tacklrsecurity.ErrAuthenticationFailed) {
			return clientErrorCause(ErrAuthenticationFailed, err, "authentication failed")
		}
		return clientErrorCause(ErrAuthenticationRequired, err, "authentication required")
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
			writeWireError(env.Conn.Writer, pr.ID, err)
			return err
		}
	}
	req, err := p.bindTurn(ctx, env, pr)
	if err != nil {
		writeWireError(env.Conn.Writer, pr.ID, err)
		return err
	}
	threadID := req.ThreadID
	turn := PromptOrResume{Prompt: durable.Prompt{
		Text:        req.Prompt,
		UserMessage: req.UserMessage,
		AgentID:     req.AgentID,
		MCPServers:  req.MCPServers,
		Auth:        req.Auth,
	}}
	if len(req.Responses) > 0 {
		resume := &durable.Resume{Auth: req.Auth, Responses: make(map[string][]byte, len(req.Responses))}
		for id, payload := range req.Responses {
			resume.Responses[id] = []byte(payload)
		}
		turn.Resume = resume
	}
	err = RunTurn(ctx, env, p, threadID, pr.ID, turn)
	if err != nil && !IsClientError(err) {
		logTurnError(err, req.AgentID, threadID)
		slog.Debug("acp turn stream ended", "error", err, "thread_id", threadID)
	}
	return err
}

func (p *acpProtocol) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl {
	if ev.Type == streaming.StreamEventInterrupt && env.Conn != nil && env.Conn.RPC != nil {
		resume, err := resolveInterruptViaACP(ctx, env, threadID, &ev)
		if err != nil {
			slog.Warn("acp interrupt resolution failed", "error", err, "thread_id", threadID)
			frames, _ := presentationToACP(threadID, streaming.StreamEvent{
				Type:  streaming.StreamEventError,
				Error: err,
			})
			frames = injectReqID(frames, reqID, true)
			return StreamControl{Frames: frames, Finished: true, Err: err}
		}
		if resume != nil {
			return StreamControl{Resume: resume}
		}
	}

	if ev.Type == streaming.StreamEventError && (errors.Is(ev.Error, context.Canceled) || errors.Is(ev.Error, ErrRequestCancelled)) {
		if env.Conn != nil && env.Conn.Writer != nil && len(reqID) > 0 {
			_ = env.Conn.Writer.WriteResult(reqID, acpPromptResult(stopReasonCancelled))
		}
		return StreamControl{Finished: true}
	}
	frames, err := presentationToACP(threadID, ev)
	if err != nil {
		return StreamControl{Err: fmt.Errorf("protocol encode: %w", err)}
	}
	terminal := ev.Type == streaming.StreamEventComplete || ev.Type == streaming.StreamEventError
	park := ev.Type == streaming.StreamEventInterrupt
	frames = injectReqID(frames, reqID, terminal)
	if park && len(reqID) > 0 && env.Conn != nil && env.Conn.Writer != nil {
		_ = env.Conn.Writer.WriteError(reqID, clientErrorf(ErrInvalidRequest,
			"turn requires user input but client cannot resolve interrupts mid-turn"))
	}
	return StreamControl{Frames: frames, Finished: terminal || park}
}

func (p *acpProtocol) OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	if !cancelled {
		// Complete and park already wrote the JSON-RPC result from OnStreamEvent.
		return nil
	}
	if len(reqID) == 0 || env.Conn == nil || env.Conn.Writer == nil {
		return nil
	}
	return env.Conn.Writer.WriteResult(reqID, acpPromptResult(stopReasonCancelled))
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
func resolveInterruptViaACP(ctx context.Context, env ProtocolEnv, threadID string, ev *streaming.StreamEvent) (map[string][]byte, error) {
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
		return resolvePermissionViaRequest(ctx, env, threadID, ev)
	case "user_selection_choice":
		if !connElicitationForm(env.Conn) {
			return nil, nil
		}
		return resolveSelectionViaElicitation(ctx, env, threadID, ev)
	default:
		return nil, fmt.Errorf("unsupported interrupt type %q", kind)
	}
}

// connElicitationForm reports form-mode elicitation support from the live RPC
// bridge. No snapshot: initialize writes caps on the bridge.
func connElicitationForm(c *Conn) bool {
	return c.RPC.GetCaps().ElicitationForm
}

func resolvePermissionViaRequest(ctx context.Context, env ProtocolEnv, threadID string, ev *streaming.StreamEvent) (map[string][]byte, error) {
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
	return map[string][]byte{interruptID: resolution}, nil
}

func resolveSelectionViaElicitation(ctx context.Context, env ProtocolEnv, threadID string, ev *streaming.StreamEvent) (map[string][]byte, error) {
	interruptID, usi, err := ParseUserSelectionFromInterruptData(ev.Data)
	if err != nil {
		return nil, fmt.Errorf("parse selection interrupt: %w", err)
	}
	params, err := SelectionToElicitationParams(threadID, ev.MessageID, usi.Question, usi.Options)
	if err != nil {
		return nil, err
	}
	raw, err := env.Conn.RPC.Call(ctx, "elicitation/create", params)
	if err != nil {
		return nil, fmt.Errorf("elicitation/create: %w", err)
	}
	action, resolution, err := ElicitationResultToSelectionPayload(raw, usi.Options)
	if err != nil {
		return nil, err
	}
	return resumeElicitation(interruptID, action, resolution,
		fmt.Errorf("user declined to answer"),
		fmt.Errorf("user cancelled the prompt"),
	)
}

func resumeElicitation(interruptID, action string, resolution []byte, declined, cancelled error) (map[string][]byte, error) {
	switch action {
	case "accept":
		return map[string][]byte{interruptID: resolution}, nil
	case "decline":
		return nil, declined
	default:
		return nil, cancelled
	}
}

// Prompt baseline (no capability bits): Text + ResourceLink are always accepted.
// Optional: image (model-gated), audio (off), embeddedContext (Resource text/blob).
func acpInitializeResultWithAuth(cat durable.Catalog, clientProtocolVersion int, methods []ACPAuthMethod, logout bool) map[string]any {
	image := false
	if cat != nil {
		if spec, ok := cat.Lookup(""); ok && spec.Options.Model != nil {
			image = spec.Options.Model.SupportsMIME("image/png")
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
				"vfs": acpVFSCapability(cat),
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
				"transports": []string{"websocket", "streamable_http"},
				"vfs":        acpVFSCapability(cat),
			},
		},
	}
}

func acpVFSCapability(cat durable.Catalog) map[string]any {
	providers := []string{}
	if cat != nil {
		if spec, ok := cat.Lookup(cat.DefaultID()); ok && spec.FSRegistry != nil {
			providers = spec.FSRegistry.Profiles()
		}
	}
	return map[string]any{
		"credentials":  true,
		"providers":    providers,
		"tokenRefresh": true,
		"tokenExpiry":  true,
		"writable":     true,
	}
}

func readHTTPBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, 16<<20))
}
