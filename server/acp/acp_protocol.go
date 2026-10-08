package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/server"

	"github.com/ryanaldo34/tacklr"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr/session"
)

const (
	// HeaderConnectionID is set on the WebSocket 101 response.
	HeaderConnectionID = "Acp-Connection-Id"

	methodVFSBind    = "_tacklr/vfs/bind"
	methodVFSRefresh = "_tacklr/vfs/refresh"
	methodVFSUnbind  = "_tacklr/vfs/unbind"
)

// acpProtocol is the native server.Protocol implementation for the Agent Client server.Protocol.
// Wire session state (cwd, mcp, config) lives here — not on SnapshotStore.
type acpProtocol struct {
	mu       sync.Mutex
	sessions map[string]*acpWireSession
	wire     server.ProtocolWireStore

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

// New returns the native ACP server.Protocol. Nil wire uses an in-memory store.
func New(wire server.ProtocolWireStore) server.Protocol {
	return NewWithAuth(wire, nil, false)
}

// NewWithAuth configures the ACP v1 presentation for a generic host
// security service. It does not implement credential verification itself.
func NewWithAuth(wire server.ProtocolWireStore, methods []ACPAuthMethod, logout bool) server.Protocol {
	if wire == nil {
		wire = server.NewMemoryWireStore()
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

func (p *acpProtocol) HTTPRoutes() []server.HTTPRoute {
	return []server.HTTPRoute{
		{Method: http.MethodGet, Pattern: "/acp", AllowUnauthenticated: true, Handler: p.handleACPGet},
	}
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func (p *acpProtocol) handleACPGet(env server.ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	if !isWebSocketUpgrade(r) {
		http.Error(w, "WebSocket upgrade required", http.StatusUpgradeRequired)
		return
	}
	p.handleACPWebSocket(env, w, r)
}

// handleACPWebSocket serves a full-duplex ACP JSON-RPC connection over WebSocket.
func (p *acpProtocol) handleACPWebSocket(env server.ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	// Register before Accept so Acp-Connection-Id is on the 101 response (RFD).
	// Bridge/writer are filled in after the socket is open.
	acpConn := env.Connections.Create(nil)
	if env.Conn != nil && env.Conn.Security != nil {
		acpConn.SetSecurityContext(*env.Conn.Security)
	}
	w.Header().Set(HeaderConnectionID, acpConn.ID)

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		env.Connections.Remove(acpConn.ID)
		slog.Warn("acp websocket accept failed", "error", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	ctx := acpConn.Context()
	mw := &jsonRPCWSMessageWriter{ctx: ctx, c: c}
	bridge := NewClientBridge(mw)
	acpConn.Writer = mw
	defer env.Connections.Remove(acpConn.ID)

	var wg sync.WaitGroup
	dispatch := func(body []byte) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			securityContext := acpConn.SecurityContext()
			reqConn := &server.Conn{
				Writer:      mw,
				Ask:         bridge,
				Security:    &securityContext,
				SetSecurity: acpConn.SetSecurityContext,
			}
			reqEnv := server.ProtocolEnv{
				Runtime:     env.Runtime,
				Agent:       env.Agent,
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
		_, data, err := c.Read(r.Context())
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
	// Socket is gone: cancel in-flight Prompt/Resume before waiting for them.
	env.Connections.Remove(acpConn.ID)
	wg.Wait()
}

func (p *acpProtocol) HandleInbound(ctx context.Context, env server.ProtocolEnv, body []byte) error {
	if err := ctx.Err(); err != nil {
		return reply(env.Conn.Writer, nil, nil, err)
	}
	pr, err := validateACPRequest(body)
	if err != nil {
		return reply(env.Conn.Writer, nil, nil, err)
	}
	if pr.Notification {
		p.handleNotification(ctx, env, pr)
		return nil
	}
	if pr.Method == "session/prompt" || pr.Method == "session/resume" {
		return p.handleSessionTurn(ctx, env, pr)
	}
	result, err := p.dispatch(ctx, env, pr)
	return reply(env.Conn.Writer, pr.ID, result, err)
}

func (p *acpProtocol) handleNotification(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) {
	if pr.Method != "session/cancel" || pr.ThreadID == "" {
		return
	}
	if _, err := p.resolveOwnedWireSession(ctx, env, pr.ThreadID, actionSessionPrompt); err == nil {
		_ = env.Runtime.Cancel(ctx, session.SessionID(pr.ThreadID))
	}
}

func (p *acpProtocol) dispatch(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) (any, error) {
	switch pr.Method {
	case "initialize":
		if env.Conn.Ask != nil {
			env.Conn.Ask.NoteHello(pr.ClientCapsRaw)
		}
		return InitializeResult(env.Agent, pr.ProtocolVersion, p.authMethods, p.logout), nil
	case "authenticate":
		if err := p.authenticate(ctx, env, pr.AuthMethodID); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "logout":
		env.Conn.EstablishSecurity(server.Context{})
		return map[string]any{}, nil
	case "session/new":
		if err := p.requireAuthentication(env); err != nil {
			return nil, err
		}
		_, result, err := p.createSession(ctx, env, pr)
		return result, err
	case "session/load":
		return p.loadSession(ctx, env, pr)
	case "session/set_config_option":
		return p.setConfig(ctx, env, pr.ThreadID, pr.ConfigID, pr.ConfigValue)
	case "session/close":
		if err := p.closeSession(ctx, env, pr.ThreadID); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "session/cancel":
		if _, err := p.resolveOwnedWireSession(ctx, env, pr.ThreadID, actionSessionPrompt); err != nil {
			return nil, err
		}
		_ = env.Runtime.Cancel(ctx, session.SessionID(pr.ThreadID))
		return map[string]any{}, nil
	case methodVFSBind:
		return p.handleVFSBind(ctx, env, pr)
	case methodVFSRefresh:
		return p.handleVFSRefresh(ctx, env, pr)
	case methodVFSUnbind:
		return p.handleVFSUnbind(ctx, env, pr)
	default:
		return nil, server.Errorf(server.ErrMethodNotFound, "method not found")
	}
}

func (p *acpProtocol) authenticate(ctx context.Context, env server.ProtocolEnv, methodID string) error {
	var method *ACPAuthMethod
	for i := range p.authMethods {
		if p.authMethods[i].ID == methodID {
			method = &p.authMethods[i]
			break
		}
	}
	if method == nil {
		return server.Errorf(server.ErrInvalidRequest, "authentication method %q was not advertised", methodID)
	}
	if env.Security == nil {
		return server.Errorf(server.ErrAuthenticationRequired, "authentication required")
	}
	binding := server.ChannelBinding{Kind: "acp"}
	if env.Conn.Security != nil {
		binding = env.Conn.Security.Binding
		if binding.Kind == "" {
			binding.Kind = "acp"
		}
	}
	securityContext, err := env.Security.Authenticate(ctx, server.Attempt{
		Scheme:  method.Scheme,
		Binding: binding,
	})
	if err != nil {
		sent := server.ErrAuthenticationRequired
		if errors.Is(err, server.ErrAuthenticationFailed) {
			sent = server.ErrAuthenticationFailed
		}
		return server.ErrorCause(sent, err, "%s", sent.Error())
	}
	env.Conn.EstablishSecurity(securityContext)
	return nil
}

func (p *acpProtocol) requireAuthentication(env server.ProtocolEnv) error {
	if len(p.authMethods) == 0 {
		return nil
	}
	if env.Conn.Security != nil && env.Conn.Security.Authenticated() {
		return nil
	}
	return server.Errorf(server.ErrAuthenticationRequired, "authentication required")
}

func (p *acpProtocol) handleSessionTurn(ctx context.Context, env server.ProtocolEnv, pr *parsedRequest) error {
	if env.Conn.Ask != nil {
		if err := env.Conn.Ask.Ready(ctx); err != nil {
			return reply(env.Conn.Writer, pr.ID, nil, err)
		}
	}
	req, err := p.bindTurn(ctx, env, pr)
	if err != nil {
		return reply(env.Conn.Writer, pr.ID, nil, err)
	}
	turn := server.PromptOrResume{Prompt: session.Prompt{
		Text:        req.Prompt,
		UserMessage: req.UserMessage,
		MCPServers:  req.MCPServers,
		Auth:        req.Auth,
	}}
	if len(req.Responses) > 0 {
		resume := &session.Resume{Auth: req.Auth, Responses: make(map[string][]byte, len(req.Responses))}
		for id, payload := range req.Responses {
			resume.Responses[id] = []byte(payload)
		}
		turn.Resume = resume
	}
	err = server.RunTurn(ctx, env, p, req.ThreadID, pr.ID, turn)
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	return reply(env.Conn.Writer, pr.ID, nil, err)
}

func (p *acpProtocol) OnStreamEvent(ctx context.Context, env server.ProtocolEnv, threadID string, ev tacklr.StreamEvent, reqID json.RawMessage) server.StreamControl {
	if ev.Type == tacklr.StreamEventInterrupt && env.Conn != nil && env.Conn.Ask != nil {
		resume, err := resolveInterruptViaACP(ctx, env, threadID, &ev)
		if err != nil {
			frames := injectReqID(presentationToACP(threadID, tacklr.StreamEvent{
				Type:  tacklr.StreamEventError,
				Error: err,
			}), reqID, true)
			return server.StreamControl{Frames: frames, Finished: true}
		}
		if resume != nil {
			return server.StreamControl{Resume: resume}
		}
	}

	if ev.Type == tacklr.StreamEventError && errors.Is(ev.Error, context.Canceled) {
		if env.Conn != nil && env.Conn.Writer != nil && len(reqID) > 0 {
			_ = env.Conn.Writer.WriteResult(reqID, acpPromptResult(stopReasonCancelled))
		}
		return server.StreamControl{Finished: true}
	}
	frames := presentationToACP(threadID, ev)
	terminal := ev.Type == tacklr.StreamEventComplete || ev.Type == tacklr.StreamEventError
	park := ev.Type == tacklr.StreamEventInterrupt
	frames = injectReqID(frames, reqID, terminal)
	if park && len(reqID) > 0 && env.Conn != nil && env.Conn.Writer != nil {
		_ = env.Conn.Writer.WriteError(reqID, server.Errorf(server.ErrInvalidRequest,
			"turn requires user input but client cannot resolve interrupts mid-turn"))
	}
	return server.StreamControl{Frames: frames, Finished: terminal || park}
}

func (p *acpProtocol) OnStreamClosed(ctx context.Context, env server.ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	if !cancelled || env.Conn == nil || env.Conn.Writer == nil {
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
		_ = json.Unmarshal(frame, &msg)
		msg["id"] = reqID
		out[i], _ = json.Marshal(msg)
	}
	return out
}

// resolveInterruptViaACP handles mid-turn interrupts over client RPC.
// Returns (nil, nil) when this interrupt kind cannot be resolved mid-turn
// (caller parks the turn). Returns (events, nil) on successful resume.
func resolveInterruptViaACP(ctx context.Context, env server.ProtocolEnv, threadID string, ev *tacklr.StreamEvent) (map[string][]byte, error) {
	var envl InterruptEventEnvelope
	_ = json.Unmarshal(ev.Data, &envl)
	switch envl.Type {
	case "tool_permission":
		return resolvePermissionViaRequest(ctx, env, threadID, envl, ev.MessageID)
	case "user_selection_choice":
		if !connElicitationForm(env.Conn) {
			return nil, nil
		}
		return resolveSelectionViaElicitation(ctx, env, threadID, envl, ev.MessageID)
	default:
		return nil, nil
	}
}

// connElicitationForm reports form-mode elicitation support from the live RPC
// bridge. No snapshot: initialize writes caps on the bridge.
func connElicitationForm(c *server.Conn) bool {
	return c != nil && c.Ask != nil && c.Ask.FormSupported()
}

func resolvePermissionViaRequest(ctx context.Context, env server.ProtocolEnv, threadID string, envl InterruptEventEnvelope, messageID string) (map[string][]byte, error) {
	var perm tacklr.ToolPermissionInterrupt
	_ = json.Unmarshal(envl.Data, &perm)
	resolution, cancelled, err := env.Conn.Ask.Permission(ctx, threadID, messageID, perm)
	if err != nil {
		return nil, err
	}
	if cancelled {
		return nil, fmt.Errorf("permission request cancelled")
	}
	return map[string][]byte{envl.InterruptId: resolution}, nil
}

func resolveSelectionViaElicitation(ctx context.Context, env server.ProtocolEnv, threadID string, envl InterruptEventEnvelope, messageID string) (map[string][]byte, error) {
	var usi tacklr.UserSelectionInterrupt
	_ = json.Unmarshal(envl.Data, &usi)
	action, resolution, err := env.Conn.Ask.Elicit(ctx, threadID, messageID, usi.Question, usi.Options)
	if err != nil {
		return nil, err
	}
	if action != "accept" {
		return nil, fmt.Errorf("elicitation %s", action)
	}
	return map[string][]byte{envl.InterruptId: resolution}, nil
}

// Prompt baseline (no capability bits): Text + ResourceLink are always accepted.
// Optional: image (model-gated), audio (off), embeddedContext (Resource text/blob).
func InitializeResult(agent tacklr.AgentOptions, clientProtocolVersion int, methods []ACPAuthMethod, logout bool) map[string]any {
	image := false
	if agent.Model != nil {
		image = agent.Model.SupportsMIME("image/png")
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
			"audio": false,
			// embeddedContext: ContentBlock::Resource (text + PDF blob).
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
				"vfs": acpVFSCapability(),
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
				"transports": []string{"websocket"},
				"vfs":        acpVFSCapability(),
			},
		},
	}
}

func acpVFSCapability() map[string]any {
	return map[string]any{
		"credentials":  true,
		"tokenRefresh": true,
		"tokenExpiry":  true,
		"writable":     true,
	}
}
