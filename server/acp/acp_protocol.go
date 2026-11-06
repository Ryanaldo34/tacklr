package acp

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/session"
)

const (
	methodVFSBind    = "_tacklr/vfs/bind"
	methodVFSRefresh = "_tacklr/vfs/refresh"
	methodVFSUnbind  = "_tacklr/vfs/unbind"
)

// acpProtocol is the native server.Protocol implementation for the Agent
// Client Protocol. It is shared by every connection and holds no state of
// theirs: what ACP owns of each session (its cwd and owner) is in wire.
type acpProtocol struct {
	wire server.ProtocolWireStore

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
// It does not change methods.
func NewWithAuth(wire server.ProtocolWireStore, methods []ACPAuthMethod, logout bool) server.Protocol {
	if wire == nil {
		wire = server.NewMemoryWireStore()
	}
	methods = slices.Clone(methods)
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
	return &acpProtocol{wire: wire, authMethods: methods, logout: logout}
}

// Path implements server.Protocol: ACP connections open at /acp.
func (p *acpProtocol) Path() string { return "/acp" }

// Open implements server.Protocol.
func (p *acpProtocol) Open(env server.Env, peer server.Peer) server.Handler {
	return &conn{
		p:        p,
		env:      env,
		peer:     peer,
		security: env.Caller,
		next:     map[session.ID]*nextTurn{},
		turns:    map[session.ID]*turn{},
	}
}

// conn is one ACP connection. It owns what the connection knows, each
// part in one place: the client's hello, its security context, what the
// client gave for each session's next turn, and the turns it follows.
// Each session's wire state (cwd, owner) is the wire store's, and
// everything else about a session is the runtime's.
type conn struct {
	p    *acpProtocol
	env  server.Env
	peer server.Peer

	hello    bool
	caps     ClientCapabilities
	security server.Context
	next     map[session.ID]*nextTurn
	// asked is the id of the last request the connection sent the client.
	asked int64
	turns map[session.ID]*turn
}

// nextTurn is what the client gave for a session's next turn: the VFS
// credentials it bound, which go with each turn, and the MCP servers it
// named on session/load, which the next turn gives the session.
type nextTurn struct {
	creds      server.CredentialBag
	mcpServers []session.MCPServer
}

// turn is a session the connection follows: the session/prompt and
// session/resume requests its end answers (none for a turn the connection
// joined on session/load), the requests it asked the client, by id, and
// how to stop following it.
type turn struct {
	requests []json.RawMessage
	asks     map[string]ask
	stop     func()
}

// ask is a request to the client for a parked turn: the answer resumes the
// interrupt. choices are a selection's options.
type ask struct {
	interrupt string
	choices   []tacklr.UserChoice
	selection bool
}

// Frame implements server.Handler.
func (c *conn) Frame(ctx context.Context, frame []byte) {
	if c.answered(ctx, frame) {
		return
	}
	pr, err := validateACPRequest(frame)
	if err != nil {
		var id json.RawMessage
		if pr != nil {
			id = pr.ID
		}
		c.reply(ctx, id, nil, err)
		return
	}
	if pr.Notification {
		c.notified(ctx, pr)
		return
	}
	if pr.Method == "session/prompt" || pr.Method == "session/resume" {
		c.startTurn(ctx, pr)
		return
	}
	result, err := c.dispatch(ctx, pr)
	c.reply(ctx, pr.ID, result, err)
}

func (c *conn) notified(ctx context.Context, pr *parsedRequest) {
	if pr.Method != "session/cancel" || pr.ThreadID == "" {
		return
	}
	if _, err := c.ownedSession(ctx, pr.ThreadID, actionSessionPrompt); err == nil {
		_ = c.env.Client.Cancel(ctx, session.ID(pr.ThreadID))
	}
}

func (c *conn) dispatch(ctx context.Context, pr *parsedRequest) (any, error) {
	switch pr.Method {
	case "initialize":
		c.hello, c.caps = true, ParseClientCapabilities(pr.ClientCapsRaw)
		return InitializeResult(c.env.Agent, pr.ProtocolVersion, c.p.authMethods, c.p.logout), nil
	case "authenticate":
		if err := c.authenticate(ctx, pr.AuthMethodID); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "logout":
		c.security = server.Context{}
		return map[string]any{}, nil
	case "session/new":
		if err := c.requireAuthentication(); err != nil {
			return nil, err
		}
		return c.createSession(ctx, pr)
	case "session/load":
		return c.loadSession(ctx, pr)
	case "session/set_config_option":
		return c.setConfig(ctx, pr.ThreadID, pr.ConfigID)
	case "session/close":
		if err := c.closeSession(ctx, pr.ThreadID); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "session/cancel":
		if _, err := c.ownedSession(ctx, pr.ThreadID, actionSessionPrompt); err != nil {
			return nil, err
		}
		_ = c.env.Client.Cancel(ctx, session.ID(pr.ThreadID))
		return map[string]any{}, nil
	case methodVFSBind:
		return c.handleVFSBind(ctx, pr)
	case methodVFSRefresh:
		return c.handleVFSRefresh(ctx, pr)
	case methodVFSUnbind:
		return c.handleVFSUnbind(ctx, pr)
	default:
		return nil, server.Errorf(server.ErrMethodNotFound, "method not found")
	}
}

func (c *conn) authenticate(ctx context.Context, methodID string) error {
	i := slices.IndexFunc(c.p.authMethods, func(m ACPAuthMethod) bool { return m.ID == methodID })
	if i < 0 {
		return server.Errorf(server.ErrInvalidRequest, "authentication method %q was not advertised", methodID)
	}
	if c.env.Security == nil {
		return server.Errorf(server.ErrAuthenticationRequired, "authentication required")
	}
	binding := c.security.Binding
	if binding.Kind == "" {
		binding.Kind = "acp"
	}
	securityContext, err := c.env.Security.Authenticate(ctx, server.Attempt{Scheme: c.p.authMethods[i].Scheme, Binding: binding})
	if err != nil {
		sent := server.ErrAuthenticationRequired
		if errors.Is(err, server.ErrAuthenticationFailed) {
			sent = server.ErrAuthenticationFailed
		}
		return server.ErrorCause(sent, err, "%s", sent.Error())
	}
	c.security = securityContext
	return nil
}

func (c *conn) requireAuthentication() error {
	if len(c.p.authMethods) == 0 || c.security.Authenticated() {
		return nil
	}
	return server.Errorf(server.ErrAuthenticationRequired, "authentication required")
}

// startTurn sends a prompt or resume to the session and follows it; the
// turn's end answers the request.
func (c *conn) startTurn(ctx context.Context, pr *parsedRequest) {
	if !c.hello {
		c.reply(ctx, pr.ID, nil, server.Errorf(server.ErrInvalidRequest, "initialize the connection before a turn"))
		return
	}
	req, err := c.bindTurn(ctx, pr)
	if err != nil {
		c.reply(ctx, pr.ID, nil, err)
		return
	}
	id := session.ID(req.ThreadID)
	joined := c.turns[id] != nil
	if err = c.follow(ctx, id, false); err == nil {
		t := c.turns[id]
		t.requests = append(t.requests, pr.ID)
		if err = c.sendTurn(ctx, id, req); err != nil {
			t.requests = slices.DeleteFunc(t.requests, func(r json.RawMessage) bool { return string(r) == string(pr.ID) })
			if !joined && len(t.requests) == 0 {
				c.unfollow(id)
			}
		}
	}
	if err != nil {
		c.reply(ctx, pr.ID, nil, err)
	}
}

// sendTurn sends req to session id: a resume when it carries responses,
// else a prompt.
func (c *conn) sendTurn(ctx context.Context, id session.ID, req turnRequest) error {
	if len(req.Responses) > 0 {
		resume := session.Resume{Auth: req.Auth, Responses: make(map[string][]byte, len(req.Responses))}
		for id, payload := range req.Responses {
			resume.Responses[id] = []byte(payload)
		}
		return c.env.Client.Resume(ctx, id, resume)
	}
	err := c.env.Client.Prompt(ctx, id, session.Prompt{
		Text:        req.Prompt,
		UserMessage: req.UserMessage,
		MCPServers:  req.MCPServers,
		Auth:        req.Auth,
	})
	if err == nil {
		// The session has the servers a load named now.
		c.nextOf(id).mcpServers = nil
	}
	return err
}

// follow follows session id from its head, unless the connection follows
// it already. With parked, it starts one event before the head: a parked
// turn's last event is its interrupt, which the client is asked again.
func (c *conn) follow(ctx context.Context, id session.ID, parked bool) error {
	if c.turns[id] != nil {
		return nil
	}
	// A session with no head yet follows from its start.
	after, _ := c.env.Client.Head(ctx, id)
	if parked && after > 0 {
		after--
	}
	stop, err := c.peer.Follow(ctx, id, after)
	if err == nil {
		c.turns[id] = &turn{asks: map[string]ask{}, stop: stop}
	}
	return err
}

// unfollow stops following session id; its open asks go with it.
func (c *conn) unfollow(id session.ID) {
	if t := c.turns[id]; t != nil {
		t.stop()
		delete(c.turns, id)
	}
}

// Deliver implements server.Handler.
func (c *conn) Deliver(ctx context.Context, id session.ID, ev session.Event) {
	if ev.Type == session.EventError && errors.Is(ev.Error, server.ErrStreamEnded) {
		c.endTurn(ctx, id, ev.Error)
		return
	}
	frames := presentationToACP(string(id), ev)
	switch ev.Type {
	case session.EventInterrupt:
		// An interrupt shows as the request that asks the client.
		c.askInterrupt(ctx, id, ev)
	case session.EventComplete, session.EventError:
		// The turn's end is the response to each of its requests.
		for _, req := range c.turns[id].requests {
			c.send(ctx, withID(frames, req)...)
		}
		c.unfollow(id)
	default:
		c.send(ctx, frames...)
	}
}

// endTurn answers each request of session id's turn with err and stops
// following it. The session's turn goes on.
func (c *conn) endTurn(ctx context.Context, id session.ID, err error) {
	if t := c.turns[id]; t != nil {
		for _, req := range t.requests {
			c.reply(ctx, req, nil, err)
		}
	}
	c.unfollow(id)
}

// askInterrupt asks the client to answer a parked turn's interrupt. A park
// the client has no form for ends the turn's requests: the client answers
// it with session/resume.
func (c *conn) askInterrupt(ctx context.Context, id session.ID, ev session.Event) {
	var envl InterruptEventEnvelope
	_ = json.Unmarshal(ev.Data, &envl)
	switch {
	case envl.Type == "tool_permission":
		var perm tacklr.ToolPermissionInterrupt
		_ = json.Unmarshal(envl.Data, &perm)
		c.request(ctx, id, "session/request_permission", PermissionToACPParams(string(id), ev.MessageID, perm),
			ask{interrupt: envl.InterruptId})
	case envl.Type == "user_selection_choice" && c.caps.ElicitationForm:
		var usi tacklr.UserSelectionInterrupt
		_ = json.Unmarshal(envl.Data, &usi)
		c.request(ctx, id, "elicitation/create", SelectionToElicitationParams(string(id), ev.MessageID, usi.Question, usi.Options),
			ask{interrupt: envl.InterruptId, choices: usi.Options, selection: true})
	default:
		c.endTurn(ctx, id, server.Errorf(server.ErrInvalidRequest,
			"turn requires user input but client cannot resolve interrupts mid-turn"))
	}
}

// request sends a request to the client and keeps a in session id's turn
// for its answer.
func (c *conn) request(ctx context.Context, id session.ID, method string, params any, a ask) {
	c.asked++
	key := strconv.FormatInt(c.asked, 10)
	c.turns[id].asks[key] = a
	frame, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": c.asked, "method": method, "params": params})
	c.send(ctx, frame)
}

// answered handles frame when it is a response, to a request the
// connection sent: the answer resumes the turn that waits on it, and a
// cancel cancels it. An answer the agent cannot use ends the turn's
// requests with an error, and the turn stays parked.
func (c *conn) answered(ctx context.Context, frame []byte) bool {
	var resp struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(frame, &resp) != nil || resp.Method != "" || resp.Result == nil && resp.Error == nil {
		return false
	}
	// A response to no open ask (its turn ended first, as after a
	// session/cancel) has nothing to resume. A response gets no answer.
	id, a, ok := c.ask(string(resp.ID))
	if !ok {
		return true
	}
	var resolution []byte
	var err error
	switch {
	case resp.Error != nil:
		err = server.Errorf(server.ErrInvalidRequest, "client rpc error %d: %s", resp.Error.Code, resp.Error.Message)
	case a.selection:
		resolution, err = selectionAnswer(resp.Result, a.choices)
	default:
		resolution, err = permissionAnswer(resp.Result)
	}
	switch {
	case errors.Is(err, errCancelled):
		// The turn's end answers its requests: stopReason cancelled.
		err = c.env.Client.Cancel(ctx, id)
	case err == nil:
		err = c.env.Client.Resume(ctx, id, session.Resume{
			Responses: map[string][]byte{a.interrupt: resolution},
			Auth:      c.nextOf(id).creds.Take(),
		})
	}
	if err != nil {
		c.endTurn(ctx, id, err)
	}
	return true
}

// errCancelled is an answer that cancels its turn: a permission with
// outcome cancelled, or an elicitation with action cancel.
var errCancelled = errors.New("the client cancelled the turn")

// ask takes the open ask whose request has id key, and its session.
func (c *conn) ask(key string) (session.ID, ask, bool) {
	for id, t := range c.turns {
		if a, ok := t.asks[key]; ok {
			delete(t.asks, key)
			return id, a, true
		}
	}
	return "", ask{}, false
}

// permissionAnswer is the resolution a session/request_permission answer
// gives.
func permissionAnswer(raw json.RawMessage) ([]byte, error) {
	resolution, cancelled, err := RequestPermissionResultToPayload(raw)
	switch {
	case err != nil:
		return nil, server.ErrorCause(server.ErrInvalidRequest, err, "%s", err.Error())
	case cancelled:
		return nil, errCancelled
	}
	return resolution, nil
}

// selectionAnswer is the resolution an elicitation/create answer gives.
func selectionAnswer(raw json.RawMessage, choices []tacklr.UserChoice) ([]byte, error) {
	action, resolution, err := ElicitationResultToSelectionPayload(raw, choices)
	switch {
	case err != nil:
		return nil, server.ErrorCause(server.ErrInvalidRequest, err, "%s", err.Error())
	case action == "cancel":
		return nil, errCancelled
	case action != "accept":
		return nil, server.Errorf(server.ErrInvalidRequest, "elicitation %s", action)
	}
	return resolution, nil
}

// nextOf is what the client gave for session id's next turn.
func (c *conn) nextOf(id session.ID) *nextTurn {
	n := c.next[id]
	if n == nil {
		n = &nextTurn{}
		c.next[id] = n
	}
	return n
}

// send writes frames to the client. A write fails only when the client
// left, which ends the connection.
func (c *conn) send(ctx context.Context, frames ...[]byte) {
	for _, f := range frames {
		_ = c.peer.Send(ctx, f)
	}
}

// reply answers request id with result, or with err.
func (c *conn) reply(ctx context.Context, id json.RawMessage, result any, err error) {
	var frame []byte
	if err != nil {
		frame, _ = json.Marshal(ErrorBody(id, err))
	} else {
		frame, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	}
	c.send(ctx, frame)
}

// withID is frames with the id of request req: the turn's end answers it.
func withID(frames [][]byte, req json.RawMessage) [][]byte {
	out := make([][]byte, len(frames))
	for i, frame := range frames {
		var msg map[string]any
		_ = json.Unmarshal(frame, &msg)
		msg["id"] = req
		out[i], _ = json.Marshal(msg)
	}
	return out
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
