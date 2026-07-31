package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/streaming"
)

// acpProtocol implements Protocol for the Agent Client Protocol.
type acpProtocol struct{}

// ACPProtocol returns the ACP wire protocol module.
func ACPProtocol() Protocol { return acpProtocol{} }

// Built-in protocol aliases (backward compatible).
var (
	ACP Protocol = ACPProtocol()
	SSE Protocol = SSEProtocol()
)

func (acpProtocol) Name() string { return "acp" }

func (p acpProtocol) HTTPRoutes() []HTTPRoute {
	return []HTTPRoute{{
		Method:  http.MethodPost,
		Pattern: "/",
		Handler: p.handleHTTP,
	}}
}

func (p acpProtocol) handleHTTP(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
	body, err := readHTTPBody(r)
	if err != nil {
		mw := &jsonRPCMessageWriter{w: w}
		_ = mw.WriteError(nil, fmt.Errorf("read body: %w", err))
		return
	}
	conn := &Conn{Writer: &jsonRPCMessageWriter{w: w}}
	// Unary HTTP has no outbound RPC bridge (no mid-turn elicitation).
	env.Conn = conn
	if err := p.HandleInbound(r.Context(), env, body); err != nil {
		slog.Debug("acp http handler", "error", err)
	}
}

func (p acpProtocol) HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error {
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
			env.Registry.CancelSession(pr.ThreadID)
		} else {
			slog.Debug("ignored notification", "method", pr.Method)
		}
		return nil
	}

	switch pr.Method {
	case "session/prompt", "session/resume":
		return p.handleSessionTurn(ctx, env, pr)
	case "initialize":
		if env.Conn.RPC != nil && len(pr.ClientCapsRaw) > 0 {
			env.Conn.Caps = ParseClientCapabilities(pr.ClientCapsRaw)
			env.Conn.RPC.Caps = env.Conn.Caps
		} else if len(pr.ClientCapsRaw) > 0 {
			env.Conn.Caps = ParseClientCapabilities(pr.ClientCapsRaw)
		}
		return env.Conn.Writer.WriteResult(pr.ID, acpInitializeResult())
	case "authenticate":
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
	case "session/new":
		view := env.Registry.CreateSession(pr.CWD, pr.MCPServers)
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{
			"sessionId":     view.SessionID,
			"configOptions": view.ConfigOptions,
		})
	case "session/load":
		view, err := env.Registry.LoadSession(pr.ThreadID, pr.CWD, pr.MCPServers)
		if err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{
			"sessionId":     view.SessionID,
			"configOptions": view.ConfigOptions,
		})
	case "session/set_config_option":
		view, err := env.Registry.SetConfigOption(pr.ThreadID, pr.ConfigID, pr.ConfigValue)
		if err != nil {
			return env.Conn.Writer.WriteError(pr.ID, err)
		}
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{
			"configOptions": view.ConfigOptions,
		})
	case "session/close":
		env.Registry.CloseSession(pr.ThreadID)
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
	case "session/cancel":
		env.Registry.CancelSession(pr.ThreadID)
		return env.Conn.Writer.WriteResult(pr.ID, map[string]any{})
	default:
		return env.Conn.Writer.WriteError(pr.ID, clientErrorf(ErrMethodNotFound, "method not found"))
	}
}

func (p acpProtocol) handleSessionTurn(ctx context.Context, env ProtocolEnv, pr *parsedRequest) error {
	req := TurnRequest{
		SessionID:  pr.ThreadID,
		Prompt:     pr.Prompt,
		Responses:  pr.Responses,
		CWD:        pr.CWD,
		MCPServers: pr.MCPServers,
	}
	stream, err := env.Registry.RunTurn(ctx, req)
	if err != nil {
		if !IsClientError(err) {
			logTurnError(err, pr.AgentID, pr.ThreadID)
		}
		_ = env.Conn.Writer.WriteError(pr.ID, err)
		return err
	}
	defer func() {
		stream.Cancel()
		stream.Close()
	}()
	threadID := pr.ThreadID
	if stream.Harness != nil && stream.Harness.SessionId != "" {
		threadID = stream.Harness.SessionId
	}
	err = runTurnStream(ctx, env, p, threadID, stream, pr.ID)
	if err != nil && !IsClientError(err) {
		slog.Debug("acp turn stream ended", "error", err, "thread_id", threadID)
	}
	return err
}

func (p acpProtocol) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl {
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

func (p acpProtocol) OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
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
		// Backward compat: older yields without type are user selections.
		kind = "user_selection_choice"
	}
	switch kind {
	case "tool_permission":
		return resolvePermissionViaRequest(ctx, env, threadID, stream, ev)
	case "user_selection_choice":
		if !env.Conn.Caps.ElicitationForm {
			return nil, nil
		}
		return resolveSelectionViaElicitation(ctx, env, threadID, stream, ev)
	default:
		return nil, fmt.Errorf("unsupported interrupt type %q", kind)
	}
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
	question := ""
	if stream.Harness != nil {
		question = tacklr.AskUserQuestionFromState(stream.Harness.Runtime, ev.MessageID)
	}
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
	switch action {
	case "accept":
		return stream.ResumeInterrupts(ctx, map[string][]byte{interruptID: resolution})
	case "decline":
		return nil, fmt.Errorf("user declined to answer")
	default: // "cancel" — ElicitationResultToSelectionPayload rejects any other action.
		return nil, fmt.Errorf("user cancelled the prompt")
	}
}

// acpInitializeResult is the ACP initialize advertisement (wire shape).
func acpInitializeResult() map[string]any {
	return map[string]any{
		"protocolVersion": 1,
		"agentCapabilities": map[string]any{
			"loadSession": false,
			"promptCapabilities": map[string]any{
				"image":           false,
				"audio":           false,
				"embeddedContext": true,
			},
			"mcpCapabilities": map[string]any{
				"http": true,
				"sse":  true,
			},
			"sessionCapabilities": map[string]any{
				"close": struct{}{},
			},
		},
		"agentInfo": map[string]string{
			"name":    "tacklr",
			"title":   "Tacklr ACP",
			"version": "0.1.0",
		},
		"authMethods": []string{},
	}
}

func readHTTPBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return readAllLimited(r.Body, 16<<20)
}
