package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/interrupt"
)

// acpRPC is an in-process ACP connection: HandleInbound plus ClientBridge
// for mid-turn client RPC (request_permission / elicitation).
type acpRPC struct {
	t      *testing.T
	ctx    context.Context
	proto  Protocol
	env    ProtocolEnv
	bridge *ClientBridge
	ch     chan []byte
}

type chanWriter struct{ ch chan []byte }

func (w *chanWriter) WriteFrame(data []byte) error {
	w.ch <- append([]byte(nil), data...)
	return nil
}

func (w *chanWriter) WriteResult(id json.RawMessage, result any) error {
	b, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	if err != nil {
		return err
	}
	w.ch <- b
	return nil
}

func (w *chanWriter) WriteError(id json.RawMessage, err error) error {
	b, mErr := json.Marshal(jsonRPCErrorBody(id, err))
	if mErr != nil {
		return mErr
	}
	w.ch <- b
	return nil
}

func newACPRPC(ctx context.Context, t *testing.T, srv *Server) *acpRPC {
	t.Helper()
	ch := make(chan []byte, 64)
	w := &chanWriter{ch: ch}
	bridge := NewClientBridge(w)
	return &acpRPC{
		t: t, ctx: ctx, proto: srv.Protocols[0], bridge: bridge, ch: ch,
		env: ProtocolEnv{Runtime: srv.Runtime, Catalog: srv.Catalog, Conn: &Conn{Writer: w, RPC: bridge}},
	}
}

func (c *acpRPC) write(s string) {
	c.t.Helper()
	var peek struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal([]byte(s), &peek)
	if peek.Method == "" {
		if !c.bridge.TryCompleteResponse([]byte(s)) {
			c.t.Fatalf("rpc response not matched: %s", s)
		}
		return
	}
	body := []byte(s)
	if peek.Method == "session/prompt" {
		go func() { _ = c.proto.HandleInbound(c.ctx, c.env, body) }()
		return
	}
	if err := c.proto.HandleInbound(c.ctx, c.env, body); err != nil {
		c.t.Logf("inbound: %v", err)
	}
}

func (c *acpRPC) frame() map[string]any {
	c.t.Helper()
	select {
	case <-c.ctx.Done():
		c.t.Fatal("context done before prompt completed")
	case raw := <-c.ch:
		var frame map[string]any
		if err := json.Unmarshal(raw, &frame); err != nil {
			c.t.Fatalf("bad frame %q: %v", raw, err)
		}
		return frame
	case <-time.After(4 * time.Second):
		c.t.Fatal("timed out waiting for server frame")
	}
	return nil
}

// TestACP_elicitationForm_resolvesInterruptAndCompletes is the mid-turn
// elicitation outcome: form-capable client is asked via elicitation/create,
// accepts a choice, harness resumes, tool result + final message stream, and
// the prompt ends with stopReason end_turn.
func TestACP_elicitationForm_resolvesInterruptAndCompletes(t *testing.T) {
	optionsJSON := `{"question":"Pick a path","options":[{"title":"Option A","description":"First","isRecommended":true},{"title":"Option B","description":"Second","isRecommended":false}]}`
	interruptTool := tacklr.NewTool(tacklr.ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
			intr, err := runtime.Park("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			choice := intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice
			return "selected: " + choice.Title, nil
		},
	})

	var invokeCount atomic.Int32
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n := invokeCount.Add(1)
			if n == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "call_ask", CallID: "call_ask", Name: "ask_user", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "thanks for choosing", IsComplete: true}
		},
	}

	r := newTestRuntime(t, strategy, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{interruptTool}}})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	rpc := newACPRPC(ctx, t, srv)
	writeLine := rpc.write
	readFrame := rpc.frame

	var (
		sessionID       string
		sawElicitation  bool
		sawToolSelected bool
		sawFinalMsg     bool
		endTurn         bool
		promptDone      bool
		promptSent      bool
		initDone        bool
		sessionReqSent  bool
	)

	writeLine(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}}`)

	for !promptDone {
		frame := readFrame()

		if res, ok := frame["result"].(map[string]any); ok {
			if idMatch(frame["id"], 1) {
				initDone = true
			}
			if sid, ok := res["sessionId"].(string); ok && sid != "" {
				sessionID = sid
			}
			if idMatch(frame["id"], 10) && res["stopReason"] == "end_turn" {
				endTurn = true
				promptDone = true
			}
		}
		if errObj, ok := frame["error"].(map[string]any); ok && idMatch(frame["id"], 10) {
			t.Fatalf("prompt error: %v", errObj)
		}

		if frame["method"] == "elicitation/create" {
			sawElicitation = true
			resp, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      frame["id"],
				"result": map[string]any{
					"action":  "accept",
					"content": map[string]any{"choice": "Option A"},
				},
			})
			writeLine(string(resp))
		}

		if frame["method"] == "session/update" {
			params, _ := frame["params"].(map[string]any)
			update, _ := params["update"].(map[string]any)
			switch update["sessionUpdate"] {
			case "tool_call_update":
				blob, _ := json.Marshal(update)
				if strings.Contains(string(blob), "selected: Option A") {
					sawToolSelected = true
				}
			case "agent_message_chunk":
				if content, ok := update["content"].(map[string]any); ok && content["text"] == "thanks for choosing" {
					sawFinalMsg = true
				}
			}
		}

		if initDone && !sessionReqSent {
			sessionReqSent = true
			writeLine(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
		}
		if sessionID != "" && !promptSent {
			promptSent = true
			writeLine(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"pick one"}]}}`)
		}
	}

	if !sawElicitation {
		t.Fatal("expected elicitation/create request to client")
	}
	if !sawToolSelected {
		t.Error("expected tool result reflecting Option A selection")
	}
	if !sawFinalMsg {
		t.Error("expected final agent message after resume")
	}
	if !endTurn {
		t.Error("expected prompt stopReason end_turn")
	}
	if got := invokeCount.Load(); got != 2 {
		t.Errorf("invokeCount = %d, want 2 (raise + continue after accept)", got)
	}
}

func idMatch(id any, want int) bool {
	switch v := id.(type) {
	case float64:
		return int(v) == want
	case json.Number:
		n, _ := v.Int64()
		return int(n) == want
	case string:
		return v == fmt.Sprintf("%d", want)
	default:
		return fmt.Sprint(id) == fmt.Sprintf("%d", want)
	}
}

// TestACP_requestPermission_rejectFailsToolAndCompletes: user rejects via
// session/request_permission; tool does not run; turn still completes.
func TestACP_requestPermission_rejectFailsToolAndCompletes(t *testing.T) {
	var ran bool
	sensitive := tacklr.NewTool(tacklr.ToolConfig{
		Name:   "sensitive",
		OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) {
			ran = true
			return "secret-ok", nil
		},
	})
	var invokeCount int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "call_sens", CallID: "call_sens", Name: "sensitive", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "moved on", IsComplete: true}
		},
	}

	r := newTestRuntime(t, strategy, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{sensitive}}})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	rpc := newACPRPC(ctx, t, srv)
	writeLine := rpc.write
	readFrame := rpc.frame
	writeLine(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`)
	writeLine(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)

	var (
		sessionID     string
		sawPermission bool
		sawDenied     bool
		sawFinalMsg   bool
		endTurn       bool
		promptDone    bool
		promptSent    bool
	)

	for !promptDone {
		frame := readFrame()

		if res, ok := frame["result"].(map[string]any); ok {
			if sid, ok := res["sessionId"].(string); ok && sid != "" {
				sessionID = sid
			}
			if idMatch(frame["id"], 21) && res["stopReason"] == "end_turn" {
				endTurn = true
				promptDone = true
			}
		}
		if errObj, ok := frame["error"].(map[string]any); ok && idMatch(frame["id"], 21) {
			t.Fatalf("prompt error: %v", errObj)
		}

		if frame["method"] == "session/request_permission" {
			sawPermission = true
			params, _ := frame["params"].(map[string]any)
			opts, _ := params["options"].([]any)
			if len(opts) < 2 {
				t.Fatalf("expected permission options, got %v", opts)
			}
			resp, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      frame["id"],
				"result": map[string]any{
					"outcome": map[string]any{
						"outcome":  "selected",
						"optionId": "reject-once",
					},
				},
			})
			writeLine(string(resp))
		}

		if frame["method"] == "session/update" {
			params, _ := frame["params"].(map[string]any)
			update, _ := params["update"].(map[string]any)
			switch update["sessionUpdate"] {
			case "tool_call_update":
				blob, _ := json.Marshal(update)
				if strings.Contains(string(blob), "rejected by the user") {
					sawDenied = true
				}
			case "agent_message_chunk":
				if content, ok := update["content"].(map[string]any); ok && content["text"] == "moved on" {
					sawFinalMsg = true
				}
			}
		}

		if sessionID != "" && !promptSent {
			promptSent = true
			writeLine(`{"jsonrpc":"2.0","id":21,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"run sensitive"}]}}`)
		}
	}

	if !sawPermission {
		t.Fatal("expected session/request_permission")
	}
	if !sawDenied {
		t.Error("expected failed tool update with rejected by the user")
	}
	if ran {
		t.Error("handler must not run on reject")
	}
	if !sawFinalMsg {
		t.Error("expected final agent message")
	}
	if !endTurn {
		t.Error("expected end_turn")
	}
}

// TestACP_requestPermission_cancelledEndsPrompt: cancelled outcome ends the turn.
func TestACP_requestPermission_cancelledEndsPrompt(t *testing.T) {
	sensitive := tacklr.NewTool(tacklr.ToolConfig{
		Name:    "sensitive",
		OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(ctx context.Context) (string, error) { return "nope", nil },
	})
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
				{ID: "c1", CallID: "c1", Name: "sensitive", Arguments: `{}`},
			}, IsComplete: true}
			ch <- tacklr.LLMResponseChunk{IsComplete: true}
		},
	}
	r := newTestRuntime(t, strategy, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{sensitive}}})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	rpc := newACPRPC(ctx, t, srv)
	write := rpc.write
	write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`)
	write(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var sessionID string
	var sawPermission, promptSent, promptDone, sawError bool

	for !promptDone {
		frame := rpc.frame()
		if res, ok := frame["result"].(map[string]any); ok {
			if sid, ok := res["sessionId"].(string); ok && sid != "" {
				sessionID = sid
			}
		}
		if frame["method"] == "session/request_permission" {
			sawPermission = true
			resp, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      frame["id"],
				"result":  map[string]any{"outcome": map[string]any{"outcome": "cancelled"}},
			})
			write(string(resp))
		}
		if errObj, ok := frame["error"].(map[string]any); ok && idMatch(frame["id"], 22) {
			sawError = true
			promptDone = true
			_ = errObj
		}
		if sessionID != "" && !promptSent {
			promptSent = true
			write(`{"jsonrpc":"2.0","id":22,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"x"}]}}`)
		}
	}
	if !sawPermission {
		t.Fatal("expected request_permission")
	}
	if !sawError {
		t.Fatal("expected prompt error after cancelled permission")
	}
}

func parkOnceStrategy(name string) *mockInferenceStrategy {
	var n atomic.Int32
	return &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if n.Add(1) == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "p1", CallID: "p1", Name: name, Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
		},
	}
}

func selectionParkTool(payload string) *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
			_, err := runtime.Park("user_selection_choice", []byte(payload))
			return "", err
		},
	})
}

func TestACP_interruptClientOutcomes(t *testing.T) {
	selectionJSON := `{"question":"Pick","options":[{"title":"A"},{"title":"B"}]}`

	t.Run("httpInboundParksWithoutRPC", func(t *testing.T) {
		r := newTestRuntime(t, parkOnceStrategy("ask_user"), durable.AgentSpec{
			Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{selectionParkTool(selectionJSON)}},
		})
		sid := acpSessionID(t, serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
		rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"x"}]}}`)
		var msg string
		for _, f := range parseACPFrames(t, rec.Body) {
			if errObj, ok := f["error"].(map[string]any); ok {
				msg, _ = errObj["message"].(string)
			}
		}
		if !strings.Contains(msg, "user input") {
			t.Fatalf("park without RPC: %s", rec.Body.String())
		}
	})

	t.Run("formOffParksTurn", func(t *testing.T) {
		r := newTestRuntime(t, parkOnceStrategy("ask_user"), durable.AgentSpec{
			Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{selectionParkTool(selectionJSON)}},
		})
		srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		rpc := newACPRPC(ctx, t, srv)
		rpc.write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
		rpc.write(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
		var sid string
		var sent, done bool
		for !done {
			frame := rpc.frame()
			if res, ok := frame["result"].(map[string]any); ok {
				if s, ok := res["sessionId"].(string); ok && s != "" {
					sid = s
				}
			}
			if frame["method"] == "elicitation/create" {
				t.Fatal("form-off client must not receive elicitation/create")
			}
			if errObj, ok := frame["error"].(map[string]any); ok && idMatch(frame["id"], 10) {
				msg, _ := errObj["message"].(string)
				if !strings.Contains(msg, "user input") {
					t.Fatalf("form-off park: %v", errObj)
				}
				done = true
			}
			if sid != "" && !sent {
				sent = true
				rpc.write(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sid + `","prompt":[{"type":"text","text":"x"}]}}`)
			}
		}
	})

	t.Run("unknownInterruptParks", func(t *testing.T) {
		waiting := tacklr.NewTool(tacklr.ToolConfig{
			Name: "wait_child",
			Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
				_, err := runtime.Park("child_waiting", []byte(`{"kind":"child_waiting"}`))
				return "", err
			},
		})
		r := newTestRuntime(t, parkOnceStrategy("wait_child"), durable.AgentSpec{
			Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{waiting}},
		})
		srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		rpc := newACPRPC(ctx, t, srv)
		rpc.write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}}`)
		rpc.write(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
		var sid string
		var sent, done bool
		for !done {
			frame := rpc.frame()
			if res, ok := frame["result"].(map[string]any); ok {
				if s, ok := res["sessionId"].(string); ok && s != "" {
					sid = s
				}
			}
			if errObj, ok := frame["error"].(map[string]any); ok && idMatch(frame["id"], 10) {
				msg, _ := errObj["message"].(string)
				if !strings.Contains(msg, "user input") {
					t.Fatalf("unknown interrupt park: %v", errObj)
				}
				done = true
			}
			if sid != "" && !sent {
				sent = true
				rpc.write(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sid + `","prompt":[{"type":"text","text":"x"}]}}`)
			}
		}
	})

	t.Run("elicitationDeclineEndsTurn", func(t *testing.T) {
		promptInterruptReply(t, selectionParkTool(selectionJSON), parkOnceStrategy("ask_user"),
			`{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}`,
			"elicitation/create",
			func(id any) map[string]any {
				return map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"action": "decline"}}
			})
	})

	t.Run("elicitationUnknownChoice", func(t *testing.T) {
		promptInterruptReply(t, selectionParkTool(selectionJSON), parkOnceStrategy("ask_user"),
			`{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}`,
			"elicitation/create",
			func(id any) map[string]any {
				return map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{
					"action": "accept", "content": map[string]any{"choice": "nope"},
				}}
			})
	})

	t.Run("elicitationBadJSON", func(t *testing.T) {
		promptInterruptReply(t, selectionParkTool(selectionJSON), parkOnceStrategy("ask_user"),
			`{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}`,
			"elicitation/create",
			func(id any) map[string]any {
				return map[string]any{"jsonrpc": "2.0", "id": id, "result": "not-an-object"}
			})
	})

	t.Run("elicitationUnknownAction", func(t *testing.T) {
		promptInterruptReply(t, selectionParkTool(selectionJSON), parkOnceStrategy("ask_user"),
			`{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}`,
			"elicitation/create",
			func(id any) map[string]any {
				return map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"action": "nope"}}
			})
	})

	t.Run("elicitationMissingChoice", func(t *testing.T) {
		promptInterruptReply(t, selectionParkTool(selectionJSON), parkOnceStrategy("ask_user"),
			`{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}`,
			"elicitation/create",
			func(id any) map[string]any {
				return map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{
					"action": "accept", "content": map[string]any{},
				}}
			})
	})

	t.Run("permissionUnknownOutcome", func(t *testing.T) {
		sensitive := tacklr.NewTool(tacklr.ToolConfig{
			Name:    "sensitive",
			OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
			Handler: func(ctx context.Context) (string, error) { return "nope", nil },
		})
		promptInterruptReply(t, sensitive, parkOnceStrategy("sensitive"),
			`{"protocolVersion":1}`,
			"session/request_permission",
			func(id any) map[string]any {
				return map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{
					"outcome": map[string]any{"outcome": "nope"},
				}}
			})
	})

	t.Run("permissionBadJSON", func(t *testing.T) {
		sensitive := tacklr.NewTool(tacklr.ToolConfig{
			Name:    "sensitive",
			OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
			Handler: func(ctx context.Context) (string, error) { return "nope", nil },
		})
		promptInterruptReply(t, sensitive, parkOnceStrategy("sensitive"),
			`{"protocolVersion":1}`,
			"session/request_permission",
			func(id any) map[string]any {
				return map[string]any{"jsonrpc": "2.0", "id": id, "result": "not-an-object"}
			})
	})

	t.Run("permissionMissingOptionID", func(t *testing.T) {
		sensitive := tacklr.NewTool(tacklr.ToolConfig{
			Name:    "sensitive",
			OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
			Handler: func(ctx context.Context) (string, error) { return "nope", nil },
		})
		promptInterruptReply(t, sensitive, parkOnceStrategy("sensitive"),
			`{"protocolVersion":1}`,
			"session/request_permission",
			func(id any) map[string]any {
				return map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{
					"outcome": map[string]any{"outcome": "selected"},
				}}
			})
	})

	t.Run("permissionCallWriteFails", func(t *testing.T) {
		sensitive := tacklr.NewTool(tacklr.ToolConfig{
			Name:    "sensitive",
			OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
			Handler: func(ctx context.Context) (string, error) { return "nope", nil },
		})
		interruptCallWriteFails(t, sensitive, parkOnceStrategy("sensitive"), "session/request_permission", false)
	})

	t.Run("elicitationCallWriteFails", func(t *testing.T) {
		interruptCallWriteFails(t, selectionParkTool(selectionJSON), parkOnceStrategy("ask_user"), "elicitation/create", true)
	})
}

func interruptCallWriteFails(t *testing.T, tool *tacklr.Tool, strategy *mockInferenceStrategy, needle string, form bool) {
	t.Helper()
	r := newTestRuntime(t, strategy, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{tool}}})
	w := &failOnRPCWriter{needle: needle}
	bridge := NewClientBridge(w)
	bridge.MarkInitialized()
	if form {
		bridge.SetCaps(ClientCapabilities{ElicitationForm: true})
	}
	proto := NewACPProtocol(NewMemoryWireStore())
	env := ProtocolEnv{Runtime: r.Runtime, Catalog: r.Catalog, Conn: &Conn{Writer: w, RPC: bridge}}
	if err := proto.HandleInbound(t.Context(), env, []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)); err != nil {
		t.Fatal(err)
	}
	sid := w.Results[0].Result.(map[string]any)["sessionId"].(string)
	_ = proto.HandleInbound(t.Context(), env, []byte(`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"x"}]}}`))
	found := len(w.Errors) > 0
	for _, frame := range w.SnapshotFrames() {
		if strings.Contains(string(frame), "cancelled") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want %s call failure or cancelled stopReason", needle)
	}
}

type failOnRPCWriter struct {
	recordingMessageWriter
	needle string
}

func (f *failOnRPCWriter) WriteFrame(data []byte) error {
	if strings.Contains(string(data), f.needle) {
		return context.Canceled
	}
	return f.recordingMessageWriter.WriteFrame(data)
}

func promptInterruptReply(t *testing.T, tool *tacklr.Tool, strategy *mockInferenceStrategy, initParams, method string, reply func(id any) map[string]any) {
	t.Helper()
	r := newTestRuntime(t, strategy, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: []*tacklr.Tool{tool}}})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	rpc := newACPRPC(ctx, t, srv)
	rpc.write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":` + initParams + `}`)
	rpc.write(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
	var sid string
	var sent, done, sawMethod bool
	for !done {
		frame := rpc.frame()
		if res, ok := frame["result"].(map[string]any); ok {
			if s, ok := res["sessionId"].(string); ok && s != "" {
				sid = s
			}
		}
		if frame["method"] == method {
			sawMethod = true
			body, _ := json.Marshal(reply(frame["id"]))
			rpc.write(string(body))
		}
		if _, ok := frame["error"].(map[string]any); ok && idMatch(frame["id"], 10) {
			done = true
		}
		if sid != "" && !sent {
			sent = true
			rpc.write(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sid + `","prompt":[{"type":"text","text":"x"}]}}`)
		}
	}
	if !sawMethod {
		t.Fatalf("expected %s", method)
	}
}

// TestACP_createPlan_streamsPlanUpdate: create_plan streams plan sessionUpdate over ACP.
func TestACP_createPlan_streamsPlanUpdate(t *testing.T) {
	var n int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n++
			if n == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "cp", CallID: "cp", Name: "create_plan", Arguments: `{"plan":"P","todos":[{"title":"One","status":"pending","description":"d"},{"title":"Two","status":"pending","description":""}]}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "planned", IsComplete: true}
		},
	}
	r := newTestRuntime(t, strategy, durable.AgentSpec{})
	recNew := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID := acpSessionID(t, recNew)
	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"plan it"}]}}`)
	frames := parseACPFrames(t, rec.Body)
	var sawPlan bool
	for _, f := range frames {
		if f["method"] != "session/update" {
			continue
		}
		params, _ := f["params"].(map[string]any)
		update, _ := params["update"].(map[string]any)
		if update["sessionUpdate"] == "plan" {
			sawPlan = true
			entries, _ := update["entries"].([]any)
			if len(entries) < 1 {
				t.Fatalf("plan entries empty: %v", update)
			}
		}
	}
	if !sawPlan {
		blob, _ := json.Marshal(frames)
		t.Fatalf("expected sessionUpdate=plan frame, got %s", blob)
	}
}

// TestACP_sessionCheckpoint_secondPromptContinuesPlan: create_plan + complete_todo
// checkpoints, then a second session/prompt loads the store and list_plan shows
// restored statuses.
func TestACP_sessionCheckpoint_secondPromptContinuesPlan(t *testing.T) {
	var mu sync.Mutex
	phase := "turn1"
	var turn1Steps, turn2Steps int

	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			mu.Lock()
			p := phase
			mu.Unlock()
			if p == "turn1" {
				turn1Steps++
				switch turn1Steps {
				case 1:
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
						{ID: "cp", CallID: "cp", Name: "create_plan", Arguments: `{"plan":"P","todos":[{"title":"Alpha","status":"pending","description":"a"},{"title":"Beta","status":"pending","description":"b"}]}`},
					}, IsComplete: true}
					ch <- tacklr.LLMResponseChunk{IsComplete: true}
				case 2:
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
						{ID: "ct", CallID: "ct", Name: "complete_todo", Arguments: `{"title":"Alpha"}`},
					}, IsComplete: true}
					ch <- tacklr.LLMResponseChunk{IsComplete: true}
				case 3:
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "handoff alpha", IsComplete: true}
				default:
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "turn1 done", IsComplete: true}
				}
				return
			}
			turn2Steps++
			if turn2Steps == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "lp", CallID: "lp", Name: "list_plan", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "turn2 continued", IsComplete: true}
		},
	}

	r := newTestRuntime(t, strategy, durable.AgentSpec{})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))

	recNew := &recordingMessageWriter{}
	srv.inbound(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`), recNew)
	sessionID := recNew.Results[0].Result.(map[string]any)["sessionId"].(string)

	rec1 := &recordingMessageWriter{}
	srv.inbound(context.Background(), []byte(`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"do the plan"}]}}`), rec1)
	if len(rec1.Errors) > 0 {
		t.Fatalf("turn1 errors: %#v", rec1.Errors)
	}

	mu.Lock()
	phase = "turn2"
	mu.Unlock()

	rec2 := &recordingMessageWriter{}
	srv.inbound(context.Background(), []byte(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"continue"}]}}`), rec2)
	if len(rec2.Errors) > 0 {
		t.Fatalf("turn2 errors: %#v", rec2.Errors)
	}

	blob, _ := json.Marshal(rec2.FramesAsMaps(t))
	out := string(blob)
	if !strings.Contains(out, "Alpha") || !strings.Contains(out, "completed") {
		t.Fatalf("second turn should list restored plan with Alpha completed; frames=%s", out)
	}
	if !strings.Contains(out, "Beta") {
		t.Fatalf("second turn should list Beta remaining; frames=%s", out)
	}
	if !strings.Contains(out, "turn2 continued") {
		t.Fatalf("expected turn2 final message; frames=%s", out)
	}
	if turn2Steps < 2 {
		t.Errorf("turn2Steps = %d, want >= 2", turn2Steps)
	}
}
