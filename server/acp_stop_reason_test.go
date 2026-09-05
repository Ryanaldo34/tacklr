package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"

	"github.com/ryanaldo34/tacklr"
)

// TestACP_prompt_stopReason_refusal: model terminal ErrModelRefused → PromptResponse refusal.
func TestACP_prompt_stopReason_refusal(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventError,
				Error:      tacklr.ErrModelRefused,
				Content:    "model refused",
				IsComplete: true,
			}
		},
	}
	assertACPStopReason(t, strategy, nil, "refusal")
}

func TestACP_prompt_stopReason_maxTokens(t *testing.T) {
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{
				Type:       tacklr.StreamEventError,
				Error:      tacklr.ErrMaxTokens,
				IsComplete: true,
			}
		},
	}
	assertACPStopReason(t, strategy, nil, "max_tokens")
}

func TestACP_prompt_stopReason_maxTurnRequests(t *testing.T) {
	// Always request a tool so the harness would loop; MaxTurnRequests=1 ends before 2nd invoke.
	ping := tacklr.NewTool(tacklr.ToolConfig{
		Name: "ping",
		Handler: func(ctx context.Context) (string, error) {
			return "pong", nil
		},
	})
	var n int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			n++
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{
					{ID: "c1", CallID: "c1", Name: "ping", Arguments: `{}`},
				},
				IsComplete: true,
			}
			ch <- tacklr.LLMResponseChunk{IsComplete: true}
		},
	}
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	r.Catalog.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{
				MaxWindowSize:   8192,
				SystemPrompt:    "test",
				MaxTurnRequests: 1,
			},
			Model: strategy,
			Tools: []*tacklr.Tool{ping},
		},
	})
	recNew := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID := acpSessionID(t, recNew)
	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"go"}]}}`)
	reason := promptStopReason(t, rec)
	if reason != "max_turn_requests" {
		t.Fatalf("stopReason = %q, want max_turn_requests (invokes=%d frames=%v)", reason, n, parseACPFrames(t, rec.Body))
	}
	if n != 1 {
		t.Errorf("model invokes = %d, want 1", n)
	}
}

func assertACPStopReason(t *testing.T, strategy *mockInferenceStrategy, tools []*tacklr.Tool, want string) {
	t.Helper()
	r := newTestRuntime(t, strategy, durable.AgentSpec{Options: tacklr.AgentOptions{Tools: tools}})
	recNew := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID := acpSessionID(t, recNew)
	rec := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"hi"}]}}`)
	got := promptStopReason(t, rec)
	if got != want {
		blob, _ := json.Marshal(parseACPFrames(t, rec.Body))
		t.Fatalf("stopReason = %q, want %q frames=%s", got, want, blob)
	}
}

func TestACP_sessionCancel_stopReasonCancelled(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			once.Do(func() { close(started) })
			<-ctx.Done()
		},
	}
	r := newTestRuntime(t, strategy, durable.AgentSpec{})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(NewMemoryWireStore()))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	rpc := newACPRPC(ctx, t, srv)
	rpc.write(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	rpc.write(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)

	var sessionID string
	for sessionID == "" {
		frame := rpc.frame()
		if res, ok := frame["result"].(map[string]any); ok {
			if sid, ok := res["sessionId"].(string); ok && sid != "" {
				sessionID = sid
			}
		}
	}
	rpc.write(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"hi"}]}}`)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not start")
	}
	rpc.write(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"` + sessionID + `"}}`)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		frame := rpc.frame()
		if res, ok := frame["result"].(map[string]any); ok && idMatch(frame["id"], 10) && res["stopReason"] == "cancelled" {
			return
		}
	}
	t.Fatal("expected prompt stopReason cancelled after session/cancel")
}

func promptStopReason(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	frames := parseACPFrames(t, rec.Body)
	for _, f := range frames {
		if res, ok := f["result"].(map[string]any); ok {
			if sr, ok := res["stopReason"].(string); ok && sr != "" {
				return sr
			}
		}
	}
	// Also accept WriteResult-only responses (single JSON object body).
	var single map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &single); err == nil {
		if res, ok := single["result"].(map[string]any); ok {
			if sr, _ := res["stopReason"].(string); sr != "" {
				return sr
			}
		}
	}
	return ""
}
