package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/ryanaldo34/tacklr"
)

func wsURL(srv *httptest.Server, path string) string {
	return strings.Replace(srv.URL, "http://", "ws://", 1) + path
}

func TestHandleWebSocket_promptStreamsEvents(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hi", IsComplete: true}
		},
	}

	srv := &Server{
		provider: &mockAgentProvider{strategy: strategy, tools: []*tacklr.Tool{}},
		store:    store,
	}

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, wsURL(httpSrv, "/"), nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer c.CloseNow()

	if err := wsjson.Write(ctx, c, turnRequest{AgentID: "default", Prompt: "hello"}); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	var events []wsServerEvent
	for {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var ev wsServerEvent
		err := wsjson.Read(readCtx, c, &ev)
		cancel()
		if err != nil {
			break
		}
		events = append(events, ev)
	}

	var foundThread, foundMessage bool
	for _, ev := range events {
		switch ev.Type {
		case "thread":
			foundThread = true
		case "message":
			if ev.Content == "hi" {
				foundMessage = true
			}
		}
	}
	if !foundThread {
		t.Errorf("expected thread event, got %+v", events)
	}
	if !foundMessage {
		t.Errorf("expected message event with 'hi', got %+v", events)
	}
}

func TestHandleWebSocket_resumeResolvesInterrupt(t *testing.T) {
	store := testStore(t)
	optionsJSON := `[{"title":"Option A","description":"First","isRecommended":true},{"title":"Option B","description":"Second","isRecommended":false}]`
	interruptTool := makeInterruptTool(t, optionsJSON)

	var callCount int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			callCount++
			if callCount == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "call_int", CallID: "call_int", Name: "ask_user", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
		},
	}

	srv := &Server{
		provider: &mockAgentProvider{strategy: strategy, tools: []*tacklr.Tool{interruptTool}},
		store:    store,
	}

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	ctx := context.Background()

	// First, prompt to raise an interrupt.
	promptConn, _, err := websocket.Dial(ctx, wsURL(httpSrv, "/"), nil)
	if err != nil {
		t.Fatalf("dial prompt websocket: %v", err)
	}
	if err := wsjson.Write(ctx, promptConn, turnRequest{AgentID: "default", Prompt: "start"}); err != nil {
		t.Fatalf("write prompt: %v", err)
	}

	var threadID, interruptID string
	for {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var ev wsServerEvent
		err := wsjson.Read(readCtx, promptConn, &ev)
		cancel()
		if err != nil {
			break
		}
		if ev.Type == "thread" {
			threadID = ev.Content
		}
		if ev.Type == "yield" {
			var payload struct {
				InterruptId string          `json:"interruptId"`
				Data        json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(ev.Data, &payload); err == nil {
				interruptID = payload.InterruptId
			}
		}
	}
	_ = promptConn.CloseNow()
	if threadID == "" {
		t.Fatal("thread id not emitted")
	}
	if interruptID == "" {
		t.Fatalf("interrupt id not emitted")
	}

	// Then resume via the /resume WebSocket endpoint.
	resumeConn, _, err := websocket.Dial(ctx, wsURL(httpSrv, "/resume"), nil)
	if err != nil {
		t.Fatalf("dial resume websocket: %v", err)
	}
	defer resumeConn.CloseNow()

	resumeBody := fmt.Sprintf(`{"agent_id":"default","thread_id":%q,"responses":{%q:{"interruptId":%q,"selectionIdx":0}}}`, threadID, interruptID, interruptID)
	var resumeMsg turnRequest
	if err := json.Unmarshal([]byte(resumeBody), &resumeMsg); err != nil {
		t.Fatalf("unmarshal resume message: %v", err)
	}
	if err := wsjson.Write(ctx, resumeConn, resumeMsg); err != nil {
		t.Fatalf("write resume: %v", err)
	}

	var foundToolResult, foundDone bool
	for {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var ev wsServerEvent
		err := wsjson.Read(readCtx, resumeConn, &ev)
		cancel()
		if err != nil {
			break
		}
		if ev.Type == "tool_result" {
			foundToolResult = true
		}
		if ev.Type == "message" && ev.Content == "done" {
			foundDone = true
		}
	}

	if !foundToolResult {
		t.Errorf("expected tool_result event after resume")
	}
	if !foundDone {
		t.Errorf("expected final done message after resume")
	}
}
