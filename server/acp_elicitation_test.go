package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/control"
)

// TestACP_elicitationForm_resolvesInterruptAndCompletes is the mid-turn
// elicitation outcome: form-capable client is asked via elicitation/create,
// accepts a choice, harness resumes, tool result + final message stream, and
// the prompt ends with stopReason end_turn.
func TestACP_elicitationForm_resolvesInterruptAndCompletes(t *testing.T) {
	optionsJSON := `[{"title":"Option A","description":"First","isRecommended":true},{"title":"Option B","description":"Second","isRecommended":false}]`
	interruptTool := tacklr.NewTool(tacklr.ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime *control.HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			choice := intr.(*control.UserSelectionInterrupt).ConfirmedChoice
			return "selected: " + choice.Title, nil
		},
	})

	var invokeCount int
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			invokeCount++
			if invokeCount == 1 {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
					{ID: "call_ask", CallID: "call_ask", Name: "ask_user", Arguments: `{}`},
				}, IsComplete: true}
				ch <- tacklr.LLMResponseChunk{IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "thanks for choosing", IsComplete: true}
		},
	}

	r := newTestRegistry(testStore(t), strategy, []*tacklr.Tool{interruptTool})
	srv := NewServer(r, ACP)

	// server reads serverIn; client writes clientToServer
	serverIn, clientToServer := io.Pipe()
	// client reads clientFromServer; server writes serverOut
	clientFromServer, serverOut := io.Pipe()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ServeStdio(ctx, serverIn, serverOut)
	}()
	t.Cleanup(func() {
		_ = clientToServer.Close()
		_ = serverIn.Close()
		_ = clientFromServer.Close()
		_ = serverOut.Close()
		cancel()
		select {
		case <-errCh:
		case <-time.After(time.Second):
		}
	})

	writeLine := func(s string) {
		t.Helper()
		if _, err := io.WriteString(clientToServer, s+"\n"); err != nil {
			t.Fatalf("write client→server: %v", err)
		}
	}

	writeLine(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}}`)
	writeLine(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)

	scanner := bufio.NewScanner(clientFromServer)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		sessionID       string
		sawElicitation  bool
		sawToolSelected bool
		sawFinalMsg     bool
		endTurn         bool
		promptDone      bool
		promptSent      bool
	)

	readFrame := func() map[string]any {
		t.Helper()
		lineCh := make(chan string, 1)
		errCh := make(chan error, 1)
		go func() {
			if scanner.Scan() {
				lineCh <- scanner.Text()
				return
			}
			errCh <- scanner.Err()
		}()
		select {
		case <-ctx.Done():
			t.Fatal("context done before prompt completed")
		case err := <-errCh:
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			t.Fatal("server closed stdout early")
		case line := <-lineCh:
			var frame map[string]any
			if err := json.Unmarshal([]byte(line), &frame); err != nil {
				t.Fatalf("bad frame %q: %v", line, err)
			}
			return frame
		case <-time.After(4 * time.Second):
			t.Fatal("timed out waiting for server frame")
		}
		return nil
	}

	for !promptDone {
		frame := readFrame()

		if res, ok := frame["result"].(map[string]any); ok {
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
	if invokeCount != 2 {
		t.Errorf("invokeCount = %d, want 2 (raise + continue after accept)", invokeCount)
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

// TestACP_sessionCheckpoint_secondPromptContinuesPlan: create_plan + complete_todo
// checkpoints, then a second session/prompt loads the store and list_plan shows
// restored statuses.
func TestACP_sessionCheckpoint_secondPromptContinuesPlan(t *testing.T) {
	store := testStore(t)
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
						{ID: "cp", CallID: "cp", Name: "create_plan", Arguments: `{"todos":[{"title":"Alpha","status":"pending","description":"a"},{"title":"Beta","status":"pending","description":"b"}]}`},
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

	r := newTestRegistry(store, strategy, nil)
	srv := NewServer(r, ACP)

	recNew := &recordingMessageWriter{}
	srv.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`), recNew)
	sessionID := recNew.Results[0].Result.(map[string]any)["sessionId"].(string)

	rec1 := &recordingMessageWriter{}
	srv.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"do the plan"}]}}`), rec1)
	if len(rec1.Errors) > 0 {
		t.Fatalf("turn1 errors: %#v", rec1.Errors)
	}

	mu.Lock()
	phase = "turn2"
	mu.Unlock()

	rec2 := &recordingMessageWriter{}
	srv.HandleMessage(context.Background(), []byte(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"continue"}]}}`), rec2)
	if len(rec2.Errors) > 0 {
		t.Fatalf("turn2 errors: %#v", rec2.Errors)
	}

	blob, _ := json.Marshal(rec2.framesAsMaps(t))
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
