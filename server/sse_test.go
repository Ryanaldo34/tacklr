package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/control"
)

func newSSERequest(t *testing.T, target string, body io.Reader) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, body)
	req.Header.Set("Accept", "text/event-stream")
	return req
}

func makeInterruptTool(t *testing.T, optionsJSON string) *tacklr.Tool {
	t.Helper()
	return tacklr.NewTool(tacklr.ToolConfig{
		Name: "ask_user",
		Handler: func(ctx context.Context, _ struct{}, runtime tacklr.HarnessRuntime) (string, error) {
			intr, err := runtime.RaiseInterrupt("user_selection_choice", []byte(optionsJSON))
			if err != nil {
				return "", err
			}
			choice := intr.(*control.UserSelectionInterrupt).ConfirmedChoice
			return "selected: " + choice.Title, nil
		},
	})
}

func parseSSEEvents(t *testing.T, body io.Reader) []sseEvent {
	t.Helper()
	var events []sseEvent
	scanner := bufio.NewScanner(body)
	var currentType string
	var currentData strings.Builder
	flush := func() {
		if currentType == "" {
			currentData.Reset()
			return
		}
		if currentType == "thread" {
			var te threadEvent
			if err := json.Unmarshal([]byte(currentData.String()), &te); err == nil {
				events = append(events, sseEvent{Type: currentType, Content: te.ThreadID})
			}
		} else {
			var ev sseEvent
			if err := json.Unmarshal([]byte(currentData.String()), &ev); err == nil {
				ev.Type = currentType
				events = append(events, ev)
			}
		}
		currentType = ""
		currentData.Reset()
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, "event: ") {
			currentType = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			currentData.WriteString(strings.TrimPrefix(line, "data: "))
		}
	}
	flush()
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan SSE body: %v", err)
	}
	return events
}

func TestHandlePrompt_generatesThreadID(t *testing.T) {
	store := testStore(t)
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hi", IsComplete: true}
		},
	}

	r := newTestRegistry(store, strategy, []*tacklr.Tool{})

	body := bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hello"}`))
	req := newSSERequest(t, "/", body)
	rec := httptest.NewRecorder()

	NewServer(r, SSE).serveHTTPSSE(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	headerThread := rec.Header().Get("X-Thread-ID")
	if headerThread == "" {
		t.Fatal("expected X-Thread-ID header")
	}

	events := parseSSEEvents(t, rec.Body)
	var foundThread bool
	for _, ev := range events {
		if ev.Type == "thread" && ev.Content == headerThread {
			foundThread = true
		}
	}
	if !foundThread {
		t.Errorf("expected thread event with id %q, got %+v", headerThread, events)
	}
}

func TestHandlePrompt_missingAcceptHeader_returnsNotAcceptable(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	body := bytes.NewReader([]byte(`{"agent_id":"default","prompt":"hello"}`))
	req := httptest.NewRequest(http.MethodPost, "/", body)
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()

	NewServer(r, SSE).serveHTTPSSE(rec, req)

	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotAcceptable)
	}
}

func TestHandleResume_resolvesInterrupt(t *testing.T) {
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

	r := newTestRegistry(store, strategy, []*tacklr.Tool{interruptTool})

	// First, prompt to raise an interrupt
	promptBody := bytes.NewReader([]byte(`{"agent_id":"default","prompt":"start"}`))
	promptReq := newSSERequest(t, "/", promptBody)
	promptRec := httptest.NewRecorder()
	NewServer(r, SSE).serveHTTPSSE(promptRec, promptReq)

	events := parseSSEEvents(t, promptRec.Body)
	var threadID, interruptID string
	for _, ev := range events {
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
	if threadID == "" {
		t.Fatal("thread id not emitted")
	}
	if interruptID == "" {
		t.Fatalf("interrupt id not emitted, events: %+v", events)
	}

	// Then resume with a valid selection
	resumeBody := fmt.Sprintf(`{"agent_id":"default","thread_id":%q,"responses":{%q:{"interruptId":%q,"selectionIdx":0}}}`, threadID, interruptID, interruptID)
	resumeReq := newSSERequest(t, "/resume", bytes.NewReader([]byte(resumeBody)))
	resumeRec := httptest.NewRecorder()
	NewServer(r, SSE).serveHTTPSSE(resumeRec, resumeReq)

	if resumeRec.Code != http.StatusOK {
		t.Fatalf("resume status = %d, want 200", resumeRec.Code)
	}

	resumeEvents := parseSSEEvents(t, resumeRec.Body)
	var foundToolResult, foundDone bool
	for _, ev := range resumeEvents {
		if ev.Type == "tool_result" {
			foundToolResult = true
		}
		if ev.Type == "message" && ev.Content == "done" {
			foundDone = true
		}
	}
	if !foundToolResult {
		t.Errorf("expected tool_result event after resume, got %+v", resumeEvents)
	}
	if !foundDone {
		t.Errorf("expected final done message after resume, got %+v", resumeEvents)
	}
}

func TestHandleResume_unknownThread_returnsError(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	body := bytes.NewReader([]byte(`{"agent_id":"default","thread_id":"nonexistent","responses":{"x":{}}}`))
	req := newSSERequest(t, "/resume", body)
	rec := httptest.NewRecorder()
	NewServer(r, SSE).serveHTTPSSE(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	events := parseSSEEvents(t, rec.Body)
	var foundError bool
	for _, ev := range events {
		if ev.Type == "error" && strings.Contains(ev.Error, "load session") {
			foundError = true
		}
	}
	if !foundError {
		t.Errorf("expected error event for unknown thread, got %+v", events)
	}
}

func TestHandleResume_invalidPayload_returnsError(t *testing.T) {
	store := testStore(t)
	optionsJSON := `[{"title":"Option A"}]`
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
			}
		},
	}

	r := newTestRegistry(store, strategy, []*tacklr.Tool{interruptTool})

	// Raise an interrupt first
	promptRec := httptest.NewRecorder()
	NewServer(r, SSE).serveHTTPSSE(promptRec, newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"start"}`))))

	events := parseSSEEvents(t, promptRec.Body)
	var threadID, interruptID string
	for _, ev := range events {
		if ev.Type == "thread" {
			threadID = ev.Content
		}
		if ev.Type == "yield" {
			var payload struct {
				InterruptId string `json:"interruptId"`
			}
			_ = json.Unmarshal(ev.Data, &payload)
			interruptID = payload.InterruptId
		}
	}
	if threadID == "" || interruptID == "" {
		t.Fatal("failed to raise interrupt in setup")
	}

	// Resume with out-of-bounds selectionIdx
	resumeBody := fmt.Sprintf(`{"agent_id":"default","thread_id":%q,"responses":{%q:{"interruptId":%q,"selectionIdx":99}}}`, threadID, interruptID, interruptID)
	resumeRec := httptest.NewRecorder()
	NewServer(r, SSE).serveHTTPSSE(resumeRec, newSSERequest(t, "/resume", bytes.NewReader([]byte(resumeBody))))

	resumeEvents := parseSSEEvents(t, resumeRec.Body)
	var foundError bool
	for _, ev := range resumeEvents {
		if ev.Type == "error" && strings.Contains(ev.Error, "invalid payload") {
			foundError = true
		}
	}
	if !foundError {
		t.Errorf("expected invalid payload error event, got %+v", resumeEvents)
	}
}
