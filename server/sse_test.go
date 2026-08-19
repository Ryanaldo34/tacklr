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
	"github.com/ryanaldo34/tacklr/interrupt"
)

func newSSERequest(t *testing.T, target string, body io.Reader) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, body)
	req.Header.Set("Accept", "text/event-stream")
	return req
}

func TestSSEProtocol_HandleInbound_noop(t *testing.T) {
	if err := SSE.HandleInbound(context.Background(), ProtocolEnv{}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSSERequest_errorAndOKPaths(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{`, "invalid JSON"},
		{`{"prompt":"x"}`, "agent_id is required"},
		{`{"agent_id":"a","responses":{"i":{}},"prompt":"x"}`, "thread_id is required"},
		{`{"agent_id":"a","thread_id":"t","responses":{"i":{}},"prompt":"x"}`, "prompt is not allowed"},
		{`{"agent_id":"a","thread_id":"t","responses":{"i":{`, "invalid JSON"},
		{`{"agent_id":"a"}`, "prompt is required"},
	}
	for _, tc := range cases {
		_, err := validateSSERequest([]byte(tc.body))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("body %s: err=%v want %q", tc.body, err, tc.want)
		}
	}
	pr, err := validateSSERequest([]byte(`{"agent_id":"default","prompt":"hi","thread_id":"t1"}`))
	if err != nil || pr.Prompt != "hi" {
		t.Fatalf("ok: %+v %v", pr, err)
	}
	// Resume responses path.
	pr, err = validateSSERequest([]byte(`{"agent_id":"default","thread_id":"t1","responses":{"i":{"optionId":"allow-once"}}}`))
	if err != nil || len(pr.Responses) != 1 {
		t.Fatalf("resume: %+v %v", pr, err)
	}
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
			choice := intr.(*interrupt.UserSelectionInterrupt).ConfirmedChoice
			return "selected: " + choice.Title, nil
		},
	})
}

func parseSSEEvents(t *testing.T, body io.Reader) []presentationEvent {
	t.Helper()
	var events []presentationEvent
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
				events = append(events, presentationEvent{Type: currentType, Content: te.ThreadID})
			}
		} else {
			var ev presentationEvent
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

	NewServer(r, SSE).HTTPMux().ServeHTTP(rec, req)

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

	NewServer(r, SSE).HTTPMux().ServeHTTP(rec, req)

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
	NewServer(r, SSE).HTTPMux().ServeHTTP(promptRec, promptReq)

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
	NewServer(r, SSE).HTTPMux().ServeHTTP(resumeRec, resumeReq)

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

func TestHandleResume_toolPermission_allowAndReject(t *testing.T) {
	// SSE yield + /resume for tool_permission: allow runs the handler; reject
	// surfaces a permission-denied tool result without executing it.
	for _, tc := range []struct {
		name       string
		optionID   string
		wantResult string
		wantDenied bool
	}{
		{name: "allow", optionID: "allow-once", wantResult: "secret-ok"},
		{name: "reject", optionID: "reject-once", wantDenied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := testStore(t)
			var ran bool
			sensitive := tacklr.NewTool(tacklr.ToolConfig{
				Name:   "sensitive",
				OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
				Handler: func(ctx context.Context) (string, error) {
					ran = true
					return "secret-ok", nil
				},
			})
			var callCount int
			strategy := &mockInferenceStrategy{
				invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
					callCount++
					if callCount == 1 {
						ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, ToolCalls: []tacklr.ToolCall{
							{ID: "call_sens", CallID: "call_sens", Name: "sensitive", Arguments: `{}`},
						}, IsComplete: true}
						ch <- tacklr.LLMResponseChunk{IsComplete: true}
						return
					}
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "done", IsComplete: true}
				},
			}
			r := newTestRegistry(store, strategy, []*tacklr.Tool{sensitive})

			promptRec := httptest.NewRecorder()
			NewServer(r, SSE).HTTPMux().ServeHTTP(promptRec, newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"start"}`))))

			events := parseSSEEvents(t, promptRec.Body)
			var threadID, interruptID string
			for _, ev := range events {
				if ev.Type == "thread" {
					threadID = ev.Content
				}
				if ev.Type == "yield" {
					var payload struct {
						InterruptId string `json:"interruptId"`
						Type        string `json:"type"`
					}
					if err := json.Unmarshal(ev.Data, &payload); err == nil {
						interruptID = payload.InterruptId
						if payload.Type != "tool_permission" {
							t.Fatalf("yield type = %q", payload.Type)
						}
					}
				}
			}
			if threadID == "" || interruptID == "" {
				t.Fatalf("missing thread/interrupt, events=%+v", events)
			}

			resumeBody := fmt.Sprintf(`{"agent_id":"default","thread_id":%q,"responses":{%q:{"optionId":%q}}}`, threadID, interruptID, tc.optionID)
			resumeRec := httptest.NewRecorder()
			NewServer(r, SSE).HTTPMux().ServeHTTP(resumeRec, newSSERequest(t, "/resume", bytes.NewReader([]byte(resumeBody))))
			if resumeRec.Code != http.StatusOK {
				t.Fatalf("resume status = %d", resumeRec.Code)
			}

			resumeEvents := parseSSEEvents(t, resumeRec.Body)
			var foundResult, foundDone, foundDenied bool
			for _, ev := range resumeEvents {
				if ev.Type == "tool_result" {
					if strings.Contains(ev.Content, "secret-ok") {
						foundResult = true
					}
					if strings.Contains(ev.Content, "permission denied") {
						foundDenied = true
					}
				}
				if ev.Type == "message" && ev.Content == "done" {
					foundDone = true
				}
			}
			if !foundDone {
				t.Errorf("expected done message, got %+v", resumeEvents)
			}
			if tc.wantDenied {
				if !foundDenied {
					t.Errorf("expected permission denied tool_result, got %+v", resumeEvents)
				}
				if ran {
					t.Error("handler ran on reject")
				}
			} else if !foundResult {
				t.Errorf("expected tool_result %q, got %+v", tc.wantResult, resumeEvents)
			}
		})
	}
}

func TestHandleResume_unknownThread_returnsError(t *testing.T) {
	store := testStore(t)
	r := newTestRegistry(store, &mockInferenceStrategy{}, []*tacklr.Tool{})

	body := bytes.NewReader([]byte(`{"agent_id":"default","thread_id":"nonexistent","responses":{"x":{}}}`))
	req := newSSERequest(t, "/resume", body)
	rec := httptest.NewRecorder()
	NewServer(r, SSE).HTTPMux().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	events := parseSSEEvents(t, rec.Body)
	var foundError bool
	for _, ev := range events {
		if ev.Type == "error" && strings.Contains(ev.ErrorText, "load session") {
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
	NewServer(r, SSE).HTTPMux().ServeHTTP(promptRec, newSSERequest(t, "/", bytes.NewReader([]byte(`{"agent_id":"default","prompt":"start"}`))))

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
	NewServer(r, SSE).HTTPMux().ServeHTTP(resumeRec, newSSERequest(t, "/resume", bytes.NewReader([]byte(resumeBody))))

	resumeEvents := parseSSEEvents(t, resumeRec.Body)
	var foundError bool
	for _, ev := range resumeEvents {
		if ev.Type == "error" && strings.Contains(ev.ErrorText, "invalid payload") {
			foundError = true
		}
	}
	if !foundError {
		t.Errorf("expected invalid payload error event, got %+v", resumeEvents)
	}
}
