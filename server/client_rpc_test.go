package server

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/control"
)

type recordingWriter struct {
	mu     sync.Mutex
	frames [][]byte
}

func (r *recordingWriter) WriteResult(id json.RawMessage, result any) error { return nil }
func (r *recordingWriter) WriteError(id json.RawMessage, err error) error   { return nil }
func (r *recordingWriter) WriteFrame(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frames = append(r.frames, append([]byte(nil), data...))
	return nil
}

func TestClientBridge_CallAndResponse(t *testing.T) {
	w := &recordingWriter{}
	b := NewClientBridge(w)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var got json.RawMessage
	var callErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		got, callErr = b.Call(ctx, "elicitation/create", map[string]any{"mode": "form"})
	}()

	// Wait for request frame
	deadline := time.Now().Add(time.Second)
	for {
		w.mu.Lock()
		n := len(w.frames)
		w.mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	w.mu.Lock()
	if len(w.frames) != 1 {
		w.mu.Unlock()
		t.Fatalf("expected 1 frame, got %d", len(w.frames))
	}
	frame := w.frames[0]
	w.mu.Unlock()

	var req struct {
		ID     int64  `json:"id"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(frame, &req); err != nil {
		t.Fatal(err)
	}
	if req.Method != "elicitation/create" {
		t.Fatalf("method = %q", req.Method)
	}

	resp, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"result":  map[string]any{"action": "accept", "content": map[string]any{"choice": "A"}},
	})
	if !b.TryCompleteResponse(resp) {
		t.Fatal("TryCompleteResponse should succeed")
	}
	<-done
	if callErr != nil {
		t.Fatal(callErr)
	}
	var res map[string]any
	if err := json.Unmarshal(got, &res); err != nil {
		t.Fatal(err)
	}
	if res["action"] != "accept" {
		t.Fatalf("result = %v", res)
	}
}

func TestParseClientCapabilities(t *testing.T) {
	caps := ParseClientCapabilities([]byte(`{
		"protocolVersion": 1,
		"clientCapabilities": {
			"elicitation": { "form": {}, "url": null }
		}
	}`))
	if !caps.ElicitationForm {
		t.Error("expected form support")
	}
	if caps.ElicitationURL {
		t.Error("url was null; should be unsupported")
	}

	caps2 := ParseClientCapabilities([]byte(`{"protocolVersion":1}`))
	if caps2.ElicitationForm || caps2.ElicitationURL {
		t.Error("missing elicitation should mean unsupported")
	}
}

func TestSelectionToElicitationParams(t *testing.T) {
	params, err := SelectionToElicitationParams("sess1", "tc1", "Pick one", []control.UserChoice{
		{Title: "A", Description: "first", IsRecommended: true},
		{Title: "B", Description: "second"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if params["mode"] != "form" || params["sessionId"] != "sess1" || params["toolCallId"] != "tc1" {
		t.Fatalf("params = %#v", params)
	}
	msg, _ := params["message"].(string)
	if !containsAll(msg, "Pick one", "A", "recommended", "B") {
		t.Fatalf("message = %q", msg)
	}
}

func TestElicitationResultToSelectionPayload(t *testing.T) {
	opts := []control.UserChoice{{Title: "A"}, {Title: "B"}}
	raw, _ := json.Marshal(map[string]any{
		"action":  "accept",
		"content": map[string]any{"choice": "B"},
	})
	action, res, err := ElicitationResultToSelectionPayload(raw, opts)
	if err != nil || action != "accept" {
		t.Fatalf("action=%s err=%v", action, err)
	}
	var payload map[string]any
	_ = json.Unmarshal(res, &payload)
	if int(payload["selectionIdx"].(float64)) != 1 {
		t.Fatalf("payload = %v", payload)
	}

	rawDec, _ := json.Marshal(map[string]any{"action": "decline"})
	action, res, err = ElicitationResultToSelectionPayload(rawDec, opts)
	if err != nil || action != "decline" || res != nil {
		t.Fatalf("decline: action=%s res=%s err=%v", action, res, err)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}

func TestParseUserSelectionFromInterruptData(t *testing.T) {
	usi := control.UserSelectionInterrupt{
		Options: []control.UserChoice{{Title: "A"}, {Title: "B"}},
	}
	serialized, err := usi.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	// Correct shape: nested object via json.RawMessage (what the harness emits).
	envelope, err := json.Marshal(map[string]any{
		"interruptId": "intr-1",
		"data":        json.RawMessage(serialized),
	})
	if err != nil {
		t.Fatal(err)
	}
	id, opts, err := ParseUserSelectionFromInterruptData(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if id != "intr-1" {
		t.Errorf("id = %q, want intr-1", id)
	}
	if len(opts) != 2 || opts[0].Title != "A" {
		t.Fatalf("opts = %#v", opts)
	}

	// Regression: []byte in map becomes a JSON string and must not be accepted as-is.
	bad, _ := json.Marshal(map[string]any{
		"interruptId": "intr-2",
		"data":        serialized, // []byte → base64 string
	})
	if _, _, err := ParseUserSelectionFromInterruptData(bad); err == nil {
		t.Fatal("expected error when data is a JSON string (base64 []byte), got nil")
	}
}
