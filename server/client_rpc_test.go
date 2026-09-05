package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
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

func TestClientBridge_callOutcomes(t *testing.T) {
	if NewClientBridge(&failRPCWriter{}).TryCompleteResponse([]byte(`{`)) {
		t.Fatal("invalid JSON is not a client response")
	}
	if NewClientBridge(&failRPCWriter{}).TryCompleteResponse([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`)) {
		t.Fatal("unknown id is not a waiter")
	}

	fail := NewClientBridge(&failRPCWriter{})
	if _, err := fail.Call(t.Context(), "x", nil); err == nil {
		t.Fatal("want write failure")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := NewClientBridge(&recordingWriter{}).Call(ctx, "x", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call: %v", err)
	}

	w := &recordingWriter{}
	b := NewClientBridge(w)
	done := make(chan error, 1)
	go func() { _, err := b.Call(t.Context(), "x", nil); done <- err }()
	deadline := time.Now().Add(time.Second)
	var id int64
	for time.Now().Before(deadline) && id == 0 {
		w.mu.Lock()
		if len(w.frames) > 0 {
			var req struct {
				ID int64 `json:"id"`
			}
			_ = json.Unmarshal(w.frames[0], &req)
			id = req.ID
		}
		w.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	errBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": -32000, "message": "nope"},
	})
	if !b.TryCompleteResponse(errBody) {
		t.Fatal("error result should complete waiter")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want client rpc error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call did not return")
	}
}

type failRPCWriter struct{}

func (failRPCWriter) WriteResult(json.RawMessage, any) error  { return nil }
func (failRPCWriter) WriteError(json.RawMessage, error) error { return nil }
func (failRPCWriter) WriteFrame([]byte) error                 { return errors.New("write down") }

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
	if caps2.ElicitationForm || caps2.ElicitationURL || caps2.VFSTokenRefresh {
		t.Error("missing elicitation should mean unsupported")
	}

	caps3 := ParseClientCapabilities([]byte(`{
		"clientCapabilities": {
			"_meta": { "tacklr": { "vfs": { "tokenRefresh": true } } }
		}
	}`))
	if !caps3.VFSTokenRefresh {
		t.Error("expected vfs tokenRefresh")
	}
}
