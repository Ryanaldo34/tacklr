package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
)

// healthProtocol is a host Protocol with one HTTP route (no Runtime turns).
type healthProtocol struct{}

func (healthProtocol) HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error {
	return nil
}

func (healthProtocol) HTTPRoutes() []HTTPRoute {
	return []HTTPRoute{{
		Method:  "GET",
		Pattern: "/healthz",
		Handler: func(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		},
	}}
}

func (healthProtocol) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, ev tacklr.StreamEvent, reqID json.RawMessage) StreamControl {
	return StreamControl{Finished: true}
}

func (healthProtocol) OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	return nil
}

func TestServer_mountsHostProtocolBesideACP(t *testing.T) {
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	srv := NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil), healthProtocol{}).AllowAnonymousNetwork()
	mux := srv.HTTPMux()

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("host route = %d %q", rec.Code, rec.Body.String())
	}

	acpRec := httptest.NewRecorder()
	mux.ServeHTTP(acpRec, httptest.NewRequest(http.MethodGet, "/acp", nil))
	if acpRec.Code != http.StatusUpgradeRequired {
		t.Fatalf("acp GET without upgrade = %d %s", acpRec.Code, acpRec.Body.String())
	}
}

func TestACPProtocol_initializeResultShape(t *testing.T) {
	result := acpInitializeResultWithAuth(nil, 1, nil, false)
	if result["protocolVersion"] != 1 {
		t.Fatalf("protocolVersion = %v", result["protocolVersion"])
	}
	// Client asks for a future major; we respond with the latest we support (1).
	if v := acpInitializeResultWithAuth(nil, 99, nil, false)["protocolVersion"]; v != 1 {
		t.Fatalf("negotiated version for client 99 = %v, want 1", v)
	}
	caps, ok := result["agentCapabilities"].(map[string]any)
	if !ok {
		t.Fatal("missing agentCapabilities")
	}
	mcpCaps, ok := caps["mcpCapabilities"].(map[string]any)
	if !ok || mcpCaps["http"] != true {
		t.Fatalf("mcpCapabilities = %v", mcpCaps)
	}
	pc, ok := caps["promptCapabilities"].(map[string]any)
	if !ok || pc["image"] != false {
		t.Fatalf("nil catalog should advertise image=false, got %v", pc)
	}
	if pc["embeddedContext"] != true || pc["audio"] != false {
		t.Fatalf("promptCapabilities = %v", pc)
	}
	capMeta, _ := caps["_meta"].(map[string]any)
	tacklrCap, _ := capMeta["tacklr"].(map[string]any)
	vfsCap, _ := tacklrCap["vfs"].(map[string]any)
	if vfsCap["credentials"] != true || vfsCap["tokenRefresh"] != true {
		t.Fatalf("agentCapabilities._meta.tacklr.vfs = %#v", vfsCap)
	}
	info, ok := result["agentInfo"].(map[string]string)
	if !ok || info["name"] == "" {
		t.Fatalf("agentInfo = %v", result["agentInfo"])
	}
	meta, _ := result["_meta"].(map[string]any)
	tacklrMeta, _ := meta["tacklr"].(map[string]any)
	transports, _ := tacklrMeta["transports"].([]string)
	if len(transports) != 1 || transports[0] != "websocket" {
		t.Fatalf("transports = %v, want [websocket]", transports)
	}
}

type pumpProto struct {
	onEvent func(tacklr.StreamEvent) StreamControl
}

func (pumpProto) HandleInbound(context.Context, ProtocolEnv, []byte) error {
	return nil
}
func (pumpProto) HTTPRoutes() []HTTPRoute { return nil }
func (p pumpProto) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, ev tacklr.StreamEvent, reqID json.RawMessage) StreamControl {
	if p.onEvent != nil {
		return p.onEvent(ev)
	}
	return StreamControl{Finished: true}
}
func (pumpProto) OnStreamClosed(context.Context, ProtocolEnv, string, json.RawMessage, bool) error {
	return nil
}

func terminalControl(ev tacklr.StreamEvent) StreamControl {
	if ev.Type == tacklr.StreamEventComplete || ev.Type == tacklr.StreamEventError {
		return StreamControl{Finished: true}
	}
	return StreamControl{}
}

// TestRunTurn_midPromptCancelThenNextPrompt is the protocol-agnostic coverage
// for Runtime.Cancel during a turn: the pump finishes, then the next prompt
// on the same session completes with new content (no leftover cancel).
func TestRunTurn_midPromptCancelThenNextPrompt(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			startedOnce.Do(func() { close(started) })
			for {
				select {
				case <-ctx.Done():
					return
				case ch <- tacklr.LLMResponseChunk{
					Type: tacklr.StreamEventMessage, Content: "early", IsComplete: false,
				}:
				}
			}
		},
	}
	k := newTestRuntime(t, strategy, durable.AgentSpec{})
	ctx := t.Context()
	id, err := k.Runtime.CreateSession(ctx, durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog}

	var sawStream atomic.Bool
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- RunTurn(ctx, env, pumpProto{onEvent: func(ev tacklr.StreamEvent) StreamControl {
			if ev.Type == tacklr.StreamEventMessage {
				sawStream.Store(true)
			}
			return terminalControl(ev)
		}}, string(id), nil, PromptOrResume{Prompt: durable.Prompt{Text: "hi"}})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("prompt did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !sawStream.Load() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for first stream event")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := k.Runtime.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-firstDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("first turn: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first turn did not finish after cancel")
	}

	strategy.invokeFn = func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{
			Type: tacklr.StreamEventMessage, Content: "after-cancel", IsComplete: true,
		}
	}
	var second []tacklr.StreamEvent
	if err := RunTurn(ctx, env, pumpProto{onEvent: func(ev tacklr.StreamEvent) StreamControl {
		second = append(second, ev)
		return terminalControl(ev)
	}}, string(id), nil, PromptOrResume{Prompt: durable.Prompt{Text: "again"}}); err != nil {
		t.Fatalf("second turn: %v", err)
	}
	var sawAfter, complete bool
	for _, ev := range second {
		if ev.Type == tacklr.StreamEventMessage && strings.Contains(ev.Content, "after-cancel") {
			sawAfter = true
		}
		if ev.Type == tacklr.StreamEventComplete {
			complete = true
		}
	}
	if !sawAfter || !complete {
		t.Fatalf("want after-cancel + complete, got %+v", second)
	}
}

func TestRunTurn_runtimeErrors(t *testing.T) {
	k := newTestRuntime(t, &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "x", IsComplete: true}
		},
	}, durable.AgentSpec{})
	env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog}
	if err := RunTurn(t.Context(), env, pumpProto{}, "missing", nil, PromptOrResume{Prompt: durable.Prompt{Text: "hi"}}); err == nil {
		t.Fatal("want subscribe missing session")
	}
	id, err := k.Runtime.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := RunTurn(t.Context(), env, pumpProto{}, string(id), nil, PromptOrResume{
		Prompt: durable.Prompt{Text: "hi", State: map[string]any{"ch": make(chan int)}},
	}); err == nil {
		t.Fatal("want prompt encode failure")
	}
	if err := RunTurn(t.Context(), env, pumpProto{}, string(id), nil, PromptOrResume{
		Resume: &durable.Resume{State: map[string]any{"ch": make(chan int)}},
	}); err == nil {
		t.Fatal("want resume encode failure")
	}
	id2, err := k.Runtime.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := RunTurn(t.Context(), env, pumpProto{onEvent: func(tacklr.StreamEvent) StreamControl {
		_ = k.Runtime.Close(t.Context(), id2)
		return StreamControl{Resume: map[string][]byte{"nope": []byte(`{}`)}}
	}}, string(id2), nil, PromptOrResume{Prompt: durable.Prompt{Text: "hi"}}); err == nil {
		t.Fatal("want resume-after-close failure")
	}
}

func TestRunTurn_protocolErrorStopsTurn(t *testing.T) {
	k := newTestRuntime(t, &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "x", IsComplete: true}
		},
	}, durable.AgentSpec{})
	id, err := k.Runtime.CreateSession(t.Context(), durable.CreateSession{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	err = RunTurn(t.Context(), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog}, pumpProto{onEvent: func(tacklr.StreamEvent) StreamControl {
		return StreamControl{Err: errors.New("encode")}
	}}, string(id), nil, PromptOrResume{Prompt: durable.Prompt{Text: "hi"}})
	if err == nil {
		t.Fatal("want protocol error")
	}
}
