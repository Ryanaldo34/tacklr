package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/streaming"
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

func (healthProtocol) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl {
	return StreamControl{Finished: true}
}

func (healthProtocol) OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	return nil
}

func TestServer_mountsHostProtocolBesideACP(t *testing.T) {
	k := newTestKernel(t, nil, durable.AgentSpec{})
	srv := NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil), healthProtocol{}).AllowAnonymousNetwork()
	mux := srv.HTTPMux()

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("host route = %d %q", rec.Code, rec.Body.String())
	}

	acpRec := httptest.NewRecorder()
	acpReq := httptest.NewRequest(http.MethodPost, "/acp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`))
	acpReq.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(acpRec, acpReq)
	if acpRec.Code != http.StatusOK || !strings.Contains(acpRec.Body.String(), "protocolVersion") {
		t.Fatalf("acp = %d %s", acpRec.Code, acpRec.Body.String())
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
}

type stubSub struct{ ch <-chan streaming.StreamEvent }

func (s stubSub) Events() <-chan streaming.StreamEvent { return s.ch }
func (s stubSub) Close() error                         { return nil }

type stubRT struct {
	headErr, promptErr, resumeErr, subErr, createErr error
	events                                           []streaming.StreamEvent
	hold                                             chan struct{}
	cancelled                                        atomic.Bool
	resumes                                          int
}

func (s *stubRT) CreateSession(context.Context, durable.CreateSession) (durable.SessionID, error) {
	if s.createErr != nil {
		return "", s.createErr
	}
	return "s", nil
}
func (s *stubRT) Prompt(context.Context, durable.SessionID, durable.Prompt) error {
	return s.promptErr
}
func (s *stubRT) Resume(context.Context, durable.SessionID, durable.Resume) error {
	s.resumes++
	return s.resumeErr
}
func (s *stubRT) Cancel(context.Context, durable.SessionID) error {
	s.cancelled.Store(true)
	return nil
}
func (s *stubRT) Close(context.Context, durable.SessionID) error { return nil }
func (s *stubRT) Head(context.Context, durable.SessionID) (durable.Seq, error) {
	if s.headErr != nil {
		return 0, s.headErr
	}
	return 3, nil
}
func (s *stubRT) Subscribe(context.Context, durable.SessionID, durable.Seq) (durable.Subscription, error) {
	if s.subErr != nil {
		return nil, s.subErr
	}
	ch := make(chan streaming.StreamEvent, len(s.events)+1)
	for _, ev := range s.events {
		ch <- ev
	}
	if s.hold == nil {
		close(ch)
	} else {
		go func() {
			<-s.hold
			close(ch)
		}()
	}
	return stubSub{ch: ch}, nil
}

type pumpProto struct {
	onEvent   func(streaming.StreamEvent) StreamControl
	onClosed  error
	closed    *atomic.Bool
	cancelled *atomic.Bool
}

func (pumpProto) HandleInbound(context.Context, ProtocolEnv, []byte) error {
	return nil
}
func (pumpProto) HTTPRoutes() []HTTPRoute { return nil }
func (p pumpProto) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl {
	if p.onEvent != nil {
		return p.onEvent(ev)
	}
	return StreamControl{Finished: true}
}
func (p pumpProto) OnStreamClosed(_ context.Context, _ ProtocolEnv, _ string, _ json.RawMessage, cancelled bool) error {
	if p.closed != nil {
		p.closed.Store(true)
	}
	if p.cancelled != nil {
		p.cancelled.Store(cancelled)
	}
	return p.onClosed
}

type failWriter struct{ err error }

func (f failWriter) WriteResult(json.RawMessage, any) error { return nil }
func (f failWriter) WriteError(json.RawMessage, error) error {
	return nil
}
func (f failWriter) WriteFrame([]byte) error { return f.err }

func TestRunTurn_outcomes(t *testing.T) {
	complete := streaming.StreamEvent{Type: streaming.StreamEventComplete}
	t.Run("nil runtime", func(t *testing.T) {
		err := RunTurn(t.Context(), ProtocolEnv{}, pumpProto{}, "s", nil, PromptOrResume{})
		if err == nil || err.Error() != "server: Runtime is required" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("prompt error", func(t *testing.T) {
		rt := &stubRT{promptErr: errors.New("prompt boom")}
		err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{}, "s", nil, PromptOrResume{})
		if err == nil || err.Error() != "prompt boom" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("resume error", func(t *testing.T) {
		rt := &stubRT{resumeErr: errors.New("resume boom")}
		err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{}, "s", nil, PromptOrResume{Resume: &durable.Resume{}})
		if err == nil || err.Error() != "resume boom" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("subscribe error", func(t *testing.T) {
		rt := &stubRT{subErr: errors.New("sub boom"), headErr: errors.New("no head")}
		err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{}, "s", nil, PromptOrResume{})
		if err == nil || err.Error() != "sub boom" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("complete", func(t *testing.T) {
		rt := &stubRT{events: []streaming.StreamEvent{complete}}
		var closed, cancelled atomic.Bool
		if err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{closed: &closed, cancelled: &cancelled}, "s", nil, PromptOrResume{}); err != nil {
			t.Fatal(err)
		}
		if !closed.Load() || cancelled.Load() {
			t.Fatalf("OnStreamClosed closed=%v cancelled=%v", closed.Load(), cancelled.Load())
		}
	})
	t.Run("event error cancels", func(t *testing.T) {
		rt := &stubRT{events: []streaming.StreamEvent{{Type: streaming.StreamEventMessage}}}
		err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{onEvent: func(streaming.StreamEvent) StreamControl {
			return StreamControl{Err: errors.New("encode")}
		}}, "s", nil, PromptOrResume{})
		if err == nil || err.Error() != "encode" || !rt.cancelled.Load() {
			t.Fatalf("err=%v cancelled=%v", err, rt.cancelled.Load())
		}
	})
	t.Run("write frame error", func(t *testing.T) {
		rt := &stubRT{events: []streaming.StreamEvent{{Type: streaming.StreamEventMessage}}}
		err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt, Conn: &Conn{Writer: failWriter{err: errors.New("write")}}}, pumpProto{onEvent: func(streaming.StreamEvent) StreamControl {
			return StreamControl{Frames: [][]byte{[]byte(`x`)}}
		}}, "s", nil, PromptOrResume{})
		if err == nil || err.Error() != "write" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("mid-turn resume then complete", func(t *testing.T) {
		rt := &stubRT{events: []streaming.StreamEvent{
			{Type: streaming.StreamEventInterrupt},
			complete,
		}}
		n := 0
		err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{onEvent: func(ev streaming.StreamEvent) StreamControl {
			n++
			if ev.Type == streaming.StreamEventInterrupt {
				return StreamControl{Resume: map[string][]byte{"c1": []byte(`{}`)}}
			}
			return StreamControl{Finished: true}
		}}, "s", nil, PromptOrResume{})
		if err != nil || rt.resumes != 1 {
			t.Fatalf("err=%v resumes=%d n=%d", err, rt.resumes, n)
		}
	})
	t.Run("mid-turn resume fails", func(t *testing.T) {
		rt := &stubRT{events: []streaming.StreamEvent{{Type: streaming.StreamEventInterrupt}}, resumeErr: errors.New("resume later")}
		err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{onEvent: func(streaming.StreamEvent) StreamControl {
			return StreamControl{Resume: map[string][]byte{"c1": []byte(`{}`)}}
		}}, "s", nil, PromptOrResume{})
		if err == nil || err.Error() != "resume later" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("ctx cancel", func(t *testing.T) {
		rt := &stubRT{hold: make(chan struct{})}
		t.Cleanup(func() { close(rt.hold) })
		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() {
			errCh <- RunTurn(ctx, ProtocolEnv{Runtime: rt}, pumpProto{}, "s", nil, PromptOrResume{})
		}()
		time.Sleep(20 * time.Millisecond)
		cancel()
		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("turn did not return")
		}
	})
	t.Run("subscription closed", func(t *testing.T) {
		rt := &stubRT{}
		if err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{onClosed: errors.New("closed bad")}, "s", nil, PromptOrResume{}); err == nil || err.Error() != "closed bad" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("subscription closed cleanly", func(t *testing.T) {
		rt := &stubRT{}
		if err := RunTurn(t.Context(), ProtocolEnv{Runtime: rt}, pumpProto{}, "s", nil, PromptOrResume{}); err != nil {
			t.Fatal(err)
		}
	})
}
