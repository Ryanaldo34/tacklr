package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/internal/livesess"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
	"github.com/ryanaldo34/tacklr/telemetry"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/session"
)

// healthProtocol is a host server.Protocol with one HTTP route (no Runtime turns).
type healthProtocol struct{}

func (healthProtocol) HandleInbound(ctx context.Context, env server.ProtocolEnv, body []byte) error {
	return nil
}

func (healthProtocol) HTTPRoutes() []server.HTTPRoute {
	return []server.HTTPRoute{{
		Method:  "GET",
		Pattern: "/healthz",
		Handler: func(env server.ProtocolEnv, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		},
	}}
}

func (healthProtocol) OnEvent(ctx context.Context, env server.ProtocolEnv, threadID string, ev session.Event, reqID json.RawMessage) server.StreamControl {
	return server.StreamControl{Finished: true}
}

func (healthProtocol) OnStreamClosed(ctx context.Context, env server.ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	return nil
}

type pumpProto struct {
	onEvent func(session.Event) server.StreamControl
}

func (pumpProto) HandleInbound(context.Context, server.ProtocolEnv, []byte) error {
	return nil
}
func (pumpProto) HTTPRoutes() []server.HTTPRoute { return nil }
func (p pumpProto) OnEvent(ctx context.Context, env server.ProtocolEnv, threadID string, ev session.Event, reqID json.RawMessage) server.StreamControl {
	if p.onEvent != nil {
		return p.onEvent(ev)
	}
	return server.StreamControl{Finished: true}
}
func (pumpProto) OnStreamClosed(context.Context, server.ProtocolEnv, string, json.RawMessage, bool) error {
	return nil
}

func terminalControl(ev session.Event) server.StreamControl {
	if ev.Type == session.EventComplete || ev.Type == session.EventError {
		return server.StreamControl{Finished: true}
	}
	return server.StreamControl{}
}

// TestRunTurn_midPromptCancelThenNextPrompt is the protocol-agnostic coverage
// for Runtime.Cancel during a turn: the pump finishes, then the next prompt
// on the same session completes with new content (no leftover cancel).
func TestRunTurn_midPromptCancelThenNextPrompt(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	var next atomic.Bool
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if next.Load() {
			ch <- tacklr.LLMResponseChunk{
				Type: session.EventMessage, Content: "after-cancel", IsComplete: true,
			}
			return
		}
		startedOnce.Do(func() { close(started) })
		for {
			select {
			case <-ctx.Done():
				return
			case ch <- tacklr.LLMResponseChunk{
				Type: session.EventMessage, Content: "early", IsComplete: false,
			}:
			}
		}
	})
	k := newTestRuntime(t, strategy, tacklr.AgentOptions{})
	ctx := t.Context()
	id, err := k.Client.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	env := server.ProtocolEnv{Client: k.Client, Agent: k.Agent}

	var sawStream atomic.Bool
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- server.RunTurn(ctx, env, pumpProto{onEvent: func(ev session.Event) server.StreamControl {
			if ev.Type == session.EventMessage {
				sawStream.Store(true)
			}
			return terminalControl(ev)
		}}, string(id), nil, server.PromptOrResume{Prompt: session.Prompt{Text: "hi"}})
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
	if err := k.Client.Cancel(ctx, id); err != nil {
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

	next.Store(true)
	var second []session.Event
	if err := server.RunTurn(ctx, env, pumpProto{onEvent: func(ev session.Event) server.StreamControl {
		second = append(second, ev)
		return terminalControl(ev)
	}}, string(id), nil, server.PromptOrResume{Prompt: session.Prompt{Text: "again"}}); err != nil {
		t.Fatalf("second turn: %v", err)
	}
	var sawAfter, complete bool
	for _, ev := range second {
		if ev.Type == session.EventMessage && strings.Contains(ev.Content, "after-cancel") {
			sawAfter = true
		}
		if ev.Type == session.EventComplete {
			complete = true
		}
	}
	if !sawAfter || !complete {
		t.Fatalf("want after-cancel + complete, got %+v", second)
	}
}

func TestRunTurn_runtimeErrors(t *testing.T) {
	k := newTestRuntime(t, testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "x", IsComplete: true}
	}), tacklr.AgentOptions{})
	env := server.ProtocolEnv{Client: k.Client, Agent: k.Agent}
	if err := server.RunTurn(t.Context(), env, pumpProto{}, "missing", nil, server.PromptOrResume{Prompt: session.Prompt{Text: "hi"}}); err == nil {
		t.Fatal("want subscribe missing session")
	}
	id, err := k.Client.CreateSession(t.Context(), session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RunTurn(t.Context(), env, pumpProto{}, string(id), nil, server.PromptOrResume{
		Prompt: session.Prompt{Text: "hi", State: map[string]any{"ch": make(chan int)}},
	}); err == nil {
		t.Fatal("want prompt encode failure")
	}
	if err := server.RunTurn(t.Context(), env, pumpProto{}, string(id), nil, server.PromptOrResume{
		Resume: &session.Resume{State: map[string]any{"ch": make(chan int)}},
	}); err == nil {
		t.Fatal("want resume encode failure")
	}
	id2, err := k.Client.CreateSession(t.Context(), session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RunTurn(t.Context(), env, pumpProto{onEvent: func(session.Event) server.StreamControl {
		_ = k.Client.Close(t.Context(), id2)
		return server.StreamControl{Resume: map[string][]byte{"nope": []byte(`{}`)}}
	}}, string(id2), nil, server.PromptOrResume{Prompt: session.Prompt{Text: "hi"}}); err == nil {
		t.Fatal("want resume-after-close failure")
	}

	// A stream that ends before the turn's result fails the turn, with
	// the stream's error when it has one.
	down := errors.New("stream down")
	for _, streamErr := range []error{down, nil} {
		err := server.RunTurn(t.Context(), server.ProtocolEnv{Client: fakeRuntime{streamErr: streamErr}, Agent: k.Agent},
			pumpProto{}, "sess", nil, server.PromptOrResume{Prompt: session.Prompt{Text: "hi"}})
		if err == nil || (streamErr != nil && !errors.Is(err, streamErr)) {
			t.Errorf("stream ended with %v: RunTurn = %v, want a failure carrying it", streamErr, err)
		}
	}
}

func TestRunTurn_protocolErrorStopsTurn(t *testing.T) {
	k := newTestRuntime(t, testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "x", IsComplete: true}
	}), tacklr.AgentOptions{})
	id, err := k.Client.CreateSession(t.Context(), session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	err = server.RunTurn(t.Context(), server.ProtocolEnv{Client: k.Client, Agent: k.Agent}, pumpProto{onEvent: func(session.Event) server.StreamControl {
		return server.StreamControl{Err: errors.New("encode")}
	}}, string(id), nil, server.PromptOrResume{Prompt: session.Prompt{Text: "hi"}})
	if err == nil {
		t.Fatal("want protocol error")
	}
}

func TestServeHTTP_respectsContextCancel(t *testing.T) {
	r := newTestRuntime(t, testkit.HTTPModel(t, nil), tacklr.AgentOptions{})
	srv := server.NewServer(r.Client, r.Agent, acp.New(nil)).AllowAnonymousNetwork()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ServeHTTP(ctx, "127.0.0.1:0") }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server.ServeHTTP did not exit")
	}
}

func TestMain(m *testing.M) {
	shutdown, err := telemetry.Init(context.Background(), telemetry.Config{})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = shutdown(context.Background())
	temporaldocker.Stop()
	os.Exit(code)
}

// fakeRuntime is a test fixture. It does not run a turn.
type fakeRuntime struct {
	create func(context.Context, session.CreateSession) (session.ID, error)
	// streamErr is why each subscription's events end; nil is a stream
	// that ends without one.
	streamErr error
}

func (f fakeRuntime) CreateSession(ctx context.Context, req session.CreateSession) (session.ID, error) {
	if f.create != nil {
		return f.create(ctx, req)
	}
	return "sess", nil
}

func (fakeRuntime) Prompt(context.Context, session.ID, session.Prompt) error { return nil }
func (fakeRuntime) Resume(context.Context, session.ID, session.Resume) error { return nil }
func (fakeRuntime) Cancel(context.Context, session.ID) error                 { return nil }
func (fakeRuntime) Close(context.Context, session.ID) error                  { return nil }
func (fakeRuntime) Head(context.Context, session.ID) (session.Seq, error)    { return 0, nil }
func (f fakeRuntime) Subscribe(context.Context, session.ID, session.Seq) (session.Subscription, error) {
	ch := make(chan session.Event)
	close(ch)
	return closedSub{ch, f.streamErr}, nil
}
func (fakeRuntime) Jobs(context.Context, session.ID) ([]session.Status, error) {
	return nil, nil
}
func (fakeRuntime) Status(context.Context, session.ID) (session.Status, error) {
	return session.Status{}, session.ErrNotFound
}

type closedSub struct {
	ch  chan session.Event
	err error
}

func (s closedSub) Events() <-chan session.Event { return s.ch }
func (s closedSub) Err() error                   { return s.err }
func (closedSub) Close() error                   { return nil }

type testRuntime struct {
	Client session.Client
	Agent  tacklr.AgentOptions
}

func fakeHost(t *testing.T) *testRuntime {
	t.Helper()
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil),
		MaxWindowSize: 8192, SystemPrompt: "test prompt"}
	return &testRuntime{Client: fakeRuntime{}, Agent: agent}
}

func newTestRuntime(t *testing.T, model tacklr.Model, spec tacklr.AgentOptions) *testRuntime {
	t.Helper()
	if spec.Model == nil {
		spec.Model = model
	}
	if spec.Model == nil {
		spec.Model = testkit.HTTPModel(t, nil)
	}
	if spec.MaxWindowSize == 0 {
		spec.MaxWindowSize = 8192
	}
	if spec.SystemPrompt == "" {
		spec.SystemPrompt = "test prompt"
	}
	return &testRuntime{
		Client: livesess.Runtime(t, spec),
		Agent:  spec,
	}
}

func newTestServer(t *testing.T) *server.Server {
	t.Helper()
	k := newTestRuntime(t, nil, tacklr.AgentOptions{})
	return server.NewServer(k.Client, k.Agent, acp.New(nil))
}

type recordingMessageWriter = testkit.RecordingWriter

func acpSessionID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["error"] != nil {
		t.Fatalf("unexpected error: %v", resp["error"])
	}
	sessionID, ok := resp["result"].(map[string]any)["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("missing sessionId in result: %v", resp)
	}
	return sessionID
}

func serveACPInbound(t *testing.T, r *testRuntime, proto server.Protocol, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mw := acp.HTTPWriter(rec)
	env := server.ProtocolEnv{Client: r.Client, Agent: r.Agent, Conn: &server.Conn{Writer: mw}}
	_ = proto.HandleInbound(t.Context(), env, []byte(body))
	return rec
}

type acpTestServer struct {
	t     *testing.T
	r     *testRuntime
	proto server.Protocol
	wire  server.ProtocolWireStore
}

func newACPTestServerWithWire(t *testing.T, r *testRuntime, wire server.ProtocolWireStore) *acpTestServer {
	t.Helper()
	return &acpTestServer{t: t, r: r, proto: acp.New(wire), wire: wire}
}

func (s *acpTestServer) rpc(body string) *httptest.ResponseRecorder {
	s.t.Helper()
	return serveACPInbound(s.t, s.r, s.proto, body)
}

func acpRPCResult(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
	}
	if errObj, ok := resp["error"]; ok && errObj != nil {
		t.Fatalf("unexpected error: %v", errObj)
	}
	res, _ := resp["result"].(map[string]any)
	return res
}

func parseACPFrames(t *testing.T, body io.Reader) []map[string]any {
	t.Helper()
	var frames []map[string]any
	dec := json.NewDecoder(body)
	for {
		var frame map[string]any
		if err := dec.Decode(&frame); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode ACP frame: %v", err)
		}
		frames = append(frames, frame)
	}
	return frames
}
