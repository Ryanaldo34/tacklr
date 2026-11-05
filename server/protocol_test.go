package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/dotest"
	"github.com/ryanaldo34/tacklr/telemetry/export"
)

func TestMain(m *testing.M) {
	shutdown, err := export.Init(context.Background(), export.Config{})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	_ = shutdown(context.Background())
	temporaldocker.Stop()
	os.Exit(code)
}

// hostProtocol is a protocol of a host's own, the server.Protocol extension
// point: one GET /healthz route, and the stream policy onEvent (nil ends
// the turn at its end).
type hostProtocol struct {
	onEvent func(session.Event) server.StreamControl
}

func (hostProtocol) HandleInbound(context.Context, server.ProtocolEnv, []byte) error { return nil }

func (hostProtocol) HTTPRoutes() []server.HTTPRoute {
	return []server.HTTPRoute{{Method: http.MethodGet, Pattern: "/healthz", Handler: func(_ server.ProtocolEnv, w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}}}
}

func (p hostProtocol) OnEvent(_ context.Context, _ server.ProtocolEnv, _ string, ev session.Event, _ json.RawMessage) server.StreamControl {
	if p.onEvent != nil {
		return p.onEvent(ev)
	}
	return server.StreamControl{Finished: ev.Type == session.EventComplete || ev.Type == session.EventError}
}

func (hostProtocol) OnStreamClosed(context.Context, server.ProtocolEnv, string, json.RawMessage, bool) error {
	return nil
}

// memoryHost is a host on durable object sessions in memory: a real
// harness whose model sends nothing.
func memoryHost(t *testing.T) *acptest.Runtime {
	t.Helper()
	_, k := objectHost(t, dotest.New, testkit.HTTPModel(t, nil))
	return k
}

// objectHost is a host on the durable object platform that newPlatform
// makes, with a real harness on model.
func objectHost(t *testing.T, newPlatform func(durableobject.Config) *dotest.Platform, model tacklr.Model) (*dotest.Platform, *acptest.Runtime) {
	t.Helper()
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, SystemPrompt: "test prompt"}
	p := newPlatform(durableobject.Config{Agent: agent, Sealer: sealer})
	return p, &acptest.Runtime{Client: durableobject.NewClient(p), Agent: agent}
}

// errHungUp is what a write to a client that hung up gets.
var errHungUp = errors.New("connection reset by peer")

// hungUp is an HTTP response whose client hung up: each write fails.
type hungUp struct{ *httptest.ResponseRecorder }

func (hungUp) Write([]byte) (int, error) { return 0, errHungUp }

// TestRunTurn_midPromptCancelThenNextPrompt: Cancel during a turn ends the
// turn, and the next prompt on the session completes with new content.
func TestRunTurn_midPromptCancelThenNextPrompt(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	var startedOnce sync.Once
	var next atomic.Bool
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if next.Load() {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "after-cancel", IsComplete: true}
			return
		}
		startedOnce.Do(func() { close(started) })
		for {
			select {
			case <-ctx.Done():
				return
			case ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "early", IsComplete: false}:
			}
		}
	})
	k := acptest.NewRuntime(t, strategy, tacklr.AgentOptions{})
	ctx := t.Context()
	id, err := k.Client.CreateSession(ctx, session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}
	env := server.ProtocolEnv{Client: k.Client, Agent: k.Agent}

	streamed := make(chan struct{})
	var streamedOnce sync.Once
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- server.RunTurn(ctx, env, hostProtocol{onEvent: func(ev session.Event) server.StreamControl {
			if ev.Type == session.EventMessage {
				streamedOnce.Do(func() { close(streamed) })
			}
			return server.StreamControl{Finished: ev.Type == session.EventComplete || ev.Type == session.EventError}
		}}, string(id), nil, server.PromptOrResume{Prompt: session.Prompt{Text: "hi"}})
	}()
	for _, step := range []struct {
		done <-chan struct{}
		what string
	}{{started, "the prompt to start"}, {streamed, "the first stream event"}} {
		select {
		case <-step.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", step.what)
		}
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
	if err := server.RunTurn(ctx, env, hostProtocol{onEvent: func(ev session.Event) server.StreamControl {
		second = append(second, ev)
		return server.StreamControl{Finished: ev.Type == session.EventComplete || ev.Type == session.EventError}
	}}, string(id), nil, server.PromptOrResume{Prompt: session.Prompt{Text: "again"}}); err != nil {
		t.Fatalf("second turn: %v", err)
	}
	var sawAfter, complete bool
	for _, ev := range second {
		sawAfter = sawAfter || ev.Type == session.EventMessage && strings.Contains(ev.Content, "after-cancel")
		complete = complete || ev.Type == session.EventComplete
	}
	if !sawAfter || !complete {
		t.Fatalf("want after-cancel + complete, got %+v", second)
	}
}

// TestRunTurn_returnsEachFailureOfTheTurn: RunTurn fails for a session the
// client does not have, a prompt or resume it cannot encode, a resume after
// the session closed, a stream that ends before the turn's result, a
// stream that breaks when the platform evicts the session's object, a
// failure of the protocol, and a frame the client cannot get.
func TestRunTurn_returnsEachFailureOfTheTurn(t *testing.T) {
	t.Parallel()
	k := acptest.NewRuntime(t, testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "x", IsComplete: true}
	}), tacklr.AgentOptions{})
	env := server.ProtocolEnv{Client: k.Client, Agent: k.Agent}
	newSession := func() string {
		t.Helper()
		id, err := k.Client.CreateSession(t.Context(), session.CreateSession{})
		if err != nil {
			t.Fatal(err)
		}
		return string(id)
	}
	hi := server.PromptOrResume{Prompt: session.Prompt{Text: "hi"}}
	closing := func(id string, ctrl server.StreamControl) hostProtocol {
		var once sync.Once
		return hostProtocol{onEvent: func(session.Event) server.StreamControl {
			once.Do(func() { _ = k.Client.Close(context.Background(), session.ID(id)) })
			return ctrl
		}}
	}
	encodeErr := errors.New("encode")
	sid, resumed, ended := newSession(), newSession(), newSession()
	// The object platform: each call goes over the object protocol, and
	// its model streams until the turn ends.
	platform, objects := objectHost(t, dotest.NewWire, testkit.HTTPModel(t, func(ctx context.Context, _ []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "early"}
		<-ctx.Done()
	}))
	objectEnv := server.ProtocolEnv{Client: objects.Client, Agent: objects.Agent}
	evict, err := objects.Client.CreateSession(t.Context(), session.CreateSession{})
	if err != nil {
		t.Fatal(err)
	}

	missing := server.RunTurn(t.Context(), objectEnv, hostProtocol{}, "missing", nil, hi)
	evicted := server.RunTurn(t.Context(), objectEnv, hostProtocol{onEvent: func(ev session.Event) server.StreamControl {
		if ev.Type == session.EventMessage {
			platform.Evict(evict)
		}
		return server.StreamControl{}
	}}, string(evict), nil, hi)
	badPrompt := server.RunTurn(t.Context(), env, hostProtocol{}, sid, nil, server.PromptOrResume{
		Prompt: session.Prompt{Text: "hi", State: map[string]any{"ch": make(chan int)}},
	})
	badResume := server.RunTurn(t.Context(), env, hostProtocol{}, sid, nil, server.PromptOrResume{
		Resume: &session.Resume{State: map[string]any{"ch": make(chan int)}},
	})
	afterClose := server.RunTurn(t.Context(), env, closing(resumed, server.StreamControl{Resume: map[string][]byte{"nope": []byte(`{}`)}}), resumed, nil, hi)
	streamEnded := server.RunTurn(t.Context(), env, closing(ended, server.StreamControl{}), ended, nil, hi)
	protocolErr := server.RunTurn(t.Context(), env, hostProtocol{onEvent: func(session.Event) server.StreamControl {
		return server.StreamControl{Err: encodeErr}
	}}, sid, nil, hi)
	writeErr := server.RunTurn(t.Context(), server.ProtocolEnv{Client: k.Client, Agent: k.Agent, Conn: &server.Conn{Writer: acp.HTTPWriter(hungUp{httptest.NewRecorder()})}},
		hostProtocol{onEvent: func(session.Event) server.StreamControl {
			return server.StreamControl{Frames: [][]byte{[]byte(`{}`)}}
		}}, sid, nil, hi)

	for name, err := range map[string]error{"missing session": missing, "bad prompt": badPrompt, "bad resume": badResume, "resume after close": afterClose} {
		if err == nil {
			t.Errorf("%s: RunTurn = nil, want a failure", name)
		}
	}
	if streamEnded == nil || !strings.Contains(streamEnded.Error(), "without a result") {
		t.Errorf("stream ended: RunTurn = %v, want the turn without a result", streamEnded)
	}
	if evicted == nil || !strings.Contains(evicted.Error(), "turn stream:") {
		t.Errorf("evicted: RunTurn = %v, want the broken stream's failure", evicted)
	}
	if !errors.Is(protocolErr, encodeErr) || !errors.Is(writeErr, errHungUp) {
		t.Errorf("protocol failure = %v, write failure = %v; want each failure", protocolErr, writeErr)
	}
}

// TestServeHTTP_servesItsRoutesUntilItsContextEnds: a server with no
// network policy does not serve; one that allows anonymous access serves
// its protocols' routes until its context ends, at once when it has ended
// already; and a server fails on an address in use and on a listener that
// cannot accept.
func TestServeHTTP_servesItsRoutesUntilItsContextEnds(t *testing.T) {
	t.Parallel()
	k := memoryHost(t)
	newServer := func() *server.Server { return server.NewServer(k.Client, k.Agent, hostProtocol{}) }
	listen := func() net.Listener {
		t.Helper()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return ln
	}
	ln, closed, refused := listen(), listen(), listen()
	_ = closed.Close()
	ended, end := context.WithCancel(t.Context())
	end()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- newServer().AllowAnonymousNetwork().Serve(ctx, ln) }()

	noPolicy := newServer().ServeHTTP(t.Context(), "127.0.0.1:0")
	noPolicyListener := newServer().Serve(t.Context(), refused)
	inUse := newServer().AllowAnonymousNetwork().ServeHTTP(t.Context(), ln.Addr().String())
	cannotAccept := newServer().AllowAnonymousNetwork().Serve(t.Context(), closed)
	endedFirst := newServer().AllowAnonymousNetwork().ServeHTTP(ended, "127.0.0.1:0")
	// The listener is open, so the request waits in its backlog until the
	// server accepts it.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String()+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	cancel()

	for name, err := range map[string]error{"ServeHTTP": noPolicy, "Serve": noPolicyListener} {
		if !errors.Is(err, server.ErrNetworkSecurityPolicyRequired) {
			t.Errorf("%s with no policy = %v, want it refused", name, err)
		}
	}
	if string(body) != "ok" || inUse == nil || cannotAccept == nil || !errors.Is(endedFirst, context.Canceled) {
		t.Fatalf("served %q, address in use = %v, closed listener = %v, ended context = %v; want ok, then each failure", body, inUse, cannotAccept, endedFirst)
	}
	select {
	case err := <-served:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve = %v, want the context's end", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after its context ended")
	}
}
