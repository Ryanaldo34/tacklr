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
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
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
// point, at /host. Its frames are commands: "follow <id>", "prompt <id>
// <text>" and "close <id>". It sends each event it gets as a JSON frame.
type hostProtocol struct{}

func (hostProtocol) Path() string { return "/host" }

func (hostProtocol) Open(env server.Env, peer server.Peer) server.Handler {
	return &hostHandler{env: env, peer: peer}
}

type hostHandler struct {
	env  server.Env
	peer server.Peer
}

func (h *hostHandler) Frame(ctx context.Context, frame []byte) {
	f := strings.Fields(string(frame))
	id := session.ID(f[1])
	switch f[0] {
	case "follow":
		if _, err := h.peer.Follow(ctx, id, 0); err != nil {
			h.send(ctx, hostEvent{Session: id, Error: err.Error()})
		}
	case "prompt":
		_ = h.env.Client.Prompt(ctx, id, session.Prompt{Text: f[2]})
	case "close":
		_ = h.env.Client.Close(ctx, id)
	}
}

func (h *hostHandler) Deliver(ctx context.Context, id session.ID, ev session.Event) {
	out := hostEvent{Session: id, Type: ev.Type, Content: ev.Content, Delta: ev.Delta, Streamed: ev.Streamed}
	if ev.Error != nil {
		out.Error, out.Ended = ev.Error.Error(), errors.Is(ev.Error, server.ErrStreamEnded)
	}
	h.send(ctx, out)
}

func (h *hostHandler) Closed(context.Context) {}

func (h *hostHandler) send(ctx context.Context, ev hostEvent) {
	raw, _ := json.Marshal(ev)
	_ = h.peer.Send(ctx, raw)
}

// hostEvent is what hostProtocol sends for one event.
type hostEvent struct {
	Session  session.ID        `json:"session"`
	Type     session.EventType `json:"type,omitempty"`
	Content  string            `json:"content,omitempty"`
	Delta    bool              `json:"delta,omitempty"`
	Streamed int               `json:"streamed,omitempty"`
	Error    string            `json:"error,omitempty"`
	Ended    bool              `json:"ended,omitempty"`
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

// TestConnection_deliversTheEventsOfEachSessionItFollows: a host's
// protocol follows sessions on one connection and gets their events: a
// message's deltas, then its complete event with the bytes the deltas
// already gave. A follow ends with an error event that the stream ended:
// with its cause when the session's object is evicted, and with none when
// the session closes. A follow of a session that is not there fails.
func TestConnection_deliversTheEventsOfEachSessionItFollows(t *testing.T) {
	t.Parallel()
	platform, k := objectHost(t, dotest.NewWire, testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if msgs[len(msgs)-1].Content == "stall" {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "early"}
			<-ctx.Done()
			return
		}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hel"}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "lo"}
		ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, IsComplete: true}
	}))
	srv := server.NewServer(k.Client, k.Agent, hostProtocol{}).AllowAnonymousNetwork()
	hs := httptest.NewServer(srv.HTTPMux())
	t.Cleanup(hs.Close)
	conn, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(hs.URL, "http")+"/host", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	send := func(cmd string) {
		t.Helper()
		if err := conn.Write(t.Context(), websocket.MessageText, []byte(cmd)); err != nil {
			t.Fatal(err)
		}
	}
	// read reads events until one that until accepts, and returns them.
	read := func(until func(hostEvent) bool) []hostEvent {
		t.Helper()
		var got []hostEvent
		for {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			_, data, err := conn.Read(ctx)
			cancel()
			if err != nil {
				t.Fatalf("read after %v: %v", got, err)
			}
			var ev hostEvent
			if err := json.Unmarshal(data, &ev); err != nil {
				t.Fatal(err)
			}
			got = append(got, ev)
			if until(ev) {
				return got
			}
		}
	}
	newSession := func() session.ID {
		t.Helper()
		id, err := k.Client.CreateSession(t.Context(), session.CreateSession{})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	spoken, evicted, closed := newSession(), newSession(), newSession()

	send("") // an empty frame is not a command
	send("follow " + string(spoken))
	send("prompt " + string(spoken) + " hi")
	turn := read(func(ev hostEvent) bool { return ev.Type == session.EventComplete })
	send("follow " + string(evicted))
	send("prompt " + string(evicted) + " stall")
	read(func(ev hostEvent) bool { return ev.Delta })
	platform.Evict(evicted)
	broken := read(func(ev hostEvent) bool { return ev.Ended })
	send("follow " + string(closed))
	send("close " + string(closed))
	ended := read(func(ev hostEvent) bool { return ev.Ended })
	send("follow missing")
	missing := read(func(ev hostEvent) bool { return ev.Session == "missing" })

	var deltas string
	var complete hostEvent
	for _, ev := range turn {
		switch {
		case ev.Type == session.EventMessage && ev.Delta:
			deltas += ev.Content
		case ev.Type == session.EventMessage:
			complete = ev
		}
	}
	if deltas != "hello" || complete.Content != "hello" || complete.Streamed != len("hello") {
		t.Errorf("deltas %q, then %+v; want the deltas, then the message with what they gave", deltas, complete)
	}
	if last := broken[len(broken)-1]; last.Session != evicted || last.Error == server.ErrStreamEnded.Error() {
		t.Errorf("evicted session's last event = %+v, want the stream's end with its cause", last)
	}
	if last := ended[len(ended)-1]; last.Session != closed || last.Error != server.ErrStreamEnded.Error() {
		t.Errorf("closed session's last event = %+v, want the stream's end", last)
	}
	if last := missing[len(missing)-1]; !strings.Contains(last.Error, "not found") {
		t.Errorf("follow of a missing session = %+v, want it not found", last)
	}
}

// TestServeHTTP_servesItsRoutesUntilItsContextEnds: a server with no
// network policy does not serve; one that allows anonymous access serves
// its protocols until its context ends, at once when it has ended
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
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String()+"/host", nil)
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
	if resp.StatusCode != http.StatusUpgradeRequired || !strings.Contains(string(body), "upgrade") {
		t.Errorf("served %d %q, want the protocol's route to ask for an upgrade", resp.StatusCode, body)
	}
	if inUse == nil || cannotAccept == nil || !errors.Is(endedFirst, context.Canceled) {
		t.Fatalf("address in use = %v, closed listener = %v, ended context = %v; want each failure", inUse, cannotAccept, endedFirst)
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
