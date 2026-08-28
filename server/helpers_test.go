package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestNewACPProtocolWithAuth_rejectsBadMethods(t *testing.T) {
	mustPanic := func(fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatal("want panic")
			}
		}()
		fn()
	}
	mustPanic(func() { NewACPProtocolWithAuth(nil, []ACPAuthMethod{{ID: "", Name: "n"}}, false) })
	mustPanic(func() {
		NewACPProtocolWithAuth(nil, []ACPAuthMethod{
			{ID: "a", Name: "A"},
			{ID: "a", Name: "Again"},
		}, false)
	})
}

func TestHandleInbound_canceledContext(t *testing.T) {
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	p := NewACPProtocol(NewMemoryWireStore())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rec := &recordingMessageWriter{}
	err := p.HandleInbound(ctx, ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: rec}}, []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestACP_handleInbound_notificationsAndUnknown(t *testing.T) {
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	p := NewACPProtocol(NewMemoryWireStore())
	rec := &recordingMessageWriter{}
	env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: rec, RPC: NewClientBridge(rec)}}
	ctx := t.Context()
	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","method":"session/foo"}`)); err != nil {
		t.Fatal(err)
	}
	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":1,"method":"nope"}`)); err != nil {
		t.Fatal(err)
	}
	if len(rec.Errors) == 0 {
		t.Fatal("expected method not found")
	}
	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":2,"method":"logout","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":3,"method":"authenticate","params":{"methodId":"missing"}}`)); err != nil {
		t.Fatal(err)
	}
}

func TestACP_handleInbound_sessionAndAuthOutcomes(t *testing.T) {
	dir := t.TempDir()
	k := newTestRuntime(t, nil, durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("docs", vfs.Local(dir)))})
	ctx := t.Context()

	authRec := &recordingMessageWriter{}
	authP := NewACPProtocolWithAuth(NewMemoryWireStore(), []ACPAuthMethod{{ID: "tok", Name: "Token"}}, true)
	var stored tacklrsecurity.Context
	authEnv := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: authRec, RPC: NewClientBridge(authRec), setSecurity: func(c tacklrsecurity.Context) { stored = c }}}
	if err := authP.HandleInbound(ctx, authEnv, []byte(`{"jsonrpc":"2.0","id":1,"method":"logout","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	_ = stored
	if err := authP.HandleInbound(ctx, authEnv, []byte(`{"jsonrpc":"2.0","id":2,"method":"authenticate","params":{"methodId":"tok"}}`)); err != nil {
		t.Fatal(err)
	}
	if len(authRec.Errors) == 0 {
		t.Fatal("authenticate without security service")
	}

	p := NewACPProtocol(NewMemoryWireStore())
	rec := &recordingMessageWriter{}
	env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: rec, RPC: NewClientBridge(rec)}}
	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)); err != nil {
		t.Fatal(err)
	}
	initBody := rec.Results[len(rec.Results)-1].Result
	raw, _ := json.Marshal(initBody)
	if !strings.Contains(string(raw), `"credentials":true`) || !strings.Contains(string(raw), `"tokenRefresh":true`) {
		t.Fatalf("initialize vfs capability: %s", raw)
	}

	_ = p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":4,"method":"session/new","params":"bad"}`))
	if len(rec.Errors) == 0 {
		t.Fatal("want invalid session/new params")
	}

	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":5,"method":"session/new","params":{"cwd":"/tmp"}}`)); err != nil {
		t.Fatal(err)
	}
	sid := ""
	for _, res := range rec.Results {
		b, _ := json.Marshal(res.Result)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if s, ok := m["sessionId"].(string); ok && s != "" {
			sid = s
		}
	}
	if sid == "" {
		t.Fatal("session/new")
	}

	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":6,"method":"session/cancel","params":{"sessionId":"`+sid+`"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":7,"method":"session/close","params":{"sessionId":"missing"}}`)); err != nil {
		t.Fatal(err)
	}

	env.Conn.RPC.MarkInitialized()
	if err := p.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"missing","prompt":[{"type":"text","text":"hi"}]}}`)); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing prompt: %v", err)
	}

	cwdPrompt := `{"jsonrpc":"2.0","id":11,"method":"session/prompt","params":{"sessionId":"` + sid + `","cwd":"/other","prompt":[{"type":"text","text":"hi"}]}}`
	if err := p.HandleInbound(ctx, env, []byte(cwdPrompt)); err == nil && len(rec.Errors) == 0 {
		t.Fatal("want cwd mismatch on prompt")
	}
}

func TestACP_httpPost_readBodyError(t *testing.T) {
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	mux := NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil)).AllowAnonymousNetwork().HTTPMux()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/acp", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Body = errReadCloser{}
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) { return 0, errors.New("read") }
func (errReadCloser) Close() error             { return nil }

func TestACP_authenticate_mapsHostErrors(t *testing.T) {
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	p := NewACPProtocolWithAuth(nil, []ACPAuthMethod{{ID: "tok", Name: "Token", Scheme: "host"}}, false)
	failing := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(context.Context, tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			return tacklrsecurity.Principal{}, tacklrsecurity.ErrAuthenticationFailed
		}),
	}
	rec := &recordingMessageWriter{}
	env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Security: failing, Conn: &Conn{Writer: rec, Security: &tacklrsecurity.Context{}}}
	if err := p.HandleInbound(t.Context(), env, []byte(`{"jsonrpc":"2.0","id":1,"method":"authenticate","params":{"methodId":"tok"}}`)); err != nil {
		t.Fatal(err)
	}
	if len(rec.Errors) == 0 {
		t.Fatal("want authentication failed")
	}

	required := &recordingMessageWriter{}
	reqEnv := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Security: &tacklrsecurity.Service{}, Conn: &Conn{Writer: required}}
	if err := p.HandleInbound(t.Context(), reqEnv, []byte(`{"jsonrpc":"2.0","id":2,"method":"authenticate","params":{"methodId":"tok"}}`)); err != nil {
		t.Fatal(err)
	}
	if len(required.Errors) == 0 {
		t.Fatal("want authentication required")
	}
}

func TestACP_RunTurn_unknownEventAndParkWithoutRPC(t *testing.T) {
	p := NewACPProtocol(nil)
	w := &recordingMessageWriter{}
	env := ProtocolEnv{Runtime: &stubRT{events: []streaming.StreamEvent{{Type: "nope"}}}, Conn: &Conn{Writer: w}}
	err := RunTurn(t.Context(), env, p, "t", json.RawMessage(`1`), PromptOrResume{})
	if err == nil {
		t.Fatal("unknown stream type")
	}

	parked := &recordingMessageWriter{}
	parkEnv := ProtocolEnv{
		Runtime: &stubRT{events: []streaming.StreamEvent{{Type: streaming.StreamEventInterrupt}}},
		Conn:    &Conn{Writer: parked},
	}
	if err := RunTurn(t.Context(), parkEnv, p, "t", json.RawMessage(`1`), PromptOrResume{}); err != nil {
		t.Fatal(err)
	}
	if len(parked.Errors) == 0 {
		t.Fatal("park without rpc should write a client error")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := RunTurn(ctx, ProtocolEnv{Runtime: &stubRT{}, Conn: &Conn{Writer: &recordingMessageWriter{}}}, p, "t", json.RawMessage(`9`), PromptOrResume{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled turn: %v", err)
	}
}

func TestACP_handleInbound_turnAndConfigEdges(t *testing.T) {
	k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	ctx := t.Context()

	nilCat := NewACPProtocol(NewMemoryWireStore())
	rec := &recordingMessageWriter{}
	env := ProtocolEnv{Runtime: k.Runtime, Conn: &Conn{Writer: rec}}
	if err := nilCat.HandleInbound(ctx, env, []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}`)); err != nil {
		t.Fatal(err)
	}

	authz := NewACPProtocol(NewMemoryWireStore())
	denied := &recordingMessageWriter{}
	deniedEnv := ProtocolEnv{
		Runtime:  k.Runtime,
		Catalog:  k.Catalog,
		Security: &tacklrsecurity.Service{},
		Conn:     &Conn{Writer: denied},
	}
	_ = authz.HandleInbound(ctx, deniedEnv, []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/t"}}`))
	if len(denied.Errors) == 0 {
		t.Fatal("want unauthenticated session/new")
	}

	createFail := NewACPProtocol(NewMemoryWireStore())
	failRec := &recordingMessageWriter{}
	failEnv := ProtocolEnv{Runtime: &stubRT{createErr: errors.New("create down")}, Catalog: k.Catalog, Conn: &Conn{Writer: failRec}}
	_ = createFail.HandleInbound(ctx, failEnv, []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/t"}}`))
	if len(failRec.Errors) == 0 {
		t.Fatal("want create session error")
	}

	p := NewACPProtocol(NewMemoryWireStore())
	w := &recordingMessageWriter{}
	bridge := NewClientBridge(w)
	live := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: w, RPC: bridge}}
	if err := p.HandleInbound(ctx, live, []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	sid := ""
	for _, res := range w.Results {
		b, _ := json.Marshal(res.Result)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if s, ok := m["sessionId"].(string); ok && s != "" {
			sid = s
		}
	}
	if sid == "" {
		t.Fatal("session/new")
	}

	if err := p.HandleInbound(ctx, live, []byte(`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sid+`","cwd":"/now"}}`)); err != nil {
		t.Fatal(err)
	}
	_ = p.HandleInbound(ctx, live, []byte(`{"jsonrpc":"2.0","id":3,"method":"session/cancel","params":{"sessionId":"missing"}}`))
	_ = p.HandleInbound(ctx, live, []byte(`{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":"bad"}`))
	_ = p.HandleInbound(ctx, live, []byte(`{"jsonrpc":"2.0","id":5,"method":"session/prompt","params":{"prompt":[{"type":"text","text":"hi"}]}}`))

	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer waitCancel()
	if err := p.HandleInbound(waitCtx, live, []byte(`{"jsonrpc":"2.0","id":6,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"hi"}]}}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait initialize: %v", err)
	}

	bridge.MarkInitialized()
	_ = p.HandleInbound(ctx, live, []byte(`{"jsonrpc":"2.0","id":7,"method":"session/resume","params":{"sessionId":"`+sid+`","responses":{"c1":{"ok":true}}}}`))
	_ = p.HandleInbound(ctx, live, []byte(`{"jsonrpc":"2.0","id":8,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"image","mimeType":"image/png","data":"AAAA"}]}}`))
	noCat := live
	noCat.Catalog = nil
	_ = p.HandleInbound(ctx, noCat, []byte(`{"jsonrpc":"2.0","id":82,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"image","mimeType":"image/png","data":"AAAA"}]}}`))
	_ = p.HandleInbound(ctx, live, []byte(`{"jsonrpc":"2.0","id":9,"method":"session/set_config_option","params":{"sessionId":"`+sid+`","configId":"model","value":"default"}}`))

	fw := &failWire{base: NewMemoryWireStore(), putErr: errors.New("put"), putOnce: 1}
	pFail := NewACPProtocol(fw)
	failW := &recordingMessageWriter{}
	failLive := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: failW}}
	created := serveACPInbound(t, k, pFail, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/x"}}`)
	sid2 := acpRPCResult(t, created)["sessionId"].(string)
	_ = pFail.HandleInbound(ctx, failLive, []byte(`{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sid2+`","configId":"model","value":"default"}}`))
	_ = pFail.HandleInbound(ctx, failLive, []byte(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"`+sid2+`","prompt":[{"type":"text","text":"hi"}]}}`))

	boom := failErrorWriter{err: errors.New("write")}
	boomEnv := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: boom, RPC: NewClientBridge(boom)}}
	boomEnv.Conn.RPC.MarkInitialized()
	_ = p.HandleInbound(ctx, boomEnv, []byte(`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":"missing","prompt":[{"type":"text","text":"hi"}]}}`))
}

func TestACP_httpGet_websocketAcceptFailure(t *testing.T) {
	k := newTestRuntime(t, nil, durable.AgentSpec{})
	mux := NewServer(k.Runtime, k.Catalog, NewACPProtocol(nil)).AllowAnonymousNetwork().HTTPMux()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/acp", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	mux.ServeHTTP(rec, req)
	if rec.Code == http.StatusSwitchingProtocols {
		t.Fatal("want websocket accept failure")
	}
}

type failErrorWriter struct{ err error }

func (f failErrorWriter) WriteResult(json.RawMessage, any) error { return nil }
func (f failErrorWriter) WriteError(json.RawMessage, error) error {
	return f.err
}
func (f failErrorWriter) WriteFrame([]byte) error { return nil }
