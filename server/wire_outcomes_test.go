package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/stores"
)

// failWire is a ProtocolWireStore that can fail Put/Get/Delete for error outcomes.
type failWire struct {
	base    *MemoryWireStore
	putErr  error
	getErr  error
	delErr  error
	putOnce int // succeed first N puts when putErr set
}

func (f *failWire) Put(ctx context.Context, sessionID string, payload []byte) error {
	if f.putErr != nil {
		if f.putOnce > 0 {
			f.putOnce--
			return f.base.Put(ctx, sessionID, payload)
		}
		return f.putErr
	}
	return f.base.Put(ctx, sessionID, payload)
}

func (f *failWire) Get(ctx context.Context, sessionID string) ([]byte, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.base.Get(ctx, sessionID)
}

func (f *failWire) Delete(ctx context.Context, sessionID string) error {
	if f.delErr != nil {
		return f.delErr
	}
	return f.base.Delete(ctx, sessionID)
}

// TestWireAndConstruct_outcomes covers wire-store / construct / streamable edge
// paths in few high-value cases (no permission/prompt suite duplication).
func TestWireAndConstruct_outcomes(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{}, nil)

	// --- construct shorthands (real mount + wire durability) ---
	if p := ACPProtocol(); p == nil || p.Name() != "acp" {
		t.Fatal("ACPProtocol")
	}
	if p := NewACPProtocolPostgres(nil); p == nil || p.Name() != "acp" {
		t.Fatal("NewACPProtocolPostgres")
	}
	if srv := NewACPServerPostgres(r, nil); srv == nil || srv.Protocols[0].Name() != "acp" {
		t.Fatal("NewACPServerPostgres")
	}
	if NewACPProtocolMemory().Name() != "acp" {
		t.Fatal("NewACPProtocolMemory")
	}
	srv := NewACPServer(r)
	if srv == nil || srv.Registry != r || len(srv.Protocols) != 1 {
		t.Fatal("NewACPServer")
	}
	wire := NewMemoryWireStore()
	srvWire := NewACPServerWithWire(r, wire)
	s := &acpTestServer{t: t, r: r, proto: srvWire.Protocols[0], wire: wire}
	rec := s.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w"}}`)
	sid, _ := acpRPCResult(t, rec)["sessionId"].(string)
	if sid == "" {
		t.Fatal("missing sessionId")
	}
	if _, err := wire.Get(t.Context(), sid); err != nil {
		t.Fatalf("wire should hold envelope: %v", err)
	}

	// --- SSE wire sessions unsupported ---
	if _, _, err := SSE.CreateSession(context.Background(), ProtocolEnv{}, nil); !errors.Is(err, ErrWireSessionUnsupported) {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := SSE.LoadSession(context.Background(), ProtocolEnv{}, "x", nil); !errors.Is(err, ErrWireSessionUnsupported) {
		t.Fatalf("LoadSession: %v", err)
	}
	if _, err := SSE.BindTurn(context.Background(), ProtocolEnv{}, "x", "", nil); !errors.Is(err, ErrWireSessionUnsupported) {
		t.Fatalf("BindTurn: %v", err)
	}
	if err := SSE.CloseSession(context.Background(), ProtocolEnv{}, "x"); !errors.Is(err, ErrWireSessionUnsupported) {
		t.Fatalf("CloseSession: %v", err)
	}

	// --- MemoryWireStore nil / missing ---
	var nilWire *MemoryWireStore
	if err := nilWire.Put(context.Background(), "a", []byte(`{}`)); err == nil {
		t.Fatal("nil Put")
	}
	if _, err := nilWire.Get(context.Background(), "a"); err == nil {
		t.Fatal("nil Get")
	}
	_ = nilWire.Delete(context.Background(), "a")
	w := NewMemoryWireStore()
	if _, err := w.Get(context.Background(), "missing"); !errors.Is(err, stores.ErrSessionNotFound) {
		t.Fatalf("missing get: %v", err)
	}
	if err := w.Put(context.Background(), "k", []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}

	// --- Registry DefaultAgent / options ---
	_ = NewRegistry(testStore(t), "d", WithTracer(nil), nil)
	r2 := NewRegistry(testStore(t), "d", WithTracer(nil), func(reg *Registry) {
		reg.tracer = nil
		reg.instruments = nil
	})
	if r2.DefaultAgent() != "d" {
		t.Fatal(r2.DefaultAgent())
	}
	if r2.HasAgent("x") {
		t.Fatal("unknown agent")
	}
	r2.RecordSessionCreated(context.Background())

	// --- ClientBridge nil caps ---
	var bridge *ClientBridge
	if bridge.GetCaps().ElicitationForm {
		t.Fatal("nil GetCaps")
	}
	bridge.SetCaps(ClientCapabilities{ElicitationForm: true})
	if connElicitationForm(nil) {
		t.Fatal("nil conn elicitation")
	}
	if connElicitationForm(&Conn{}) {
		t.Fatal("no rpc means no elicitation")
	}
	formBridge := NewClientBridge(&recordingWriter{})
	formBridge.SetCaps(ClientCapabilities{ElicitationForm: true})
	if !connElicitationForm(&Conn{RPC: formBridge}) {
		t.Fatal("live bridge caps")
	}

	// --- Streamable HTTP protocol error matrix (one server, many outcomes) ---
	hs, _ := startACPStreamServer(t, r)
	assertStatus := func(name string, method, path, body string, headers map[string]string, want int) {
		t.Helper()
		var req *http.Request
		var err error
		if method == http.MethodGet || method == http.MethodDelete {
			req, err = http.NewRequest(method, hs.URL+path, nil)
		} else {
			req, err = http.NewRequest(method, hs.URL+path, strings.NewReader(body))
		}
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := streamHTTPClient().Do(req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s: status=%d want %d", name, resp.StatusCode, want)
		}
	}

	// DELETE without connection id → 400
	assertStatus("DELETE no id", http.MethodDelete, "/acp", "", nil, http.StatusBadRequest)
	// DELETE unknown → 404
	assertStatus("DELETE unknown", http.MethodDelete, "/acp", "", map[string]string{
		HeaderAcpConnectionID: "nope",
	}, http.StatusNotFound)
	// Wrong Content-Type → 415
	assertStatus("POST text/plain", http.MethodPost, "/acp", `{}`, map[string]string{
		"Content-Type": "text/plain",
	}, http.StatusUnsupportedMediaType)
	// Empty body → 400
	assertStatus("empty body", http.MethodPost, "/acp", "", map[string]string{
		"Content-Type": "application/json",
	}, http.StatusBadRequest)
	// Batch → 501
	assertStatus("batch", http.MethodPost, "/acp", `[{}]`, map[string]string{
		"Content-Type": "application/json",
	}, http.StatusNotImplemented)
	// Invalid JSON → 400
	assertStatus("bad json", http.MethodPost, "/acp", `{`, map[string]string{
		"Content-Type": "application/json",
	}, http.StatusBadRequest)
	// POST without connection after non-init → 400
	assertStatus("post no conn", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}`,
		map[string]string{"Content-Type": "application/json"},
		http.StatusBadRequest)
	// POST with charset accepted
	req, _ := http.NewRequest(http.MethodPost, hs.URL+"/acp", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := streamHTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "protocolVersion") {
		t.Fatalf("init charset: %d %s", resp.StatusCode, body)
	}
	// initialize with connection id → 400
	assertStatus("init with conn", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":1}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: "already",
		}, http.StatusBadRequest)
	// Unsupported protocol version still yields JSON-RPC error body (200)
	resp = acpPOST(t, hs, `{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":0}}`, nil)
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bad version status=%d", resp.StatusCode)
	}
	_ = b

	connID, _ := acpInitialize(t, hs)
	// SSE missing Accept → 406
	assertStatus("sse no accept", http.MethodGet, "/acp", "", map[string]string{
		HeaderAcpConnectionID: connID,
	}, http.StatusNotAcceptable)
	// SSE missing connection → 400
	assertStatus("sse no conn", http.MethodGet, "/acp", "", map[string]string{
		"Accept": "text/event-stream",
	}, http.StatusBadRequest)
	// SSE unknown connection → 404
	assertStatus("sse unknown", http.MethodGet, "/acp", "", map[string]string{
		"Accept":              "text/event-stream",
		HeaderAcpConnectionID: "missing-conn",
	}, http.StatusNotFound)
	// session/prompt without session header → 400
	assertStatus("prompt no session hdr", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":9,"method":"session/prompt","params":{"sessionId":"x","prompt":[{"type":"text","text":"hi"}]}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: connID,
		}, http.StatusBadRequest)
	// client JSON-RPC response demux on unknown conn → 404
	assertStatus("response unknown conn", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":99,"result":{}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: "ghost",
		}, http.StatusNotFound)
	// client response on known conn → 202
	assertStatus("response ok", http.MethodPost, "/acp",
		`{"jsonrpc":"2.0","id":100,"result":{}}`,
		map[string]string{
			"Content-Type":        "application/json",
			HeaderAcpConnectionID: connID,
		}, http.StatusAccepted)

	// Double connection SSE → 409
	sse1 := openACPSSE(t, hs, connID, "")
	defer sse1.Body.Close()
	req, _ = http.NewRequest(http.MethodGet, hs.URL+"/acp", nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(HeaderAcpConnectionID, connID)
	resp, err = streamHTTPClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second SSE status=%d want 409", resp.StatusCode)
	}

	// --- Direct handler: Connections nil → 500 ---
	p := NewACPProtocol(NewMemoryWireStore()).(*acpProtocol)
	regEnv := ProtocolEnv{Registry: r}
	for _, fn := range []struct {
		name string
		call func(http.ResponseWriter, *http.Request)
	}{
		{"post", func(w http.ResponseWriter, hr *http.Request) {
			p.handleACPPost(regEnv, w, hr)
		}},
		{"sse", func(w http.ResponseWriter, hr *http.Request) {
			p.handleACPStreamSSE(regEnv, w, hr)
		}},
		{"delete", func(w http.ResponseWriter, hr *http.Request) {
			p.handleACPDelete(regEnv, w, hr)
		}},
	} {
		rec := httptest.NewRecorder()
		hr := httptest.NewRequest(http.MethodPost, "/acp", strings.NewReader(`{}`))
		hr.Header.Set("Content-Type", "application/json")
		hr.Header.Set("Accept", "text/event-stream")
		fn.call(rec, hr)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s nil Connections: %d", fn.name, rec.Code)
		}
	}

	// connection not ready (Bridge/Writer nil)
	reg := NewConnectionRegistry()
	half := reg.Create(nil, nil)
	notReady := httptest.NewRecorder()
	hr := httptest.NewRequest(http.MethodPost, "/acp", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}`))
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set(HeaderAcpConnectionID, half.ID)
	p.handleACPPost(ProtocolEnv{Registry: r, Connections: reg}, notReady, hr)
	if notReady.Code != http.StatusInternalServerError {
		t.Fatalf("not ready: %d", notReady.Code)
	}

	// --- Connection routing outcomes ---
	var cnil *Connection
	if cnil.Context() == nil {
		t.Fatal("nil connection context")
	}
	cnil.rememberRoute(nil, "x", "")
	_ = cnil.takeRoute(nil)
	cnil.noteSession("")
	if _, _, err := cnil.attachConnSSE(nil, nil); err == nil {
		t.Fatal("nil attachConn")
	}
	if _, _, err := cnil.attachSessionSSE("s", nil, nil); err == nil {
		t.Fatal("nil attachSession")
	}
	if err := cnil.deliver("s", []byte(`{}`), false); err == nil {
		t.Fatal("nil deliver")
	}
	if reg.Get("") != nil {
		t.Fatal("empty get")
	}
	reg.Remove("")
	reg.Remove("missing")
	// Create without registry still returns a live connection
	local := (*ConnectionRegistry)(nil).Create(nil, nil)
	if local == nil || local.ID == "" {
		t.Fatal("nil registry Create")
	}
	local.shutdown()
	local.shutdown() // already closed
	if _, _, err := local.attachConnSSE(httptest.NewRecorder(), httptest.NewRecorder()); !errors.Is(err, errSSESinkClosed) {
		t.Fatalf("attach closed: %v", err)
	}
	if _, _, err := local.attachSessionSSE("s1", httptest.NewRecorder(), httptest.NewRecorder()); !errors.Is(err, errSSESinkClosed) {
		t.Fatalf("attach session closed: %v", err)
	}
	if err := local.deliver("s1", []byte(`{}`), false); !errors.Is(err, errSSESinkClosed) {
		t.Fatalf("deliver closed: %v", err)
	}

	// Live connection: attach session, default drop when session SSE is late
	live := NewConnectionRegistry().Create(NewClientBridge(&httpBufferWriter{}), &httpBufferWriter{})
	recW := httptest.NewRecorder()
	detach, sink, err := live.attachConnSSE(recW, recW)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.writeOpen(); err != nil {
		t.Fatal(err)
	}
	if err := live.deliver("sess-x", []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-x"}}`), false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(recW.Body.String(), "sess-x") {
		t.Fatal("default must not fall back to connection SSE")
	}
	fbReg := NewConnectionRegistry()
	fbReg.LateSessionSSEFallback = true
	fb := fbReg.Create(NewClientBridge(&httpBufferWriter{}), &httpBufferWriter{})
	fbRec := httptest.NewRecorder()
	fbDetach, fbSink, err := fb.attachConnSSE(fbRec, fbRec)
	if err != nil {
		t.Fatal(err)
	}
	if err := fbSink.writeOpen(); err != nil {
		t.Fatal(err)
	}
	if err := fb.deliver("sess-x", []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-x"}}`), false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fbRec.Body.String(), "sess-x") {
		t.Fatal("opt-in fallback should deliver on connection SSE")
	}
	fbDetach()
	// extractSessionID helpers
	if extractSessionIDFromJSONRPC([]byte(`not-json`)) != "" {
		t.Fatal("bad json extract")
	}
	if extractSessionIDFromJSONRPC([]byte(`{"params":{"sessionId":"abc"}}`)) != "abc" {
		t.Fatal("params extract")
	}
	if extractSessionIDFromJSONRPC([]byte(`{"result":{}}`)) != "" {
		t.Fatal("empty result extract")
	}
	if isJSONContentType("") {
		t.Fatal("empty content type")
	}
	// closed sink write
	closedSink := &sseSink{w: recW, f: recW, closed: true}
	if err := closedSink.writeJSONRPC([]byte(`{}`)); !errors.Is(err, errSSESinkClosed) {
		t.Fatalf("closed sink: %v", err)
	}
	// session double-attach
	detachSess, sessSink, err := live.attachSessionSSE("s2", httptest.NewRecorder(), httptest.NewRecorder())
	if err != nil {
		t.Fatal(err)
	}
	if err := sessSink.writeOpen(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := live.attachSessionSSE("s2", httptest.NewRecorder(), httptest.NewRecorder()); err == nil {
		t.Fatal("want session SSE conflict")
	}
	// acpStreamWriter WriteResult / WriteError with routes
	sw := &acpStreamWriter{conn: live}
	live.rememberRoute(json.RawMessage(`1`), "session/new", "")
	if err := sw.WriteResult(json.RawMessage(`1`), map[string]any{"sessionId": "s2"}); err != nil {
		t.Fatal(err)
	}
	live.rememberRoute(json.RawMessage(`2`), "session/prompt", "s2")
	if err := sw.WriteError(json.RawMessage(`2`), errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	// WriteFrame routes by request id then by body sessionId
	live.rememberRoute(json.RawMessage(`3`), "session/prompt", "s2")
	if err := sw.WriteFrame([]byte(`{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := sw.WriteFrame([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s2","update":{}}}`)); err != nil {
		t.Fatal(err)
	}
	detachSess()
	detach()

	// --- httpBufferWriter error path ---
	buf := &httpBufferWriter{}
	if err := buf.WriteError(json.RawMessage(`9`), clientErrorf(ErrInvalidRequest, "x")); err != nil {
		t.Fatal(err)
	}
	if len(buf.buf) == 0 {
		t.Fatal("expected error body")
	}
	if err := buf.WriteFrame([]byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	// --- Wire session error outcomes ---
	// Create invalid / Load invalid / empty session / close+load
	p2 := NewACPProtocol(NewMemoryWireStore()).(*acpProtocol)
	env := ProtocolEnv{Registry: r}
	if _, _, err := p2.CreateSession(context.Background(), env, json.RawMessage(`{`)); err == nil {
		t.Fatal("want invalid create params")
	}
	if _, err := p2.LoadSession(context.Background(), env, "", nil); err == nil {
		t.Fatal("empty sessionId")
	}
	if _, err := p2.LoadSession(context.Background(), env, "x", json.RawMessage(`{`)); err == nil {
		t.Fatal("want invalid load params")
	}
	if _, err := p2.BindTurn(context.Background(), env, "", "session/prompt", nil); err == nil {
		t.Fatal("empty sessionId bind")
	}
	// persist failure on create
	fw := &failWire{base: NewMemoryWireStore(), putErr: fmt.Errorf("put fail")}
	pFail := NewACPProtocol(fw).(*acpProtocol)
	if _, _, err := pFail.CreateSession(context.Background(), env, json.RawMessage(`{"cwd":"/t"}`)); err == nil {
		t.Fatal("want put fail")
	}
	// create ok then load with put fail on re-persist
	fw2 := &failWire{base: NewMemoryWireStore(), putErr: fmt.Errorf("put2"), putOnce: 1}
	pFail2 := NewACPProtocol(fw2).(*acpProtocol)
	sid2, _, err := pFail2.CreateSession(context.Background(), env, json.RawMessage(`{"cwd":"/t"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pFail2.LoadSession(context.Background(), env, sid2, json.RawMessage(`{"cwd":"/t"}`)); err == nil {
		t.Fatal("want load put fail")
	}
	// corrupt durable envelope
	badWire := NewMemoryWireStore()
	_ = badWire.Put(context.Background(), "corrupt", []byte(`{not-json`))
	pBad := NewACPProtocol(badWire).(*acpProtocol)
	if _, err := pBad.LoadSession(context.Background(), env, "corrupt", nil); err == nil {
		t.Fatal("want decode fail")
	}
	// wire Get non-not-found error
	fw3 := &failWire{base: NewMemoryWireStore(), getErr: fmt.Errorf("db down")}
	pGet := NewACPProtocol(fw3).(*acpProtocol)
	if _, err := pGet.LoadSession(context.Background(), env, "any", nil); err == nil {
		t.Fatal("want get fail")
	}
	// Close delete fail
	fw4 := &failWire{base: NewMemoryWireStore(), delErr: fmt.Errorf("del fail")}
	_ = fw4.base.Put(context.Background(), "delme", []byte(`{"cwd":"/x"}`))
	pDel := NewACPProtocol(fw4).(*acpProtocol)
	// put session in memory then close
	pDel.sessions = map[string]*acpWireSession{"delme": {cwd: "/x"}}
	if err := pDel.CloseSession(context.Background(), env, "delme"); err == nil {
		t.Fatal("want delete fail")
	}
	// no default agent on bind
	emptyReg := NewRegistry(testStore(t), "")
	pEmpty := NewACPProtocol(NewMemoryWireStore()).(*acpProtocol)
	sidEmpty, _, err := pEmpty.CreateSession(context.Background(), ProtocolEnv{Registry: emptyReg}, json.RawMessage(`{"cwd":"/e"}`))
	if err != nil {
		t.Fatal(err)
	}
	prompt, _ := json.Marshal(map[string]any{
		"sessionId": sidEmpty,
		"prompt":    []map[string]any{{"type": "text", "text": "hi"}},
	})
	if _, err := pEmpty.BindTurn(context.Background(), ProtocolEnv{Registry: emptyReg}, sidEmpty, "session/prompt", prompt); err == nil {
		t.Fatal("want no agent")
	}
	// empty text prompt
	pOk := NewACPProtocol(NewMemoryWireStore()).(*acpProtocol)
	sidOk, _, err := pOk.CreateSession(context.Background(), env, json.RawMessage(`{"cwd":"/ok"}`))
	if err != nil {
		t.Fatal(err)
	}
	badPrompt, _ := json.Marshal(map[string]any{
		"sessionId": sidOk,
		"prompt":    []map[string]any{{"type": "text", "text": ""}},
	})
	if _, err := pOk.BindTurn(context.Background(), env, sidOk, "session/prompt", badPrompt); err == nil {
		t.Fatal("want invalid prompt")
	}
	// cwd mismatch on bind
	cwdPrompt, _ := json.Marshal(map[string]any{
		"sessionId": sidOk,
		"cwd":       "/other",
		"prompt":    []map[string]any{{"type": "text", "text": "hi"}},
	})
	if _, err := pOk.BindTurn(context.Background(), env, sidOk, "session/prompt", cwdPrompt); err == nil {
		t.Fatal("want cwd mismatch")
	}
	// Load cwd mismatch + empty configValues envelope path
	_ = pOk.persistWire(context.Background(), sidOk, &acpWireSession{cwd: "/ok", configValues: nil, owner: "local"})
	// force reload from wire with empty config
	pOk.mu.Lock()
	delete(pOk.sessions, sidOk)
	pOk.mu.Unlock()
	if _, err := pOk.LoadSession(context.Background(), env, sidOk, json.RawMessage(`{"cwd":"/other"}`)); err == nil {
		t.Fatal("want load cwd mismatch")
	}
	// resolveWireSession nil protocol
	var pNil *acpProtocol
	if _, err := pNil.resolveWireSession(context.Background(), "x"); err == nil {
		t.Fatal("nil protocol resolve")
	}
	_ = pNil.persistWire(context.Background(), "x", nil)
	// envelope nil configValues
	_ = (&acpWireSession{}).envelope()
	_ = wireSessionFromEnvelope(acpWireEnvelope{})

	// Close then Load → not found
	if err := pOk.CloseSession(context.Background(), env, sidOk); err != nil {
		t.Fatal(err)
	}
	if _, err := pOk.LoadSession(context.Background(), env, sidOk, nil); err == nil {
		t.Fatal("want not found after close")
	}

	// Postgres constructor default key + nil conn
	pg := NewPostgresWireStore(nil, "")
	if pg.protocol != "acp" {
		t.Fatal(pg.protocol)
	}
	if err := pg.Put(context.Background(), "x", nil); err == nil {
		t.Fatal("nil conn put")
	}
	if _, err := pg.Get(context.Background(), "x"); err == nil {
		t.Fatal("nil conn get")
	}
	_ = pg.Delete(context.Background(), "x")
}

// TestJSONRPCWSMessageWriter_error covers the ACP WebSocket writer error path
// (wsMessageWriter is the SSE-era twin; ACP uses jsonRPCWSMessageWriter).
func TestJSONRPCWSMessageWriter_error(t *testing.T) {
	up := make(chan *websocket.Conn, 1)
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		up <- c
		time.Sleep(200 * time.Millisecond)
		_ = c.Close(websocket.StatusNormalClosure, "")
	}))
	t.Cleanup(hs.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")
	client, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(websocket.StatusNormalClosure, "")

	var serverConn *websocket.Conn
	select {
	case serverConn = <-up:
	case <-time.After(2 * time.Second):
		t.Fatal("no server conn")
	}
	mw := &jsonRPCWSMessageWriter{ctx: ctx, c: serverConn}
	if err := mw.WriteResult(json.RawMessage(`1`), map[string]string{"ok": "1"}); err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteError(json.RawMessage(`2`), clientErrorf(ErrInvalidRequest, "ws-jsonrpc-err")); err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteFrame([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	// Drain client reads so writes complete
	for i := 0; i < 3; i++ {
		readCtx, c := context.WithTimeout(ctx, 300*time.Millisecond)
		_, data, err := client.Read(readCtx)
		c()
		if err != nil {
			break
		}
		if i == 1 && !strings.Contains(string(data), "ws-jsonrpc-err") {
			t.Fatalf("error frame = %s", data)
		}
	}
}

// TestStreamable_promptOnSessionSSE is a single happy-path streamable outcome:
// initialize → dual SSE → session/new → prompt → end_turn on session stream.
func TestStreamable_promptOnSessionSSE(t *testing.T) {
	r := newTestRegistry(testStore(t), &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hi", IsComplete: true}
		},
	}, nil)
	hs, _ := startACPStreamServer(t, r)
	connID, _ := acpInitialize(t, hs)
	sessionID, frames, closeFn := streamableSession(t, hs, connID)
	defer closeFn()

	_ = acpPOST(t, hs, fmt.Sprintf(
		`{"jsonrpc":"2.0","id":10,"method":"session/prompt","params":{"sessionId":%q,"prompt":[{"type":"text","text":"hello"}]}}`,
		sessionID), map[string]string{
		HeaderAcpConnectionID: connID,
		HeaderAcpSessionID:    sessionID,
	}).Body.Close()

	// Wait for end_turn result
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		frame := frames.next(t, time.Until(deadline))
		if res, ok := frame["result"].(map[string]any); ok {
			if res["stopReason"] == "end_turn" {
				return
			}
		}
	}
	t.Fatal("no end_turn")
}
