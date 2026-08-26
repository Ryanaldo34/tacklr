package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
)

func TestServeHTTP_listenCancelAndMountedHandlers(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(nil)).AllowAnonymousNetwork()

	rec := serveACPInbound(t, r, srv.Protocols[0], `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "protocolVersion") {
		t.Fatalf("acp: %d %s", rec.Code, rec.Body.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ServeHTTP(ctx, "127.0.0.1:0") }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ServeHTTP did not exit")
	}
}

func TestWaitHTTPServer_listenAndCancel(t *testing.T) {
	errCh := make(chan error, 1)
	errCh <- errors.New("bind")
	if err := waitHTTPServer(context.Background(), func(context.Context) error { return nil }, errCh); err == nil || err.Error() != "bind" {
		t.Fatalf("listen: %v", err)
	}
	errCh = make(chan error, 1)
	errCh <- http.ErrServerClosed
	if err := waitHTTPServer(context.Background(), func(context.Context) error { return nil }, errCh); err != nil {
		t.Fatalf("closed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	errCh = make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		errCh <- http.ErrServerClosed
	}()
	if err := waitHTTPServer(ctx, func(context.Context) error { return nil }, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel closed: %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	errCh = make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		errCh <- errors.New("listen fail")
	}()
	if err := waitHTTPServer(ctx, func(context.Context) error { return errors.New("shutdown") }, errCh); err == nil || err.Error() != "listen fail" {
		t.Fatalf("cancel err: %v", err)
	}
}

func TestHTTPMux_unregisteredProtocolPaths(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	acpOnly := NewServer(r.Runtime, r.Catalog, NewACPProtocol(nil)).AllowAnonymousNetwork()
	rec := httptest.NewRecorder()
	acpOnly.HTTPMux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{}`))))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("root path on acp mux status = %d", rec.Code)
	}
}

func TestNewServer_panicsWithoutRuntimeOrProtocol(t *testing.T) {
	t.Run("nil runtime", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		_ = NewServer(nil, nil, NewACPProtocol(nil))
	})
	t.Run("nil protocol", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		k := newEmptyRuntime()
		_ = NewServer(k.Runtime, k.Catalog, nil)
	})
	t.Run("no protocols", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		k := newEmptyRuntime()
		_ = NewServer(k.Runtime, k.Catalog)
	})
}

func TestLogTurnError(t *testing.T) {
	logTurnError(errors.New("boom internal"), "a", "t")
	logTurnError(clientErrorf(ErrAgentNotFound, "missing"), "a", "t")
}

// TestHandleInbound_lifecycleMethods exercises initialize, session CRUD/config,
// authenticate, and unknown method.
func TestHandleInbound_lifecycleMethods(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	srv := NewServer(r.Runtime, r.Catalog, NewACPProtocol(nil))
	w := &recordingMessageWriter{}
	bridge := NewClientBridge(w)
	env := ProtocolEnv{Runtime: r.Runtime, Catalog: r.Catalog, Conn: &Conn{Writer: w, RPC: bridge}}

	send := func(body string) {
		_ = srv.Protocols[0].HandleInbound(context.Background(), env, []byte(body))
	}

	send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"elicitation":{"form":{}}}}}`)
	send(`{"jsonrpc":"2.0","id":2,"method":"authenticate","params":{}}`)
	send(`{"jsonrpc":"2.0","id":3,"method":"session/new","params":{"cwd":"/tmp"}}`)

	var sessionID string
	for _, res := range w.Results {
		b, _ := json.Marshal(res.Result)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if s, ok := m["sessionId"].(string); ok && s != "" {
			sessionID = s
		}
	}
	if sessionID == "" {
		t.Fatal("session/new did not return sessionId")
	}

	send(fmt.Sprintf(`{"jsonrpc":"2.0","id":4,"method":"session/set_config_option","params":{"sessionId":%q,"configId":"model","value":"default"}}`, sessionID))
	send(fmt.Sprintf(`{"jsonrpc":"2.0","id":5,"method":"session/load","params":{"sessionId":%q,"cwd":"/tmp"}}`, sessionID))
	send(fmt.Sprintf(`{"jsonrpc":"2.0","id":6,"method":"session/close","params":{"sessionId":%q}}`, sessionID))
	send(fmt.Sprintf(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":%q}}`, sessionID))
	send(`{"jsonrpc":"2.0","id":7,"method":"totally_unknown","params":{}}`)
	send(`{`)
	if len(w.Results)+len(w.Errors) < 3 {
		t.Fatalf("expected multiple results/errors, got results=%d errors=%d", len(w.Results), len(w.Errors))
	}
}

func TestServeHTTP_listenError(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	err := NewServer(r.Runtime, r.Catalog, NewACPProtocol(nil)).AllowAnonymousNetwork().ServeHTTP(context.Background(), "127.0.0.1:99999x")
	if err == nil {
		t.Fatal("expected listen error")
	}
}

func TestACP_handleInbound_invalidJSON(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	rec := serveACPRaw(t, r, `{`)
	if rec.Body.Len() == 0 {
		t.Fatal("expected error body")
	}
}
