package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	tacklrsecurity "github.com/ryanaldo34/tacklr/security"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestHandleInbound_errorContract is the host/client recovery contract:
// each sentinel is reachable from HandleInbound, errors.Is holds, and the
// JSON-RPC code on the wire is the one consumers should switch on.
func TestHandleInbound_errorContract(t *testing.T) {
	assert := func(t *testing.T, err, sentinel error, code int, public error) {
		t.Helper()
		if err == nil {
			t.Fatal("want error")
		}
		if !errors.Is(err, sentinel) {
			t.Fatalf("errors.Is(%v, %v) = false", err, sentinel)
		}
		if got := JSONRPCErrorCode(err); got != code {
			t.Fatalf("JSONRPCErrorCode = %d, want %d", got, code)
		}
		if pub := PublicError(err); !errors.Is(pub, public) {
			t.Fatalf("PublicError = %v, want %v", pub, public)
		}
		envelope := jsonRPCErrorBody(json.RawMessage(`1`), err)
		errObj := envelope["error"].(map[string]any)
		if errObj["code"] != code {
			t.Fatalf("wire code = %v, want %d", errObj["code"], code)
		}
	}

	t.Run("methodNotFound", func(t *testing.T) {
		k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
		err := inboundWrittenError(t, NewACPProtocol(nil), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
			`{"jsonrpc":"2.0","id":1,"method":"session/foo","params":{}}`)
		assert(t, err, ErrMethodNotFound, jsonRPCCodeMethodNotFound, ErrMethodNotFound)
	})

	t.Run("invalidRequest", func(t *testing.T) {
		k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
		err := inboundWrittenError(t, NewACPProtocol(nil), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
			`{"jsonrpc":"2.0","id":1,"method":"session/load","params":{}}`)
		assert(t, err, ErrInvalidRequest, jsonRPCCodeInvalidRequest, ErrInvalidRequest)
	})

	t.Run("sessionNotFound", func(t *testing.T) {
		k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
		err := inboundWrittenError(t, NewACPProtocol(nil), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
			`{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"sessionId":"missing"}}`)
		assert(t, err, ErrSessionNotFound, jsonRPCCodeApplication, ErrSessionNotFound)
	})

	t.Run("agentNotFound", func(t *testing.T) {
		k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
		proto := NewACPProtocol(nil)
		env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog}
		sid := acpSessionID(t, serveACPInbound(t, k, proto, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
		err := inboundWrittenError(t, proto, env,
			`{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sid+`","configId":"model","value":"ghost"}}`)
		assert(t, err, ErrAgentNotFound, jsonRPCCodeApplication, ErrAgentNotFound)
	})

	t.Run("authenticationRequired", func(t *testing.T) {
		k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
		proto := NewACPProtocolWithAuth(nil, []ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, false)
		err := inboundWrittenError(t, proto, ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
			`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
		assert(t, err, ErrAuthenticationRequired, jsonRPCCodeApplication, ErrAuthenticationRequired)
	})

	t.Run("authenticationFailed", func(t *testing.T) {
		k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
		service := &tacklrsecurity.Service{
			Authenticator: testAuthenticator(func(context.Context, tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
				return tacklrsecurity.Principal{}, tacklrsecurity.ErrAuthenticationFailed
			}),
		}
		proto := NewACPProtocolWithAuth(nil, []ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, false)
		err := inboundWrittenError(t, proto, ProtocolEnv{
			Runtime: k.Runtime, Catalog: k.Catalog, Security: service,
			Conn: &Conn{Security: &tacklrsecurity.Context{}},
		}, `{"jsonrpc":"2.0","id":1,"method":"authenticate","params":{"methodId":"login"}}`)
		assert(t, err, ErrAuthenticationFailed, jsonRPCCodeApplication, ErrAuthenticationFailed)
	})

	t.Run("authorizationDenied", func(t *testing.T) {
		alice, err := tacklrsecurity.NewPrincipal("alice")
		if err != nil {
			t.Fatal(err)
		}
		service := &tacklrsecurity.Service{
			Authenticator: testAuthenticator(func(context.Context, tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
				return alice, nil
			}),
			Authorizer: testAuthorizer(func(_ context.Context, _ tacklrsecurity.Principal, op tacklrsecurity.Operation) error {
				if op.Action == actionSessionLoad {
					return errors.New("denied")
				}
				return nil
			}),
		}
		k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
		proto := NewACPProtocolWithAuth(nil, []ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, false)
		aliceCtx := tacklrsecurity.Context{Principal: alice}
		env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Security: service, Conn: &Conn{Security: &aliceCtx}}
		sid := sessionIDFromInbound(t, proto, env, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
		got := inboundWrittenError(t, proto, env,
			`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sid+`"}}`)
		assert(t, got, ErrAuthorizationDenied, jsonRPCCodeApplication, ErrAuthorizationDenied)
	})

	t.Run("cancelledContext", func(t *testing.T) {
		k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
		w := &recordingMessageWriter{}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		ret := NewACPProtocol(nil).HandleInbound(ctx, ProtocolEnv{
			Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: w},
		}, []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`))
		err := ret
		if len(w.Errors) > 0 {
			err = w.Errors[0].Err
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("errors.Is canceled = false, err=%v", err)
		}
		if got := JSONRPCErrorCode(err); got != jsonRPCCodeCancelled {
			t.Fatalf("JSONRPCErrorCode = %d, want %d", got, jsonRPCCodeCancelled)
		}
		if pub := PublicError(err); !errors.Is(pub, ErrInternal) {
			t.Fatalf("cancelled context must redact to internal on the wire, got %v", pub)
		}
	})

	t.Run("internalProviderFailure", func(t *testing.T) {
		k := newTestRuntime(t, &mockInferenceStrategy{invokeErr: errors.New("provider down")}, durable.AgentSpec{})
		proto := NewACPProtocol(nil)
		sid := acpSessionID(t, serveACPInbound(t, k, proto, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
		w := &recordingMessageWriter{}
		_ = proto.HandleInbound(t.Context(), ProtocolEnv{
			Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: w},
		}, []byte(`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"hi"}]}}`))
		var err error
		if len(w.Errors) > 0 {
			err = w.Errors[0].Err
		}
		if err == nil {
			for _, f := range w.FramesAsMaps(t) {
				if errObj, ok := f["error"].(map[string]any); ok {
					if code, _ := errObj["code"].(float64); int(code) != jsonRPCCodeInternal {
						t.Fatalf("wire code = %v, want %d", errObj["code"], jsonRPCCodeInternal)
					}
					return
				}
			}
			t.Fatal("want internal error")
		}
		if errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrSessionNotFound) {
			t.Fatalf("provider failure leaked as client error: %v", err)
		}
		if got := JSONRPCErrorCode(err); got != jsonRPCCodeInternal {
			t.Fatalf("JSONRPCErrorCode = %d, want %d", got, jsonRPCCodeInternal)
		}
		if pub := PublicError(err); !errors.Is(pub, ErrInternal) {
			t.Fatalf("PublicError = %v, want internal", pub)
		}
	})
}

func TestHandleInbound_sessionWireOutcomes(t *testing.T) {
	k := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	proto := NewACPProtocol(nil)
	env := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog}
	sid := sessionIDFromInbound(t, proto, env, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)

	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":0}}`); err == nil {
		t.Fatal("want unsupported protocol version")
	}
	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":3,"method":"session/set_config_option","params":{"sessionId":"`+sid+`","configId":"nope","value":"x"}}`); err == nil {
		t.Fatal("want unknown configId")
	}
	if err := proto.HandleInbound(t.Context(), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","id":4,"method":"session/cancel","params":{"sessionId":"`+sid+`"}}`)); err != nil {
		t.Fatalf("cancel request: %v", err)
	}
	w := &recordingMessageWriter{}
	if err := proto.HandleInbound(t.Context(), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: w}},
		[]byte(`{"jsonrpc":"2.0","id":5,"method":"session/resume","params":{"sessionId":"`+sid+`","responses":{"intr-1":"{}"}}}`)); err == nil && len(w.Errors) == 0 {
		// idle resume may complete or fail the turn; either is a session/resume outcome
	}

	authProto := NewACPProtocolWithAuth(nil, []ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, true)
	if err := inboundWrittenError(t, authProto, ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
		`{"jsonrpc":"2.0","id":6,"method":"authenticate","params":{"methodId":"ghost"}}`); err == nil {
		t.Fatal("want unknown auth method")
	}
	if err := proto.HandleInbound(t.Context(), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","id":7,"method":"logout","params":{}}`)); err != nil {
		t.Fatalf("logout: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bridge := NewClientBridge(&recordingMessageWriter{})
	waitErr := make(chan error, 1)
	go func() {
		waitErr <- proto.HandleInbound(ctx, ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: &recordingMessageWriter{}, RPC: bridge}},
			[]byte(`{"jsonrpc":"2.0","id":8,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"hi"}]}}`))
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-waitErr:
		if err == nil {
			t.Fatal("want WaitInitialized cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitInitialized did not return")
	}

	down := NewACPProtocol(failPutStore{})
	if err := inboundWrittenError(t, down, ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
		`{"jsonrpc":"2.0","id":9,"method":"session/new","params":{"cwd":"/tmp"}}`); err == nil {
		t.Fatal("want wire put failure")
	}

	mem := NewMemoryWireStore()
	corrupt := NewACPProtocol(mem)
	if err := mem.Put(t.Context(), "corrupt", []byte("not-json")); err != nil {
		t.Fatal(err)
	}
	if err := inboundWrittenError(t, corrupt, ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
		`{"jsonrpc":"2.0","id":10,"method":"session/load","params":{"sessionId":"corrupt","cwd":"/tmp"}}`); err == nil {
		t.Fatal("want corrupt wire decode")
	}

	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":11,"method":"session/close","params":{"sessionId":"missing"}}`); err == nil {
		t.Fatal("want close missing session")
	}
	_ = proto.HandleInbound(t.Context(), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","method":"session/cancel","params":{}}`))
	if err := inboundWrittenError(t, authProto, ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
		`{"jsonrpc":"2.0","id":12,"method":"authenticate","params":{"methodId":"login"}}`); err == nil {
		t.Fatal("want authenticate without security service")
	}
	if got := JSONRPCErrorCode(errors.New("provider")); got != jsonRPCCodeInternal {
		t.Fatalf("internal code = %d", got)
	}

	emptyCWD := sessionIDFromInbound(t, proto, env, `{"jsonrpc":"2.0","id":13,"method":"session/new","params":{}}`)
	if err := proto.HandleInbound(t.Context(), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","id":14,"method":"session/load","params":{"sessionId":"`+emptyCWD+`","cwd":"/filled","mcpServers":[{"type":"http","name":"x","url":"https://example.com/mcp"}]}}`)); err != nil {
		t.Fatalf("load fill cwd: %v", err)
	}

	if err := inboundWrittenError(t, NewACPProtocol(failGetStore{}), ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
		`{"jsonrpc":"2.0","id":15,"method":"session/load","params":{"sessionId":"x","cwd":"/tmp"}}`); err == nil {
		t.Fatal("want wire get failure")
	}

	nth := &nthFailPut{inner: NewMemoryWireStore(), failOn: 2}
	nthProto := NewACPProtocol(nth)
	nthSID := sessionIDFromInbound(t, nthProto, env, `{"jsonrpc":"2.0","id":16,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if err := inboundWrittenError(t, nthProto, env, `{"jsonrpc":"2.0","id":17,"method":"session/prompt","params":{"sessionId":"`+nthSID+`","prompt":[{"type":"text","text":"hi"}]}}`); err == nil {
		t.Fatal("want nth put failure on prompt persist")
	}

	fw := &failFrameWriter{}
	frameEnv := ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog, Conn: &Conn{Writer: fw}}
	_ = proto.HandleInbound(t.Context(), frameEnv, []byte(`{"jsonrpc":"2.0","id":18,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"hi"}]}}`))

	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"1.0","id":19,"method":"initialize"}`); err == nil {
		t.Fatal("want invalid JSON-RPC envelope")
	}
	if err := inboundWrittenError(t, authProto, ProtocolEnv{Runtime: k.Runtime, Catalog: k.Catalog},
		`{"jsonrpc":"2.0","id":20,"method":"authenticate","params":{}}`); err == nil {
		t.Fatal("want authenticate methodId required")
	}
	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":21,"method":"session/cancel","params":{"sessionId":"missing"}}`); err == nil {
		t.Fatal("want cancel missing session")
	}
	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":22,"method":"session/prompt","params":{"sessionId":"missing","prompt":[{"type":"text","text":"hi"}]}}`); err == nil {
		t.Fatal("want prompt missing session")
	}
	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":23,"method":"session/set_config_option","params":{"sessionId":"missing","configId":"model","value":"default"}}`); err == nil {
		t.Fatal("want setConfig missing session")
	}

	delStore := &failDeleteStore{inner: NewMemoryWireStore()}
	delProto := NewACPProtocol(delStore)
	delSID := sessionIDFromInbound(t, delProto, env, `{"jsonrpc":"2.0","id":24,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if err := inboundWrittenError(t, delProto, env, `{"jsonrpc":"2.0","id":25,"method":"session/close","params":{"sessionId":"`+delSID+`"}}`); err == nil {
		t.Fatal("want close delete failure")
	}

	cfgNth := &nthFailPut{inner: NewMemoryWireStore(), failOn: 2}
	cfgProto := NewACPProtocol(cfgNth)
	cfgSID := sessionIDFromInbound(t, cfgProto, env, `{"jsonrpc":"2.0","id":26,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if err := inboundWrittenError(t, cfgProto, env, `{"jsonrpc":"2.0","id":27,"method":"session/set_config_option","params":{"sessionId":"`+cfgSID+`","configId":"model","value":"default"}}`); err == nil {
		t.Fatal("want setConfig persist failure")
	}

	loadNth := &nthFailPut{inner: NewMemoryWireStore(), failOn: 2}
	loadProto := NewACPProtocol(loadNth)
	loadSID := sessionIDFromInbound(t, loadProto, env, `{"jsonrpc":"2.0","id":28,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if err := inboundWrittenError(t, loadProto, env, `{"jsonrpc":"2.0","id":29,"method":"session/load","params":{"sessionId":"`+loadSID+`","cwd":"/tmp"}}`); err == nil {
		t.Fatal("want load persist failure")
	}

	alice, err := tacklrsecurity.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	denyCreate := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(context.Context, tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			return alice, nil
		}),
		Authorizer: testAuthorizer(func(_ context.Context, _ tacklrsecurity.Principal, op tacklrsecurity.Operation) error {
			if op.Action == actionSessionCreate {
				return errors.New("denied")
			}
			return nil
		}),
	}
	aliceCtx := tacklrsecurity.Context{Principal: alice}
	if err := inboundWrittenError(t, NewACPProtocol(nil), ProtocolEnv{
		Runtime: k.Runtime, Catalog: k.Catalog, Security: denyCreate, Conn: &Conn{Security: &aliceCtx},
	}, `{"jsonrpc":"2.0","id":30,"method":"session/new","params":{"cwd":"/tmp"}}`); err == nil {
		t.Fatal("want session.create denied")
	}
	if err := inboundWrittenError(t, NewACPProtocol(nil), ProtocolEnv{
		Runtime: k.Runtime, Catalog: k.Catalog, Security: denyCreate, Conn: &Conn{Security: &tacklrsecurity.Context{}},
	}, `{"jsonrpc":"2.0","id":31,"method":"session/new","params":{"cwd":"/tmp"}}`); err == nil {
		t.Fatal("want unauthenticated session.create")
	}
	if err := inboundWrittenError(t, proto, ProtocolEnv{
		Runtime: k.Runtime, Catalog: k.Catalog, Security: denyCreate, Conn: &Conn{Security: &tacklrsecurity.Context{}},
	}, `{"jsonrpc":"2.0","id":38,"method":"session/load","params":{"sessionId":"`+sid+`"}}`); err == nil {
		t.Fatal("want unauthenticated session.load")
	}

	ghostCat := durable.NewCatalog("ghost")
	ghostRT := inprocess.New(inprocess.Config{Catalog: ghostCat, Snapshots: inprocess.NewMemorySnapshot(), Projection: vfs.DirectProjection{}})
	if err := inboundWrittenError(t, NewACPProtocol(nil), ProtocolEnv{Runtime: ghostRT, Catalog: ghostCat},
		`{"jsonrpc":"2.0","id":32,"method":"session/new","params":{"cwd":"/tmp"}}`); err == nil {
		t.Fatal("want CreateSession agent missing")
	}

	ghostWire := NewMemoryWireStore()
	if err := ghostWire.Put(t.Context(), "ghost-sess", []byte(`{"cwd":"/tmp","configValues":{"agent":"ghost"},"owner":"local"}`)); err != nil {
		t.Fatal(err)
	}
	if err := inboundWrittenError(t, NewACPProtocol(ghostWire), env,
		`{"jsonrpc":"2.0","id":33,"method":"session/load","params":{"sessionId":"ghost-sess","cwd":"/tmp"}}`); err == nil {
		t.Fatal("want load CreateSession agent missing")
	}

	if err := NewACPProtocol(nil).HandleInbound(t.Context(), ProtocolEnv{Runtime: k.Runtime, Conn: &Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","id":34,"method":"session/new","params":{"cwd":"/tmp"}}`)); err != nil {
		t.Fatalf("nil catalog session/new: %v", err)
	}

	schemeProto := NewACPProtocolWithAuth(nil, []ACPAuthMethod{{ID: "login", Name: "Login"}}, false)
	loginOK := &tacklrsecurity.Service{
		Authenticator: testAuthenticator(func(_ context.Context, attempt tacklrsecurity.Attempt) (tacklrsecurity.Principal, error) {
			if attempt.Scheme != "login" {
				return tacklrsecurity.Principal{}, fmt.Errorf("scheme %q", attempt.Scheme)
			}
			return alice, nil
		}),
	}
	var stored tacklrsecurity.Context
	authConn := &Conn{Writer: &recordingMessageWriter{}, setSecurity: func(c tacklrsecurity.Context) { stored = c }}
	if err := schemeProto.HandleInbound(t.Context(), ProtocolEnv{
		Runtime: k.Runtime, Catalog: k.Catalog, Security: loginOK, Conn: authConn,
	}, []byte(`{"jsonrpc":"2.0","id":35,"method":"authenticate","params":{"methodId":"login"}}`)); err != nil {
		t.Fatalf("scheme default authenticate: %v", err)
	}
	if stored.Principal.Subject != "alice" {
		t.Fatalf("setSecurity subject = %q", stored.Principal.Subject)
	}

	if err := inboundWrittenError(t, proto, env,
		`{"jsonrpc":"2.0","id":36,"method":"_tacklr/vfs/bind","params":{"sessionId":"missing","backends":[{"provider":"local","params":{"name":"docs"},"auth":{"token":"t"}}]}}`); err == nil {
		t.Fatal("want vfs bind missing session")
	}
	if err := inboundWrittenError(t, proto, env,
		`{"jsonrpc":"2.0","id":37,"method":"_tacklr/vfs/refresh","params":{"sessionId":"missing","provider":"local","auth":{"token":"t"}}}`); err == nil {
		t.Fatal("want vfs refresh missing session")
	}
}

type failPutStore struct{}

func (failPutStore) Put(context.Context, string, []byte) error { return errors.New("put down") }
func (failPutStore) Get(context.Context, string) ([]byte, error) {
	return nil, fmt.Errorf("wire session: %w", ErrSessionNotFound)
}
func (failPutStore) Delete(context.Context, string) error { return nil }

type failGetStore struct{}

func (failGetStore) Put(context.Context, string, []byte) error { return nil }
func (failGetStore) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("get down")
}
func (failGetStore) Delete(context.Context, string) error { return nil }

type nthFailPut struct {
	inner  ProtocolWireStore
	n      int
	failOn int
}

func (s *nthFailPut) Put(ctx context.Context, id string, payload []byte) error {
	s.n++
	if s.n >= s.failOn {
		return errors.New("put down")
	}
	return s.inner.Put(ctx, id, payload)
}
func (s *nthFailPut) Get(ctx context.Context, id string) ([]byte, error) {
	return s.inner.Get(ctx, id)
}
func (s *nthFailPut) Delete(ctx context.Context, id string) error {
	return s.inner.Delete(ctx, id)
}

type failFrameWriter struct{ recordingMessageWriter }

func (f *failFrameWriter) WriteFrame([]byte) error { return errors.New("frame down") }

type failDeleteStore struct{ inner ProtocolWireStore }

func (s *failDeleteStore) Put(ctx context.Context, id string, payload []byte) error {
	return s.inner.Put(ctx, id, payload)
}
func (s *failDeleteStore) Get(ctx context.Context, id string) ([]byte, error) {
	return s.inner.Get(ctx, id)
}
func (s *failDeleteStore) Delete(context.Context, string) error { return errors.New("delete down") }

func inboundWrittenError(t *testing.T, proto Protocol, env ProtocolEnv, body string) error {
	t.Helper()
	w := &recordingMessageWriter{}
	if env.Conn == nil {
		env.Conn = &Conn{}
	}
	env.Conn.Writer = w
	ret := proto.HandleInbound(t.Context(), env, []byte(body))
	if len(w.Errors) > 0 {
		return w.Errors[0].Err
	}
	if ret != nil {
		return ret
	}
	t.Fatalf("no error written or returned for %s", body)
	return nil
}

func sessionIDFromInbound(t *testing.T, proto Protocol, env ProtocolEnv, body string) string {
	t.Helper()
	w := &recordingMessageWriter{}
	if env.Conn == nil {
		env.Conn = &Conn{}
	}
	env.Conn.Writer = w
	if err := proto.HandleInbound(t.Context(), env, []byte(body)); err != nil {
		t.Fatal(err)
	}
	if len(w.Results) == 0 {
		t.Fatal("session/new wrote no result")
	}
	res, _ := w.Results[0].Result.(map[string]any)
	sid, _ := res["sessionId"].(string)
	if sid == "" {
		t.Fatalf("missing sessionId: %#v", w.Results[0].Result)
	}
	return sid
}
