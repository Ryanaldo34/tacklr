package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"

	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/session"
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
		if got := acp.ErrorCode(err); got != code {
			t.Fatalf("acp.ErrorCode = %d, want %d", got, code)
		}
		if pub := server.PublicError(err); !errors.Is(pub, public) {
			t.Fatalf("server.PublicError = %v, want %v", pub, public)
		}
		envelope := acp.ErrorBody(json.RawMessage(`1`), err)
		errObj := envelope["error"].(map[string]any)
		if errObj["code"] != code {
			t.Fatalf("wire code = %v, want %d", errObj["code"], code)
		}
	}

	t.Run("methodNotFound", func(t *testing.T) {
		k := fakeHost(t)
		err := inboundWrittenError(t, acp.New(nil), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
			`{"jsonrpc":"2.0","id":1,"method":"session/foo","params":{}}`)
		assert(t, err, server.ErrMethodNotFound, acp.CodeMethodNotFound, server.ErrMethodNotFound)
	})

	t.Run("invalidRequest", func(t *testing.T) {
		k := fakeHost(t)
		err := inboundWrittenError(t, acp.New(nil), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
			`{"jsonrpc":"2.0","id":1,"method":"session/load","params":{}}`)
		assert(t, err, server.ErrInvalidRequest, acp.CodeInvalidRequest, server.ErrInvalidRequest)
	})

	t.Run("sessionNotFound", func(t *testing.T) {
		k := fakeHost(t)
		err := inboundWrittenError(t, acp.New(nil), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
			`{"jsonrpc":"2.0","id":1,"method":"session/load","params":{"sessionId":"missing"}}`)
		assert(t, err, server.ErrSessionNotFound, acp.CodeApplication, server.ErrSessionNotFound)
	})

	t.Run("unknownConfig", func(t *testing.T) {
		k := fakeHost(t)
		proto := acp.New(nil)
		env := server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent}
		sid := acpSessionID(t, serveACPInbound(t, k, proto, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
		err := inboundWrittenError(t, proto, env,
			`{"jsonrpc":"2.0","id":2,"method":"session/set_config_option","params":{"sessionId":"`+sid+`","configId":"model","value":"ghost"}}`)
		assert(t, err, server.ErrInvalidRequest, acp.CodeInvalidRequest, server.ErrInvalidRequest)
	})

	t.Run("authenticationRequired", func(t *testing.T) {
		k := fakeHost(t)
		proto := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, false)
		err := inboundWrittenError(t, proto, server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
			`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
		assert(t, err, server.ErrAuthenticationRequired, acp.CodeApplication, server.ErrAuthenticationRequired)
	})

	t.Run("authenticationFailed", func(t *testing.T) {
		k := fakeHost(t)
		service := &server.Service{
			Authenticator: testAuthenticator(func(context.Context, server.Attempt) (server.Principal, error) {
				return server.Principal{}, server.ErrAuthenticationFailed
			}),
		}
		proto := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, false)
		err := inboundWrittenError(t, proto, server.ProtocolEnv{
			Runtime: k.Runtime, Agent: k.Agent, Security: service,
			Conn: &server.Conn{Security: &server.Context{}},
		}, `{"jsonrpc":"2.0","id":1,"method":"authenticate","params":{"methodId":"login"}}`)
		assert(t, err, server.ErrAuthenticationFailed, acp.CodeApplication, server.ErrAuthenticationFailed)
	})

	t.Run("authorizationDenied", func(t *testing.T) {
		alice, err := server.NewPrincipal("alice")
		if err != nil {
			t.Fatal(err)
		}
		service := &server.Service{
			Authenticator: testAuthenticator(func(context.Context, server.Attempt) (server.Principal, error) {
				return alice, nil
			}),
			Authorizer: testAuthorizer(func(_ context.Context, _ server.Principal, op server.Operation) error {
				if op.Action == "session.load" {
					return errors.New("denied")
				}
				return nil
			}),
		}
		k := fakeHost(t)
		proto := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, false)
		aliceCtx := server.Context{Principal: alice}
		env := server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent, Security: service, Conn: &server.Conn{Security: &aliceCtx}}
		sid := sessionIDFromInbound(t, proto, env, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
		got := inboundWrittenError(t, proto, env,
			`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"`+sid+`"}}`)
		assert(t, got, server.ErrAuthorizationDenied, acp.CodeApplication, server.ErrAuthorizationDenied)
	})

	t.Run("cancelledContext", func(t *testing.T) {
		k := fakeHost(t)
		w := &recordingMessageWriter{}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		ret := acp.New(nil).HandleInbound(ctx, server.ProtocolEnv{
			Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: w},
		}, []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`))
		err := ret
		if len(w.Errors) > 0 {
			err = w.Errors[0].Err
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("errors.Is canceled = false, err=%v", err)
		}
		if got := acp.ErrorCode(err); got != acp.CodeCancelled {
			t.Fatalf("acp.ErrorCode = %d, want %d", got, acp.CodeCancelled)
		}
		if pub := server.PublicError(err); !errors.Is(pub, server.ErrInternal) {
			t.Fatalf("cancelled context must redact to internal on the wire, got %v", pub)
		}
	})

	t.Run("internalProviderFailure", func(t *testing.T) {
		k := newTestRuntime(t, testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventError, Error: errors.New("provider down"), Content: "provider down"}
		}), tacklr.AgentOptions{})
		proto := acp.New(nil)
		sid := acpSessionID(t, serveACPInbound(t, k, proto, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
		w := &recordingMessageWriter{}
		_ = proto.HandleInbound(t.Context(), server.ProtocolEnv{
			Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: w},
		}, []byte(`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"hi"}]}}`))
		var err error
		if len(w.Errors) > 0 {
			err = w.Errors[0].Err
		}
		if err == nil {
			for _, f := range w.FramesAsMaps(t) {
				if errObj, ok := f["error"].(map[string]any); ok {
					if code, _ := errObj["code"].(float64); int(code) != acp.CodeInternal {
						t.Fatalf("wire code = %v, want %d", errObj["code"], acp.CodeInternal)
					}
					return
				}
			}
			t.Fatal("want internal error")
		}
		if errors.Is(err, server.ErrInvalidRequest) || errors.Is(err, server.ErrSessionNotFound) {
			t.Fatalf("provider failure leaked as client error: %v", err)
		}
		if got := acp.ErrorCode(err); got != acp.CodeInternal {
			t.Fatalf("acp.ErrorCode = %d, want %d", got, acp.CodeInternal)
		}
		if pub := server.PublicError(err); !errors.Is(pub, server.ErrInternal) {
			t.Fatalf("server.PublicError = %v, want internal", pub)
		}
	})
}

func TestHandleInbound_sessionWireOutcomes(t *testing.T) {
	k := newTestRuntime(t, testkit.HTTPModel(t, nil), tacklr.AgentOptions{})
	proto := acp.New(nil)
	env := server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent}
	sid := sessionIDFromInbound(t, proto, env, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)

	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":0}}`); err == nil {
		t.Fatal("want unsupported protocol version")
	}
	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":3,"method":"session/set_config_option","params":{"sessionId":"`+sid+`","configId":"nope","value":"x"}}`); err == nil {
		t.Fatal("want unknown configId")
	}
	if err := proto.HandleInbound(t.Context(), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","id":4,"method":"session/cancel","params":{"sessionId":"`+sid+`"}}`)); err != nil {
		t.Fatalf("cancel request: %v", err)
	}
	w := &recordingMessageWriter{}
	if err := proto.HandleInbound(t.Context(), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: w}},
		[]byte(`{"jsonrpc":"2.0","id":5,"method":"session/resume","params":{"sessionId":"`+sid+`","responses":{"intr-1":"{}"}}}`)); err != nil {
		t.Fatalf("session/resume: %v", err)
	}
	var frames strings.Builder
	for _, f := range w.Frames {
		frames.Write(f)
	}
	if !strings.Contains(frames.String(), "intr-1") {
		t.Fatalf("idle resume should report unknown interrupt, frames=%s errors=%v", frames.String(), w.Errors)
	}

	authProto := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login", Scheme: "host"}}, true)
	if err := inboundWrittenError(t, authProto, server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
		`{"jsonrpc":"2.0","id":6,"method":"authenticate","params":{"methodId":"ghost"}}`); err == nil {
		t.Fatal("want unknown auth method")
	}
	if err := proto.HandleInbound(t.Context(), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","id":7,"method":"logout","params":{}}`)); err != nil {
		t.Fatalf("logout: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bridge := acp.NewClientBridge(&recordingMessageWriter{})
	waitErr := make(chan error, 1)
	go func() {
		waitErr <- proto.HandleInbound(ctx, server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: &recordingMessageWriter{}, Ask: bridge}},
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

	down := acp.New(failPutStore{})
	if err := inboundWrittenError(t, down, server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
		`{"jsonrpc":"2.0","id":9,"method":"session/new","params":{"cwd":"/tmp"}}`); err == nil {
		t.Fatal("want wire put failure")
	}

	mem := server.NewMemoryWireStore()
	corrupt := acp.New(mem)
	if err := mem.Put(t.Context(), "corrupt", []byte("not-json")); err != nil {
		t.Fatal(err)
	}
	if err := inboundWrittenError(t, corrupt, server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
		`{"jsonrpc":"2.0","id":10,"method":"session/load","params":{"sessionId":"corrupt","cwd":"/tmp"}}`); err == nil {
		t.Fatal("want corrupt wire decode")
	}

	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":11,"method":"session/close","params":{"sessionId":"missing"}}`); err == nil {
		t.Fatal("want close missing session")
	}
	_ = proto.HandleInbound(t.Context(), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","method":"session/cancel","params":{}}`))
	if err := inboundWrittenError(t, authProto, server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
		`{"jsonrpc":"2.0","id":12,"method":"authenticate","params":{"methodId":"login"}}`); err == nil {
		t.Fatal("want authenticate without security service")
	}
	if got := acp.ErrorCode(errors.New("provider")); got != acp.CodeInternal {
		t.Fatalf("internal code = %d", got)
	}

	emptyCWD := sessionIDFromInbound(t, proto, env, `{"jsonrpc":"2.0","id":13,"method":"session/new","params":{}}`)
	if err := proto.HandleInbound(t.Context(), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","id":14,"method":"session/load","params":{"sessionId":"`+emptyCWD+`","cwd":"/filled","mcpServers":[{"type":"http","name":"x","url":"https://example.com/mcp"}]}}`)); err != nil {
		t.Fatalf("load fill cwd: %v", err)
	}

	if err := inboundWrittenError(t, acp.New(failGetStore{}), server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
		`{"jsonrpc":"2.0","id":15,"method":"session/load","params":{"sessionId":"x","cwd":"/tmp"}}`); err == nil {
		t.Fatal("want wire get failure")
	}

	nth := &nthFailPut{inner: server.NewMemoryWireStore(), failOn: 2}
	nthProto := acp.New(nth)
	nthSID := sessionIDFromInbound(t, nthProto, env, `{"jsonrpc":"2.0","id":16,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if err := inboundWrittenError(t, nthProto, env, `{"jsonrpc":"2.0","id":17,"method":"session/prompt","params":{"sessionId":"`+nthSID+`","prompt":[{"type":"text","text":"hi"}]}}`); err == nil {
		t.Fatal("want nth put failure on prompt persist")
	}

	fw := &failFrameWriter{}
	frameEnv := server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent, Conn: &server.Conn{Writer: fw}}
	_ = proto.HandleInbound(t.Context(), frameEnv, []byte(`{"jsonrpc":"2.0","id":18,"method":"session/prompt","params":{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"hi"}]}}`))

	if err := inboundWrittenError(t, proto, env, `{"jsonrpc":"1.0","id":19,"method":"initialize"}`); err == nil {
		t.Fatal("want invalid JSON-RPC envelope")
	}
	if err := inboundWrittenError(t, authProto, server.ProtocolEnv{Runtime: k.Runtime, Agent: k.Agent},
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

	delStore := &failDeleteStore{inner: server.NewMemoryWireStore()}
	delProto := acp.New(delStore)
	delSID := sessionIDFromInbound(t, delProto, env, `{"jsonrpc":"2.0","id":24,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if err := inboundWrittenError(t, delProto, env, `{"jsonrpc":"2.0","id":25,"method":"session/close","params":{"sessionId":"`+delSID+`"}}`); err == nil {
		t.Fatal("want close delete failure")
	}

	cfgNth := &nthFailPut{inner: server.NewMemoryWireStore(), failOn: 2}
	cfgProto := acp.New(cfgNth)
	cfgSID := sessionIDFromInbound(t, cfgProto, env, `{"jsonrpc":"2.0","id":26,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if err := inboundWrittenError(t, cfgProto, env, `{"jsonrpc":"2.0","id":27,"method":"session/set_config_option","params":{"sessionId":"`+cfgSID+`","configId":"model","value":"default"}}`); err == nil {
		t.Fatal("want setConfig persist failure")
	}

	loadNth := &nthFailPut{inner: server.NewMemoryWireStore(), failOn: 2}
	loadProto := acp.New(loadNth)
	loadSID := sessionIDFromInbound(t, loadProto, env, `{"jsonrpc":"2.0","id":28,"method":"session/new","params":{"cwd":"/tmp"}}`)
	if err := inboundWrittenError(t, loadProto, env, `{"jsonrpc":"2.0","id":29,"method":"session/load","params":{"sessionId":"`+loadSID+`","cwd":"/tmp"}}`); err == nil {
		t.Fatal("want load persist failure")
	}

	alice, err := server.NewPrincipal("alice")
	if err != nil {
		t.Fatal(err)
	}
	denyCreate := &server.Service{
		Authenticator: testAuthenticator(func(context.Context, server.Attempt) (server.Principal, error) {
			return alice, nil
		}),
		Authorizer: testAuthorizer(func(_ context.Context, _ server.Principal, op server.Operation) error {
			if op.Action == "session.create" {
				return errors.New("denied")
			}
			return nil
		}),
	}
	aliceCtx := server.Context{Principal: alice}
	if err := inboundWrittenError(t, acp.New(nil), server.ProtocolEnv{
		Runtime: k.Runtime, Agent: k.Agent, Security: denyCreate, Conn: &server.Conn{Security: &aliceCtx},
	}, `{"jsonrpc":"2.0","id":30,"method":"session/new","params":{"cwd":"/tmp"}}`); err == nil {
		t.Fatal("want session.create denied")
	}
	if err := inboundWrittenError(t, acp.New(nil), server.ProtocolEnv{
		Runtime: k.Runtime, Agent: k.Agent, Security: denyCreate, Conn: &server.Conn{Security: &server.Context{}},
	}, `{"jsonrpc":"2.0","id":31,"method":"session/new","params":{"cwd":"/tmp"}}`); err == nil {
		t.Fatal("want unauthenticated session.create")
	}
	if err := inboundWrittenError(t, proto, server.ProtocolEnv{
		Runtime: k.Runtime, Agent: k.Agent, Security: denyCreate, Conn: &server.Conn{Security: &server.Context{}},
	}, `{"jsonrpc":"2.0","id":38,"method":"session/load","params":{"sessionId":"`+sid+`"}}`); err == nil {
		t.Fatal("want unauthenticated session.load")
	}

	downRT := fakeRuntime{create: func(context.Context, session.CreateSession) (session.SessionID, error) {
		return "", errors.New("runtime down")
	}}
	if err := inboundWrittenError(t, acp.New(nil), server.ProtocolEnv{Runtime: downRT, Agent: k.Agent},
		`{"jsonrpc":"2.0","id":32,"method":"session/new","params":{"cwd":"/tmp"}}`); err == nil {
		t.Fatal("want CreateSession failure")
	}

	if err := acp.New(nil).HandleInbound(t.Context(), server.ProtocolEnv{Runtime: k.Runtime, Conn: &server.Conn{Writer: &recordingMessageWriter{}}},
		[]byte(`{"jsonrpc":"2.0","id":34,"method":"session/new","params":{"cwd":"/tmp"}}`)); err != nil {
		t.Fatalf("nil catalog session/new: %v", err)
	}

	schemeProto := acp.NewWithAuth(nil, []acp.ACPAuthMethod{{ID: "login", Name: "Login"}}, false)
	loginOK := &server.Service{
		Authenticator: testAuthenticator(func(_ context.Context, attempt server.Attempt) (server.Principal, error) {
			if attempt.Scheme != "login" {
				return server.Principal{}, fmt.Errorf("scheme %q", attempt.Scheme)
			}
			return alice, nil
		}),
	}
	var stored server.Context
	authConn := &server.Conn{Writer: &recordingMessageWriter{}, SetSecurity: func(c server.Context) { stored = c }}
	if err := schemeProto.HandleInbound(t.Context(), server.ProtocolEnv{
		Runtime: k.Runtime, Agent: k.Agent, Security: loginOK, Conn: authConn,
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
	return nil, fmt.Errorf("wire session: %w", server.ErrSessionNotFound)
}
func (failPutStore) Delete(context.Context, string) error { return nil }

type failGetStore struct{}

func (failGetStore) Put(context.Context, string, []byte) error { return nil }
func (failGetStore) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("get down")
}
func (failGetStore) Delete(context.Context, string) error { return nil }

type nthFailPut struct {
	inner  server.ProtocolWireStore
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

type failDeleteStore struct{ inner server.ProtocolWireStore }

func (s *failDeleteStore) Put(ctx context.Context, id string, payload []byte) error {
	return s.inner.Put(ctx, id, payload)
}
func (s *failDeleteStore) Get(ctx context.Context, id string) ([]byte, error) {
	return s.inner.Get(ctx, id)
}
func (s *failDeleteStore) Delete(context.Context, string) error { return errors.New("delete down") }

func inboundWrittenError(t *testing.T, proto server.Protocol, env server.ProtocolEnv, body string) error {
	t.Helper()
	w := &recordingMessageWriter{}
	if env.Conn == nil {
		env.Conn = &server.Conn{}
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

func sessionIDFromInbound(t *testing.T, proto server.Protocol, env server.ProtocolEnv, body string) string {
	t.Helper()
	w := &recordingMessageWriter{}
	if env.Conn == nil {
		env.Conn = &server.Conn{}
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
