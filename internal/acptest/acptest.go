// Package acptest drives the ACP protocol in tests: a session client and
// agent, single RPCs over an HTTP recorder, and decoding of the frames and
// errors the protocol writes.
package acptest

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/livesess"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/workspace"
)

// Runtime is the session client and agent an ACP request runs against.
// Workspace says the sessions have a workspace, so clients may bind to it.
type Runtime struct {
	Client    session.Client
	Agent     tacklr.AgentOptions
	Workspace bool
}

// NewRuntime runs spec on the live Temporal stack, with no workspace. A nil
// spec.Model takes model, else a model that sends nothing; the window and
// system prompt get test defaults.
func NewRuntime(t *testing.T, model tacklr.Model, spec tacklr.AgentOptions) *Runtime {
	t.Helper()
	return NewWorkspaceRuntime(t, model, spec, workspace.Config{})
}

// NewWorkspaceRuntime is NewRuntime with ws as the agent's workspace.
func NewWorkspaceRuntime(t *testing.T, model tacklr.Model, spec tacklr.AgentOptions, ws workspace.Config) *Runtime {
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
	return &Runtime{Client: livesess.Runtime(t, spec, ws), Agent: spec, Workspace: ws.Open != nil}
}

// Serve runs one ACP request body against r through proto and returns what
// it wrote.
func Serve(t *testing.T, r *Runtime, proto server.Protocol, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	env := server.ProtocolEnv{Client: r.Client, Agent: r.Agent, Workspace: r.Workspace, Conn: &server.Conn{Writer: acp.HTTPWriter(rec)}}
	_ = proto.HandleInbound(t.Context(), env, []byte(body))
	return rec
}

// Server is one ACP protocol over its own wire store, for tests that send
// several RPCs to the same sessions.
type Server struct {
	t     *testing.T
	r     *Runtime
	proto server.Protocol
	Wire  server.ProtocolWireStore
}

// NewServer is an ACP server for r that keeps its wire state in wire.
func NewServer(t *testing.T, r *Runtime, wire server.ProtocolWireStore) *Server {
	t.Helper()
	return &Server{t: t, r: r, proto: acp.New(wire), Wire: wire}
}

// RPC runs one request body and returns what the server wrote.
func (s *Server) RPC(body string) *httptest.ResponseRecorder {
	s.t.Helper()
	return Serve(s.t, s.r, s.proto, body)
}

// Result is the result object of a JSON-RPC response; an error response
// fails the test.
func Result(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
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

// SessionID is the sessionId a session/new response returned.
func SessionID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	id, _ := Result(t, rec)["sessionId"].(string)
	if id == "" {
		t.Fatalf("missing sessionId in %s", rec.Body.String())
	}
	return id
}

// Frames decodes the JSON frames written to body, in order.
func Frames(t *testing.T, body io.Reader) []map[string]any {
	t.Helper()
	var frames []map[string]any
	dec := json.NewDecoder(body)
	for {
		var frame map[string]any
		if err := dec.Decode(&frame); err == io.EOF {
			return frames
		} else if err != nil {
			t.Fatalf("decode ACP frame: %v", err)
		}
		frames = append(frames, frame)
	}
}

// WrittenError is the error proto answers body with, as an HTTP client
// gets it; it fails the test when there is none.
func WrittenError(t *testing.T, proto server.Protocol, env server.ProtocolEnv, body string) error {
	t.Helper()
	if env.Conn == nil {
		env.Conn = &server.Conn{}
	}
	env.Conn.Writer = acp.HTTPWriter(httptest.NewRecorder())
	err := proto.HandleInbound(t.Context(), env, []byte(body))
	if err == nil {
		t.Fatalf("no error for %s", body)
	}
	return err
}
