// Package acptest drives ACP in tests as an editor does: a session client
// and agent, a server on a real HTTP listener, and a client on a WebSocket
// that reads the frames in order and answers the agent's requests.
package acptest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

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

// Server is an anonymous ACP server for r, with its wire state in wire
// (nil: in memory).
func Server(r *Runtime, wire server.ProtocolWireStore) *server.Server {
	srv := server.NewServer(r.Client, r.Agent, acp.New(wire)).AllowAnonymousNetwork()
	srv.Workspace = r.Workspace
	return srv
}

// Listen serves srv on a test HTTP listener and returns the ws:// URL of
// its /acp.
func Listen(t *testing.T, srv *server.Server) string {
	t.Helper()
	hs := httptest.NewServer(srv.HTTPMux())
	t.Cleanup(hs.Close)
	return "ws" + strings.TrimPrefix(hs.URL, "http") + "/acp"
}

// Client is an ACP client on one WebSocket.
type Client struct {
	t    *testing.T
	conn *websocket.Conn
	next int
}

// Dial connects to url with header and says nothing.
func Dial(t *testing.T, url string, header http.Header) *Client {
	t.Helper()
	conn, resp, err := websocket.Dial(t.Context(), url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	conn.SetReadLimit(64 << 20)
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	return &Client{t: t, conn: conn, next: 1000}
}

// Connect dials url and says hello with the initialize params.
func Connect(t *testing.T, url, initialize string) *Client {
	t.Helper()
	c := Dial(t, url, nil)
	if resp := c.Call("initialize", initialize); resp["error"] != nil {
		t.Fatalf("initialize = %v", resp)
	}
	return c
}

// Open is a connection to a new anonymous ACP server for r that said
// hello.
func Open(t *testing.T, r *Runtime) *Client {
	t.Helper()
	return Connect(t, Listen(t, Server(r, nil)), `{"protocolVersion":1}`)
}

// Use makes the client's failures fail t.
func (c *Client) Use(t *testing.T) *Client {
	c.t = t
	return c
}

// Send writes one frame.
func (c *Client) Send(body string) {
	c.t.Helper()
	if err := c.conn.Write(c.t.Context(), websocket.MessageText, []byte(body)); err != nil {
		c.t.Fatalf("ws write: %v", err)
	}
}

// Close closes the socket, as an editor that quits does.
func (c *Client) Close() { _ = c.conn.Close(websocket.StatusNormalClosure, "") }

// Frame is the next frame the agent sends.
func (c *Client) Frame() map[string]any {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), 15*time.Second)
	defer cancel()
	_, data, err := c.conn.Read(ctx)
	if err != nil {
		c.t.Fatalf("ws read: %v", err)
	}
	var frame map[string]any
	if err := json.Unmarshal(data, &frame); err != nil {
		c.t.Fatalf("unmarshal frame %q: %v", data, err)
	}
	return frame
}

// Request sends method with params and returns its id.
func (c *Client) Request(method, params string) int {
	c.t.Helper()
	c.next++
	c.Send(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, c.next, method, params))
	return c.next
}

// Until reads frames until the response to id, answering each request the
// agent makes with answer, and returns that response and the frames before
// it. A request with no answer fails the test.
func (c *Client) Until(id int, answer func(req map[string]any) any) (map[string]any, []map[string]any) {
	c.t.Helper()
	var before []map[string]any
	for {
		frame := c.Frame()
		if method, _ := frame["method"].(string); method != "" && frame["id"] != nil {
			if answer == nil {
				c.t.Fatalf("unexpected request %v", frame)
			}
			body, _ := json.Marshal(answer(frame))
			c.Send(string(body))
			continue
		}
		if frame["method"] == nil && frame["id"] == float64(id) {
			return frame, before
		}
		before = append(before, frame)
	}
}

// Call sends method with params and returns its response.
func (c *Client) Call(method, params string) map[string]any {
	c.t.Helper()
	resp, _ := c.Until(c.Request(method, params), nil)
	return resp
}

// NewSession makes a session in cwd and returns its id.
func (c *Client) NewSession(cwd string) string {
	c.t.Helper()
	return SessionID(c.t, c.Call("session/new", `{"cwd":"`+cwd+`"}`))
}

// Prompt sends blocks (a JSON array of content blocks) to session sid and
// returns the request id.
func (c *Client) Prompt(sid, blocks string) int {
	c.t.Helper()
	return c.Request("session/prompt", `{"sessionId":"`+sid+`","prompt":`+blocks+`}`)
}

// Say sends text to session sid and returns how the turn ended and the
// frames before.
func (c *Client) Say(sid, text string) (map[string]any, []map[string]any) {
	c.t.Helper()
	return c.Until(c.Prompt(sid, `[{"type":"text","text":"`+text+`"}]`), nil)
}

// Result is the result object of a response; an error response fails the
// test.
func Result(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	if errObj := resp["error"]; errObj != nil {
		t.Fatalf("unexpected error: %v", errObj)
	}
	res, _ := resp["result"].(map[string]any)
	return res
}

// Error is the error object of a response; a response with none fails the
// test.
func Error(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	errObj, _ := resp["error"].(map[string]any)
	if errObj == nil {
		t.Fatalf("expected an error, got %v", resp)
	}
	return errObj
}

// Message is the message of an error response; a response with no error
// fails the test.
func Message(t *testing.T, resp map[string]any) string {
	t.Helper()
	msg, _ := Error(t, resp)["message"].(string)
	return msg
}

// SessionID is the sessionId a session/new response returned.
func SessionID(t *testing.T, resp map[string]any) string {
	t.Helper()
	id, _ := Result(t, resp)["sessionId"].(string)
	if id == "" {
		t.Fatalf("missing sessionId in %v", resp)
	}
	return id
}

// StopReason is the stopReason a prompt's response ends with; "" when it
// is an error.
func StopReason(resp map[string]any) string {
	res, _ := resp["result"].(map[string]any)
	sr, _ := res["stopReason"].(string)
	return sr
}

// Updates are the session/update payloads among frames, in order.
func Updates(frames []map[string]any) []map[string]any {
	var out []map[string]any
	for _, f := range frames {
		if params, ok := f["params"].(map[string]any); ok && f["method"] == "session/update" {
			update, _ := params["update"].(map[string]any)
			out = append(out, update)
		}
	}
	return out
}
