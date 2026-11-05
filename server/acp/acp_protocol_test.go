package acp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"
)

// startACPWSServer serves r's ACP over HTTP, as a host does.
func startACPWSServer(t *testing.T, r *acptest.Runtime) (*httptest.Server, *server.Server) {
	t.Helper()
	srv := server.NewServer(r.Client, r.Agent, acp.New(server.NewMemoryWireStore()))
	hs := httptest.NewServer(srv.HTTPMux())
	t.Cleanup(hs.Close)
	return hs, srv
}

// wsClient is an ACP client on a WebSocket to the server's /acp route.
type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
	// ID is the Acp-Connection-Id of the upgrade response.
	ID   string
	next int
}

// dial connects to hs and says hello with the initialize params.
func dial(t *testing.T, hs *httptest.Server, initialize string) *wsClient {
	t.Helper()
	conn, resp, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(hs.URL, "http")+"/acp", nil)
	if err != nil {
		t.Fatalf("dial /acp: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	c := &wsClient{t: t, conn: conn, ID: resp.Header.Get(acp.HeaderConnectionID)}
	c.send("") // an empty message is not a request
	c.call("initialize", initialize)
	return c
}

func (c *wsClient) send(body string) {
	c.t.Helper()
	if err := c.conn.Write(c.t.Context(), websocket.MessageText, []byte(body)); err != nil {
		c.t.Fatalf("ws write: %v", err)
	}
}

func (c *wsClient) frame() map[string]any {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.t.Context(), 5*time.Second)
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

// request sends method with params and returns its id.
func (c *wsClient) request(method, params string) int {
	c.t.Helper()
	c.next++
	c.send(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, c.next, method, params))
	return c.next
}

// until reads frames until the response to id, answering each request the
// agent makes with answer, and returns that response and what came first.
func (c *wsClient) until(id int, answer func(req map[string]any) any) (map[string]any, []map[string]any) {
	c.t.Helper()
	var before []map[string]any
	for {
		frame := c.frame()
		if method, _ := frame["method"].(string); method != "" && frame["id"] != nil {
			if answer == nil {
				c.t.Fatalf("unexpected request %v", frame)
			}
			body, _ := json.Marshal(answer(frame))
			c.send(string(body))
			continue
		}
		if frame["id"] == float64(id) {
			return frame, before
		}
		before = append(before, frame)
	}
}

// call sends method with params and returns its response.
func (c *wsClient) call(method, params string) map[string]any {
	c.t.Helper()
	resp, _ := c.until(c.request(method, params), nil)
	return resp
}

// newSession makes a session and returns its id.
func (c *wsClient) newSession() string {
	c.t.Helper()
	resp := c.call("session/new", `{"cwd":"/tmp"}`)
	sid, _ := resp["result"].(map[string]any)["sessionId"].(string)
	if sid == "" {
		c.t.Fatalf("session/new = %v", resp)
	}
	return sid
}

// prompt sends text to sid and returns the request id.
func (c *wsClient) prompt(sid, text string) int {
	c.t.Helper()
	return c.request("session/prompt", `{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"`+text+`"}]}`)
}

// TestACP_WS_cancelAndDisconnectEndTheTurn: a message that is not a
// request is refused; session/cancel ends a running prompt with stopReason
// cancelled; and a client that closes its socket cancels its running model
// call, and its connection leaves the registry.
func TestACP_WS_cancelAndDisconnectEndTheTurn(t *testing.T) {
	started := map[string]chan struct{}{"cancel": make(chan struct{}, 1), "disconnect": make(chan struct{}, 1)}
	stopped := map[string]chan struct{}{"cancel": make(chan struct{}, 1), "disconnect": make(chan struct{}, 1)}
	signal := func(ch chan struct{}) {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		text := msgs[len(msgs)-1].Content
		signal(started[text])
		<-ctx.Done()
		signal(stopped[text])
	})
	hs, srv := startACPWSServer(t, acptest.NewRuntime(t, model, tacklr.AgentOptions{}))
	wait := func(ch chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(8 * time.Second):
			t.Fatalf("timed out waiting for the model call to %s", what)
		}
	}
	c := dial(t, hs, `{"protocolVersion":1}`)
	d := dial(t, hs, `{"protocolVersion":1}`)
	registered := srv.Connections.Get(d.ID) != nil

	c.send(`{`)
	c.send(`{"jsonrpc":"2.0","id":99,"result":{}}`)
	notRequests := []any{c.frame()["error"], c.frame()["error"]}
	sid := c.newSession()
	id := c.prompt(sid, "cancel")
	wait(started["cancel"], "start")
	c.send(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"` + sid + `"}}`)
	cancelled, _ := c.until(id, nil)
	d.prompt(d.newSession(), "disconnect")
	wait(started["disconnect"], "start")
	_ = d.conn.Close(websocket.StatusNormalClosure, "")
	wait(stopped["disconnect"], "stop")
	removed := false
	for deadline := time.Now().Add(2 * time.Second); !removed && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		removed = srv.Connections.Get(d.ID) == nil
	}

	for _, e := range notRequests {
		if code, _ := e.(map[string]any)["code"].(float64); int(code) != acp.CodeInvalidRequest {
			t.Errorf("message that is not a request = %v, want invalid request", e)
		}
	}
	if res, _ := cancelled["result"].(map[string]any); res["stopReason"] != "cancelled" {
		t.Errorf("cancelled prompt = %v, want stopReason cancelled", cancelled)
	}
	if d.ID == "" || !registered || !removed {
		t.Errorf("connection %q registered %v, removed after close %v; want both", d.ID, registered, removed)
	}
}
