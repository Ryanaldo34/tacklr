package acp_test

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/server/acp"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/workspace"
)

// TestACP_vfsBindRefreshUnbind: a client binds a backend to the session's
// workspace, and the agent reads a file from it; the agent refreshes and
// unbinds bindings, and refuses a bind with no backends, a refresh of no
// binding or with no provider, and an unbind with no session; after the
// unbind the file is not found.
func TestACP_vfsBindRefreshUnbind(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	strategy := testkit.HTTPModel(t, func(ctx context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		if n := len(msgs); n > 0 {
			last := msgs[n-1]
			if last != nil && last.Role == message.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: last.Content, IsComplete: true}
				return
			}
		}
		ch <- tacklr.LLMResponseChunk{
			Type: session.EventFunctionCall,
			ToolCalls: []message.ToolCall{{
				ID: "read-1", CallID: "read-1", Name: "read",
				Arguments: `{"path":"/workspace/docs/hello.txt"}`,
			}},
			IsComplete: true,
		}
	})
	r := acptest.NewWorkspaceRuntime(t, strategy, tacklr.AgentOptions{}, workspace.Config{Open: func(ctx context.Context, sid string, req vfs.Request) (*vfs.MountSession, error) {
		if _, ok := vfs.BindingByName(req.Bindings, "docs"); !ok {
			if _, ok := vfs.BindingByName(req.Bindings, "local"); !ok {
				return vfs.Tree()(ctx, sid, req)
			}
		}
		return vfs.Tree(vfs.At("docs", vfs.Local(dir)))(ctx, sid, req)
	}})

	proto := acp.New(nil)
	env := server.ProtocolEnv{Client: r.Client, Agent: r.Agent, Workspace: r.Workspace}
	rpc := func(body string) *httptest.ResponseRecorder { return acptest.Serve(t, r, proto, body) }
	sessionID := acptest.SessionID(t, rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`))
	for _, backends := range []string{
		`[{"provider":"local","params":{"name":"docs"},"auth":{"token":"tok1"}}]`,
		`[{"provider":"local","point":"/docs","auth":{"token":"tok1b"}}]`,
		`[{"provider":"other","point":"/other","auth":{"token":"tok-other"}}]`,
		`[{"provider":"nope","params":{"name":"x"}}]`,
	} {
		acptest.Result(t, rpc(`{"jsonrpc":"2.0","id":2,"method":"_tacklr/vfs/bind","params":{"sessionId":"`+sessionID+`","backends":`+backends+`}}`))
	}

	prompt := `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"read"}]}}`
	recPrompt := rpc(prompt)
	body := recPrompt.Body.String()
	if !strings.Contains(body, "from-workspace") {
		t.Fatalf("want workspace file in prompt stream, got %s", body)
	}

	expires := time.Now().Add(time.Hour).UTC()
	withExpiry := `{"jsonrpc":"2.0","id":31,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[{"profile":"local","params":{"name":"docs"},"auth":{"token":"tok-exp","expiresAt":"` + expires.Format(time.RFC3339Nano) + `"},"readOnly":false}]}}`
	acptest.Result(t, rpc(withExpiry))
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":32,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[]}}`,
		`{"jsonrpc":"2.0","id":33,"method":"_tacklr/vfs/refresh","params":{"sessionId":"` + sessionID + `","provider":"ghost","auth":{"token":"x"}}}`,
		`{"jsonrpc":"2.0","id":34,"method":"_tacklr/vfs/refresh","params":{"sessionId":"` + sessionID + `"}}`,
		`{"jsonrpc":"2.0","id":35,"method":"_tacklr/vfs/unbind","params":{}}`,
	} {
		acptest.WrittenError(t, proto, env, body)
	}

	refresh := `{"jsonrpc":"2.0","id":4,"method":"_tacklr/vfs/refresh","params":{"sessionId":"` + sessionID + `","provider":"local","auth":{"token":"tok2"}}}`
	acptest.Result(t, rpc(refresh))
	for _, params := range []string{`"point":"/other"`, `"provider":"local"`, `"name":"docs"`} {
		acptest.Result(t, rpc(`{"jsonrpc":"2.0","id":5,"method":"_tacklr/vfs/unbind","params":{"sessionId":"`+sessionID+`",`+params+`}}`))
	}
	rec2 := rpc(`{"jsonrpc":"2.0","id":6,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"again"}]}}`)
	out := rec2.Body.String()
	if strings.Contains(out, "from-workspace") {
		t.Fatalf("workspace still readable after unbind: %s", out)
	}
	if !strings.Contains(out, "not found") && !strings.Contains(out, "tool") {
		t.Fatalf("want missing workspace after unbind, got %s", out)
	}
}
