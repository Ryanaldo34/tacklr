package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestACP_vfsBindRefreshUnbind(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	strategy := &mockInferenceStrategy{
		invokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if n := len(msgs); n > 0 {
				last := msgs[n-1]
				if last != nil && last.Role == tacklr.RoleTool {
					ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: last.Content, IsComplete: true}
					return
				}
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "read-1", CallID: "read-1", Name: "read",
					Arguments: `{"path":"/workspace/docs/hello.txt"}`,
				}},
				IsComplete: true,
			}
		},
	}
	r := newTestRuntime(t, strategy, durable.AgentSpec{OpenVFS: func(ctx context.Context, sid string, req vfs.Request) (*vfs.MountSession, error) {
		if _, ok := vfs.BindingByName(req.Bindings, "docs"); !ok {
			if _, ok := vfs.BindingByName(req.Bindings, "local"); !ok {
				return vfs.Tree()(ctx, sid, req)
			}
		}
		return vfs.Tree(vfs.At("docs", vfs.Local(dir)))(ctx, sid, req)
	}})

	recNew := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID := acpRPCResult(t, recNew)["sessionId"].(string)

	w := &recordingMessageWriter{}
	bridge := NewClientBridge(w)
	bridge.SetCaps(ClientCapabilities{VFSTokenRefresh: true})
	proto := acpProtocolFor(r)
	env := ProtocolEnv{Runtime: r.Runtime, Catalog: r.Catalog, Conn: &Conn{Writer: w, RPC: bridge}}
	bind := `{"jsonrpc":"2.0","id":2,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[{"provider":"local","params":{"name":"docs"},"auth":{"token":"tok1"}}]}}`
	if err := proto.HandleInbound(t.Context(), env, []byte(bind)); err != nil {
		t.Fatal(err)
	}
	rebind := `{"jsonrpc":"2.0","id":21,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[{"provider":"local","point":"/docs","auth":{"token":"tok1b"}}]}}`
	if err := proto.HandleInbound(t.Context(), env, []byte(rebind)); err != nil {
		t.Fatal(err)
	}
	second := `{"jsonrpc":"2.0","id":23,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[{"provider":"other","point":"/other","auth":{"token":"tok-other"}}]}}`
	if err := proto.HandleInbound(t.Context(), env, []byte(second)); err != nil {
		t.Fatal(err)
	}
	unknown := `{"jsonrpc":"2.0","id":22,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[{"provider":"nope","params":{"name":"x"}}]}}`
	if err := proto.HandleInbound(t.Context(), env, []byte(unknown)); err != nil {
		t.Fatal(err)
	}

	prompt := `{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"read"}]}}`
	recPrompt := serveACPRaw(t, r, prompt)
	body := recPrompt.Body.String()
	if !strings.Contains(body, "from-workspace") {
		t.Fatalf("want workspace file in prompt stream, got %s", body)
	}

	refresh := `{"jsonrpc":"2.0","id":4,"method":"_tacklr/vfs/refresh","params":{"sessionId":"` + sessionID + `","provider":"local","auth":{"token":"tok2"}}}`
	recRef := serveACPRaw(t, r, refresh)
	if recRef.Body.Len() == 0 {
		t.Fatal("refresh returned empty")
	}

	unbindProvider := `{"jsonrpc":"2.0","id":5,"method":"_tacklr/vfs/unbind","params":{"sessionId":"` + sessionID + `","provider":"local"}}`
	_ = serveACPRaw(t, r, unbindProvider)
	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":6,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"again"}]}}`)
	out := rec2.Body.String()
	if strings.Contains(out, "from-workspace") {
		t.Fatalf("workspace still readable after unbind: %s", out)
	}
	if !strings.Contains(out, "not found") && !strings.Contains(out, "tool") {
		t.Fatalf("want missing workspace after unbind, got %s", out)
	}
}

func TestACP_vfsBindRefreshUnbind_errorPaths(t *testing.T) {
	r := newTestRuntime(t, &mockInferenceStrategy{}, durable.AgentSpec{})
	recNew := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID := acpRPCResult(t, recNew)["sessionId"].(string)

	assertErr := func(body, want string) {
		t.Helper()
		rec := serveACPRaw(t, r, body)
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("got %s want %q", rec.Body.String(), want)
		}
	}
	assertErr(`{"jsonrpc":"2.0","id":2,"method":"_tacklr/vfs/bind","params":[]}`, "invalid bind params")
	assertErr(`{"jsonrpc":"2.0","id":3,"method":"_tacklr/vfs/bind","params":{"backends":[{"provider":"local"}]}}`, "sessionId is required")
	assertErr(`{"jsonrpc":"2.0","id":4,"method":"_tacklr/vfs/bind","params":{"sessionId":"`+sessionID+`"}}`, "backends is required")
	assertErr(`{"jsonrpc":"2.0","id":5,"method":"_tacklr/vfs/refresh","params":[]}`, "invalid refresh params")
	assertErr(`{"jsonrpc":"2.0","id":6,"method":"_tacklr/vfs/refresh","params":{"sessionId":"`+sessionID+`"}}`, "sessionId and provider are required")
	assertErr(`{"jsonrpc":"2.0","id":7,"method":"_tacklr/vfs/refresh","params":{"sessionId":"`+sessionID+`","provider":"missing"}}`, "no vfs binding")
	assertErr(`{"jsonrpc":"2.0","id":8,"method":"_tacklr/vfs/unbind","params":[]}`, "invalid unbind params")
	assertErr(`{"jsonrpc":"2.0","id":9,"method":"_tacklr/vfs/unbind","params":{}}`, "sessionId is required")

	exp := `{"jsonrpc":"2.0","id":10,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[{"profile":"local","params":{"name":"docs"},"readOnly":false,"auth":{"token":"tok","expiresAt":"2030-01-01T00:00:00Z"}}]}}`
	rec := serveACPRaw(t, r, exp)
	if strings.Contains(rec.Body.String(), `"error"`) && !strings.Contains(rec.Body.String(), `"mounted"`) {
		t.Fatalf("bind with profile/expires: %s", rec.Body.String())
	}
	unbindNamed := `{"jsonrpc":"2.0","id":11,"method":"_tacklr/vfs/unbind","params":{"sessionId":"` + sessionID + `","name":"docs"}}`
	_ = serveACPRaw(t, r, unbindNamed)

	bad := `{"jsonrpc":"2.0","id":12,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[{"provider":""}]}}`
	recBad := serveACPRaw(t, r, bad)
	if !strings.Contains(recBad.Body.String(), "errors") && !strings.Contains(recBad.Body.String(), "error") {
		t.Fatalf("invalid binding: %s", recBad.Body.String())
	}
}
