package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
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
		return vfs.Tree(vfs.At("docs", builtins.Local(dir)))(ctx, sid, req)
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

	expires := time.Now().Add(time.Hour).UTC()
	withExpiry := `{"jsonrpc":"2.0","id":31,"method":"_tacklr/vfs/bind","params":{"sessionId":"` + sessionID + `","backends":[{"profile":"local","params":{"name":"docs"},"auth":{"token":"tok-exp","expiresAt":"` + expires.Format(time.RFC3339Nano) + `"},"readOnly":false}]}}`
	if err := proto.HandleInbound(t.Context(), env, []byte(withExpiry)); err != nil {
		t.Fatal(err)
	}
	emptyBackends := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":32,"method":"_tacklr/vfs/bind","params":{"sessionId":"`+sessionID+`","backends":[]}}`)
	if emptyBackends == nil {
		t.Fatal("want backends required")
	}
	missingRefresh := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":33,"method":"_tacklr/vfs/refresh","params":{"sessionId":"`+sessionID+`","provider":"ghost","auth":{"token":"x"}}}`)
	if missingRefresh == nil {
		t.Fatal("want no vfs binding")
	}
	noProvider := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":34,"method":"_tacklr/vfs/refresh","params":{"sessionId":"`+sessionID+`"}}`)
	if noProvider == nil {
		t.Fatal("want provider required")
	}
	noSession := inboundWrittenError(t, proto, env, `{"jsonrpc":"2.0","id":35,"method":"_tacklr/vfs/unbind","params":{}}`)
	if noSession == nil {
		t.Fatal("want sessionId required")
	}

	refresh := `{"jsonrpc":"2.0","id":4,"method":"_tacklr/vfs/refresh","params":{"sessionId":"` + sessionID + `","provider":"local","auth":{"token":"tok2"}}}`
	recRef := serveACPRaw(t, r, refresh)
	if recRef.Body.Len() == 0 {
		t.Fatal("refresh returned empty")
	}
	_ = proto.HandleInbound(t.Context(), env, []byte(`{"jsonrpc":"2.0","id":36,"method":"_tacklr/vfs/unbind","params":{"sessionId":"`+sessionID+`","point":"/other"}}`))
	unbindProvider := `{"jsonrpc":"2.0","id":5,"method":"_tacklr/vfs/unbind","params":{"sessionId":"` + sessionID + `","provider":"local"}}`
	_ = serveACPRaw(t, r, unbindProvider)
	_ = proto.HandleInbound(t.Context(), env, []byte(`{"jsonrpc":"2.0","id":37,"method":"_tacklr/vfs/unbind","params":{"sessionId":"`+sessionID+`","name":"docs"}}`))
	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":6,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"again"}]}}`)
	out := rec2.Body.String()
	if strings.Contains(out, "from-workspace") {
		t.Fatalf("workspace still readable after unbind: %s", out)
	}
	if !strings.Contains(out, "not found") && !strings.Contains(out, "tool") {
		t.Fatalf("want missing workspace after unbind, got %s", out)
	}
}
