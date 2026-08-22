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
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.LocalFactory{ID: "local", Base: dir}); err != nil {
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
	r := newTestKernel(strategy, nil, durable.AgentSpec{FSRegistry: fsReg})

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

	unbind := `{"jsonrpc":"2.0","id":5,"method":"_tacklr/vfs/unbind","params":{"sessionId":"` + sessionID + `","point":"docs"}}`
	_ = serveACPRaw(t, r, unbind)
	rec2 := serveACPRaw(t, r, `{"jsonrpc":"2.0","id":6,"method":"session/prompt","params":{"sessionId":"`+sessionID+`","prompt":[{"type":"text","text":"again"}]}}`)
	out := rec2.Body.String()
	if strings.Contains(out, "from-workspace") {
		t.Fatalf("workspace still readable after unbind: %s", out)
	}
	if !strings.Contains(out, "not found") && !strings.Contains(out, "tool") {
		t.Fatalf("want missing workspace after unbind, got %s", out)
	}
}
