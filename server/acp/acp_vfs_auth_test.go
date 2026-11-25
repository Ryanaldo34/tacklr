package acp_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/acptest"
	"github.com/ryanaldo34/tacklr/internal/testkit"
)

// TestACP_vfsBindRefreshUnbind: a client binds a backend to the session's
// workspace, and the turn's tools get the binding with its token (as
// run_command passes it to the sandbox); the client refreshes and unbinds
// bindings, and the server refuses a bind with no backends, a refresh of no
// binding or with no provider, and an unbind with no session; after the
// unbind the turn has no docs binding.
func TestACP_vfsBindRefreshUnbind(t *testing.T) {
	t.Parallel()
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
			Type:       session.EventFunctionCall,
			ToolCalls:  []message.ToolCall{{ID: "bound-1", CallID: "bound-1", Name: "bound", Arguments: `{}`}},
			IsComplete: true,
		}
	})
	bound := tacklr.NewTool(tacklr.ToolConfig{Name: "bound", Handler: func(_ context.Context, _ struct{}, cc *tacklr.CallContext) (string, error) {
		var out []string
		for _, b := range cc.Bindings() {
			out = append(out, b.Alias()+"="+b.Auth.Token)
		}
		return "bound: " + strings.Join(out, ","), nil
	}})
	r := acptest.NewWorkspaceRuntime(t, strategy, tacklr.AgentOptions{Tools: []*tacklr.Tool{bound}}, true)

	c := acptest.Connect(t, acptest.Listen(t, acptest.Server(r, nil)), `{"protocolVersion":1}`)
	sessionID := c.NewSession("/tmp")
	for _, backends := range []string{
		`[{"provider":"local","params":{"name":"docs"},"auth":{"token":"tok1"}}]`,
		`[{"provider":"local","point":"/docs","auth":{"token":"tok1b"}}]`,
		`[{"provider":"other","point":"/other","auth":{"token":"tok-other"}}]`,
		`[{"provider":"nope","params":{"name":"x"}}]`,
	} {
		acptest.Result(t, c.Call("_tacklr/vfs/bind", `{"sessionId":"`+sessionID+`","backends":`+backends+`}`))
	}

	read, frames := c.Say(sessionID, "read")
	body, _ := json.Marshal(append(frames, read))
	if !strings.Contains(string(body), "docs=tok1") {
		t.Fatalf("want the docs binding with its token in the turn, got %s", body)
	}

	expires := time.Now().Add(time.Hour).UTC()
	acptest.Result(t, c.Call("_tacklr/vfs/bind", `{"sessionId":"`+sessionID+`","backends":[{"profile":"local","params":{"name":"docs"},"auth":{"token":"tok-exp","expiresAt":"`+expires.Format(time.RFC3339Nano)+`"},"readOnly":false}]}`))
	for method, params := range map[string]string{
		"_tacklr/vfs/bind":    `{"sessionId":"` + sessionID + `","backends":[]}`,
		"_tacklr/vfs/refresh": `{"sessionId":"` + sessionID + `","provider":"ghost","auth":{"token":"x"}}`,
		"_tacklr/vfs/unbind":  `{}`,
	} {
		acptest.Error(t, c.Call(method, params))
	}
	acptest.Error(t, c.Call("_tacklr/vfs/refresh", `{"sessionId":"`+sessionID+`"}`))

	acptest.Result(t, c.Call("_tacklr/vfs/refresh", `{"sessionId":"`+sessionID+`","provider":"local","auth":{"token":"tok2"}}`))
	for _, params := range []string{`"point":"/other"`, `"provider":"local"`, `"name":"docs"`} {
		acptest.Result(t, c.Call("_tacklr/vfs/unbind", `{"sessionId":"`+sessionID+`",`+params+`}`))
	}
	again, frames := c.Say(sessionID, "again")
	blob, _ := json.Marshal(append(frames, again))
	out := string(blob)
	if !strings.Contains(out, "bound: ") || strings.Contains(out, "docs=") {
		t.Fatalf("want the turn without the docs binding after unbind, got %s", out)
	}
}
