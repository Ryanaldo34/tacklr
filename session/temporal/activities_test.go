package temporal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestActivities_childTurnUsesParentSecrets(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-parent"), 0o644); err != nil {
		t.Fatal(err)
	}
	var gotToken string
	open := vfs.Tree(vfs.At("docs", vfs.Local(dir)))
	agent := tacklr.AgentOptions{Model: testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
	}), MaxWindowSize: 8192,

		OpenVFS: func(ctx context.Context, sessionID string, req vfs.Request) (*vfs.MountSession, error) {
			if len(req.Bindings) > 0 {
				gotToken = req.Bindings[0].Auth.Token
			}
			return open(ctx, sessionID, req)
		}}
	store := session.NewMemorySecretStorage()
	parentAuth := session.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "parent-tok"},
	}}}
	if err := store.Put(t.Context(), "parent", session.Secrets{Auth: parentAuth}); err != nil {
		t.Fatal(err)
	}
	acts := newActs(agent, session.NewMemoryEventLog(), true)
	acts.Secrets = store
	if _, err := acts.Commit(t.Context(), commitInput{
		stepSession: stepSession{SessionID: "child", Identity: session.Identity{
			Parent: "parent",
			Mounts: session.ApplyAuth(nil, parentAuth),
		}},
		In: tacklr.CommitIn{Users: []*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}}},
	}); err != nil {
		t.Fatal(err)
	}
	if gotToken != "parent-tok" {
		t.Fatalf("OpenVFS token=%q", gotToken)
	}
}

// TestActivities_commitIsTheOnlyWriterAndAppliesItsInputOnce: Tool steps
// of two calls that change shared state at once save nothing; the Commit of
// their round applies both calls' writes in call order; and a Commit run
// again after it saved (its result lost) finds the snapshot past its base
// and applies nothing again.
func TestActivities_commitIsTheOnlyWriterAndAppliesItsInputOnce(t *testing.T) {
	calls := []tacklr.ToolCall{{ID: "a", CallID: "a", Name: "set", Arguments: `{"v":"a"}`}, {ID: "b", CallID: "b", Name: "set", Arguments: `{"v":"b"}`}}
	model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, IsComplete: true, ToolCalls: calls}
	})
	set := tacklr.NewTool(tacklr.ToolConfig{Name: "set", Handler: func(_ context.Context, args struct {
		V string `json:"v"`
	}, rt *tacklr.HarnessRuntime) (string, error) {
		if err := rt.StateSet("by-"+args.V, true); err != nil {
			return "", err
		}
		return "set", rt.StateSet("last", args.V)
	}})
	acts := newActs(tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{set}}, session.NewMemoryEventLog(), true)
	id := session.SessionID("writer")
	rev, err := acts.Commit(t.Context(), commitInput{stepSession: stepSession{SessionID: id}, In: tacklr.CommitIn{Users: []*tacklr.Message{{Role: tacklr.RoleUser, Content: "go"}}}})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := acts.Inference(t.Context(), stepSession{SessionID: id})
	if err != nil {
		t.Fatal(err)
	}
	round := tacklr.NewRound(reply)
	for i, call := range reply.Calls {
		if round.Outcomes[i], err = acts.Tool(t.Context(), toolInput{stepSession: stepSession{SessionID: id}, Run: tacklr.ToolRun{Call: call}}); err != nil {
			t.Fatal(err)
		}
	}
	_, afterTools, err := acts.Snapshots.Load(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	in := commitInput{stepSession: stepSession{SessionID: id}, Base: rev, In: tacklr.CommitIn{Round: round}}

	committed, firstErr := acts.Commit(t.Context(), in)
	again, againErr := acts.Commit(t.Context(), in)

	cp, _, err := acts.Snapshots.Load(t.Context(), id)
	if err != nil || firstErr != nil || againErr != nil {
		t.Fatal(err, firstErr, againErr)
	}
	if afterTools != rev || again != committed {
		t.Fatalf("revisions: after the commit %s, tools %s, commit %s, again %s; want tools to save nothing and the second commit to apply nothing", rev, afterTools, committed, again)
	}
	raw, _ := json.Marshal(cp)
	for _, want := range []string{`"by-a":true`, `"by-b":true`, `"last":"b"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("checkpoint %s, want %s: both calls' writes, the later call's last", raw, want)
		}
	}
	if n := strings.Count(string(raw), `"content":"set"`); n != 2 {
		t.Fatalf("checkpoint has %d tool results, want the round committed once", n)
	}
}

func TestActivities_commitRejectsAPromptTheModelCannotRead(t *testing.T) {
	model := testkit.TextHTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
	})
	acts := newActs(tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}, session.NewMemoryEventLog(), true)
	image := &tacklr.Message{Role: tacklr.RoleUser, ContentParts: []tacklr.ContentPart{{Type: tacklr.ContentTypeInputImage, FileData: &tacklr.FileData{MIMEType: "image/png"}}}}

	_, err := acts.Commit(t.Context(), commitInput{stepSession: stepSession{SessionID: "text-only"}, In: tacklr.CommitIn{Users: []*tacklr.Message{image}}})

	if err == nil || !strings.Contains(err.Error(), "unsupported content type") {
		t.Fatalf("commit = %v, want the image rejected", err)
	}
}
