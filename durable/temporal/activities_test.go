package temporal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestActivities_unknownAgentAndDirectCall(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	snaps := inprocess.NewMemorySnapshot()
	log := inprocess.NewMemoryEventLog()
	acts := &activities{Catalog: cat, Snapshots: snaps, Fallback: log, DisableStreams: true, Secrets: durable.NewMemorySecretStorage()}
	_, err := acts.Inference(t.Context(), inferenceInput{SessionID: "s", Rec: durable.Snapshot{AgentID: "nope"}})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("missing agent: %v", err)
	}
	_, err = acts.Tool(t.Context(), toolInput{SessionID: "s", Rec: durable.Snapshot{AgentID: "nope"}, Call: tacklr.ToolCall{ID: "c", Name: "x"}})
	if !errors.Is(err, durable.ErrAgentNotFound) {
		t.Fatalf("tool missing agent: %v", err)
	}
	out, err := acts.Inference(t.Context(), inferenceInput{
		SessionID: "s", Rec: durable.Snapshot{AgentID: "default"},
		User:  &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
		State: map[string]any{"user": "Ryan"},
	})
	if err != nil || !out.Complete {
		t.Fatalf("direct inference: %+v %v", out, err)
	}
	snap, _, err := snaps.Load(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.Checkpoint.UserState()["user"]) != `"Ryan"` {
		t.Fatalf("userState=%s", snap.Checkpoint.UserState()["user"])
	}
	if snap.AgentID != "default" {
		t.Fatalf("snapshot agent=%q", snap.AgentID)
	}
}

func TestActivities_childTurnUsesParentSecrets(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("from-parent"), 0o644); err != nil {
		t.Fatal(err)
	}
	var gotToken string
	open := vfs.Tree(vfs.At("docs", builtins.Local(dir)))
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
		OpenVFS: func(ctx context.Context, sessionID string, req vfs.Request) (*vfs.MountSession, error) {
			if len(req.Bindings) > 0 {
				gotToken = req.Bindings[0].Auth.Token
			}
			return open(ctx, sessionID, req)
		},
	})
	store := durable.NewMemorySecretStorage()
	parentAuth := durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "local",
		Params:   map[string]string{vfs.ParamName: "docs"},
		Auth:     vfs.Credential{Token: "parent-tok"},
	}}}
	if err := store.Put(t.Context(), "parent", durable.Secrets{Auth: parentAuth}); err != nil {
		t.Fatal(err)
	}
	acts := newActs(cat, inprocess.NewMemoryEventLog(), true)
	acts.Secrets = store
	if _, err := acts.Inference(t.Context(), inferenceInput{
		SessionID: "child",
		Rec: durable.Snapshot{
			Parent:  "parent",
			AgentID: "default",
			Mounts:  adapter.ApplyAuth(nil, parentAuth),
		},
		User: &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
	}); err != nil {
		t.Fatal(err)
	}
	if gotToken != "parent-tok" {
		t.Fatalf("OpenVFS token=%q", gotToken)
	}
}
