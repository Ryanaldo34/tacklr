package temporal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

type failPutSecrets struct{}

func (failPutSecrets) Put(context.Context, durable.SessionID, durable.Secrets) error {
	return errors.New("vault sealed")
}
func (failPutSecrets) Get(context.Context, durable.SessionID) (durable.Secrets, error) {
	return durable.Secrets{}, nil
}
func (failPutSecrets) Delete(context.Context, durable.SessionID) error { return nil }

type nopWorkflowClient struct{ client.Client }

func (nopWorkflowClient) SignalWorkflow(context.Context, string, string, string, any) error {
	return nil
}
func (nopWorkflowClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	return nil, errors.New("no query")
}

func TestRuntime_promptFailsWhenVaultSealed(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	rt := New(nopWorkflowClient{}, Config{
		Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Secrets: failPutSecrets{}, DisableStreams: true,
		Fallback: inprocess.NewMemoryEventLog(),
	})
	err := rt.Prompt(t.Context(), "s", durable.Prompt{
		Text: "x",
		Auth: durable.AuthContext{Bindings: []vfs.Binding{{
			Provider: "gdrive", Auth: vfs.Credential{Token: "tok"},
		}}},
	})
	if err == nil || !strings.Contains(err.Error(), "vault sealed") {
		t.Fatalf("put fail: %v", err)
	}
}

func TestRuntime_closeDeletesSecrets(t *testing.T) {
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	store := durable.NewMemorySecretStorage()
	if err := store.Put(t.Context(), "s", durable.Secrets{Auth: durable.AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive", Auth: vfs.Credential{Token: "tok"},
	}}}}); err != nil {
		t.Fatal(err)
	}
	rt := New(nopWorkflowClient{}, Config{
		Catalog: cat, Snapshots: inprocess.NewMemorySnapshot(), Secrets: store, DisableStreams: true,
		Fallback: inprocess.NewMemoryEventLog(),
	})
	if err := rt.Close(t.Context(), "s"); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(t.Context(), "s")
	if err != nil || len(got.Auth.Bindings) != 0 {
		t.Fatalf("close left secrets: %+v %v", got, err)
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
