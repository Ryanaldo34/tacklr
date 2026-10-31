package temporal

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
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
	if _, err := acts.Inference(t.Context(), session.InferenceInput{
		SessionID: "child",
		Identity: session.Identity{
			Parent: "parent",
			Mounts: session.ApplyAuth(nil, parentAuth),
		},
		User: &tacklr.Message{Role: tacklr.RoleUser, Content: "hi"},
	}); err != nil {
		t.Fatal(err)
	}
	if gotToken != "parent-tok" {
		t.Fatalf("OpenVFS token=%q", gotToken)
	}
}

// TestActivities_aRetriedToolsStepReportsWhatItsFirstAttemptRecorded: the
// Tools step saves, then its result is lost and Temporal runs it again. The
// retry loads the saved state and reports the same park, await and child
// starts, without running a tool again, whether the round stayed open or
// committed.
func TestActivities_aRetriedToolsStepReportsWhatItsFirstAttemptRecorded(t *testing.T) {
	spawn := func(id, block string) tacklr.ToolCall {
		return tacklr.ToolCall{ID: id, CallID: id, Name: "spawn_specialist",
			Arguments: `{"specialist":"researcher","task_description_and_context":"look","block":` + block + `}`}
	}
	child := func(call string) string { return string(session.ChildSessionID("retry", "researcher", call)) }
	free := tacklr.ToolCall{ID: "f1", CallID: "f1", Name: "free", Arguments: `{}`}
	for _, tc := range []struct {
		name  string
		calls []tacklr.ToolCall
		check func([]session.ToolOutput) bool
	}{
		{"an open round", []tacklr.ToolCall{{ID: "g1", CallID: "g1", Name: "gate", Arguments: `{}`}, spawn("sp1", "true"), spawn("sp2", "false"), free},
			func(o []session.ToolOutput) bool {
				return len(o) == 4 && o[0].Interrupted && o[0].InterruptID == "g1" &&
					o[1].AwaitID == session.SessionID(child("sp1")) && len(o[1].Starts) == 1 && o[1].Starts[0].ID == child("sp1") && o[1].Starts[0].Specialist &&
					o[2].AwaitID == "" && len(o[2].Starts) == 1 && o[2].Starts[0].ID == child("sp2")
			}},
		{"a committed round", []tacklr.ToolCall{spawn("sp2", "false"), free},
			func(o []session.ToolOutput) bool {
				return len(o) == 2 && len(o[0].Starts) == 1 && o[0].Starts[0].ID == child("sp2")
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var freeRuns atomic.Int32
			var seen any
			model := testkit.HTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventFunctionCall, IsComplete: true, ToolCalls: tc.calls}
			})
			agent := tacklr.AgentOptions{Model: model, MaxWindowSize: 8192,
				Specialists: []*tacklr.Specialist{{Name: "researcher", Model: model}},
				Tools: []*tacklr.Tool{
					tacklr.NewTool(tacklr.ToolConfig{Name: "gate", OnCall: []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall}, Handler: func(context.Context) (string, error) { return "gate-ok", nil }}),
					// free sees the host's mode, then changes it.
					tacklr.NewTool(tacklr.ToolConfig{Name: "free", Handler: func(_ context.Context, rt tacklr.HarnessRuntime) (string, error) {
						freeRuns.Add(1)
						seen, _ = rt.StateGet("mode")
						return "free-ok", rt.StateSet("mode", "tool")
					}}),
				}}
			acts := newActs(agent, session.NewMemoryEventLog(), true)
			if _, err := acts.Inference(t.Context(), session.InferenceInput{SessionID: "retry", User: &tacklr.Message{Role: tacklr.RoleUser, Content: "go"}}); err != nil {
				t.Fatal(err)
			}
			in := session.ToolsInput{SessionID: "retry", Seq: 1, State: map[string]any{"mode": "host"}}

			first, firstErr := acts.Tools(t.Context(), in)
			retried, retryErr := acts.Tools(t.Context(), in)

			if firstErr != nil || retryErr != nil {
				t.Fatal(firstErr, retryErr)
			}
			if !reflect.DeepEqual(first, retried) || freeRuns.Load() != 1 {
				t.Fatalf("retried outputs = %+v after free ran %d times, want %+v and no tool run again", retried, freeRuns.Load(), first)
			}
			if !tc.check(first) {
				t.Fatalf("outputs = %+v", first)
			}
			if tc.name != "a committed round" {
				return
			}
			// The next step's tool sees the first attempt's change: the retry
			// did not apply the host state again.
			if _, err := acts.Inference(t.Context(), session.InferenceInput{SessionID: "retry"}); err != nil {
				t.Fatal(err)
			}
			if _, err := acts.Tools(t.Context(), session.ToolsInput{SessionID: "retry", Seq: 2}); err != nil {
				t.Fatal(err)
			}
			if seen != "tool" {
				t.Fatalf("mode after a retried step = %v, want the tool's change kept over the host's value", seen)
			}
		})
	}
}

func TestActivities_inferenceRejectsAPromptTheModelCannotRead(t *testing.T) {
	model := testkit.TextHTTPModel(t, func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
	})
	acts := newActs(tacklr.AgentOptions{Model: model, MaxWindowSize: 8192}, session.NewMemoryEventLog(), true)
	image := &tacklr.Message{Role: tacklr.RoleUser, ContentParts: []tacklr.ContentPart{{Type: tacklr.ContentTypeInputImage, FileData: &tacklr.FileData{MIMEType: "image/png"}}}}

	_, err := acts.Inference(t.Context(), session.InferenceInput{SessionID: "text-only", User: image})

	if err == nil || !strings.Contains(err.Error(), "unsupported content type") {
		t.Fatalf("inference = %v, want the image rejected", err)
	}
}
