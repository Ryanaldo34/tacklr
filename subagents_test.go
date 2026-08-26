package tacklr

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

func TestNewTurnManager_rejectsInvalidSpecialists(t *testing.T) {
	ok := &mockStrategy{}
	t.Run("nil model", func(t *testing.T) {
		if _, err := NewTurnManager(context.Background(), AgentOptions{Config: Config{MaxWindowSize: 8192}}); err == nil {
			t.Fatal("expected constructor error")
		}
	})
	cases := []struct {
		name  string
		specs []*Specialist
	}{
		{"nil spec", []*Specialist{nil}},
		{"empty name", []*Specialist{{Name: "", Model: ok}}},
		{"nil model", []*Specialist{{Name: "w", Model: nil}}},
		{"duplicate name", []*Specialist{
			{Name: "w", Model: ok},
			{Name: "w", Model: ok},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTurnManager(context.Background(), AgentOptions{
				Config: Config{MaxWindowSize: 8192}, Model: ok, Specialists: tc.specs,
			}); err == nil {
				t.Fatal("expected constructor error")
			}
		})
	}
}

func TestSystemPrompt_listsSpecialistsSorted(t *testing.T) {
	var n int
	h := mustNewTurnManager(t, AgentOptions{
		Config: Config{MaxWindowSize: 8192},
		Model: &mockStrategy{
			invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
				n++
				if n == 1 {
					ch <- LLMResponseChunk{Type: StreamEventFunctionCall, ToolCalls: []ToolCall{
						toolCall("sp1", "spawn_specialist", `{"specialist":"alpha","task_description_and_context":"x"}`),
						toolCall("ls1", "list_children", `{}`),
						toolCall("gc1", "get_child", `{"child_id":"missing"}`),
						toolCall("cc1", "cancel_child", `{"child_id":"missing"}`),
					}, IsComplete: true}
					return
				}
				ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "done", IsComplete: true}
			},
		},
		Specialists: []*Specialist{
			{Name: "zebra", Model: &mockStrategy{}, Description: "last"},
			{Name: "alpha", Model: &mockStrategy{}},
		},
	})
	t.Cleanup(h.Close)
	prompt := h.constructSystemPrompt()
	ai := strings.Index(prompt, " - alpha\n")
	zi := strings.Index(prompt, " - zebra: last\n")
	if ai < 0 || zi < 0 || ai > zi {
		t.Fatalf("want alpha then zebra in prompt: %s", prompt)
	}
	rt := turnRuntime(h)
	res, err := h.findTool("list_children", "").invoke(t.Context(), `{}`, rt)
	if err != nil || !strings.Contains(strings.ToLower(res.output), "no child sessions") {
		t.Fatalf("list_children: %q %v", res.output, err)
	}
}

func TestWithSpecialist_sharesHostMountWriteAndCatalog(t *testing.T) {
	parent, ms, eng, ns := vfsIndexHarness(t, true)
	_ = parent
	worker := mustNewTurnManager(t, AgentOptions{
		MountSession:    ms,
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
		writeUnattended: true,
	}.WithSpecialist(&Specialist{Name: "researcher", Model: &mockStrategy{}}))
	t.Cleanup(worker.Close)
	ctx := context.Background()
	scope := brain.Scope{Namespace: &ns}

	for _, name := range []string{"read", "write", "run_command", "read_object", "search", "index_file"} {
		if worker.findTool(name, "") == nil {
			t.Fatalf("worker catalog missing %s", name)
		}
	}

	const body = "from-worker-body\n"
	if _, err := runWriteTool(t, worker, worker.findTool("write", ""), `{"path":"/work/from-worker.txt","content":`+jsonString(body)+`}`); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadText(ctx, "/work/from-worker.txt")
	if err != nil || got.Text() != body {
		text := ""
		if got != nil {
			text = got.Text()
		}
		t.Fatalf("parent ReadText after worker write: %q err=%v", text, err)
	}

	if _, err := worker.findTool("run_command", "").invoke(ctx, `{"command":"ls work"}`, turnRuntime(worker)); !errors.Is(err, vfs.ErrFuseNotMounted) {
		t.Fatalf("run_command without HostDir: %v", err)
	}

	const phraseA = "worker-index-share-aaa"
	if err := ms.WriteFile(ctx, "/work/tracked.txt", []byte(phraseA+"\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := runWriteTool(t, worker, worker.findTool("index_file", ""), `{"path":"/work/tracked.txt"}`); err != nil {
		t.Fatal(err)
	}
	if hit := waitSearchHit(t, eng, scope, phraseA, 3*time.Second); hit.Properties[vfsindex.PropVFSPath] != "/work/tracked.txt" {
		t.Fatalf("index vfs_path: %+v", hit.Properties)
	}
}

func TestWithSpecialist_inheritsParentSkills(t *testing.T) {
	ctx := context.Background()
	pack := t.TempDir()
	d := filepath.Join(pack, "research")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: research\ndescription: Research carefully\n---\n\nAlways verify claims.\n"
	if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "work", Base: work}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(vfs.LocalFactory{ID: "pack", Base: pack, Skills: "."}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession(t.Name(), reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "work"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	opts := AgentOptions{
		Config:       Config{MaxWindowSize: 8192, MaxTurnRequests: 4},
		Model:        &mockStrategy{},
		MountSession: ms,
	}
	parent := mustNewTurnManager(t, opts)
	t.Cleanup(parent.Close)
	if parent.findTool("read_skill", "") == nil {
		t.Fatal("parent missing read_skill")
	}
	inherited := opts.WithSpecialist(&Specialist{Name: "researcher", Model: &mockStrategy{}})
	if inherited.Config.MaxTurnRequests != 4 || inherited.MountSession != ms {
		t.Fatalf("WithSpecialist = %+v", inherited.Config)
	}

	worker := mustNewTurnManager(t, inherited)
	t.Cleanup(worker.Close)
	skill := worker.findTool("read_skill", "")
	if skill == nil {
		t.Fatal("worker missing read_skill")
	}
	res, err := skill.invoke(ctx, `{"name":"research"}`, turnRuntime(worker))
	if err != nil || !strings.Contains(res.output, "Always verify claims") {
		t.Fatalf("worker read_skill: %v %s", err, res.output)
	}
}
