package tacklr

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestRunCommand_catDirtyAndFalseExit(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ctx := context.Background()
	ms, rt := newRunCommandSession(t)
	const body = "dirty body unique phrase xyzzy-tacklr\n"
	if err := ms.WriteFile(ctx, "/work/note.md", []byte("old\n")); err != nil {
		t.Fatal(err)
	}
	doc, err := ms.ReadText(ctx, "/work/note.md")
	if err != nil {
		t.Fatal(err)
	}
	doc.SetText(body)
	if err := ms.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := ms.FuseMount(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	tool := newRunCommand(ms, false)
	res, err := tool.invoke(ctx, `{"command":"cat work/note.md"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "exit=0") || !strings.Contains(res.output, body) {
		t.Fatalf("cat dirty: %s", res.output)
	}

	res, err = tool.invoke(ctx, `{"command":"false"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "exit=1") {
		t.Fatalf("false: %s", res.output)
	}

	if _, err := exec.LookPath("rg"); err == nil {
		res, err = tool.invoke(ctx, `{"command":"rg -F xyzzy-tacklr work"}`, rt)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.output, "xyzzy-tacklr") {
			t.Fatalf("rg dirty: %s", res.output)
		}
	}
}

func TestRunCommand_deadlineKillsProcess(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ms, rt := newRunCommandSession(t)
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	tool := newRunCommand(ms, false)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := tool.invoke(ctx, `{"command":"sleep 10"}`, rt)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}

func TestRunCommand_truncatesOver1MiB(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ms, rt := newRunCommandSession(t)
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	tool := newRunCommand(ms, false)
	res, err := tool.invoke(context.Background(), `{"command":"head -c 2097152 /dev/zero"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "exit=0") || !strings.Contains(res.output, "truncated=true") || !strings.Contains(res.output, "output truncated") {
		head := res.output
		if len(head) > 240 {
			head = head[:240]
		}
		t.Fatalf("truncate: %s", head)
	}
}

func newRunCommandSession(t *testing.T) (*vfs.MountSession, HarnessRuntime) {
	t.Helper()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession(t.Name(), reg)
	if err := ms.Mount(t.Context(), vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	h := NewAgent(t.Context(), AgentOptions{
		SessionID:    t.Name(),
		Store:        stores.NewInMemoryStore(),
		MountSession: ms,
		Model:        &mockStrategy{},
	})
	return ms, turnRuntime(h)
}
