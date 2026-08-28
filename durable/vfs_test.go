package durable

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/vfs"
)

type downProjection struct{}

func (downProjection) Available() bool                        { return false }
func (downProjection) Attach(*vfs.MountSession, string) error { return nil }

func TestOpenTurnVFS_nilWhenNoOpenOrProjection(t *testing.T) {
	ms, err := OpenTurnVFS(t.Context(), "s", AgentSpec{}, nil, vfs.DirectProjection{})
	if err != nil || ms != nil {
		t.Fatalf("no OpenVFS: %v %v", ms, err)
	}
	ms, err = OpenTurnVFS(t.Context(), "s", AgentSpec{OpenVFS: vfs.Tree(vfs.At("scratch", builtins.Local(t.TempDir())))}, nil, downProjection{})
	if err != nil || ms != nil {
		t.Fatalf("projection down: %v %v", ms, err)
	}
	CloseTurnVFS(nil, "s", "test")
	ClearSessionVFS(nil, "s")
}

func TestOpenTurnSessions_skillsWithoutProjection(t *testing.T) {
	ctx := t.Context()
	pack := t.TempDir()
	d := filepath.Join(pack, "research")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "SKILL.md"), []byte("---\nname: research\ndescription: d\n---\n\nbody\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ws, skills, err := OpenTurnSessions(ctx, "s", AgentSpec{
		OpenVFS:    vfs.Tree(vfs.At("work", builtins.Local(t.TempDir()))),
		OpenSkills: vfs.Tree(vfs.At("skills", builtins.Local(pack))),
	}, nil, downProjection{})
	if err != nil || ws != nil || skills == nil {
		t.Fatalf("workspace=%v skills=%v err=%v", ws, skills, err)
	}
	t.Cleanup(func() { CloseTurnTrees(ws, skills, "s", "test") })
	got, err := skills.ReadFile(ctx, "/workspace/skills/research/SKILL.md")
	if err != nil || !strings.Contains(string(got), "body") {
		t.Fatalf("skills read: %q %v", got, err)
	}
}

func TestOpenTurnSessions_skillsError(t *testing.T) {
	_, _, err := OpenTurnSessions(t.Context(), "s", AgentSpec{
		OpenVFS: vfs.Tree(vfs.At("work", builtins.Local(t.TempDir()))),
		OpenSkills: func(context.Context, string, vfs.Request) (*vfs.MountSession, error) {
			return nil, os.ErrPermission
		},
	}, nil, vfs.DirectProjection{})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("err=%v", err)
	}
}

func TestOpenTurnVFS_treeUnderWorkspace(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	ms, err := OpenTurnVFS(ctx, "sess", AgentSpec{
		OpenVFS: vfs.Tree(vfs.At("docs", builtins.Local(dir))),
	}, nil, vfs.DirectProjection{})
	if err != nil || ms == nil {
		t.Fatalf("open: %v %v", ms, err)
	}
	t.Cleanup(func() { CloseTurnVFS(ms, "sess", "test") })
	b, err := ms.ReadFile(ctx, "/workspace/docs/hello.txt")
	if err != nil || string(b) != "hi" {
		t.Fatalf("read: %q %v", b, err)
	}
}

func TestOpenTurnVFS_driveSkippedWithoutToken(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	ms, err := OpenTurnVFS(ctx, "s", AgentSpec{
		OpenVFS: vfs.Tree(vfs.At("scratch", builtins.Local(dir))),
	}, nil, vfs.DirectProjection{})
	if err != nil || ms == nil {
		t.Fatalf("open: %v %v", ms, err)
	}
	t.Cleanup(func() { CloseTurnVFS(ms, "s", "test") })
	if _, err := ms.Stat(ctx, "/workspace/drive"); err == nil {
		t.Fatal("drive member should be absent")
	}
}

type failAttach struct{ err error }

func (failAttach) Available() bool                          { return true }
func (f failAttach) Attach(*vfs.MountSession, string) error { return f.err }

func TestOpenTurnVFS_attachError(t *testing.T) {
	_, err := OpenTurnVFS(t.Context(), "s", AgentSpec{
		OpenVFS: vfs.Tree(vfs.At("scratch", builtins.Local(t.TempDir()))),
	}, nil, failAttach{err: os.ErrPermission})
	if err == nil {
		t.Fatal("want attach error")
	}
}

func TestOpenTurnVFS_unknownAtName(t *testing.T) {
	_, err := OpenTurnVFS(t.Context(), "s", AgentSpec{
		OpenVFS: vfs.Tree(vfs.At("", builtins.Local(t.TempDir()))),
	}, nil, vfs.DirectProjection{})
	if err == nil {
		t.Fatal("want At name error")
	}
}

func TestClearSessionVFS_noop(t *testing.T) {
	cat := NewCatalog("default")
	cat.Register("default", AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{}, Config: tacklr.Config{MaxWindowSize: 8192}},
		OpenVFS: vfs.Tree(vfs.At("scratch", builtins.Local(t.TempDir()))),
	})
	ClearSessionVFS(cat, "s1")
}

func TestBindingsForTurn_skipsEmptyAliasAndEmptyToken(t *testing.T) {
	recipes := []MountRecipe{
		{Provider: "gdrive", Alias: ""},
		{Provider: "gdrive", Alias: "docs", Params: nil},
	}
	got := BindingsForTurn(recipes, AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Auth:     vfs.Credential{Token: "   "},
	}}})
	if len(got) != 0 {
		t.Fatalf("want none, got %+v", got)
	}
	got = BindingsForTurn(recipes, AuthContext{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Auth:     vfs.Credential{Token: "tok"},
	}}})
	if len(got) != 1 || got[0].Params[vfs.ParamName] != "docs" {
		t.Fatalf("token-only provider: %+v", got)
	}
}
