package adapter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/builtins"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/vfs"
)

type downProjection struct{}

func (downProjection) Available() bool                        { return false }
func (downProjection) Attach(*vfs.MountSession, string) error { return nil }

func TestOpenTurnVFS_nilWhenNoOpenOrProjection(t *testing.T) {
	ms, err := OpenTurnVFS(t.Context(), "s", durable.AgentSpec{}, nil, vfs.DirectProjection{})
	if err != nil || ms != nil {
		t.Fatalf("no OpenVFS: %v %v", ms, err)
	}
	ms, err = OpenTurnVFS(t.Context(), "s", durable.AgentSpec{OpenVFS: vfs.Tree(vfs.At("scratch", builtins.Local(t.TempDir())))}, nil, downProjection{})
	if err != nil || ms != nil {
		t.Fatalf("projection down: %v %v", ms, err)
	}
	CloseTurnVFS(nil)
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
	ws, skills, err := OpenTurnSessions(ctx, "s", durable.AgentSpec{
		OpenVFS:    vfs.Tree(vfs.At("work", builtins.Local(t.TempDir()))),
		OpenSkills: vfs.Tree(vfs.At("skills", builtins.Local(pack))),
	}, nil, downProjection{})
	if err != nil || ws != nil || skills == nil {
		t.Fatalf("workspace=%v skills=%v err=%v", ws, skills, err)
	}
	t.Cleanup(func() { CloseTurnTrees(ws, skills) })
	got, err := skills.ReadFile(ctx, "/workspace/skills/research/SKILL.md")
	if err != nil || !strings.Contains(string(got), "body") {
		t.Fatalf("skills read: %q %v", got, err)
	}
}

func TestOpenTurnSessions_skillsError(t *testing.T) {
	_, _, err := OpenTurnSessions(t.Context(), "s", durable.AgentSpec{
		OpenVFS: vfs.Tree(vfs.At("work", builtins.Local(t.TempDir()))),
		OpenSkills: func(context.Context, string, vfs.Request) (*vfs.MountSession, error) {
			return nil, os.ErrPermission
		},
	}, nil, vfs.DirectProjection{})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("err=%v", err)
	}
}

type failAttach struct{ err error }

func (failAttach) Available() bool                          { return true }
func (f failAttach) Attach(*vfs.MountSession, string) error { return f.err }

func TestOpenTurnVFS_attachError(t *testing.T) {
	_, err := OpenTurnVFS(t.Context(), "s", durable.AgentSpec{
		OpenVFS: vfs.Tree(vfs.At("scratch", builtins.Local(t.TempDir()))),
	}, nil, failAttach{err: os.ErrPermission})
	if err == nil {
		t.Fatal("want attach error")
	}
}
