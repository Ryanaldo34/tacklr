package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

// mustMountTree mounts members as a host that reads Word and Excel files
// does: with the vfs/adapters codecs.
func mustMountTree(t testing.TB, sessionID string, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	return mustMountRequest(t, sessionID, vfs.Request{}, members...)
}

// mustMountRequest is mustMountTree with req's bindings (user tokens).
func mustMountRequest(t testing.TB, sessionID string, req vfs.Request, members ...vfs.Member) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(members...).WithCodecs(adapters.Codecs()...)(t.Context(), sessionID, req)
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func toolNames(tools []*tacklr.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		names = append(names, t.Name())
	}
	return names
}

func writeSkill(t *testing.T, root, dir, name string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: " + name + "\ndescription: " + name + " skill\n---\n\nDo " + name + " things."
	if err := os.WriteFile(filepath.Join(root, dir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestConfigBind_addsTheWorkspaceToTheTurn: Bind adds the workspace tools
// after the host's own, the index tools only with a brain, the workspace's
// skills unless the agent has its own loader, and an Overflow under
// OverflowPath.
func TestConfigBind_addsTheWorkspaceToTheTurn(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, filepath.Join(root, "skills"), "research", "research")
	ms := mustMountTree(t, "bind", vfs.At("work", vfs.Local(root)))
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	host := tacklr.NewTool(tacklr.ToolConfig{Name: "host", Handler: func(context.Context) (string, error) { return "", nil }})
	own := skills.Loader{Root: t.TempDir()}
	cfg := Config{SkillsPath: "/workspace/work/skills", OverflowPath: "/workspace/work/overflow"}

	plain, plainErr := cfg.Bind(tacklr.AgentOptions{Tools: []*tacklr.Tool{host}}, ms)
	withBrain, brainErr := cfg.Bind(tacklr.AgentOptions{Brain: eng, SearchNamespace: storetest.Namespace(t, "id", uuid.NewString()), SkillsLoader: own}, ms)
	loaded, loadErr := plain.SkillsLoader.Load(t.Context())
	kept, keepErr := plain.Overflow.Keep(t.Context(), "s1", "call-1", "the full result")
	read, readErr := toolsOn(ms)["read"](t.Context(), `{"path":"`+kept+`"}`)

	if plainErr != nil || !slices.Equal(toolNames(plain.Tools), []string{"host", "read", "write", "write_document", "write_spreadsheet"}) {
		t.Errorf("Bind = %v %v, want the host tool then the workspace tools", toolNames(plain.Tools), plainErr)
	}
	if brainErr != nil || !slices.Equal(toolNames(withBrain.Tools), []string{"read", "write", "write_document", "write_spreadsheet", "index_file", "unindex"}) || withBrain.SkillsLoader != own {
		t.Errorf("Bind with a brain and a loader = %v %v, want the index tools and the agent's own loader", toolNames(withBrain.Tools), brainErr)
	}
	if loadErr != nil || len(loaded) != 1 || loaded[0].Path != "/workspace/work/skills/research/SKILL.md" {
		t.Errorf("workspace skills = %+v %v, want research from SkillsPath", loaded, loadErr)
	}
	if keepErr != nil || kept != "/workspace/work/overflow/s1/call-1.txt" || readErr != nil || !strings.Contains(read, "the full result") {
		t.Errorf("overflow kept %q %v, read %q %v, want the result where read finds it", kept, keepErr, read, readErr)
	}
}

// TestConfigBind_refusesAnOverflowPathItCannotWrite: an OverflowPath
// outside the mounts or on a read-only mount fails the turn.
func TestConfigBind_refusesAnOverflowPathItCannotWrite(t *testing.T) {
	ms := mustMountTree(t, "overflow", vfs.At("work", vfs.Local(t.TempDir())), vfs.At("ro", vfs.Local(t.TempDir())).ReadOnly())

	_, outside := Config{OverflowPath: "/elsewhere"}.Bind(tacklr.AgentOptions{}, ms)
	_, readOnly := Config{OverflowPath: "/workspace/ro/overflow"}.Bind(tacklr.AgentOptions{}, ms)

	if outside == nil || readOnly == nil || !strings.Contains(readOnly.Error(), "read-only") {
		t.Fatalf("outside the mounts = %v, on a read-only mount = %v, want both refused", outside, readOnly)
	}
}

// TestOverflow_keepFailsWhenItCannotWrite: a file where the session's
// directory goes, or a directory where the result goes, fails Keep.
func TestOverflow_keepFailsWhenItCannotWrite(t *testing.T) {
	ctx := t.Context()
	ms := mustMountTree(t, "overflow-fail", vfs.At("work", vfs.Local(t.TempDir())))
	o, err := Overflow(ms, "/workspace/work/overflow")
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Route(ctx, "/workspace/work/overflow").MkdirAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ms.Route(ctx, "/workspace/work/overflow/file-session").WriteFile(ctx, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := ms.Route(ctx, "/workspace/work/overflow/s1/dir-call.txt").MkdirAll(ctx); err != nil {
		t.Fatal(err)
	}

	_, noDir := o.Keep(ctx, "file-session", "c1", "x")
	_, noFile := o.Keep(ctx, "s1", "dir-call", "x")

	if noDir == nil || noFile == nil {
		t.Fatalf("keep under a file = %v, keep over a directory = %v, want both to fail", noDir, noFile)
	}
}

// TestSkills_loadsSkillDirectoriesFromTheWorkspace: skills load from the
// default directory, sorted, skipping files and symlinks; a workspace
// without the directory has none; a directory that cannot be read fails.
func TestSkills_loadsSkillDirectoriesFromTheWorkspace(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "beta", "beta")
	writeSkill(t, root, "alpha", "alpha")
	if err := os.WriteFile(filepath.Join(root, "readme.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "alpha"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	locked := t.TempDir()
	writeSkill(t, locked, "x", "x")
	ms := mustMountTree(t, "skills", vfs.At("skills", vfs.Local(root)), vfs.At("locked", vfs.Local(locked)))
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })

	loaded, err := Skills(ms, "").Load(t.Context())
	none, noneErr := Skills(mustMountTree(t, "no-skills", vfs.At("work", vfs.Local(t.TempDir()))), "").Load(t.Context())
	_, lockedErr := Skills(ms, "/workspace/locked").Load(t.Context())

	if err != nil || len(loaded) != 2 || loaded[0].Name != "alpha" || loaded[1].Name != "beta" || loaded[0].Path != "/workspace/skills/alpha/SKILL.md" {
		t.Errorf("loaded = %+v %v, want alpha then beta from /workspace/skills", loaded, err)
	}
	if noneErr != nil || none != nil {
		t.Errorf("no skills directory = %+v %v, want none", none, noneErr)
	}
	if lockedErr == nil || errors.Is(lockedErr, vfs.ErrNotExist) {
		t.Errorf("unreadable skills directory = %v, want an error (tests must not run as root)", lockedErr)
	}
}
