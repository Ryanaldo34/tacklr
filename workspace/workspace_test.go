package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryanaldo34/tacklr"
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

// TestConfigBind_addsTheWorkspaceToTheTurn: Bind adds the workspace's
// skills unless the agent has its own loader.
func TestConfigBind_addsTheWorkspaceToTheTurn(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeSkill(t, filepath.Join(root, "skills"), "research", "research")
	ms := mustMountTree(t, "bind", vfs.At("work", vfs.Local(root)))
	own := skills.Loader{Root: t.TempDir()}
	cfg := Config{SkillsPath: "/workspace/work/skills"}

	plain := cfg.Bind(tacklr.AgentOptions{}, ms)
	withLoader := cfg.Bind(tacklr.AgentOptions{SkillsLoader: own}, ms)
	loaded, loadErr := plain.SkillsLoader.Load(t.Context())

	if withLoader.SkillsLoader != own {
		t.Errorf("Bind with a loader = %v, want the agent's own loader kept", withLoader.SkillsLoader)
	}
	if loadErr != nil || len(loaded) != 1 || loaded[0].Path != "/workspace/work/skills/research/SKILL.md" {
		t.Errorf("workspace skills = %+v %v, want research from SkillsPath", loaded, loadErr)
	}
}

// TestSkills_loadsSkillDirectoriesFromTheWorkspace: skills load from the
// default directory, sorted, skipping files and symlinks; a workspace
// without the directory has none; a directory that cannot be read fails.
func TestSkills_loadsSkillDirectoriesFromTheWorkspace(t *testing.T) {
	t.Parallel()
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
