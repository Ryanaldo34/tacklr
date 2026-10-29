package vfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// GitRepo configures one git remote as a host-owned mount.
type GitRepo struct {
	// URL is the remote (https://…, ssh://… or user@host:path).
	URL string
	// Auth is the service-account credential for the clone. Nil reaches
	// public remotes only.
	Auth GitCredential
	// Ref is the branch or tag to check out. Empty uses the remote HEAD.
	Ref string
	// Depth limits clone history (0 = full history).
	Depth int
	// Dir is the host directory that holds checkouts on worker disk. Each
	// session gets Dir/<sessionID>/<member name>.
	Dir string
}

// Git clones a remote onto local disk with the git CLI and mounts the working
// tree like Local: writes go to the checkout on disk. The agent runs every git
// command (status, commit, push, …) itself through run_command, so the
// environment needs git, and pushing needs credentials the environment gives
// the shell. The clone credential is not kept.
//
// The first open for a session clones; later opens on the same worker reuse
// the checkout as the agent left it. A checkout lives on worker disk only:
// work that is not pushed is lost when a session moves to another worker.
//
//	vfs.Tree(vfs.At("api", vfs.Git(api)), vfs.At("docs", vfs.Git(docs)).ReadOnly())
func Git(repo GitRepo) Open {
	return func(ctx context.Context, sessionID string, b Binding) (Provider, error) {
		if strings.TrimSpace(repo.URL) == "" || strings.TrimSpace(repo.Dir) == "" {
			return nil, errors.New("vfs: git URL and Dir are required")
		}
		name := b.Params[ParamName]
		if err := errors.Join(safeSegment(sessionID), safeSegment(name)); err != nil {
			return nil, err
		}
		dir := filepath.Join(repo.Dir, sessionID, name)
		// Never run git with a credential inside an existing checkout: the agent
		// controls its .git/config (core.fsmonitor, core.sshCommand, hooks), so
		// git would run agent-chosen commands outside the jail.
		if _, err := os.Stat(filepath.Join(dir, ".git")); errors.Is(err, os.ErrNotExist) {
			if err := repo.clone(ctx, dir); err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
		return NewLocalProvider(dir)
	}
}

// clone clones into a temp sibling of dir and renames it into place, so a
// failed or interrupted clone never leaves a half checkout at dir.
func (r GitRepo) clone(ctx context.Context, dir string) error {
	parent := filepath.Dir(dir)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return fmt.Errorf("vfs: git checkout dir: %w", err)
	}
	tmp, err := os.MkdirTemp(parent, ".clone-*")
	if err != nil {
		return fmt.Errorf("vfs: git checkout dir: %w", err)
	}
	defer os.RemoveAll(tmp)

	// credential.helper= drops the worker's own helpers (keychain, store), so
	// only Auth is offered and nothing is saved.
	args := []string{"-c", "credential.helper=", "clone", "--quiet"}
	if r.Ref != "" {
		args = append(args, "--branch", r.Ref)
	}
	if r.Depth > 0 {
		args = append(args, "--depth", fmt.Sprint(r.Depth))
	}
	args = append(args, "--", r.URL, tmp)
	cmd := exec.CommandContext(ctx, "git", args...)
	// No prompts: a bad credential fails the clone instead of hanging it.
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_ASKPASS=", "SSH_ASKPASS=")
	if r.Auth != nil {
		env, cleanup, err := r.Auth.cloneEnv(ctx, r.URL)
		if err != nil {
			return err
		}
		defer cleanup()
		cmd.Env = append(cmd.Env, env...)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("vfs: git clone %s: %w: %s", r.URL, err, strings.TrimSpace(string(out)))
	}
	if err := os.Rename(tmp, dir); err != nil {
		return fmt.Errorf("vfs: git checkout dir: %w", err)
	}
	return nil
}
