package vfs_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/internal/testgit"
	"github.com/ryanaldo34/tacklr/vfs"
)

func gitTokenRepo(t *testing.T, srv *testgit.Server) vfs.GitRepo {
	t.Helper()
	return vfs.GitRepo{
		URL:  srv.HTTPURL,
		Auth: vfs.GitToken("x-access-token", func(context.Context) (string, error) { return srv.Token, nil }),
		Dir:  t.TempDir(),
	}
}

func mountGit(t *testing.T, sessionID string, repo vfs.GitRepo) *vfs.MountSession {
	t.Helper()
	ms, err := vfs.Tree(vfs.At("code", vfs.Git(repo)))(t.Context(), sessionID, vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// A deploy key clones a ref over SSH at the depth the repo asks for.
func TestGit_sshKeyClonesRefAtDepth(t *testing.T) {
	t.Parallel()
	// Arrange
	ctx := t.Context()
	srv := testgit.Start(t, map[string]string{"docs/index.md": "# Site\n"})
	repo := vfs.GitRepo{
		URL:   srv.SSHURL,
		Auth:  vfs.GitSSHKey(srv.KeyPEM, srv.KnownHosts),
		Ref:   testgit.Base,
		Depth: 1,
		Dir:   t.TempDir(),
	}

	// Act
	ms := mountGit(t, "s2", repo)
	text, err := ms.Route(ctx, "/workspace/code/docs/index.md").ReadText(ctx)

	// Assert
	if err != nil || text.Text() != "# Site\n" {
		t.Fatalf("index.md: %v", err)
	}
	if got := testgit.Run(t, filepath.Join(repo.Dir, "s2", "code"), "rev-parse", "--is-shallow-repository"); got != "true" {
		t.Fatalf("shallow = %q", got)
	}
}

// TestGit_rejectsBadConfigAndCredentials: an open fails for a repo with no
// URL, a token or deploy key the host refuses or cannot make, a ref or
// repository the host does not have, a host key that is not known, and an
// unsafe session id. It also fails when the disk refuses the checkout: a
// directory it cannot make, a temp clone directory it cannot make, a
// checkout path under a file, and a checkout path that has files but no
// clone.
func TestGit_rejectsBadConfigAndCredentials(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	srv := testgit.Start(t, map[string]string{"README.md": "# hi\n"})
	other := testgit.Start(t, map[string]string{"a": "a"})
	good := gitTokenRepo(t, srv)
	withRepo := func(edit func(*vfs.GitRepo)) vfs.GitRepo { r := good; edit(&r); r.Dir = t.TempDir(); return r }
	token := func(tok string, err error) vfs.GitCredential {
		return vfs.GitToken("x", func(context.Context) (string, error) { return tok, err })
	}
	cases := map[string]vfs.GitRepo{
		"missing url":      withRepo(func(r *vfs.GitRepo) { r.URL = "" }),
		"wrong token":      withRepo(func(r *vfs.GitRepo) { r.Auth = token("wrong", nil) }),
		"empty token":      withRepo(func(r *vfs.GitRepo) { r.Auth = token(" ", nil) }),
		"token error":      withRepo(func(r *vfs.GitRepo) { r.Auth = token("", errors.New("vault down")) }),
		"nil token source": withRepo(func(r *vfs.GitRepo) { r.Auth = vfs.GitToken("x", nil) }),
		"missing ref":      withRepo(func(r *vfs.GitRepo) { r.Ref = "release" }),
		"empty ssh key": withRepo(func(r *vfs.GitRepo) {
			r.URL, r.Auth = srv.SSHURL, vfs.GitSSHKey(nil, srv.KnownHosts)
		}),
		"unknown host key": withRepo(func(r *vfs.GitRepo) {
			r.URL, r.Auth = srv.SSHURL, vfs.GitSSHKey(srv.KeyPEM, other.KnownHosts)
		}),
		"unknown deploy key": withRepo(func(r *vfs.GitRepo) {
			r.URL, r.Auth = srv.SSHURL, vfs.GitSSHKey(other.KeyPEM, srv.KnownHosts)
		}),
		"missing repository": withRepo(func(r *vfs.GitRepo) {
			r.URL, r.Auth = strings.Replace(srv.SSHURL, "/repo.git", "/nope.git", 1), vfs.GitSSHKey(srv.KeyPEM, srv.KnownHosts)
		}),
	}
	for name, repo := range cases {
		if _, err := vfs.Git(repo)(ctx, "s3", mount.Binding{Params: map[string]string{mount.ParamName: "code"}}); err == nil {
			t.Errorf("%s: open succeeded", name)
		}
	}
	if _, err := vfs.Git(good)(ctx, "..", mount.Binding{Params: map[string]string{mount.ParamName: "code"}}); err == nil {
		t.Error("unsafe session id accepted")
	}
	for name, prepare := range map[string]func(dir string) error{
		"checkout under a file":   func(dir string) error { return os.WriteFile(filepath.Join(dir, "s3"), nil, 0o600) },
		"checkout dir not made":   func(dir string) error { return os.Chmod(dir, 0o500) },
		"temp clone dir not made": func(dir string) error { return os.Mkdir(filepath.Join(dir, "s3"), 0o500) },
		"checkout with files and no clone": func(dir string) error {
			return os.MkdirAll(filepath.Join(dir, "s3", "code", "kept"), 0o750)
		},
	} {
		repo := withRepo(func(*vfs.GitRepo) {})
		t.Cleanup(func() { _ = os.Chmod(repo.Dir, 0o750); _ = os.Chmod(filepath.Join(repo.Dir, "s3"), 0o750) })
		if err := prepare(repo.Dir); err != nil {
			t.Fatal(err)
		}
		if _, err := vfs.Git(repo)(ctx, "s3", mount.Binding{Params: map[string]string{mount.ParamName: "code"}}); err == nil {
			t.Errorf("%s: open succeeded", name)
		}
	}
}

// A deploy key needs a temp directory for the key files, so a clone fails
// when there is none. It stays serial: it sets TMPDIR for the process.
func TestGit_sshKeyNeedsATempDir(t *testing.T) {
	repo := vfs.GitRepo{
		URL:  "ssh://git@127.0.0.1:1/repo.git",
		Auth: vfs.GitSSHKey([]byte("key"), []byte("known host")),
		Dir:  t.TempDir(),
	}
	t.Setenv("TMPDIR", filepath.Join(repo.Dir, "missing"))

	_, err := vfs.Git(repo)(t.Context(), "s", mount.Binding{Params: map[string]string{mount.ParamName: "code"}})

	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open with no temp dir = %v, want ErrNotExist", err)
	}
}
