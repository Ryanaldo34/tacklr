package vfs_test

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

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
	t.Cleanup(func() { _ = ms.Close() })
	return ms
}

func TestGit_tokenCloneIsAWorkingTreeTheAgentCommitsAndPushesWithGit(t *testing.T) {
	// Arrange
	ctx := t.Context()
	srv := testgit.Start(t, map[string]string{"README.md": "# hi\n"})
	repo := gitTokenRepo(t, srv)
	ms := mountGit(t, "s1", repo)
	checkout := filepath.Join(repo.Dir, "s1", "code")
	pushAuth := "http.extraHeader=Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("agent:"+srv.Token))

	// Act
	readme, readErr := ms.Route(ctx, "/workspace/code/README.md").ReadFile(ctx)
	writeErr := errors.Join(
		ms.Route(ctx, "/workspace/code/src").MkdirAll(ctx),
		ms.Route(ctx, "/workspace/code/src/app.go").WriteFile(ctx, []byte("package app\n")),
	)
	testgit.Run(t, checkout, "add", "--all")
	testgit.Run(t, checkout, "commit", "--quiet", "-m", "add app")
	testgit.Run(t, checkout, "-c", pushAuth, "push", "--quiet", "origin", "HEAD:refs/heads/agent")
	next := mountGit(t, "s1", repo)
	reopened, reopenErr := next.Route(ctx, "/workspace/code/src/app.go").ReadFile(ctx)

	// Assert
	if readErr != nil || string(readme) != "# hi\n" || writeErr != nil {
		t.Fatalf("read %q %v, write %v", readme, readErr, writeErr)
	}
	if got := srv.Show(t, "agent", "src/app.go"); got != "package app" {
		t.Fatalf("pushed app.go = %q", got)
	}
	if got := testgit.Run(t, checkout, "remote", "get-url", "origin"); got != srv.HTTPURL {
		t.Fatalf("origin = %q", got)
	}
	if reopenErr != nil || string(reopened) != "package app\n" ||
		testgit.Run(t, checkout, "log", "-1", "--format=%s") != "add app" {
		t.Fatalf("next activity lost the checkout: %q %v", reopened, reopenErr)
	}
}

func TestGit_sshKeyClonesRefAtDepth(t *testing.T) {
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

func TestGit_rejectsBadConfigAndCredentials(t *testing.T) {
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
	}
	for name, repo := range cases {
		if _, err := vfs.Git(repo)(ctx, "s3", vfs.Binding{Params: map[string]string{vfs.ParamName: "code"}}); err == nil {
			t.Errorf("%s: open succeeded", name)
		}
	}
	if _, err := vfs.Git(good)(ctx, "..", vfs.Binding{Params: map[string]string{vfs.ParamName: "code"}}); err == nil {
		t.Error("unsafe session id accepted")
	}
	blocked := withRepo(func(*vfs.GitRepo) {})
	if err := os.WriteFile(filepath.Join(blocked.Dir, "s3"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := vfs.Git(blocked)(ctx, "s3", vfs.Binding{Params: map[string]string{vfs.ParamName: "code"}}); err == nil {
		t.Error("checkout under a file accepted")
	}
}
