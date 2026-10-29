package vfs

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
)

// GitCredential is a service-account credential for cloning one git remote.
// Only GitToken and GitSSHKey make one. It reaches the clone through the git
// process environment and temp files that are removed after the clone, so it
// never lands in the checkout's .git/config, a Binding or a checkpoint.
type GitCredential interface {
	// cloneEnv returns environment entries for `git clone` and a cleanup that
	// removes anything it wrote to disk.
	cloneEnv(ctx context.Context, url string) (env []string, cleanup func(), err error)
}

// GitToken authenticates HTTPS remotes with basic auth. user is the name the
// git host expects with a token ("x-access-token" on GitHub, "oauth2" on
// GitLab). token is called once for each clone, so the host can rotate it.
func GitToken(user string, token func(ctx context.Context) (string, error)) GitCredential {
	return gitToken{user: user, token: token}
}

type gitToken struct {
	user  string
	token func(ctx context.Context) (string, error)
}

func (c gitToken) cloneEnv(ctx context.Context, url string) ([]string, func(), error) {
	if c.token == nil {
		return nil, nil, errors.New("vfs: git token source required")
	}
	tok, err := c.token(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("vfs: git token: %w", err)
	}
	if strings.TrimSpace(tok) == "" {
		return nil, nil, errors.New("vfs: git token is empty")
	}
	basic := base64.StdEncoding.EncodeToString([]byte(c.user + ":" + tok))
	// http.<url>.extraHeader scopes the header to this remote. Config from
	// the environment is not written to .git/config.
	return []string{
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=http." + url + ".extraHeader",
		"GIT_CONFIG_VALUE_0=Authorization: Basic " + basic,
	}, func() {}, nil
}

// GitSSHKey authenticates SSH remotes with an unencrypted deploy key. The SSH
// user comes from the URL (git@host:org/repo.git). knownHosts holds
// known_hosts lines; the server key must match one of them, so there is no
// trust-on-first-use.
func GitSSHKey(privateKeyPEM, knownHosts []byte) GitCredential {
	return gitSSHKey{key: privateKeyPEM, knownHosts: knownHosts}
}

type gitSSHKey struct {
	key        []byte
	knownHosts []byte
}

func (c gitSSHKey) cloneEnv(context.Context, string) ([]string, func(), error) {
	if len(bytes.TrimSpace(c.key)) == 0 || len(bytes.TrimSpace(c.knownHosts)) == 0 {
		return nil, nil, errors.New("vfs: git ssh key and known_hosts are required")
	}
	var files []string
	cleanup := func() {
		for _, f := range files {
			_ = os.Remove(f)
		}
	}
	for _, data := range [][]byte{c.key, c.knownHosts} {
		name, err := writeTemp(data)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		files = append(files, name)
	}
	ssh := fmt.Sprintf("ssh -F /dev/null -i %q -o IdentitiesOnly=yes -o BatchMode=yes "+
		"-o StrictHostKeyChecking=yes -o UserKnownHostsFile=%q", files[0], files[1])
	return []string{"GIT_SSH_COMMAND=" + ssh}, cleanup, nil
}

// writeTemp writes data to a new 0600 temp file.
func writeTemp(data []byte) (string, error) {
	f, err := os.CreateTemp("", "tacklr-git-*")
	if err != nil {
		return "", err
	}
	_, werr := f.Write(data)
	if err := errors.Join(werr, f.Close()); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}
