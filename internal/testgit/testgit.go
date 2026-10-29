// Package testgit runs an in-process git host for one bare repository: smart
// HTTP (git http-backend over net/http/cgi) behind a basic-auth token, and SSH
// (git upload-pack / receive-pack) behind one deploy key. It needs the git CLI
// and no Docker.
package testgit

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Base is the branch Start seeds.
const Base = "main"

// Server is one seeded bare repository served over HTTP and SSH.
type Server struct {
	// HTTPURL accepts basic auth with any user and Token as the password.
	HTTPURL string
	Token   string
	// SSHURL accepts KeyPEM; KnownHosts pins the server host key.
	SSHURL     string
	KeyPEM     []byte
	KnownHosts []byte

	root string
}

// Start seeds files (path → content) as one commit on Base and serves it. It
// skips the test when the git CLI is not installed.
func Start(t testing.TB, files map[string]string) *Server {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git CLI not installed")
	}
	s := &Server{Token: "test-token", root: t.TempDir()}
	s.seed(t, files)
	s.startHTTP(t)
	s.startSSH(t)
	return s
}

func (s *Server) bare() string { return filepath.Join(s.root, "repo.git") }

func (s *Server) seed(t testing.TB, files map[string]string) {
	t.Helper()
	work := t.TempDir()
	Run(t, work, "init", "--quiet", "--initial-branch="+Base)
	for name, body := range files {
		full := filepath.Join(work, filepath.FromSlash(name))
		must(t, os.MkdirAll(filepath.Dir(full), 0o750))
		must(t, os.WriteFile(full, []byte(body), 0o600))
	}
	Run(t, work, "add", "--all")
	Run(t, work, "commit", "--quiet", "--allow-empty", "-m", "seed")
	Run(t, s.root, "clone", "--quiet", "--bare", work, s.bare())
	Run(t, s.bare(), "config", "http.receivepack", "true")
}

// Run runs git in dir with a fixed identity and no user or system config,
// and returns its trimmed output.
func Run(t testing.TB, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// Show returns name at rev in the served repository.
func (s *Server) Show(t testing.TB, rev, name string) string {
	t.Helper()
	return Run(t, s.bare(), "show", rev+":"+name)
}

func (s *Server) startHTTP(t testing.TB) {
	gitPath, err := exec.LookPath("git")
	must(t, err)
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + s.root, "GIT_HTTP_EXPORT_ALL=1"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pass, ok := r.BasicAuth(); !ok || pass != s.Token {
			w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	s.HTTPURL = srv.URL + "/repo.git"
}

func (s *Server) startSSH(t testing.TB) {
	t.Helper()
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	must(t, err)
	hostKey, err := ssh.NewSignerFromKey(hostPriv)
	must(t, err)
	clientPub, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	must(t, err)
	block, err := ssh.MarshalPrivateKey(clientPriv, "")
	must(t, err)
	s.KeyPEM = pem.EncodeToMemory(block)
	authorized, err := ssh.NewPublicKey(clientPub)
	must(t, err)

	cfg := &ssh.ServerConfig{PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if string(key.Marshal()) == string(authorized.Marshal()) {
			return &ssh.Permissions{}, nil
		}
		return nil, errors.New("unknown key")
	}}
	cfg.AddHostKey(hostKey)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serveSSH(conn, cfg)
		}
	}()
	addr := ln.Addr().String()
	s.SSHURL = "ssh://git@" + addr + "/repo.git"
	s.KnownHosts = []byte(knownhosts.Line([]string{knownhosts.Normalize(addr)}, hostKey.PublicKey()) + "\n")
}

func (s *Server) serveSSH(conn net.Conn, cfg *ssh.ServerConfig) {
	sc, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer sc.Close()
	go ssh.DiscardRequests(reqs)
	for nc := range chans {
		if nc.ChannelType() != "session" {
			_ = nc.Reject(ssh.UnknownChannelType, "session only")
			continue
		}
		ch, creqs, err := nc.Accept()
		if err != nil {
			return
		}
		go s.serveSession(ch, creqs)
	}
}

// serveSession runs "git-upload-pack '/repo.git'" (or receive-pack) against
// the served root.
func (s *Server) serveSession(ch ssh.Channel, reqs <-chan *ssh.Request) {
	defer ch.Close()
	for req := range reqs {
		if req.Type != "exec" {
			_ = req.Reply(req.Type == "env", nil)
			continue
		}
		var exe struct{ Command string }
		err := ssh.Unmarshal(req.Payload, &exe)
		svc, arg, _ := strings.Cut(exe.Command, " ")
		if err != nil || (svc != "git-upload-pack" && svc != "git-receive-pack") {
			_ = req.Reply(false, nil)
			return
		}
		_ = req.Reply(true, nil)
		repo := filepath.Join(s.root, filepath.Base(strings.Trim(arg, "'\"/")))
		cmd := exec.Command("git", strings.TrimPrefix(svc, "git-"), repo)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = ch, ch, ch.Stderr()
		status := uint32(0)
		if err := cmd.Run(); err != nil {
			status = 1
			_, _ = io.WriteString(ch.Stderr(), err.Error())
		}
		_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{status}))
		return
	}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
