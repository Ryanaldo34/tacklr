// Package sandboxtest starts a sandbox supervisor with the real Rust worker
// for tests: a test certificate authority, a supervisor on a loopback port,
// and workers written as shell scripts that break the protocol. The first
// test that needs the worker builds it and its packages (make
// sandbox-packages, cargo build --release) once per test binary.
package sandboxtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ryanaldo34/tacklr/sandbox"
	"github.com/ryanaldo34/tacklr/sandbox/supervisor"
	"github.com/ryanaldo34/tacklr/vfs"
)

// worker is the real Rust worker binary and its packages directory.
var worker struct {
	once     sync.Once
	err      error
	binary   string
	packages string
}

// Worker builds the real worker and its packages once per test binary,
// one test binary at a time, and returns the binary's path and the
// packages directory.
func Worker(t *testing.T) (binary, packages string) {
	t.Helper()
	worker.once.Do(func() {
		_, file, _, _ := runtime.Caller(0)
		root := filepath.Join(filepath.Dir(file), "..", "..")
		crate := filepath.Join(root, "sandbox", "worker")
		lock, err := os.OpenFile(filepath.Join(crate, "packages", ".build.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			worker.err = err
			return
		}
		defer lock.Close()
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
			worker.err = err
			return
		}
		if output, err := exec.Command("make", "-C", root, "sandbox-worker").CombinedOutput(); err != nil {
			worker.err = fmt.Errorf("build the sandbox worker: %w\n%s", err, output)
			return
		}
		worker.binary = filepath.Join(crate, "target", "release", "tacklr-sandbox-worker")
		worker.packages = filepath.Join(crate, "packages")
	})
	require.NoError(t, worker.err)
	return worker.binary, worker.packages
}

// Client starts a supervisor with the real worker over workspace (nil: an
// empty tree) and returns a client of it.
func Client(t *testing.T, workspace vfs.OpenVFS) *sandbox.Client {
	t.Helper()
	ca := NewAuthority(t)
	return sandbox.NewClient(Start(t, ca, supervisor.Config{Workspace: workspace}).Address, ClientConfig(t, ca, ca))
}

// Host is a running supervisor.
type Host struct {
	Address string
	// Stop ends Serve and returns its error. It runs at cleanup too.
	Stop func() error
}

// Start serves a supervisor on a loopback port. Empty fields of
// config get the real worker and ca's server TLS.
func Start(t *testing.T, ca *Authority, config supervisor.Config) Host {
	t.Helper()
	binary, packages := Worker(t)
	if config.WorkerPath == "" {
		config.WorkerPath = binary
	}
	config.PackagesDir = packages
	config.TLS = ca.ServerConfig(t)
	sup, err := supervisor.New(config)
	require.NoError(t, err)
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- sup.Serve(ctx, listener) }()
	var result error
	stopped := false
	stop := func() error {
		if !stopped {
			stopped = true
			cancel()
			result = <-served
		}
		return result
	}
	t.Cleanup(func() { require.NoError(t, stop()) })
	return Host{Address: listener.Addr().String(), Stop: stop}
}

// ScriptWorker writes a shell script that takes the place of the worker
// binary: a worker that does not follow the protocol.
func ScriptWorker(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker")
	script := "#!/bin/sh\n" + strings.Join(lines, "\n") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o700)) //nolint:gosec // the script must be executable
	return path
}

// SendFrame is a shell command that writes body as one control frame on
// the worker's socket (fd 3).
func SendFrame(body string) string {
	return "printf '" + frame(1, nil, body) + "' >&3"
}

// SendFS is a shell command that writes each request as a file frame with
// no bytes, all in one write on the worker's socket (fd 3).
func SendFS(requests ...string) string {
	var frames strings.Builder
	for _, request := range requests {
		frames.WriteString(frame(4, binary.BigEndian.AppendUint32(nil, uint32(len(request))), request)) //nolint:gosec // test requests are short
	}
	return "printf '" + frames.String() + "' >&3"
}

// frame is one frame of kind for printf: its header, prefix and body.
func frame(kind byte, prefix []byte, body string) string {
	header := binary.BigEndian.AppendUint32(nil, uint32(len(prefix)+len(body))) //nolint:gosec // test bodies are short
	var out strings.Builder
	for _, b := range append(append(header, kind), prefix...) {
		fmt.Fprintf(&out, `\%03o`, b)
	}
	out.WriteString(body)
	return out.String()
}

// Authority is a test certificate authority.
type Authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	// Pool holds the authority's certificate.
	Pool *x509.CertPool
}

func NewAuthority(t *testing.T) *Authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tacklr test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &Authority{cert: cert, key: key, Pool: pool}
}

func (a *Authority) issue(t *testing.T, template *x509.Certificate) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template.SerialNumber = big.NewInt(time.Now().UnixNano())
	template.NotBefore = time.Now().Add(-time.Hour)
	template.NotAfter = time.Now().Add(time.Hour)
	template.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// ServerConfig is a supervisor TLS config for 127.0.0.1 that trusts
// clients of a.
func (a *Authority) ServerConfig(t *testing.T) *tls.Config {
	t.Helper()
	certificate := a.issue(t, &x509.Certificate{
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	return &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: a.Pool, MinVersion: tls.VersionTLS13}
}

// ClientConfig is a client TLS config that trusts servers of trust and has
// a client certificate from issuer.
func ClientConfig(t *testing.T, trust, issuer *Authority) *tls.Config {
	t.Helper()
	certificate := issuer.issue(t, &x509.Certificate{
		URIs:        []*url.URL{{Scheme: "spiffe", Host: "tacklr.test", Path: "/runtime"}},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	return &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: trust.Pool, MinVersion: tls.VersionTLS13}
}
