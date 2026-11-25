// Package sandboxtest starts a sandbox supervisor that boots real
// Firecracker microVMs for tests: a test certificate authority, a
// supervisor on a loopback port, and guests written in Python that break
// the protocol. The first test that needs the guest image builds it (make
// sandbox-image) once per test binary. The tests need /dev/kvm.
package sandboxtest

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
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

// image is the guest image: Firecracker, the kernel and the root file
// system.
var image struct {
	once  sync.Once
	err   error
	paths Image
}

// Image is the paths of the guest image.
type Image struct {
	Firecracker, Kernel, Rootfs string
}

// BuildImage builds the guest image once per test binary, one test binary
// at a time (make sandbox-image), and returns its paths. It fails the
// test on a host without /dev/kvm: the sandbox tests boot real microVMs.
func BuildImage(t *testing.T) Image {
	t.Helper()
	image.once.Do(func() {
		if _, err := os.Stat("/dev/kvm"); err != nil {
			image.err = fmt.Errorf("the sandbox tests boot Firecracker microVMs and need /dev/kvm: %w", err)
			return
		}
		_, file, _, _ := runtime.Caller(0)
		root := filepath.Join(filepath.Dir(file), "..", "..")
		out := filepath.Join(root, "sandbox", "image", "out")
		if err := os.MkdirAll(out, 0o750); err != nil {
			image.err = err
			return
		}
		lock, err := os.OpenFile(filepath.Join(out, ".build.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			image.err = err
			return
		}
		defer lock.Close()
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
			image.err = err
			return
		}
		if output, err := exec.Command("make", "-C", root, "sandbox-image").CombinedOutput(); err != nil {
			image.err = fmt.Errorf("build the sandbox image: %w\n%s", err, output)
			return
		}
		image.paths = Image{
			Firecracker: filepath.Join(out, "firecracker"),
			Kernel:      filepath.Join(out, "vmlinux"),
			Rootfs:      filepath.Join(out, "rootfs.ext4"),
		}
	})
	require.NoError(t, image.err)
	return image.paths
}

// Client starts a supervisor with the real guest over workspace (nil: an
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

// Start serves a supervisor on a loopback port. Empty fields of config get
// the guest image and ca's server TLS. The VMs' log is in the test's
// output when the test fails.
func Start(t *testing.T, ca *Authority, config supervisor.Config) Host {
	t.Helper()
	paths := BuildImage(t)
	if config.Firecracker == "" {
		config.Firecracker = paths.Firecracker
	}
	config.Kernel = paths.Kernel
	config.Rootfs = paths.Rootfs
	config.TLS = ca.ServerConfig(t)
	log := &syncBuffer{}
	config.Log = log
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("the VMs' log:\n%s", log.String())
		}
	})
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

// syncBuffer is a buffer that the VMs write their log to concurrently.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Guest is a guest init that takes the place of the worker: a Python
// program that does not follow the protocol. It connects to the host's
// control port as s, then runs each statement. send(m) writes the dict m
// as one control frame. A statement may not have a double quote or a
// newline.
func Guest(statements ...string) []string {
	setup := "import socket,struct,json,time,os;" +
		"s=socket.socket(socket.AF_VSOCK,socket.SOCK_STREAM);s.connect((2,1024));" +
		"send=lambda m:s.sendall(struct.pack('>IB',len(json.dumps(m)),1)+json.dumps(m).encode())"
	return []string{"/usr/bin/python3", "-c", strings.Join(append([]string{setup}, statements...), ";")}
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
