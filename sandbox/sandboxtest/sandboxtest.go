// Package sandboxtest starts sandbox supervisors for tests: a test
// certificate authority, a supervisor on a loopback port, and FakeVM, a
// stand-in for Firecracker whose guest follows a script. A test that boots
// real Firecracker microVMs builds the guest image (make sandbox-image)
// once per test binary, and needs /dev/kvm.
package sandboxtest

import (
	"bytes"
	"cmp"
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

// Client starts a supervisor that boots real VMs over workspace (nil: an
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

// Start serves a supervisor on a loopback port with ca's server TLS. A
// config with no Firecracker boots real VMs of the guest image
// (BuildImage); one with a Firecracker (such as FakeVM) runs it with no
// kernel or root file system. The VMs' log is in the test's output when
// the test fails.
func Start(t *testing.T, ca *Authority, config supervisor.Config) Host {
	t.Helper()
	if config.Firecracker == "" {
		paths := BuildImage(t)
		config.Firecracker, config.Kernel, config.Rootfs = paths.Firecracker, paths.Kernel, paths.Rootfs
	}
	config.Kernel = cmp.Or(config.Kernel, "/dev/null")
	config.Rootfs = cmp.Or(config.Rootfs, "/dev/null")
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

// fakeVM is the fakevm program, built once per test binary.
var fakeVM struct {
	once   sync.Once
	err    error
	binary string
}

// FakeVM is a Firecracker for a test that boots no VM: the fakevm program
// (sandbox/sandboxtest/fakevm), which the supervisor starts as it starts
// Firecracker, and which does steps (see the program) as the VM's guest.
// It is named firecracker.
func FakeVM(t *testing.T, steps ...string) string {
	t.Helper()
	fakeVM.once.Do(func() {
		_, file, _, _ := runtime.Caller(0)
		dir, err := os.MkdirTemp("", "tacklr-fakevm-")
		if err != nil {
			fakeVM.err = err
			return
		}
		fakeVM.binary = filepath.Join(dir, "fakevm")
		build := exec.Command("go", "build", "-o", fakeVM.binary, "./fakevm") //nolint:gosec // G204: builds this package's own program
		build.Dir = filepath.Dir(file)
		if output, err := build.CombinedOutput(); err != nil {
			fakeVM.err = fmt.Errorf("build fakevm: %w\n%s", err, output)
		}
	})
	require.NoError(t, fakeVM.err)
	dir := t.TempDir()
	path := filepath.Join(dir, "firecracker")
	require.NoError(t, os.Symlink(fakeVM.binary, path))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "steps.txt"), []byte(strings.Join(steps, "\n")+"\n"), 0o600))
	return path
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
