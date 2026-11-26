package supervisor_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/sandbox"
	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/sandbox/sandboxtest"
	"github.com/ryanaldo34/tacklr/sandbox/supervisor"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// serving is the steps of a fake VM whose guest is the real worker over
// bash, with no workspace mounted.
var serving = []string{"connect", "serve"}

// ready is the steps of a fake VM whose guest connects and sends ready.
var ready = []string{"connect", `send {"type":"ready"}`}

// fake serves a supervisor whose VMs are a fake VM of steps (FakeVM), with
// config's other fields, and returns a client of it and the host.
func fake(t *testing.T, config supervisor.Config, steps ...string) (*sandbox.Client, sandboxtest.Host) {
	t.Helper()
	ca := sandboxtest.NewAuthority(t)
	config.Firecracker = sandboxtest.FakeVM(t, steps...)
	host := sandboxtest.Start(t, ca, config)
	return sandbox.NewClient(host.Address, sandboxtest.ClientConfig(t, ca, ca)), host
}

// gone reports whether process pid has ended.
func gone(pid int) func() bool {
	return func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		return errors.Is(err, fs.ErrNotExist)
	}
}

func TestAVMRunsAScriptOnItsWorkspaceAndPowersOffOnStop(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	dir, tree := workspace(t)
	worker, err := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{Workspace: tree}).Address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "names.txt"), []byte("b\na\n"), 0o600))
	var stdout, stderr strings.Builder

	code, runErr := worker.Run(t.Context(), "cd work && sort names.txt | tee sorted.txt; mkdir out && mv sorted.txt out/ && truncate -s 1 names.txt && id -u >&2; exit 3", &stdout, &stderr)
	exit, stopErr := worker.Stop(t.Context())

	require.Equal(t, "firecracker", worker.Enforcement().Hypervisor)
	require.NoError(t, runErr)
	require.Equal(t, 3, code)
	require.Equal(t, "a\nb\n", stdout.String())
	require.Equal(t, "1000\n", stderr.String())
	sorted, err := os.ReadFile(filepath.Join(dir, "out", "sorted.txt"))
	require.NoError(t, err)
	require.Equal(t, "a\nb\n", string(sorted))
	names, err := os.ReadFile(filepath.Join(dir, "names.txt"))
	require.NoError(t, err)
	require.Equal(t, "b", string(names))
	require.NoError(t, stopErr)
	require.Equal(t, sandbox.Exit{Code: 0}, exit)
}

func TestAVMReadsADocumentAsTextAndCannotWriteIt(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	_, tree := workspace(t)
	worker, err := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{Workspace: tree}).Address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	var stdout, stderr strings.Builder

	code, err := worker.Run(t.Context(), "cat work/Spec.docx; echo changed > work/Spec.docx", &stdout, &stderr)

	require.NoError(t, err)
	require.Equal(t, 1, code)
	require.Contains(t, stdout.String(), "Title")
	require.Contains(t, stderr.String(), "Spec.docx")
}

func TestAVMRefreshesABindingsTokenThroughTheClientAndAnswersPingsWhileItWaits(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	client := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{Workspace: gatedWorkspace(t)}).Address, sandboxtest.ClientConfig(t, ca, ca))
	var refreshes atomic.Int32
	var session atomic.Value
	var asked atomic.Value
	var started atomic.Bool
	var holding sync.Once
	held, release := make(chan struct{}), make(chan struct{})
	client.Refresh = func(_ context.Context, s string, b mount.Binding) (mount.Credential, error) {
		refreshes.Add(1)
		session.Store(s)
		asked.Store(b)
		if started.Load() {
			holding.Do(func() {
				close(held)
				<-release
			})
		}
		return mount.Credential{Token: "fresh", ExpiresAt: time.Now().Add(30 * time.Second)}, nil
	}
	worker, err := client.Start(t.Context(), "s", []mount.Binding{gatedBinding})
	require.NoError(t, err)
	defer worker.Close()
	atStart := refreshes.Load()
	started.Store(true)
	var stdout strings.Builder
	ran := make(chan error, 1)
	go func() {
		_, err := worker.Run(t.Context(), "cat drive/readme.txt", &stdout, io.Discard)
		ran <- err
	}()
	<-held
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	pingErr := worker.Ping(ctx)
	close(release)
	runErr := <-ran

	require.NoError(t, pingErr)
	require.NoError(t, runErr)
	require.Equal(t, "hello", stdout.String())
	require.Positive(t, atStart)
	require.Greater(t, refreshes.Load(), atStart)
	require.Equal(t, "s", session.Load())
	binding := asked.Load().(mount.Binding)
	require.Equal(t, "drive", binding.Alias())
	require.Empty(t, binding.Auth.Token)
}

func TestARunEndsWhenItsVMDiesWhileARefreshWaitsForTheClient(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	host := sandboxtest.Start(t, ca, supervisor.Config{Workspace: gatedWorkspace(t)})
	client := sandbox.NewClient(host.Address, sandboxtest.ClientConfig(t, ca, ca))
	var started atomic.Bool
	var asking sync.Once
	asked := make(chan struct{})
	client.Refresh = func(ctx context.Context, _ string, _ mount.Binding) (mount.Credential, error) {
		if !started.Load() {
			return mount.Credential{Token: "fresh", ExpiresAt: time.Now().Add(30 * time.Second)}, nil
		}
		asking.Do(func() { close(asked) })
		<-ctx.Done()
		return mount.Credential{}, ctx.Err()
	}
	worker, err := client.Start(t.Context(), "s", []mount.Binding{gatedBinding})
	require.NoError(t, err)
	defer worker.Close()
	started.Store(true)
	pid := childPID(t, "firecracker")
	ran := make(chan error, 1)
	go func() {
		_, err := worker.Run(t.Context(), "cat drive/readme.txt", io.Discard, io.Discard)
		ran <- err
	}()
	<-asked

	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))

	require.ErrorIs(t, <-ran, sandbox.ErrExited)
	require.NoError(t, host.Stop())
}

func TestWorkerIsReadyAnswersPingsAndExitsOnStop(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, serving...)

	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	pingErr := worker.Ping(t.Context())
	exit, stopErr := worker.Stop(t.Context())
	pingAfterExit := worker.Ping(t.Context())

	require.Equal(t, "firecracker", worker.Enforcement().Hypervisor)
	require.NoError(t, pingErr)
	require.NoError(t, stopErr)
	require.Equal(t, sandbox.Exit{Code: 0}, exit)
	require.ErrorIs(t, pingAfterExit, sandbox.ErrExited)
}

func TestRunStreamsTheScriptsOutputAndReturnsItsExitCode(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, serving...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	var stdout, stderr strings.Builder

	code, err := worker.Run(t.Context(), "printf 'b\\na\\n' | sort; echo warn >&2; exit 3", &stdout, &stderr)

	require.NoError(t, err)
	require.Equal(t, 3, code)
	require.Equal(t, "a\nb\n", stdout.String())
	require.Equal(t, "warn\n", stderr.String())
}

func TestClosingAWorkerKillsItsVM(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, serving...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	pid := childPID(t, "firecracker")

	worker.Close()

	require.Eventually(t, gone(pid), 5*time.Second, 10*time.Millisecond)
}

func TestSupervisorKillsAGuestThatSendsAFrameOfAKindItDoesNotKnow(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, append(ready, "read 5", "raw 00000002097b7d", "hang")...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", io.Discard, io.Discard)

	require.ErrorIs(t, err, sandbox.ErrExited)
}

func TestRunRefusesAnEmptyScript(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, serving...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "", io.Discard, io.Discard)

	require.EqualError(t, err, "sandbox: the script is empty")
}

func TestRunEndsWithItsContextAndKillsTheWorker(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, serving...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	pid := childPID(t, "firecracker")
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err = worker.Run(ctx, "sleep 30", io.Discard, io.Discard)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Eventually(t, gone(pid), 5*time.Second, 10*time.Millisecond)
}

func TestRunReportsAScriptTheWorkersRuntimeCouldNotRun(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, append(ready, "read 5", `send {"type":"done","id":1,"code":-1,"error":"bash could not start"}`, "hang")...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", io.Discard, io.Discard)

	require.EqualError(t, err, "sandbox: the worker could not run the script: bash could not start")
}

func TestRunReportsAWorkerThatEndsDuringTheRun(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, append(ready, "read 5", "exit 1")...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", io.Discard, io.Discard)

	require.ErrorIs(t, err, sandbox.ErrExited)
}

func TestRunCommandRunsTheModelsScriptInTheSandbox(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, serving...)
	call := toolCaller(t, sandbox.RunCommand(client))

	note, noteErr := call(`{"command":"printf 'sandboxed note\\n'"}`)
	empty, emptyErr := call(`{"command":" "}`)
	flood, floodErr := call(`{"command":"yes | head -c 1100000"}`)
	unended, unendedErr := call(`{"command":"printf done"}`)

	require.NoError(t, errors.Join(noteErr, emptyErr, floodErr, unendedErr))
	require.Equal(t, "exit=0 truncated=false\n--- stdout ---\ndone\n--- stderr ---\n", unended)
	require.Equal(t, "exit=0 truncated=false\n--- stdout ---\nsandboxed note\n--- stderr ---\n", note)
	require.Contains(t, empty, "command is required")
	require.True(t, strings.HasPrefix(flood, "exit=0 truncated=true\noutput truncated\n"), flood[:64])
}

func TestRunCommandReportsAWorkerThatCannotStartOrRun(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	refused := sandboxtest.Start(t, ca, supervisor.Config{Firecracker: filepath.Join(t.TempDir(), "missing")}).Address
	ending, _ := fake(t, supervisor.Config{}, append(ready, "read 5", "exit 1")...)

	startResult, startErr := toolCaller(t, sandbox.RunCommand(sandbox.NewClient(refused, sandboxtest.ClientConfig(t, ca, ca))))(`{"command":"true"}`)
	runResult, runErr := toolCaller(t, sandbox.RunCommand(ending))(`{"command":"true"}`)

	require.NoError(t, errors.Join(startErr, runErr))
	require.Equal(t, "run_command: sandbox: the supervisor refused the worker: 500 the worker could not start", startResult)
	require.Equal(t, "run_command: sandbox: the worker has ended", runResult)
}

func TestStopKillsAWorkerThatDoesNotExit(t *testing.T) {
	client, _ := fake(t, supervisor.Config{StopTimeout: 50 * time.Millisecond}, append(ready, "close", "hang")...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	exit, err := worker.Stop(t.Context())

	require.NoError(t, err)
	require.Equal(t, sandbox.Exit{Code: -1, Signal: "SIGKILL"}, exit)
}

func TestPingEndsWithItsContextWhenTheWorkerDoesNotAnswer(t *testing.T) {
	client, _ := fake(t, supervisor.Config{}, append(ready, "hang")...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err = worker.Ping(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStopEndsWithItsContextWhenTheWorkerDoesNotExit(t *testing.T) {
	client, _ := fake(t, supervisor.Config{StopTimeout: time.Hour}, append(ready, "hang")...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err = worker.Stop(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStartReportsAGuestThatEndsBeforeItIsReady(t *testing.T) {
	for name, steps := range map[string][]string{
		"before it connects": {"exit 3"},
		"after it connects":  {"connect", "exit 3"},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := fake(t, supervisor.Config{}, steps...)

			_, err := client.Start(t.Context(), "s", nil)

			var exitErr *sandbox.ExitError
			require.ErrorAs(t, err, &exitErr)
			require.Equal(t, sandbox.Exit{Code: 3}, exitErr.Exit)
			require.EqualError(t, err, "sandbox: the worker ended before it was ready: code 3")
		})
	}
}

func TestStartReportsAGuestThatDoesNotConnectInTheBootTimeout(t *testing.T) {
	client, _ := fake(t, supervisor.Config{BootTimeout: 200 * time.Millisecond}, "hang")

	_, err := client.Start(t.Context(), "s", nil)

	var exitErr *sandbox.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, sandbox.Exit{Code: -1, Signal: "SIGKILL"}, exitErr.Exit)
}

func TestSupervisorKillsAWorkerThatBreaksTheProtocol(t *testing.T) {
	client, _ := fake(t, supervisor.Config{StopTimeout: time.Hour}, append(ready, `send {"type":"ready"}`, "hang")...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	exit, err := worker.Stop(t.Context())

	require.NoError(t, err)
	require.Equal(t, sandbox.Exit{Code: -1, Signal: "SIGKILL"}, exit)
}

func TestSupervisorEndsTheStreamOfAClientThatBreaksTheProtocol(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{Firecracker: sandboxtest.FakeVM(t, serving...)}).Address
	send, response := rawStream(t, ca, address, wire.Message{Type: wire.TypeOpen, Session: "s"})
	ready, err := wire.Read(response.Body)
	require.NoError(t, err)
	require.Equal(t, wire.TypeReady, ready.Message.Type)

	require.NoError(t, wire.Write(send, wire.Message{Type: wire.TypeReady}))
	_, err = wire.Read(response.Body)

	require.ErrorIs(t, err, io.EOF)
}

func TestSupervisorRefusesAStreamThatDoesNotStartWithOpen(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{Firecracker: sandboxtest.FakeVM(t, serving...)}).Address

	_, response := rawStream(t, ca, address, wire.Message{Type: wire.TypePing, ID: 1})

	require.Equal(t, http.StatusBadRequest, response.StatusCode)
}

func TestStartRefusesBindingsLargerThanAFrame(t *testing.T) {
	client := sandbox.NewClient("127.0.0.1:1", &tls.Config{MinVersion: tls.VersionTLS13})
	huge := []mount.Binding{{Provider: "gdrive", Params: map[string]string{"name": strings.Repeat("x", wire.MaxBody)}}}

	_, err := client.Start(t.Context(), "s", huge)

	require.ErrorContains(t, err, "is larger than 1048576")
}

func TestSupervisorRefusesAWorkerWhoseWorkspaceCannotOpen(t *testing.T) {
	client, _ := fake(t, supervisor.Config{
		Workspace: func(context.Context, string, vfs.Request) (*vfs.MountSession, error) {
			return nil, errors.New("drive token expired")
		},
	}, serving...)

	_, err := client.Start(t.Context(), "s", nil)

	require.EqualError(t, err, "sandbox: the supervisor refused the worker: 502 the workspace could not open: drive token expired")
}

func TestStartIsRefusedWhenTheClientDoesNotRefreshAnExpiredToken(t *testing.T) {
	refused := func(context.Context, string, mount.Binding) (mount.Credential, error) {
		return mount.Credential{}, errors.New("no refresh token")
	}
	for name, refresh := range map[string]func(context.Context, string, mount.Binding) (mount.Credential, error){"without a refresh": nil, "with a refresh that fails": refused} {
		t.Run(name, func(t *testing.T) {
			client, _ := fake(t, supervisor.Config{Workspace: gatedWorkspace(t)}, serving...)
			client.Refresh = refresh

			_, err := client.Start(t.Context(), "s", []mount.Binding{gatedBinding})

			var refusal *sandbox.RefusedError
			require.ErrorAs(t, err, &refusal)
			require.Equal(t, http.StatusBadGateway, refusal.StatusCode)
			require.Contains(t, refusal.Message, "auth expired")
		})
	}
}

func TestStartEndsWithItsContextWhileARefreshWaits(t *testing.T) {
	client, host := fake(t, supervisor.Config{Workspace: gatedWorkspace(t)}, serving...)
	ctx, cancel := context.WithCancel(t.Context())
	client.Refresh = func(refreshCtx context.Context, _ string, _ mount.Binding) (mount.Credential, error) {
		cancel()
		<-refreshCtx.Done()
		return mount.Credential{}, refreshCtx.Err()
	}

	_, err := client.Start(ctx, "s", []mount.Binding{gatedBinding})

	require.Error(t, err)
	require.NoError(t, host.Stop())
}

// gatedBinding is a Drive folder that holds readme.txt, with a token that
// has expired.
var gatedBinding = mount.Binding{
	Provider: mount.ProviderGoogleDrive,
	Params:   map[string]string{mount.ParamName: "drive", mount.ParamFolderID: "root-b"},
	Auth:     mount.Credential{Token: "stale", ExpiresAt: time.Now().Add(-time.Hour)},
}

// gatedWorkspace mounts the Drive folder of gatedBinding, on a fake Drive
// server, at /workspace/drive. Drive refreshes the expired token through
// the binding's holder before its first call.
func gatedWorkspace(t *testing.T) vfs.OpenVFS {
	t.Helper()
	return func(ctx context.Context, session string, req vfs.Request) (*vfs.MountSession, error) {
		b, _ := vfs.BindingByName(req.Bindings, "drive")
		return vfs.Tree(vfs.At("drive", testdrive.Open(t, testdrive.Tree(), req.Holder(b))))(ctx, session, req)
	}
}

// rawStream opens a worker stream whose first frame is first, and returns
// the stream's writer and the supervisor's response.
func rawStream(t *testing.T, ca *sandboxtest.Authority, address string, first wire.Message) (*io.PipeWriter, *http.Response) {
	t.Helper()
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: sandboxtest.ClientConfig(t, ca, ca), Protocols: protocols}}
	var opening bytes.Buffer
	require.NoError(t, wire.Write(&opening, first))
	requests, send := io.Pipe()
	t.Cleanup(func() { _ = send.Close() })
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, (&url.URL{Scheme: "https", Host: address, Path: wire.StreamPath}).String(), io.MultiReader(&opening, requests))
	require.NoError(t, err)
	response, err := httpClient.Do(request)
	require.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })
	return send, response
}

func TestSupervisorRefusesAClientThatAuthorizeDenies(t *testing.T) {
	client, _ := fake(t, supervisor.Config{
		Authorize: func(client *x509.Certificate) error {
			return fmt.Errorf("%s may not start workers", client.URIs[0])
		},
	}, serving...)

	_, err := client.Start(t.Context(), "s", nil)

	require.EqualError(t, err, "sandbox: the supervisor refused the worker: 403 spiffe://tacklr.test/runtime may not start workers")
}

func TestSupervisorReportsAFirecrackerThatCannotStart(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{Firecracker: filepath.Join(t.TempDir(), "missing")}).Address

	_, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)

	var refused *sandbox.RefusedError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, http.StatusInternalServerError, refused.StatusCode)
}

func TestSupervisorRefusesAClientCertificateFromAnotherAuthority(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{Firecracker: sandboxtest.FakeVM(t, serving...)}).Address

	_, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, sandboxtest.NewAuthority(t))).Start(t.Context(), "s", nil)

	require.ErrorContains(t, err, "sandbox: start a worker:")
}

func TestServeStopsItsWorkersWhenItsContextEnds(t *testing.T) {
	client, host := fake(t, supervisor.Config{}, serving...)
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	pid := childPID(t, "firecracker")

	serveErr := host.Stop()
	_, stopErr := worker.Stop(t.Context())

	require.NoError(t, serveErr)
	require.ErrorIs(t, stopErr, io.EOF)
	require.True(t, gone(pid)())
}

func TestServeFailsWithoutAServerCertificate(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	sup, err := supervisor.New(supervisor.Config{
		Firecracker: "/usr/bin/firecracker",
		Kernel:      "/dev/null",
		Rootfs:      "/dev/null",
		TLS:         &tls.Config{ClientCAs: ca.Pool, MinVersion: tls.VersionTLS13},
	})
	require.NoError(t, err)
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	err = sup.Serve(t.Context(), listener)

	require.ErrorContains(t, err, "open :")
}

func TestNewRefusesAConfigItCannotRunSafely(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	valid := func() supervisor.Config {
		return supervisor.Config{Firecracker: "/usr/bin/firecracker", Kernel: "/dev/null", Rootfs: "/dev/null", TLS: ca.ServerConfig(t)}
	}
	cases := []struct {
		name   string
		change func(*supervisor.Config)
		err    string
	}{
		{"a relative Firecracker path", func(c *supervisor.Config) { c.Firecracker = "firecracker" }, "supervisor: Firecracker must be an absolute path"},
		{"a relative kernel path", func(c *supervisor.Config) { c.Kernel = "vmlinux" }, "supervisor: Kernel must be an absolute path"},
		{"a relative root file system path", func(c *supervisor.Config) { c.Rootfs = "rootfs.ext4" }, "supervisor: Rootfs must be an absolute path"},
		{"no TLS", func(c *supervisor.Config) { c.TLS = nil }, "supervisor: TLS is required"},
		{"no client roots", func(c *supervisor.Config) { c.TLS.ClientCAs = nil }, "supervisor: TLS.ClientCAs is required to verify clients"},
		{"a per-client TLS config", func(c *supervisor.Config) {
			c.TLS.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, nil }
		}, "supervisor: TLS.GetConfigForClient is not supported"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			config := valid()
			c.change(&config)

			_, err := supervisor.New(config)

			require.EqualError(t, err, c.err)
		})
	}
}

// workspace is a host's tree for the supervisor: a work mount over a
// temporary directory that holds a Word document.
func workspace(t *testing.T) (string, vfs.OpenVFS) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Spec.docx"), specDocument(t), 0o600))
	return dir, vfs.Tree(vfs.At("work", vfs.Local(dir))).WithCodecs(adapters.Codecs()...)
}

// specDocument is a Word document of a Title heading and a text paragraph.
func specDocument(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("word/document.xml")
	require.NoError(t, err)
	_, err = io.WriteString(w, `<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>`+
		`<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>Title</w:t></w:r></w:p><w:p><w:r><w:t>text</w:t></w:r></w:p></w:body></w:document>`)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// toolCaller runs tool in a harness that allows it, and returns a function
// that calls it with arguments.
func toolCaller(t *testing.T, tool *tacklr.Tool) func(arguments string) (string, error) {
	t.Helper()
	opts := tacklr.AgentOptions{Model: testkit.HTTPModel(t, nil), MaxWindowSize: 8192, Tools: []*tacklr.Tool{tool}, AllowTools: []string{tool.Name()}}
	h, err := tacklr.NewHarness(t.Context(), tacklr.BindTurn(opts, "sandbox", nil, nil))
	require.NoError(t, err)
	t.Cleanup(h.Close)
	out, stop := tacklr.PipeEvents(func(session.Event) {})
	t.Cleanup(stop)
	return func(arguments string) (string, error) {
		call := tacklr.NewCall(message.ToolCall{ID: "c1", CallID: "c1", Name: tool.Name(), Arguments: arguments}, 0)
		c, err := h.RunCall(t.Context(), call, out)
		return c.Body.Text(), err
	}
}

// childPID returns the pid of this test process's one child whose command
// name starts with name (the kernel keeps 15 bytes of it).
func childPID(t *testing.T, name string) int {
	t.Helper()
	stats, err := filepath.Glob("/proc/[0-9]*/stat")
	require.NoError(t, err)
	parent := strconv.Itoa(os.Getpid())
	for _, path := range stats {
		stat, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		open, end := strings.IndexByte(string(stat), '('), strings.LastIndexByte(string(stat), ')')
		fields := strings.Fields(string(stat[end+1:]))
		if strings.HasPrefix(name, string(stat[open+1:end])) && fields[1] == parent {
			pid, err := strconv.Atoi(strings.Fields(string(stat))[0])
			require.NoError(t, err)
			return pid
		}
	}
	t.Fatalf("no child process %s", name)
	return 0
}
