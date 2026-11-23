package supervisor_test

import (
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

func TestWorkerIsReadyAnswersPingsAndExitsOnStop(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	client := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{}).Address, sandboxtest.ClientConfig(t, ca, ca))

	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	pingErr := worker.Ping(t.Context())
	exit, stopErr := worker.Stop(t.Context())
	pingAfterExit := worker.Ping(t.Context())

	require.GreaterOrEqual(t, worker.Enforcement().LandlockABI, 6)
	require.NoError(t, pingErr)
	require.NoError(t, stopErr)
	require.Equal(t, sandbox.Exit{Code: 0}, exit)
	require.ErrorIs(t, pingAfterExit, sandbox.ErrExited)
}

func TestClosingAWorkerKillsItsProcess(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	client := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{}).Address, sandboxtest.ClientConfig(t, ca, ca))
	worker, err := client.Start(t.Context(), "s", nil)
	require.NoError(t, err)
	pid := childPID(t, "tacklr-sandbox-worker")

	worker.Close()

	require.Eventually(t, func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		return errors.Is(err, fs.ErrNotExist)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRunStreamsTheScriptsOutputAndReturnsItsExitCode(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	dir, tree := workspace(t)
	worker, err := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{Workspace: tree}).Address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "names.txt"), []byte("b\na\n"), 0o600))
	var stdout, stderr strings.Builder

	code, err := worker.Run(t.Context(), "cd work && sort names.txt | tee sorted.txt; mkdir out && mv sorted.txt out/ && truncate -s 1 names.txt && rm Spec.docx 2>/dev/null; echo warn >&2; exit 3", &stdout, &stderr)

	require.NoError(t, err)
	require.Equal(t, 3, code)
	require.Equal(t, "a\nb\n", stdout.String())
	require.Equal(t, "warn\n", stderr.String())
	sorted, err := os.ReadFile(filepath.Join(dir, "out", "sorted.txt"))
	require.NoError(t, err)
	require.Equal(t, "a\nb\n", string(sorted))
	names, err := os.ReadFile(filepath.Join(dir, "names.txt"))
	require.NoError(t, err)
	require.Equal(t, "b", string(names))
}

func TestRunReadsADocumentAsTextAndCannotWriteIt(t *testing.T) {
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

func TestSupervisorKillsAWorkerThatSendsAFileRequestThatIsNotValid(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		WorkerPath: sandboxtest.ScriptWorker(t,
			sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"head -c 1 <&3 >/dev/null",
			"printf '\\000\\000\\000\\002\\004{}' >&3",
			"exec sleep 3600"),
	}).Address
	worker, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", io.Discard, io.Discard)

	require.ErrorIs(t, err, sandbox.ErrExited)
}

func TestRunRefusesAnEmptyScript(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	worker, err := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{}).Address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "", io.Discard, io.Discard)

	require.EqualError(t, err, "sandbox: the script is empty")
}

func TestRunEndsWithItsContextAndKillsTheWorker(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	worker, err := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{}).Address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	pid := childPID(t, "tacklr-sandbox-worker")
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err = worker.Run(ctx, "sleep 30", io.Discard, io.Discard)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Eventually(t, func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		return errors.Is(err, fs.ErrNotExist)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRunReportsAScriptTheWorkersRuntimeCouldNotRun(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		WorkerPath: sandboxtest.ScriptWorker(t,
			sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"head -c 1 <&3 >/dev/null",
			sandboxtest.SendFrame(`{"type":"done","id":1,"code":-1,"error":"wasm trap: out of bounds memory access"}`),
			"exec sleep 3600"),
	}).Address
	worker, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", io.Discard, io.Discard)

	require.EqualError(t, err, "sandbox: the worker could not run the script: wasm trap: out of bounds memory access")
}

func TestRunReportsAWorkerThatEndsDuringTheRun(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		WorkerPath: sandboxtest.ScriptWorker(t,
			sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"head -c 1 <&3 >/dev/null",
			"exit 1"),
	}).Address
	worker, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", io.Discard, io.Discard)

	require.ErrorIs(t, err, sandbox.ErrExited)
}

func TestRunCommandRunsTheModelsScriptInTheSandbox(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	dir, tree := workspace(t)
	client := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{Workspace: tree}).Address, sandboxtest.ClientConfig(t, ca, ca))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "note.txt"), []byte("sandboxed note\n"), 0o600))
	call := toolCaller(t, sandbox.RunCommand(client))

	cat, catErr := call(`{"command":"cat work/note.txt"}`)
	empty, emptyErr := call(`{"command":" "}`)
	flood, floodErr := call(`{"command":"yes | head -c 1100000"}`)
	unended, unendedErr := call(`{"command":"printf done"}`)

	require.NoError(t, errors.Join(catErr, emptyErr, floodErr, unendedErr))
	require.Equal(t, "exit=0 truncated=false\n--- stdout ---\ndone\n--- stderr ---\n", unended)
	require.Equal(t, "exit=0 truncated=false\n--- stdout ---\nsandboxed note\n--- stderr ---\n", cat)
	require.Contains(t, empty, "command is required")
	require.True(t, strings.HasPrefix(flood, "exit=0 truncated=true\noutput truncated\n"), flood[:64])
}

func TestRunCommandReportsAWorkerThatCannotStartOrRun(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	refused := sandboxtest.Start(t, ca, supervisor.Config{WorkerPath: filepath.Join(t.TempDir(), "missing")}).Address
	ending := sandboxtest.Start(t, ca, supervisor.Config{
		WorkerPath: sandboxtest.ScriptWorker(t, sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`), "head -c 1 <&3 >/dev/null", "exit 1"),
	}).Address

	startResult, startErr := toolCaller(t, sandbox.RunCommand(sandbox.NewClient(refused, sandboxtest.ClientConfig(t, ca, ca))))(`{"command":"true"}`)
	runResult, runErr := toolCaller(t, sandbox.RunCommand(sandbox.NewClient(ending, sandboxtest.ClientConfig(t, ca, ca))))(`{"command":"true"}`)

	require.NoError(t, errors.Join(startErr, runErr))
	require.Equal(t, "run_command: sandbox: the supervisor refused the worker: 500 the worker could not start", startResult)
	require.Equal(t, "run_command: sandbox: the worker has ended", runResult)
}

func TestStopKillsAWorkerThatDoesNotExit(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		WorkerPath:  sandboxtest.ScriptWorker(t, sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`), "exec 3>&-", "exec sleep 3600"),
		StopTimeout: 50 * time.Millisecond,
	}).Address
	worker, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	exit, err := worker.Stop(t.Context())

	require.NoError(t, err)
	require.Equal(t, sandbox.Exit{Code: -1, Signal: "SIGKILL"}, exit)
}

func TestPingEndsWithItsContextWhenTheWorkerDoesNotAnswer(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		WorkerPath: sandboxtest.ScriptWorker(t, sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`), "exec sleep 3600"),
	}).Address
	worker, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err = worker.Ping(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStopEndsWithItsContextWhenTheWorkerDoesNotExit(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		WorkerPath:  sandboxtest.ScriptWorker(t, sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`), "exec sleep 3600"),
		StopTimeout: time.Hour,
	}).Address
	worker, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err = worker.Stop(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStartReportsAWorkerThatEndsBeforeItIsReady(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{WorkerPath: sandboxtest.ScriptWorker(t, "exit 3")}).Address

	_, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)

	var exitErr *sandbox.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, sandbox.Exit{Code: 3}, exitErr.Exit)
	require.EqualError(t, err, "sandbox: the worker ended before it was ready: code 3")
}

func TestSupervisorKillsAWorkerThatBreaksTheProtocol(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		WorkerPath: sandboxtest.ScriptWorker(t,
			sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"exec sleep 3600"),
		StopTimeout: time.Hour,
	}).Address
	worker, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()

	exit, err := worker.Stop(t.Context())

	require.NoError(t, err)
	require.Equal(t, sandbox.Exit{Code: -1, Signal: "SIGKILL"}, exit)
}

func TestSupervisorEndsTheStreamOfAClientThatBreaksTheProtocol(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{}).Address
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
	address := sandboxtest.Start(t, ca, supervisor.Config{}).Address

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
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		Workspace: func(context.Context, string, vfs.Request) (*vfs.MountSession, error) {
			return nil, errors.New("drive token expired")
		},
	}).Address

	_, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)

	require.EqualError(t, err, "sandbox: the supervisor refused the worker: 502 the workspace could not open: drive token expired")
}

func TestStartAndRunRefreshABindingsTokenThroughTheClient(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	client := sandbox.NewClient(sandboxtest.Start(t, ca, supervisor.Config{Workspace: gatedWorkspace(t)}).Address, sandboxtest.ClientConfig(t, ca, ca))
	var refreshes atomic.Int32
	var session atomic.Value
	var asked atomic.Value
	client.Refresh = func(_ context.Context, s string, b mount.Binding) (mount.Credential, error) {
		refreshes.Add(1)
		session.Store(s)
		asked.Store(b)
		return mount.Credential{Token: "fresh", ExpiresAt: time.Now().Add(30 * time.Second)}, nil
	}
	worker, err := client.Start(t.Context(), "s", []mount.Binding{gatedBinding})
	require.NoError(t, err)
	defer worker.Close()
	atStart := refreshes.Load()
	var stdout strings.Builder

	code, err := worker.Run(t.Context(), "cat drive/readme.txt", &stdout, io.Discard)

	require.NoError(t, err)
	require.Equal(t, 0, code)
	require.Equal(t, "hello", stdout.String())
	require.Positive(t, atStart)
	require.Greater(t, refreshes.Load(), atStart)
	require.Equal(t, "s", session.Load())
	binding := asked.Load().(mount.Binding)
	require.Equal(t, "drive", binding.Alias())
	require.Empty(t, binding.Auth.Token)
}

func TestStartIsRefusedWhenTheClientDoesNotRefreshAnExpiredToken(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{Workspace: gatedWorkspace(t)}).Address
	refused := func(context.Context, string, mount.Binding) (mount.Credential, error) {
		return mount.Credential{}, errors.New("no refresh token")
	}
	for name, refresh := range map[string]func(context.Context, string, mount.Binding) (mount.Credential, error){"without a refresh": nil, "with a refresh that fails": refused} {
		t.Run(name, func(t *testing.T) {
			client := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca))
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
	ca := sandboxtest.NewAuthority(t)
	host := sandboxtest.Start(t, ca, supervisor.Config{Workspace: gatedWorkspace(t)})
	client := sandbox.NewClient(host.Address, sandboxtest.ClientConfig(t, ca, ca))
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

func TestRunEndsWhenTheWorkerDiesWhileARefreshWaitsForTheClient(t *testing.T) {
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
	pid := childPID(t, "tacklr-sandbox-worker")
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

func TestSupervisorRelaysTheClientWhileAFileRequestWaitsForARefresh(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	host := sandboxtest.Start(t, ca, supervisor.Config{
		Workspace: gatedWorkspace(t),
		WorkerPath: sandboxtest.ScriptWorker(t,
			sandboxtest.SendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"head -c 1 <&3 >/dev/null",
			sandboxtest.SendFS(`{"id":1,"op":"stat","path":"drive/readme.txt"}`, `{"id":2,"op":"stat","path":"drive/readme.txt"}`),
			"exec sleep 3600"),
	})
	client := sandbox.NewClient(host.Address, sandboxtest.ClientConfig(t, ca, ca))
	var started atomic.Bool
	var asks atomic.Int32
	var asking sync.Once
	asked, release := make(chan struct{}), make(chan struct{})
	client.Refresh = func(context.Context, string, mount.Binding) (mount.Credential, error) {
		if started.Load() && asks.Add(1) == 1 {
			asking.Do(func() { close(asked) })
			<-release
		}
		return mount.Credential{Token: "fresh", ExpiresAt: time.Now().Add(30 * time.Second)}, nil
	}
	worker, err := client.Start(t.Context(), "s", []mount.Binding{gatedBinding})
	require.NoError(t, err)
	defer worker.Close()
	started.Store(true)
	go func() { _, _ = worker.Run(t.Context(), "true", io.Discard, io.Discard) }()
	<-asked
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	pingErr := worker.Ping(ctx)
	close(release)

	require.ErrorIs(t, pingErr, context.DeadlineExceeded)
	require.Eventually(t, func() bool { return asks.Load() >= 2 }, 5*time.Second, 10*time.Millisecond, "the second file request never asked for its token")
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
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{
		Authorize: func(client *x509.Certificate) error {
			return fmt.Errorf("%s may not start workers", client.URIs[0])
		},
	}).Address

	_, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)

	require.EqualError(t, err, "sandbox: the supervisor refused the worker: 403 spiffe://tacklr.test/runtime may not start workers")
}

func TestSupervisorReportsAWorkerBinaryThatCannotStart(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{WorkerPath: filepath.Join(t.TempDir(), "missing")}).Address

	_, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)

	var refused *sandbox.RefusedError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, http.StatusInternalServerError, refused.StatusCode)
}

func TestSupervisorRefusesAClientCertificateFromAnotherAuthority(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	address := sandboxtest.Start(t, ca, supervisor.Config{}).Address

	_, err := sandbox.NewClient(address, sandboxtest.ClientConfig(t, ca, sandboxtest.NewAuthority(t))).Start(t.Context(), "s", nil)

	require.ErrorContains(t, err, "sandbox: start a worker:")
}

func TestServeStopsItsWorkersWhenItsContextEnds(t *testing.T) {
	ca := sandboxtest.NewAuthority(t)
	host := sandboxtest.Start(t, ca, supervisor.Config{})
	worker, err := sandbox.NewClient(host.Address, sandboxtest.ClientConfig(t, ca, ca)).Start(t.Context(), "s", nil)
	require.NoError(t, err)
	defer worker.Close()
	pid := childPID(t, "tacklr-sandbox-worker")

	serveErr := host.Stop()
	_, stopErr := worker.Stop(t.Context())

	require.NoError(t, serveErr)
	require.ErrorIs(t, stopErr, io.EOF)
	_, statErr := os.Stat("/proc/" + strconv.Itoa(pid))
	require.ErrorIs(t, statErr, fs.ErrNotExist)
}

func TestServeFailsWithoutAServerCertificate(t *testing.T) {
	binary, packages := sandboxtest.Worker(t)
	ca := sandboxtest.NewAuthority(t)
	sup, err := supervisor.New(supervisor.Config{
		WorkerPath:  binary,
		PackagesDir: packages,
		TLS:         &tls.Config{ClientCAs: ca.Pool, MinVersion: tls.VersionTLS13},
	})
	require.NoError(t, err)
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	err = sup.Serve(t.Context(), listener)

	require.ErrorContains(t, err, "open :")
}

func TestNewRefusesAConfigItCannotRunSafely(t *testing.T) {
	binary, packages := sandboxtest.Worker(t)
	ca := sandboxtest.NewAuthority(t)
	valid := func() supervisor.Config {
		return supervisor.Config{WorkerPath: binary, PackagesDir: packages, TLS: ca.ServerConfig(t)}
	}
	cases := []struct {
		name   string
		change func(*supervisor.Config)
		err    string
	}{
		{"a relative worker path", func(c *supervisor.Config) { c.WorkerPath = "worker" }, "supervisor: WorkerPath must be an absolute path"},
		{"a relative packages path", func(c *supervisor.Config) { c.PackagesDir = "packages" }, "supervisor: PackagesDir must be an absolute path"},
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
	tree := vfs.Tree(vfs.At("work", vfs.Local(dir))).WithCodecs(adapters.Codecs()...)
	ms, err := tree(t.Context(), "seed", vfs.Request{})
	require.NoError(t, err)
	spec := "<h1>Title</h1><p>text</p>"
	_, err = ms.Apply(t.Context(), "/workspace/work/Spec.docx", vfs.Mutation{Content: &spec})
	require.NoError(t, err)
	return dir, tree
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
