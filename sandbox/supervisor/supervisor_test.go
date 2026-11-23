package supervisor_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/sandbox"
	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/sandbox/supervisor"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

// workerBinary is the real Rust worker and packagesDir its packages, built
// once by TestMain.
var workerBinary, packagesDir string

func TestMain(m *testing.M) {
	dir, err := filepath.Abs("../worker")
	if err != nil {
		panic(err)
	}
	packages := exec.Command("make", "-C", "../..", "sandbox-packages")
	build := exec.Command("cargo", "build", "--release", "--locked")
	build.Dir = dir
	for _, step := range []*exec.Cmd{packages, build} {
		if output, err := step.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build the sandbox worker: %v\n%s", err, output)
			os.Exit(1)
		}
	}
	workerBinary = filepath.Join(dir, "target", "release", "tacklr-sandbox-worker")
	packagesDir = filepath.Join(dir, "packages")
	os.Exit(m.Run())
}

func TestWorkerIsReadyAnswersPingsAndExitsOnStop(t *testing.T) {
	ca := newAuthority(t)
	client := sandbox.NewClient(startSupervisor(t, ca, supervisor.Config{}).address, clientConfig(t, ca, ca))

	worker, err := client.Start(t.Context())
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
	ca := newAuthority(t)
	client := sandbox.NewClient(startSupervisor(t, ca, supervisor.Config{}).address, clientConfig(t, ca, ca))
	worker, err := client.Start(t.Context())
	require.NoError(t, err)
	pid := childPID(t, filepath.Base(workerBinary))

	worker.Close()

	require.Eventually(t, func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		return errors.Is(err, fs.ErrNotExist)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRunStreamsTheScriptsOutputAndReturnsItsExitCode(t *testing.T) {
	ca := newAuthority(t)
	worker, err := sandbox.NewClient(startSupervisor(t, ca, supervisor.Config{}).address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()
	dir, files := workspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "names.txt"), []byte("b\na\n"), 0o600))
	var stdout, stderr strings.Builder

	code, err := worker.Run(t.Context(), "cd work && sort names.txt | tee sorted.txt; mkdir out && mv sorted.txt out/ && truncate -s 1 names.txt && rm Spec.docx 2>/dev/null; echo warn >&2; exit 3", files, &stdout, &stderr)

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
	ca := newAuthority(t)
	worker, err := sandbox.NewClient(startSupervisor(t, ca, supervisor.Config{}).address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()
	_, files := workspace(t)
	var stdout, stderr strings.Builder

	code, err := worker.Run(t.Context(), "cat work/Spec.docx; echo changed > work/Spec.docx", files, &stdout, &stderr)

	require.NoError(t, err)
	require.Equal(t, 1, code)
	require.Contains(t, stdout.String(), "Title")
	require.Contains(t, stderr.String(), "Spec.docx")
}

func TestRunClosesAWorkerThatSendsAFileRequestThatIsNotValid(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{
		WorkerPath: scriptWorker(t,
			sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"head -c 1 <&3 >/dev/null",
			"printf '\\000\\000\\000\\002\\004{}' >&3",
			"exec sleep 3600"),
	}).address
	worker, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", vfs.Files{}, io.Discard, io.Discard)

	require.EqualError(t, err, "wire: the file frame is too short")
}

func TestRunRefusesAnEmptyScript(t *testing.T) {
	ca := newAuthority(t)
	worker, err := sandbox.NewClient(startSupervisor(t, ca, supervisor.Config{}).address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "", vfs.Files{}, io.Discard, io.Discard)

	require.EqualError(t, err, "sandbox: the script is empty")
}

func TestRunEndsWithItsContextAndKillsTheWorker(t *testing.T) {
	ca := newAuthority(t)
	worker, err := sandbox.NewClient(startSupervisor(t, ca, supervisor.Config{}).address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	pid := childPID(t, filepath.Base(workerBinary))
	_, files := workspace(t)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err = worker.Run(ctx, "sleep 30", files, io.Discard, io.Discard)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Eventually(t, func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		return errors.Is(err, fs.ErrNotExist)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRunReportsAScriptTheWorkersRuntimeCouldNotRun(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{
		WorkerPath: scriptWorker(t,
			sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"head -c 1 <&3 >/dev/null",
			sendFrame(`{"type":"done","id":1,"code":-1,"error":"wasm trap: out of bounds memory access"}`),
			"exec sleep 3600"),
	}).address
	worker, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", vfs.Files{}, io.Discard, io.Discard)

	require.EqualError(t, err, "sandbox: the worker could not run the script: wasm trap: out of bounds memory access")
}

func TestRunReportsAWorkerThatEndsDuringTheRun(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{
		WorkerPath: scriptWorker(t,
			sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"head -c 1 <&3 >/dev/null",
			"exit 1"),
	}).address
	worker, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()

	_, err = worker.Run(t.Context(), "true", vfs.Files{}, io.Discard, io.Discard)

	require.ErrorIs(t, err, sandbox.ErrExited)
}

func TestRunCommandRunsTheModelsScriptInTheSandbox(t *testing.T) {
	ca := newAuthority(t)
	client := sandbox.NewClient(startSupervisor(t, ca, supervisor.Config{}).address, clientConfig(t, ca, ca))
	dir, files := workspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "note.txt"), []byte("sandboxed note\n"), 0o600))
	call := toolCaller(t, sandbox.RunCommand(client, files))

	cat, catErr := call(`{"command":"cat work/note.txt"}`)
	empty, emptyErr := call(`{"command":" "}`)
	flood, floodErr := call(`{"command":"yes | head -c 1100000"}`)

	require.NoError(t, errors.Join(catErr, emptyErr, floodErr))
	require.Equal(t, "exit=0 truncated=false\n--- stdout ---\nsandboxed note\n--- stderr ---\n", cat)
	require.Contains(t, empty, "command is required")
	require.True(t, strings.HasPrefix(flood, "exit=0 truncated=true\noutput truncated\n"), flood[:64])
}

func TestRunCommandReportsAWorkerThatCannotStartOrRun(t *testing.T) {
	ca := newAuthority(t)
	refused := startSupervisor(t, ca, supervisor.Config{WorkerPath: filepath.Join(t.TempDir(), "missing")}).address
	ending := startSupervisor(t, ca, supervisor.Config{
		WorkerPath: scriptWorker(t, sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`), "head -c 1 <&3 >/dev/null", "exit 1"),
	}).address
	_, files := workspace(t)

	startResult, startErr := toolCaller(t, sandbox.RunCommand(sandbox.NewClient(refused, clientConfig(t, ca, ca)), files))(`{"command":"true"}`)
	runResult, runErr := toolCaller(t, sandbox.RunCommand(sandbox.NewClient(ending, clientConfig(t, ca, ca)), files))(`{"command":"true"}`)

	require.NoError(t, errors.Join(startErr, runErr))
	require.Equal(t, "run_command: sandbox: the supervisor refused the worker: 500 the worker could not start", startResult)
	require.Equal(t, "run_command: sandbox: the worker has ended", runResult)
}

func TestStopKillsAWorkerThatDoesNotExit(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{
		WorkerPath:  scriptWorker(t, sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`), "exec 3>&-", "exec sleep 3600"),
		StopTimeout: 50 * time.Millisecond,
	}).address
	worker, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()

	exit, err := worker.Stop(t.Context())

	require.NoError(t, err)
	require.Equal(t, sandbox.Exit{Code: -1, Signal: "SIGKILL"}, exit)
}

func TestPingEndsWithItsContextWhenTheWorkerDoesNotAnswer(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{
		WorkerPath: scriptWorker(t, sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`), "exec sleep 3600"),
	}).address
	worker, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err = worker.Ping(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStopEndsWithItsContextWhenTheWorkerDoesNotExit(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{
		WorkerPath:  scriptWorker(t, sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`), "exec sleep 3600"),
		StopTimeout: time.Hour,
	}).address
	worker, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err = worker.Stop(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestStartReportsAWorkerThatEndsBeforeItIsReady(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{WorkerPath: scriptWorker(t, "exit 3")}).address

	_, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())

	var exitErr *sandbox.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, sandbox.Exit{Code: 3}, exitErr.Exit)
	require.EqualError(t, err, "sandbox: the worker ended before it was ready: code 3")
}

func TestSupervisorKillsAWorkerThatBreaksTheProtocol(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{
		WorkerPath: scriptWorker(t,
			sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			sendFrame(`{"type":"ready","enforcement":{"landlock_abi":6}}`),
			"exec sleep 3600"),
		StopTimeout: time.Hour,
	}).address
	worker, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()

	exit, err := worker.Stop(t.Context())

	require.NoError(t, err)
	require.Equal(t, sandbox.Exit{Code: -1, Signal: "SIGKILL"}, exit)
}

func TestSupervisorEndsTheStreamOfAClientThatBreaksTheProtocol(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{}).address
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: clientConfig(t, ca, ca), Protocols: protocols}}
	requests, send := io.Pipe()
	defer send.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, (&url.URL{Scheme: "https", Host: address, Path: wire.StreamPath}).String(), requests)
	require.NoError(t, err)
	response, err := httpClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	ready, err := wire.Read(response.Body)
	require.NoError(t, err)
	require.Equal(t, wire.TypeReady, ready.Message.Type)

	require.NoError(t, wire.Write(send, wire.Message{Type: wire.TypeReady}))
	_, err = wire.Read(response.Body)

	require.ErrorIs(t, err, io.EOF)
}

func TestSupervisorRefusesAClientThatAuthorizeDenies(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{
		Authorize: func(client *x509.Certificate) error {
			return fmt.Errorf("%s may not start workers", client.URIs[0])
		},
	}).address

	_, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())

	require.EqualError(t, err, "sandbox: the supervisor refused the worker: 403 spiffe://tacklr.test/runtime may not start workers")
}

func TestSupervisorRefusesAWorkerOverItsLimit(t *testing.T) {
	ca := newAuthority(t)
	client := sandbox.NewClient(startSupervisor(t, ca, supervisor.Config{MaxWorkers: 1}).address, clientConfig(t, ca, ca))
	first, err := client.Start(t.Context())
	require.NoError(t, err)
	defer first.Close()

	_, err = client.Start(t.Context())

	var refused *sandbox.RefusedError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, http.StatusServiceUnavailable, refused.StatusCode)
}

func TestSupervisorReportsAWorkerBinaryThatCannotStart(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{WorkerPath: filepath.Join(t.TempDir(), "missing")}).address

	_, err := sandbox.NewClient(address, clientConfig(t, ca, ca)).Start(t.Context())

	var refused *sandbox.RefusedError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, http.StatusInternalServerError, refused.StatusCode)
}

func TestSupervisorRefusesAClientCertificateFromAnotherAuthority(t *testing.T) {
	ca := newAuthority(t)
	address := startSupervisor(t, ca, supervisor.Config{}).address

	_, err := sandbox.NewClient(address, clientConfig(t, ca, newAuthority(t))).Start(t.Context())

	require.ErrorContains(t, err, "sandbox: start a worker:")
}

func TestServeStopsItsWorkersWhenItsContextEnds(t *testing.T) {
	ca := newAuthority(t)
	host := startSupervisor(t, ca, supervisor.Config{})
	worker, err := sandbox.NewClient(host.address, clientConfig(t, ca, ca)).Start(t.Context())
	require.NoError(t, err)
	defer worker.Close()
	pid := childPID(t, filepath.Base(workerBinary))

	serveErr := host.stop()
	_, stopErr := worker.Stop(t.Context())

	require.NoError(t, serveErr)
	require.ErrorIs(t, stopErr, io.EOF)
	_, statErr := os.Stat("/proc/" + strconv.Itoa(pid))
	require.ErrorIs(t, statErr, fs.ErrNotExist)
}

func TestServeFailsWithoutAServerCertificate(t *testing.T) {
	ca := newAuthority(t)
	sup, err := supervisor.New(supervisor.Config{
		WorkerPath:  workerBinary,
		PackagesDir: packagesDir,
		MaxWorkers:  1,
		TLS:         &tls.Config{ClientCAs: ca.pool, MinVersion: tls.VersionTLS13},
	})
	require.NoError(t, err)
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	err = sup.Serve(t.Context(), listener)

	require.ErrorContains(t, err, "open :")
}

func TestNewRefusesAConfigItCannotRunSafely(t *testing.T) {
	ca := newAuthority(t)
	valid := func() supervisor.Config {
		return supervisor.Config{WorkerPath: workerBinary, PackagesDir: packagesDir, MaxWorkers: 1, TLS: ca.serverConfig(t)}
	}
	cases := []struct {
		name   string
		change func(*supervisor.Config)
		err    string
	}{
		{"a relative worker path", func(c *supervisor.Config) { c.WorkerPath = "worker" }, "supervisor: WorkerPath must be an absolute path"},
		{"a relative packages path", func(c *supervisor.Config) { c.PackagesDir = "packages" }, "supervisor: PackagesDir must be an absolute path"},
		{"no worker slots", func(c *supervisor.Config) { c.MaxWorkers = 0 }, "supervisor: MaxWorkers must be 1 or more"},
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

// workspace is a work mount over a temporary directory, with a Word
// document, as the worker's /workspace.
func workspace(t *testing.T) (string, vfs.Files) {
	t.Helper()
	dir := t.TempDir()
	ms, err := vfs.Tree(vfs.At("work", vfs.Local(dir))).WithCodecs(adapters.Codecs()...)(t.Context(), t.Name(), vfs.Request{})
	require.NoError(t, err)
	spec := "<h1>Title</h1><p>text</p>"
	_, err = ms.Apply(t.Context(), "/workspace/work/Spec.docx", vfs.Mutation{Content: &spec})
	require.NoError(t, err)
	return dir, vfs.NewFiles(ms)
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

// host is a running supervisor.
type host struct {
	address string
	// stop ends Serve and returns its error. It runs at cleanup too.
	stop func() error
}

// startSupervisor serves a supervisor on a loopback port. Empty fields of
// config get the real worker, 4 slots and ca's server TLS.
func startSupervisor(t *testing.T, ca *authority, config supervisor.Config) host {
	t.Helper()
	if config.WorkerPath == "" {
		config.WorkerPath = workerBinary
	}
	config.PackagesDir = packagesDir
	if config.MaxWorkers == 0 {
		config.MaxWorkers = 4
	}
	config.TLS = ca.serverConfig(t)
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
	return host{address: listener.Addr().String(), stop: stop}
}

// scriptWorker writes a shell script that takes the place of the worker
// binary: a worker that does not follow the protocol.
func scriptWorker(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker")
	script := "#!/bin/sh\n" + strings.Join(lines, "\n") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o700))
	return path
}

// sendFrame is a shell command that writes body as one control frame on
// the worker's socket (fd 3).
func sendFrame(body string) string {
	header := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	header = append(header, 1)
	var command strings.Builder
	command.WriteString("printf '")
	for _, b := range header {
		fmt.Fprintf(&command, `\%03o`, b)
	}
	command.WriteString(body)
	command.WriteString("' >&3")
	return command.String()
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

// authority is a test certificate authority.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newAuthority(t *testing.T) *authority {
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
	return &authority{cert: cert, key: key, pool: pool}
}

func (a *authority) issue(t *testing.T, template *x509.Certificate) tls.Certificate {
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

// serverConfig is a supervisor TLS config for 127.0.0.1 that trusts
// clients of a.
func (a *authority) serverConfig(t *testing.T) *tls.Config {
	t.Helper()
	certificate := a.issue(t, &x509.Certificate{
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	return &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: a.pool, MinVersion: tls.VersionTLS13}
}

// clientConfig is a client TLS config that trusts servers of trust and has
// a client certificate from issuer.
func clientConfig(t *testing.T, trust, issuer *authority) *tls.Config {
	t.Helper()
	certificate := issuer.issue(t, &x509.Certificate{
		URIs:        []*url.URL{{Scheme: "spiffe", Host: "tacklr.test", Path: "/runtime"}},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	return &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: trust.pool, MinVersion: tls.VersionTLS13}
}
