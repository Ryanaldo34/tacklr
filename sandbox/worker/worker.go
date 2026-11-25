// Package worker is the init of each sandbox microVM that the supervisor
// (package sandbox/supervisor) boots with Firecracker. The guest is the
// sandbox, and the worker runs inside it: it is not trusted by the host.
//
// The worker's command (cmd/tacklr-sandbox-worker) mounts the guest's file
// systems, then /workspace over 9P2000.L on a vsock connection to the host,
// which the supervisor serves from the session's tree: the guest never
// holds a credential. Then it connects to the host's control port and
// calls Serve, which sends ready and runs each script the supervisor sends
// with bash. On stop, or when the host closes the connection, it powers
// the VM off.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
)

// outputDelay is how long a run waits for its output after bash exits. A
// background process that keeps the output open does not hold the run.
const outputDelay = 100 * time.Millisecond

// errNotARequest is the error of a frame from the supervisor that is not a
// request.
var errNotARequest = errors.New("worker: the frame is not a request")

// Runner runs scripts with bash.
type Runner struct {
	// Bash is the path of bash.
	Bash string
	// Dir is the scripts' working directory.
	Dir string
	// Credential is the user and groups the scripts run as. Nil is the
	// worker's own.
	Credential *syscall.Credential
	// Env is the scripts' whole environment.
	Env []string
}

// Run runs script with bash -c and returns its exit code: 128 and the
// signal number when a signal ended it. Each chunk of its stdout and
// stderr goes to output with its frame kind as it comes. The run ends
// when bash exits: what its background processes write later is dropped.
// It returns an error if bash cannot start, or the first error of output,
// which kills bash.
func (r Runner) Run(script string, output func(kind byte, data []byte) error) (int, error) {
	ctx, kill := context.WithCancelCause(context.Background())
	defer kill(nil)
	cmd := exec.CommandContext(ctx, r.Bash, "-c", script) //nolint:gosec // G204: running the script is the point
	cmd.Dir = r.Dir
	cmd.Env = r.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: r.Credential}
	cmd.Stdout = stream{wire.KindStdout, output, kill}
	cmd.Stderr = stream{wire.KindStderr, output, kill}
	cmd.WaitDelay = outputDelay
	err := cmd.Run()
	if cause := context.Cause(ctx); cause != nil {
		return -1, cause
	}
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		if status, _ := exit.Sys().(syscall.WaitStatus); status.Signaled() {
			return 128 + int(status.Signal()), nil
		}
		return exit.ExitCode(), nil
	case errors.Is(err, exec.ErrWaitDelay):
		return cmd.ProcessState.ExitCode(), nil
	}
	return cmd.ProcessState.ExitCode(), err
}

// stream is one output of a run. An error of output kills the run.
type stream struct {
	kind   byte
	output func(byte, []byte) error
	kill   context.CancelCauseFunc
}

func (s stream) Write(p []byte) (int, error) {
	if err := s.output(s.kind, p); err != nil {
		s.kill(err)
		return 0, err
	}
	return len(p), nil
}

// work is a request that the request loop does: the reader answers pings
// itself.
type work struct {
	message wire.Message
	err     error
}

// Serve sends ready, then answers the supervisor's requests on conn until
// it sends stop or closes the connection. Runs go one at a time, with
// runner, and the worker answers pings while one goes on. It returns an
// error if the connection fails or the supervisor sends a frame that is
// not a request.
func Serve(conn io.ReadWriter, runner Runner) error {
	var mu sync.Mutex
	send := func(write func(io.Writer) error) error {
		mu.Lock()
		defer mu.Unlock()
		return write(conn)
	}
	reply := func(m wire.Message) error {
		return send(func(w io.Writer) error { return wire.Write(w, m) })
	}
	if err := reply(wire.Message{Type: wire.TypeReady}); err != nil {
		return err
	}
	requests := make(chan work)
	done := make(chan struct{})
	defer close(done)
	go read(conn, reply, requests, done)
	for request := range requests {
		if request.err != nil {
			return request.err
		}
		if request.message.Type == wire.TypeStop {
			return nil
		}
		m := wire.Message{Type: wire.TypeDone, ID: request.message.ID}
		code, err := runner.Run(request.message.Script, func(kind byte, data []byte) error {
			return send(func(w io.Writer) error { return wire.WriteData(w, kind, data) })
		})
		m.Code = &code
		if err != nil {
			m.Error = err.Error()
		}
		if err := reply(m); err != nil {
			return err
		}
	}
	return nil
}

// read reads the supervisor's frames: it answers pings, and sends stop,
// run and the error that ends the reads to requests. It closes requests at
// the end of the connection, and stops when done closes.
func read(conn io.Reader, reply func(wire.Message) error, requests chan<- work, done <-chan struct{}) {
	for {
		f, err := wire.Read(conn)
		m := f.Message
		switch {
		case errors.Is(err, io.EOF):
			close(requests)
			return
		case err == nil && f.Kind == wire.KindControl && m.Type == wire.TypePing:
			_ = reply(wire.Message{Type: wire.TypePong, ID: m.ID})
			continue
		case err == nil && f.Kind == wire.KindControl && (m.Type == wire.TypeStop || m.Type == wire.TypeRun):
		case err == nil:
			err = fmt.Errorf("%w: kind %d, type %q", errNotARequest, f.Kind, m.Type)
		}
		select {
		case requests <- work{message: m, err: err}:
		case <-done:
			return
		}
		if err != nil {
			return
		}
	}
}
