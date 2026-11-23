// Package sandbox is the session runtime's client for sandboxed workers.
//
// A supervisor (package sandbox/supervisor) runs on each worker host. The
// client opens one HTTP/2 stream over mTLS to it for each worker. The
// worker is a separate process (sandbox/worker, in Rust) in new namespaces,
// under Landlock and a seccomp allow-list. It runs untrusted, agent-written
// code: Run runs a bash script in the worker's workspace.
//
//	client := sandbox.NewClient("worker-host:7443", tlsConfig)
//	worker, err := client.Start(ctx, sessionID, bindings)
//	if err != nil { ... }
//	defer worker.Close()
//	code, err := worker.Run(ctx, "ls work | wc -l", &stdout, &stderr)
//	exit, err := worker.Stop(ctx)
//
// A worker lives as long as its stream. Close ends the stream, and the
// supervisor then kills the worker.
package sandbox

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// maxRefusal is the most bytes of a refusal's body that an error keeps.
const maxRefusal = 1 << 10

// Enforcement is what a worker enforces on itself.
type Enforcement = wire.Enforcement

// Exit is how a worker process ended. Signal SIGSYS means that seccomp
// killed the worker. Code 125 means that the worker could not apply its
// sandbox.
type Exit = wire.Exit

// ErrExited is the error of a request to a worker that has ended.
var ErrExited = errors.New("sandbox: the worker has ended")

// errEmptyScript is the error of Run with an empty script.
var errEmptyScript = errors.New("sandbox: the script is empty")

// errClosed ends the request body when the client closes a worker.
var errClosed = errors.New("sandbox: the client closed the worker")

// ExitError is the error of Start when the worker ends before it is ready.
type ExitError struct {
	Exit Exit
}

func (e *ExitError) Error() string {
	return strings.TrimSpace(fmt.Sprintf("sandbox: the worker ended before it was ready: code %d %s", e.Exit.Code, e.Exit.Signal))
}

// RunError is the error of Run when the worker's runtime could not run the
// script, for a reason other than the script's exit.
type RunError struct {
	Message string
}

func (e *RunError) Error() string {
	return "sandbox: the worker could not run the script: " + e.Message
}

// RefusedError is the error of Start when the supervisor refuses to start a
// worker: 400 when the stream does not start with open, 403 when the client
// is not authorized, 502 when the session's workspace cannot open, 500 when the worker binary
// cannot start.
type RefusedError struct {
	StatusCode int
	Message    string
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("sandbox: the supervisor refused the worker: %d %s", e.StatusCode, e.Message)
}

// Client starts workers on one supervisor.
type Client struct {
	// Refresh, if set, gets a new credential for one of a worker's
	// bindings when the supervisor asks: the binding's token is near
	// expiry, or its source refused it. It gets the worker's session and
	// the binding without its token. Nil refuses each refresh. Set it
	// before Start.
	Refresh func(ctx context.Context, session string, binding mount.Binding) (mount.Credential, error)

	url  *url.URL
	http *http.Client
}

// NewClient returns a client for the supervisor at address (host:port).
// config must have the client certificate and the roots that verify the
// supervisor. The client requires TLS 1.3 and HTTP/2.
func NewClient(address string, config *tls.Config) *Client {
	clientTLS := config.Clone()
	clientTLS.MinVersion = tls.VersionTLS13
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	return &Client{
		url: &url.URL{Scheme: "https", Host: address, Path: wire.StreamPath},
		http: &http.Client{Transport: &http.Transport{
			TLSClientConfig: clientTLS,
			Protocols:       protocols,
		}},
	}
}

// Start starts a worker for session and returns when it is ready. The
// supervisor opens the session's tree with bindings (the turn's mounts and
// their credentials), and the worker sees it at /workspace. While it does,
// Start answers its token requests with Refresh. ctx limits only the wait.
// The worker lives until Close, or until it ends.
func (c *Client) Start(ctx context.Context, session string, bindings []mount.Binding) (*Worker, error) {
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopWaiting := context.AfterFunc(ctx, cancel)
	defer stopWaiting()
	var open bytes.Buffer
	if err := wire.Write(&open, wire.Message{Type: wire.TypeOpen, Session: session, Bindings: bindings}); err != nil {
		cancel()
		return nil, err
	}
	requests, send := io.Pipe()
	request := (&http.Request{
		Method: http.MethodPost,
		URL:    c.url,
		Header: http.Header{},
		Body: struct {
			io.Reader
			io.Closer
		}{io.MultiReader(&open, requests), requests},
	}).WithContext(streamCtx)
	response, err := c.http.Do(request)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("sandbox: start a worker: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, maxRefusal))
		_ = response.Body.Close()
		cancel()
		return nil, &RefusedError{StatusCode: response.StatusCode, Message: strings.TrimSpace(string(message))}
	}
	w := &Worker{
		session: session,
		refresh: c.Refresh,
		cancel:  cancel,
		send:    send,
		pongs:   map[uint64]chan struct{}{},
		done:    make(chan struct{}),
	}
	for {
		frame, err := wire.Read(response.Body)
		m := frame.Message
		switch {
		case err == nil && m.Type == wire.TypeToken && m.Binding != nil:
			w.answerToken(streamCtx, m.ID, *m.Binding)
			continue
		case err == nil && m.Type == wire.TypeReady && m.Enforcement != nil:
			w.enforcement = *m.Enforcement
			go w.read(streamCtx, response.Body)
			return w, nil
		case err == nil:
			err = startError(m)
		}
		_ = response.Body.Close()
		cancel()
		return nil, err
	}
}

// startError is the error of a message before ready that is not a token
// request.
func startError(m wire.Message) error {
	switch {
	case m.Type == wire.TypeExit && m.Exit != nil:
		return &ExitError{Exit: *m.Exit}
	case m.Type == wire.TypeRefused && m.Code != nil:
		return &RefusedError{StatusCode: *m.Code, Message: m.Error}
	}
	return fmt.Errorf("sandbox: the supervisor sent %q before ready", m.Type)
}

// Worker is one running worker.
type Worker struct {
	enforcement Enforcement
	session     string
	refresh     func(ctx context.Context, session string, binding mount.Binding) (mount.Credential, error)
	cancel      context.CancelFunc

	// running lets one run go at a time.
	running sync.Mutex

	// mu guards send, nextID, pongs and run.
	mu     sync.Mutex
	send   *io.PipeWriter
	nextID uint64
	pongs  map[uint64]chan struct{}
	run    *run

	// done closes when the stream has ended. exit and err are set before.
	done chan struct{}
	exit *Exit
	err  error
}

// Enforcement is what the worker enforces on itself.
func (w *Worker) Enforcement() Enforcement {
	return w.enforcement
}

// Ping asks the worker to answer and waits for the answer. It returns
// ErrExited if the worker ends first.
func (w *Worker) Ping(ctx context.Context) error {
	w.mu.Lock()
	w.nextID++
	id := w.nextID
	pong := make(chan struct{})
	w.pongs[id] = pong
	_ = wire.Write(w.send, wire.Message{Type: wire.TypePing, ID: id})
	w.mu.Unlock()
	select {
	case <-pong:
		return nil
	case <-w.done:
		return ErrExited
	case <-ctx.Done():
		w.mu.Lock()
		delete(w.pongs, id)
		w.mu.Unlock()
		return ctx.Err()
	}
}

// Stop asks the worker to exit and returns its exit. A worker that does not
// exit in the supervisor's stop timeout is killed (SIGKILL). If the worker
// has already ended, Stop returns how it ended.
func (w *Worker) Stop(ctx context.Context) (Exit, error) {
	w.mu.Lock()
	_ = wire.Write(w.send, wire.Message{Type: wire.TypeStop})
	w.mu.Unlock()
	select {
	case <-w.done:
		if w.exit == nil {
			return Exit{}, w.err
		}
		return *w.exit, nil
	case <-ctx.Done():
		return Exit{}, ctx.Err()
	}
}

// Run runs script with bash in the worker's /workspace and returns its exit
// code. The script's stdout and stderr go to stdout and stderr as they come.
// Runs on one worker go one at a time.
//
// Run returns a *RunError if the worker's runtime could not run the script,
// and ErrExited if the worker ends first. A run cannot be stopped part way:
// if ctx ends first, Run closes the worker and returns ctx.Err().
func (w *Worker) Run(ctx context.Context, script string, stdout, stderr io.Writer) (int, error) {
	if script == "" {
		return -1, errEmptyScript
	}
	w.running.Lock()
	defer w.running.Unlock()
	w.mu.Lock()
	w.nextID++
	current := &run{id: w.nextID, stdout: stdout, stderr: stderr, done: make(chan wire.Message, 1)}
	w.run = current
	_ = wire.Write(w.send, wire.Message{Type: wire.TypeRun, ID: current.id, Script: script})
	w.mu.Unlock()
	select {
	case done := <-current.done:
		if done.Error != "" {
			return -1, &RunError{Message: done.Error}
		}
		return *done.Code, nil
	case <-w.done:
		return -1, ErrExited
	case <-ctx.Done():
		w.Close()
		return -1, ctx.Err()
	}
}

// run is the run in progress on a worker.
type run struct {
	id             uint64
	stdout, stderr io.Writer
	done           chan wire.Message
}

// Close ends the stream, so the supervisor kills the worker, and waits for
// the stream to end. Call it when done with the worker, also after Stop.
func (w *Worker) Close() {
	_ = w.send.CloseWithError(errClosed)
	w.cancel()
	<-w.done
}

// read takes the supervisor's messages until the stream ends. It answers
// each token request on its own, so output keeps coming while a refresh
// waits.
func (w *Worker) read(ctx context.Context, body io.ReadCloser) {
	defer close(w.done)
	defer w.cancel()
	defer body.Close()
	for {
		frame, err := wire.Read(body)
		if err != nil {
			w.end(fmt.Errorf("sandbox: the stream ended before the worker's exit: %w", err))
			return
		}
		w.mu.Lock()
		current := w.run
		w.mu.Unlock()
		m := frame.Message
		switch {
		case frame.Kind == wire.KindStdout:
			_, _ = current.stdout.Write(frame.Data)
		case frame.Kind == wire.KindStderr:
			_, _ = current.stderr.Write(frame.Data)
		case m.Type == wire.TypeDone:
			w.mu.Lock()
			w.run = nil
			w.mu.Unlock()
			current.done <- m
		case m.Type == wire.TypePong:
			w.mu.Lock()
			pong := w.pongs[m.ID]
			delete(w.pongs, m.ID)
			w.mu.Unlock()
			if pong != nil {
				close(pong)
			}
		case m.Type == wire.TypeToken && m.Binding != nil:
			go w.answerToken(ctx, m.ID, *m.Binding)
		case m.Type == wire.TypeExit:
			w.exit = m.Exit
			w.end(ErrExited)
			return
		}
	}
}

// answerToken replies to the supervisor's token request id with a new
// credential of binding from Refresh, or with why there is none.
func (w *Worker) answerToken(ctx context.Context, id uint64, binding mount.Binding) {
	reply := wire.Message{Type: wire.TypeToken, ID: id}
	if w.refresh == nil {
		reply.Error = "the session runtime does not refresh tokens"
	} else if credential, err := w.refresh(ctx, w.session, binding); err != nil {
		reply.Error = err.Error()
	} else {
		reply.Credential = &credential
	}
	w.mu.Lock()
	_ = wire.Write(w.send, reply)
	w.mu.Unlock()
}

// end records err and makes every later send fail.
func (w *Worker) end(err error) {
	w.err = err
	_ = w.send.CloseWithError(ErrExited)
}
