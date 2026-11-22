// Package sandbox is the session runtime's client for sandboxed workers.
//
// A supervisor (package sandbox/supervisor) runs on each worker host. The
// client opens one HTTP/2 stream over mTLS to it for each worker. The
// worker is a separate process (sandbox/worker, in Rust) in new namespaces,
// under Landlock and a seccomp allow-list. It runs untrusted, agent-written
// code. In this version it has only its life cycle: ready, ping and stop.
//
//	client := sandbox.NewClient("worker-host:7443", tlsConfig)
//	worker, err := client.Start(ctx)
//	if err != nil { ... }
//	defer worker.Close()
//	exit, err := worker.Stop(ctx)
//
// A worker lives as long as its stream. Close ends the stream, and the
// supervisor then kills the worker.
package sandbox

import (
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

// errClosed ends the request body when the client closes a worker.
var errClosed = errors.New("sandbox: the client closed the worker")

// ExitError is the error of Start when the worker ends before it is ready.
type ExitError struct {
	Exit Exit
}

func (e *ExitError) Error() string {
	return strings.TrimSpace(fmt.Sprintf("sandbox: the worker ended before it was ready: code %d %s", e.Exit.Code, e.Exit.Signal))
}

// RefusedError is the error of Start when the supervisor refuses to start a
// worker: 403 when the client is not authorized, 503 when the host runs its
// largest number of workers, 500 when the worker binary cannot start.
type RefusedError struct {
	StatusCode int
	Message    string
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("sandbox: the supervisor refused the worker: %d %s", e.StatusCode, e.Message)
}

// Client starts workers on one supervisor.
type Client struct {
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

// Start starts a worker and returns when it is ready. ctx limits only the
// wait. The worker lives until Close, or until it ends.
func (c *Client) Start(ctx context.Context) (*Worker, error) {
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopWaiting := context.AfterFunc(ctx, cancel)
	defer stopWaiting()
	requests, send := io.Pipe()
	request := (&http.Request{
		Method: http.MethodPost,
		URL:    c.url,
		Header: http.Header{},
		Body:   requests,
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
	first, err := wire.Read(response.Body)
	if err == nil && (first.Type != wire.TypeReady || first.Enforcement == nil) {
		err = startError(first)
	}
	if err != nil {
		_ = response.Body.Close()
		cancel()
		return nil, err
	}
	w := &Worker{
		enforcement: *first.Enforcement,
		cancel:      cancel,
		send:        send,
		pongs:       map[uint64]chan struct{}{},
		done:        make(chan struct{}),
	}
	go w.read(response.Body)
	return w, nil
}

// startError is the error of a first message that is not ready.
func startError(first wire.Message) error {
	if first.Type == wire.TypeExit && first.Exit != nil {
		return &ExitError{Exit: *first.Exit}
	}
	return fmt.Errorf("sandbox: the supervisor sent %q before ready", first.Type)
}

// Worker is one running worker.
type Worker struct {
	enforcement Enforcement
	cancel      context.CancelFunc

	// mu guards send, nextID and pongs.
	mu     sync.Mutex
	send   *io.PipeWriter
	nextID uint64
	pongs  map[uint64]chan struct{}

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

// Close ends the stream, so the supervisor kills the worker, and waits for
// the stream to end. Call it when done with the worker, also after Stop.
func (w *Worker) Close() {
	_ = w.send.CloseWithError(errClosed)
	w.cancel()
	<-w.done
}

// read takes the supervisor's messages until the stream ends.
func (w *Worker) read(body io.ReadCloser) {
	defer close(w.done)
	defer w.cancel()
	defer body.Close()
	for {
		m, err := wire.Read(body)
		if err != nil {
			w.end(fmt.Errorf("sandbox: the stream ended before the worker's exit: %w", err))
			return
		}
		switch m.Type {
		case wire.TypePong:
			w.mu.Lock()
			pong := w.pongs[m.ID]
			delete(w.pongs, m.ID)
			w.mu.Unlock()
			if pong != nil {
				close(pong)
			}
		case wire.TypeExit:
			w.exit = m.Exit
			w.end(ErrExited)
			return
		}
	}
}

// end records err and makes every later send fail.
func (w *Worker) end(err error) {
	w.err = err
	_ = w.send.CloseWithError(ErrExited)
}
