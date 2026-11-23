// Package supervisor runs sandboxed workers on one host for the session
// runtime (package sandbox is the client).
//
// A client opens one HTTP/2 stream over mTLS for each worker, and names the
// session and its bindings first. The supervisor opens the session's tree
// with Config.Workspace, then starts the worker binary (sandbox/worker) in
// new user, pid, mount, network, IPC, UTS and cgroup namespaces, with the
// worker's end of a Unix socket pair as fd 3. The worker applies Landlock
// and seccomp to itself and sends ready. Then the supervisor relays
// messages between the stream and the socket, and serves the worker's file
// requests from the tree itself: only a run's output and its done reach the
// client, and no credential reaches the worker.
//
// The worker is not trusted. The supervisor reads at most wire.MaxBody per
// frame from it, accepts only ready (once, first), pong, and the output,
// file requests and done of a run the client started, and sends on only
// the fields of those messages. If the worker sends anything else, the
// supervisor kills it. A file request reaches only the session's tree,
// through vfs.Files, and only the files that worker opened.
//
// A worker lives as long as its stream. When the stream ends, the
// supervisor kills the worker. A worker that closes its socket, or gets a
// stop request, has the stop timeout to exit before the supervisor kills
// it. When the worker ends, the supervisor sends its exit on the stream and
// ends the stream. A worker that seccomp killed
// has the signal SIGSYS. A worker that could not apply its sandbox has the
// exit code 125. That includes a worker that could not load its packages.
package supervisor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/vfs"
)

const (
	defaultStopTimeout = 5 * time.Second
	readHeaderTimeout  = 10 * time.Second
)

// Config configures a Supervisor.
type Config struct {
	// WorkerPath is the absolute path of the worker binary.
	WorkerPath string
	// PackagesDir is the absolute path of the worker's packages: the
	// pinned WASIX packages and their compiled modules (make
	// sandbox-packages). The worker reads it before its jail applies.
	PackagesDir string
	// MaxWorkers is the largest number of workers that run at one time. A
	// request for one more worker gets 503 Service Unavailable.
	MaxWorkers int
	// TLS is the server's TLS configuration. It must have the server
	// certificate (Certificates or GetCertificate) and ClientCAs, the roots
	// that verify client certificates. The supervisor sets TLS 1.3 as the
	// minimum version and requires a verified client certificate.
	// GetConfigForClient is refused, because it could replace those
	// settings.
	TLS *tls.Config
	// Authorize, if set, decides if a verified client may start a worker.
	// It gets the client's leaf certificate. An error gets 403 Forbidden.
	// If it is nil, every client that ClientCAs verifies may start workers.
	Authorize func(client *x509.Certificate) error
	// Workspace opens a session's tree for its workers: the host's tree, as
	// its session runtime builds it, with the bindings the client sends.
	// The supervisor serves the worker's file requests from it; the worker
	// never holds a credential. Nil is an empty tree.
	Workspace vfs.OpenVFS
	// StopTimeout is the time a worker has to exit after a stop request.
	// After it, the supervisor kills the worker. Zero is 5 seconds.
	StopTimeout time.Duration
}

// Supervisor starts and watches the workers of one host.
type Supervisor struct {
	config Config
	tls    *tls.Config
	slots  chan struct{}
}

// New checks config and returns a Supervisor.
func New(config Config) (*Supervisor, error) {
	switch {
	case !filepath.IsAbs(config.WorkerPath):
		return nil, errors.New("supervisor: WorkerPath must be an absolute path")
	case !filepath.IsAbs(config.PackagesDir):
		return nil, errors.New("supervisor: PackagesDir must be an absolute path")
	case config.MaxWorkers < 1:
		return nil, errors.New("supervisor: MaxWorkers must be 1 or more")
	case config.TLS == nil:
		return nil, errors.New("supervisor: TLS is required")
	case config.TLS.ClientCAs == nil:
		return nil, errors.New("supervisor: TLS.ClientCAs is required to verify clients")
	case config.TLS.GetConfigForClient != nil:
		return nil, errors.New("supervisor: TLS.GetConfigForClient is not supported")
	}
	if config.StopTimeout == 0 {
		config.StopTimeout = defaultStopTimeout
	}
	serverTLS := config.TLS.Clone()
	serverTLS.MinVersion = tls.VersionTLS13
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	return &Supervisor{
		config: config,
		tls:    serverTLS,
		slots:  make(chan struct{}, config.MaxWorkers),
	}, nil
}

// Serve accepts mTLS connections on listener until ctx ends. When ctx
// ends, Serve kills every worker, waits until each stream has ended, and
// returns nil.
func (s *Supervisor) Serve(ctx context.Context, listener net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+wire.StreamPath, s.serveWorker)
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	server := &http.Server{
		Handler:           mux,
		TLSConfig:         s.tls,
		Protocols:         protocols,
		ReadHeaderTimeout: readHeaderTimeout,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	shutdown := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = server.Shutdown(context.WithoutCancel(ctx))
		close(shutdown)
	})
	err := server.ServeTLS(listener, "", "")
	if stop() {
		return err
	}
	<-shutdown
	return nil
}

func (s *Supervisor) serveWorker(w http.ResponseWriter, r *http.Request) {
	if s.config.Authorize != nil {
		if err := s.config.Authorize(r.TLS.PeerCertificates[0]); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		http.Error(w, "the supervisor runs its largest number of workers", http.StatusServiceUnavailable)
		return
	}
	open, err := wire.Read(r.Body)
	if err != nil || open.Message.Type != wire.TypeOpen {
		http.Error(w, "the stream must start with open", http.StatusBadRequest)
		return
	}
	var files vfs.Files
	if s.config.Workspace != nil {
		ms, err := s.config.Workspace(r.Context(), open.Message.Session, vfs.Request{Bindings: open.Message.Bindings})
		if err != nil {
			http.Error(w, "the workspace could not open: "+err.Error(), http.StatusBadGateway)
			return
		}
		files = vfs.NewFiles(ms)
	}
	p, err := startProcess(s.config.WorkerPath, s.config.PackagesDir)
	if err != nil {
		http.Error(w, "the worker could not start", http.StatusInternalServerError)
		return
	}
	defer p.close()
	w.WriteHeader(http.StatusOK)
	stream := http.NewResponseController(w)
	_ = stream.Flush()
	s.relay(r.Context(), r.Body, func(f wire.Frame) {
		if writeFrame(w, f) == nil {
			_ = stream.Flush()
		}
	}, p, files)
}

// relay moves frames between the client's stream and the worker until one
// of them ends, and serves the worker's file requests from files. When it
// ends, the files the worker left open are written back.
func (s *Supervisor) relay(ctx context.Context, body io.Reader, send func(wire.Frame), p *process, files vfs.Files) {
	done := make(chan struct{})
	defer close(done)
	client := receive(body, done)
	worker := receive(p.conn, done)
	var writes sync.Mutex
	toWorker := func(f wire.Frame) {
		writes.Lock()
		defer writes.Unlock()
		_ = writeFrame(p.conn, f)
	}
	requests, served := make(chan []byte), make(chan struct{})
	go func() {
		defer close(served)
		fs := &server{files: files, open: map[uint64]*vfs.OpenFile{}}
		_ = fs.serve(ctx, requests, func(reply wire.FSReply, data []byte) {
			writes.Lock()
			defer writes.Unlock()
			_ = wire.WriteFS(p.conn, reply, data)
		})
	}()
	defer func() {
		close(requests)
		<-served
	}()
	// The file server stops before the stream ends only on a request that
	// is not valid.
	broken := served
	var stopTimeout <-chan time.Time
	startStopTimeout := func() {
		if stopTimeout == nil {
			stopTimeout = time.After(s.config.StopTimeout)
		}
	}
	ready, runs := false, 0
	for {
		select {
		case r := <-client:
			forward, valid := fromClient(r.frame)
			if r.err != nil || !valid {
				return
			}
			switch forward.Message.Type {
			case wire.TypeStop:
				startStopTimeout()
			case wire.TypeRun:
				runs++
			}
			toWorker(forward)
		case r := <-worker:
			forward, valid := fromWorker(r.frame, ready, runs)
			switch {
			case errors.Is(r.err, io.EOF):
				worker = nil
				startStopTimeout()
			case r.err != nil || !valid:
				p.kill()
				worker = nil
			case forward.Kind == wire.KindFS:
				select {
				case requests <- forward.Data:
				case <-served:
				}
			default:
				ready = true
				if forward.Message.Type == wire.TypeDone {
					runs--
				}
				send(forward)
			}
		case <-broken:
			broken = nil
			p.kill()
		case <-stopTimeout:
			p.kill()
		case <-p.exited:
			send(wire.Frame{Kind: wire.KindControl, Message: wire.Message{Type: wire.TypeExit, Exit: p.exit()}})
			return
		case <-ctx.Done():
			return
		}
	}
}

// fromClient returns the frame to send to the worker for a frame from the
// client, and false if the client may not send f.
func fromClient(f wire.Frame) (wire.Frame, bool) {
	m := f.Message
	control := func(m wire.Message) (wire.Frame, bool) {
		return wire.Frame{Kind: wire.KindControl, Message: m}, true
	}
	switch {
	case m.Type == wire.TypePing && m.ID != 0:
		return control(wire.Message{Type: wire.TypePing, ID: m.ID})
	case m.Type == wire.TypeStop:
		return control(wire.Message{Type: wire.TypeStop})
	case m.Type == wire.TypeRun && m.ID != 0 && m.Script != "":
		return control(wire.Message{Type: wire.TypeRun, ID: m.ID, Script: m.Script})
	}
	return wire.Frame{}, false
}

// writeFrame writes f as its kind.
func writeFrame(w io.Writer, f wire.Frame) error {
	if f.Kind == wire.KindControl {
		return wire.Write(w, f.Message)
	}
	return wire.WriteData(w, f.Kind, f.Data)
}

// fromWorker returns the frame to send to the client for a frame from the
// worker, and false if the worker may not send f now. ready tells if the
// worker has sent ready, and runs how many runs it has not finished.
func fromWorker(f wire.Frame, ready bool, runs int) (wire.Frame, bool) {
	m := f.Message
	control := func(m wire.Message) (wire.Frame, bool) {
		return wire.Frame{Kind: wire.KindControl, Message: m}, true
	}
	switch {
	case f.Kind != wire.KindControl:
		return f, ready && runs > 0
	case m.Type == wire.TypeReady && !ready && m.Enforcement != nil:
		return control(wire.Message{Type: wire.TypeReady, Enforcement: m.Enforcement})
	case m.Type == wire.TypePong && ready && m.ID != 0:
		return control(wire.Message{Type: wire.TypePong, ID: m.ID})
	case m.Type == wire.TypeDone && runs > 0 && m.ID != 0 && m.Code != nil:
		return control(wire.Message{Type: wire.TypeDone, ID: m.ID, Code: m.Code, Error: m.Error})
	}
	return wire.Frame{}, false
}

// received is one result of a read: a frame, or the error that ended the
// reads.
type received struct {
	frame wire.Frame
	err   error
}

// receive reads messages from r and sends each result on the channel. It
// stops after the first error, or when done closes.
func receive(r io.Reader, done <-chan struct{}) <-chan received {
	results := make(chan received)
	go func() {
		for {
			f, err := wire.Read(r)
			select {
			case results <- received{frame: f, err: err}:
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return results
}
