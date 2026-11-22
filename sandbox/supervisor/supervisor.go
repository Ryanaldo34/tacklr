// Package supervisor runs sandboxed workers on one host for the session
// runtime (package sandbox is the client).
//
// A client opens one HTTP/2 stream over mTLS for each worker. The
// supervisor starts the worker binary (sandbox/worker) in new user, pid,
// mount, network, IPC, UTS and cgroup namespaces, with the worker's end of a
// Unix socket pair as fd 3. The worker applies Landlock and seccomp to
// itself and sends ready. Then the supervisor relays messages between the
// stream and the socket.
//
// The worker is not trusted. The supervisor reads at most wire.MaxBody per
// frame from it, accepts only ready (once, first) and pong, and sends on
// only the fields of those messages. If the worker sends anything else, the
// supervisor kills it.
//
// A worker lives as long as its stream. When the stream ends, the
// supervisor kills the worker. A worker that closes its socket, or gets a
// stop request, has the stop timeout to exit before the supervisor kills
// it. When the worker ends, the supervisor sends its exit on the stream and
// ends the stream. A worker that seccomp killed
// has the signal SIGSYS. A worker that could not apply its sandbox has the
// exit code 125.
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
	"time"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
)

const (
	defaultStopTimeout = 5 * time.Second
	readHeaderTimeout  = 10 * time.Second
)

// Config configures a Supervisor.
type Config struct {
	// WorkerPath is the absolute path of the worker binary.
	WorkerPath string
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
	p, err := startProcess(s.config.WorkerPath)
	if err != nil {
		http.Error(w, "the worker could not start", http.StatusInternalServerError)
		return
	}
	defer p.close()
	w.WriteHeader(http.StatusOK)
	stream := http.NewResponseController(w)
	_ = stream.Flush()
	s.relay(r.Context(), r.Body, func(m wire.Message) {
		if wire.Write(w, m) == nil {
			_ = stream.Flush()
		}
	}, p)
}

// relay moves messages between the client's stream and the worker until
// one of them ends.
func (s *Supervisor) relay(ctx context.Context, body io.Reader, send func(wire.Message), p *process) {
	done := make(chan struct{})
	defer close(done)
	client := receive(body, done)
	worker := receive(p.conn, done)
	var stopTimeout <-chan time.Time
	startStopTimeout := func() {
		if stopTimeout == nil {
			stopTimeout = time.After(s.config.StopTimeout)
		}
	}
	ready := false
	for {
		select {
		case r := <-client:
			forward, valid := fromClient(r.message)
			if r.err != nil || !valid {
				return
			}
			if forward.Type == wire.TypeStop {
				startStopTimeout()
			}
			_ = wire.Write(p.conn, forward)
		case r := <-worker:
			forward, valid := fromWorker(r.message, ready)
			switch {
			case errors.Is(r.err, io.EOF):
				worker = nil
				startStopTimeout()
			case r.err != nil || !valid:
				p.kill()
				worker = nil
			default:
				ready = true
				send(forward)
			}
		case <-stopTimeout:
			p.kill()
		case <-p.exited:
			send(wire.Message{Type: wire.TypeExit, Exit: p.exit()})
			return
		case <-ctx.Done():
			return
		}
	}
}

// fromClient returns the message to send to the worker for a message from
// the client, and false if the client may not send m.
func fromClient(m wire.Message) (wire.Message, bool) {
	switch {
	case m.Type == wire.TypePing && m.ID != 0:
		return wire.Message{Type: wire.TypePing, ID: m.ID}, true
	case m.Type == wire.TypeStop:
		return wire.Message{Type: wire.TypeStop}, true
	}
	return wire.Message{}, false
}

// fromWorker returns the message to send to the client for a message from
// the worker, and false if the worker may not send m now. ready tells if the
// worker has sent ready.
func fromWorker(m wire.Message, ready bool) (wire.Message, bool) {
	switch {
	case m.Type == wire.TypeReady && !ready && m.Enforcement != nil:
		return wire.Message{Type: wire.TypeReady, Enforcement: m.Enforcement}, true
	case m.Type == wire.TypePong && ready && m.ID != 0:
		return wire.Message{Type: wire.TypePong, ID: m.ID}, true
	}
	return wire.Message{}, false
}

// received is one result of a read: a message, or the error that ended
// the reads.
type received struct {
	message wire.Message
	err     error
}

// receive reads messages from r and sends each result on the channel. It
// stops after the first error, or when done closes.
func receive(r io.Reader, done <-chan struct{}) <-chan received {
	results := make(chan received)
	go func() {
		for {
			m, err := wire.Read(r)
			select {
			case results <- received{message: m, err: err}:
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
