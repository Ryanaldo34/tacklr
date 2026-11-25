// Package supervisor runs sandboxed workers on one host for the session
// runtime (package sandbox is the client). Each worker is a Firecracker
// microVM.
//
// A client opens one HTTP/2 stream over mTLS for each worker, and names the
// session and its bindings first. The supervisor opens the session's tree
// with Config.Workspace, then boots a VM: Firecracker in new user, pid,
// mount, network, IPC, UTS and cgroup namespaces, with the guest kernel,
// the read-only root file system image, no network device, and a vsock
// device. The guest's init, the worker (sandbox/worker), mounts
// /workspace over 9P2000.L on one vsock connection, which the supervisor
// serves from the tree itself, and sends ready on another. Then the
// supervisor relays control frames between the stream and the guest: only
// a run's output and its done reach the client, and no credential reaches
// the VM. When a binding's token is near expiry or its source refuses it,
// the supervisor asks the client for a new credential on the stream
// (vfs.Request.Refresh).
//
// The guest is not trusted. The supervisor reads at most wire.MaxBody per
// frame from it, accepts only ready (once, first), pong, and the output
// and done of a run the client started, and sends on only the fields of
// those messages. If the guest sends anything else, the supervisor kills
// the VM. A 9P request reaches only the session's tree, through vfs.Files.
//
// A worker lives as long as its stream. When the stream ends, the
// supervisor kills the VM. A guest that closes its control connection, or
// gets a stop request, has the stop timeout to power off before the
// supervisor kills the VM. When the VM ends, the supervisor sends
// Firecracker's exit on the stream and ends the stream.
package supervisor

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

const (
	defaultStopTimeout = 5 * time.Second
	defaultBootTimeout = 10 * time.Second
	defaultVCPUs       = 1
	defaultMemoryMiB   = 512
	readHeaderTimeout  = 10 * time.Second
)

// Config configures a Supervisor.
type Config struct {
	// Firecracker is the absolute path of the firecracker binary. The
	// supervisor's user must be able to open /dev/kvm.
	Firecracker string
	// Kernel is the absolute path of the guest kernel (make sandbox-image):
	// it has 9P and vsock built in.
	Kernel string
	// Rootfs is the absolute path of the guest's root file system image
	// (make sandbox-image): ext4 with the worker as its init. Every VM
	// opens it read-only.
	Rootfs string
	// VCPUs is the vCPU count of each VM. Zero is 1.
	VCPUs int
	// MemoryMiB is the memory of each VM. Zero is 512.
	MemoryMiB int
	// Init, if set, is the guest's first process and its arguments, in
	// place of the worker. No argument may have a quote or a newline.
	Init []string
	// Log, if set, gets Firecracker's log and each guest's console.
	Log io.Writer
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
	// The supervisor serves the guest's /workspace from it; the guest
	// never holds a credential. The request's Refresh asks the client for a
	// new credential of a binding while the stream is open: build each
	// binding's TokenHolder with Request.Holder. Nil is an empty tree.
	Workspace vfs.OpenVFS
	// BootTimeout is the time a VM has to boot and connect. After it, the
	// supervisor kills the VM. Zero is 10 seconds.
	BootTimeout time.Duration
	// StopTimeout is the time a worker has to exit after a stop request.
	// After it, the supervisor kills the VM. Zero is 5 seconds.
	StopTimeout time.Duration
}

// Supervisor starts and watches the workers of one host.
type Supervisor struct {
	config  Config
	machine machine
	tls     *tls.Config
}

// enforcement is how a supervisor isolates each worker.
var enforcement = wire.Enforcement{Hypervisor: "firecracker"}

// New checks config and returns a Supervisor.
func New(config Config) (*Supervisor, error) {
	switch {
	case !filepath.IsAbs(config.Firecracker):
		return nil, errors.New("supervisor: Firecracker must be an absolute path")
	case !filepath.IsAbs(config.Kernel):
		return nil, errors.New("supervisor: Kernel must be an absolute path")
	case !filepath.IsAbs(config.Rootfs):
		return nil, errors.New("supervisor: Rootfs must be an absolute path")
	case config.TLS == nil:
		return nil, errors.New("supervisor: TLS is required")
	case config.TLS.ClientCAs == nil:
		return nil, errors.New("supervisor: TLS.ClientCAs is required to verify clients")
	case config.TLS.GetConfigForClient != nil:
		return nil, errors.New("supervisor: TLS.GetConfigForClient is not supported")
	}
	m := machine{
		firecracker: config.Firecracker,
		kernel:      config.Kernel,
		rootfs:      config.Rootfs,
		vcpus:       cmp.Or(config.VCPUs, defaultVCPUs),
		memoryMiB:   cmp.Or(config.MemoryMiB, defaultMemoryMiB),
		init:        config.Init,
		log:         config.Log,
	}
	if _, err := m.bootArgs(); err != nil {
		return nil, err
	}
	config.StopTimeout = cmp.Or(config.StopTimeout, defaultStopTimeout)
	config.BootTimeout = cmp.Or(config.BootTimeout, defaultBootTimeout)
	if config.Workspace == nil {
		config.Workspace = vfs.Tree()
	}
	serverTLS := config.TLS.Clone()
	serverTLS.MinVersion = tls.VersionTLS13
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	return &Supervisor{config: config, machine: m, tls: serverTLS}, nil
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
	open, err := wire.Read(r.Body)
	if err != nil || open.Message.Type != wire.TypeOpen {
		http.Error(w, "the stream must start with open", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	stream := http.NewResponseController(w)
	_ = stream.Flush()
	var sends sync.Mutex
	send := func(f wire.Frame) {
		sends.Lock()
		defer sends.Unlock()
		if writeFrame(w, f) == nil {
			_ = stream.Flush()
		}
	}
	refuse := func(status int, message string) {
		send(wire.Frame{Kind: wire.KindControl, Message: wire.Message{Type: wire.TypeRefused, Code: &status, Error: message}})
	}
	tokens := newTokens(send)
	done := make(chan struct{})
	defer close(done)
	client := receive(r.Body, done, tokens.deliver)
	ms, err := s.config.Workspace(r.Context(), open.Message.Session, vfs.Request{Bindings: open.Message.Bindings, Refresh: tokens.refresh})
	if err != nil {
		refuse(http.StatusBadGateway, "the workspace could not open: "+err.Error())
		return
	}
	p, err := startProcess(s.machine)
	if err != nil {
		refuse(http.StatusInternalServerError, "the worker could not start")
		return
	}
	defer p.close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		if conn, err := p.accept(p.workspace, s.config.BootTimeout); err == nil {
			serveWorkspace(r.Context(), vfs.NewFiles(ms), conn)
		}
	}()
	defer func() {
		p.kill()
		<-served
	}()
	if p.conn, err = p.accept(p.control, s.config.BootTimeout); err != nil {
		p.kill()
		<-p.exited
		send(wire.Frame{Kind: wire.KindControl, Message: wire.Message{Type: wire.TypeExit, Exit: p.exit()}})
		return
	}
	s.relay(r.Context(), client, send, p)
}

// tokens asks the client of one stream for new credentials of its
// bindings, and routes the client's replies.
type tokens struct {
	mu      sync.Mutex
	send    func(wire.Frame)
	nextID  uint64
	pending map[uint64]chan wire.Message
}

// newTokens asks over send, which writes a frame to the client.
func newTokens(send func(wire.Frame)) *tokens {
	return &tokens{send: send, pending: map[uint64]chan wire.Message{}}
}

// refresh asks the client for a new credential of b and waits for the
// reply, or for ctx, which ends with the stream. b goes without its token.
func (t *tokens) refresh(ctx context.Context, b mount.Binding) (mount.Credential, error) {
	b.Auth = mount.Credential{}
	t.mu.Lock()
	t.nextID++
	id := t.nextID
	reply := make(chan wire.Message, 1)
	t.pending[id] = reply
	t.send(wire.Frame{Kind: wire.KindControl, Message: wire.Message{Type: wire.TypeToken, ID: id, Binding: &b}})
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		delete(t.pending, id)
		t.mu.Unlock()
	}()
	select {
	case m := <-reply:
		if m.Credential == nil {
			return mount.Credential{}, fmt.Errorf("supervisor: the client did not refresh the token of %s: %s", b.Provider, m.Error)
		}
		return *m.Credential, nil
	case <-ctx.Done():
		return mount.Credential{}, ctx.Err()
	}
}

// deliver hands the client's reply m to the refresh that waits for it.
func (t *tokens) deliver(m wire.Message) {
	t.mu.Lock()
	reply := t.pending[m.ID]
	delete(t.pending, m.ID)
	t.mu.Unlock()
	if reply != nil {
		reply <- m
	}
}

// relay moves frames between the client's stream and the guest until one
// of them ends.
func (s *Supervisor) relay(ctx context.Context, client <-chan received, send func(wire.Frame), p *process) {
	done := make(chan struct{})
	defer close(done)
	worker := receive(p.conn, done, nil)
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
			_ = writeFrame(p.conn, forward)
		case r := <-worker:
			forward, valid := fromWorker(r.frame, ready, runs)
			switch {
			case errors.Is(r.err, io.EOF):
				worker = nil
				startStopTimeout()
			case r.err != nil || !valid:
				p.kill()
				worker = nil
			default:
				ready = true
				if forward.Message.Type == wire.TypeDone {
					runs--
				}
				send(forward)
			}
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
// guest, and false if the guest may not send f now. Ready goes on with the
// supervisor's enforcement. ready tells if the
// worker has sent ready, and runs how many runs it has not finished.
func fromWorker(f wire.Frame, ready bool, runs int) (wire.Frame, bool) {
	m := f.Message
	control := func(m wire.Message) (wire.Frame, bool) {
		return wire.Frame{Kind: wire.KindControl, Message: m}, true
	}
	switch {
	case f.Kind != wire.KindControl:
		return f, ready && runs > 0
	case m.Type == wire.TypeReady && !ready:
		return control(wire.Message{Type: wire.TypeReady, Enforcement: &enforcement})
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

// receive reads messages from r and sends each result on the channel, but
// hands each token message to token when it is set. It stops after the
// first error, or when done closes.
func receive(r io.Reader, done <-chan struct{}, token func(wire.Message)) <-chan received {
	results := make(chan received)
	go func() {
		for {
			f, err := wire.Read(r)
			if token != nil && err == nil && f.Kind == wire.KindControl && f.Message.Type == wire.TypeToken {
				token(f.Message)
				continue
			}
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
