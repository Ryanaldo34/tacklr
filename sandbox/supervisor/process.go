package supervisor

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
)

// workerUID and workerGID are the IDs of the worker inside its user
// namespace (nobody). They are not 0, so the worker has no capabilities
// in its namespaces after exec.
const (
	workerUID = 65534
	workerGID = 65534
)

// workerNamespaces are the namespaces each worker gets. The network
// namespace has only a loopback device that is down, so the worker has no
// network before Landlock applies.
const workerNamespaces = syscall.CLONE_NEWUSER | syscall.CLONE_NEWPID | syscall.CLONE_NEWNS |
	syscall.CLONE_NEWNET | syscall.CLONE_NEWIPC | syscall.CLONE_NEWUTS | syscall.CLONE_NEWCGROUP

// process is one running worker and the supervisor's end of its socket.
type process struct {
	cmd  *exec.Cmd
	conn net.Conn
	// exited closes after the process has ended and is reaped. state is set
	// before it closes.
	exited chan struct{}
	state  *os.ProcessState
}

// startProcess starts the worker binary at path in new namespaces, with an
// empty environment, the root directory as its working directory, and the
// worker's end of a Unix socket pair as fd 3. Its stdin, stdout and stderr
// are /dev/null.
func startProcess(path string) (*process, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("supervisor: socket pair: %w", err)
	}
	ours := os.NewFile(uintptr(fds[0]), "sandbox-supervisor")
	theirs := os.NewFile(uintptr(fds[1]), "sandbox-worker")
	defer theirs.Close()
	conn, err := net.FileConn(ours)
	_ = ours.Close()
	if err != nil {
		return nil, fmt.Errorf("supervisor: socket: %w", err)
	}
	cmd := &exec.Cmd{
		Path:       path,
		Args:       []string{path},
		Env:        []string{},
		Dir:        "/",
		ExtraFiles: []*os.File{theirs},
		SysProcAttr: &syscall.SysProcAttr{
			Cloneflags:                 workerNamespaces,
			UidMappings:                []syscall.SysProcIDMap{{ContainerID: workerUID, HostID: os.Getuid(), Size: 1}},
			GidMappings:                []syscall.SysProcIDMap{{ContainerID: workerGID, HostID: os.Getgid(), Size: 1}},
			GidMappingsEnableSetgroups: false,
			Setsid:                     true,
			Pdeathsig:                  syscall.SIGKILL,
		},
	}
	if err := cmd.Start(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("supervisor: start the worker: %w", err)
	}
	p := &process{cmd: cmd, conn: conn, exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		p.state = cmd.ProcessState
		close(p.exited)
	}()
	return p, nil
}

// kill sends SIGKILL to the worker. The worker is the first process of its
// pid namespace, so the whole namespace ends with it.
func (p *process) kill() {
	_ = p.cmd.Process.Kill()
}

// close kills the worker, waits until it is reaped, and closes the socket.
func (p *process) close() {
	p.kill()
	<-p.exited
	_ = p.conn.Close()
}

// exit is how the process ended. Call it after exited closes.
func (p *process) exit() *wire.Exit {
	status, _ := p.state.Sys().(syscall.WaitStatus)
	if status.Signaled() {
		return &wire.Exit{Code: -1, Signal: unix.SignalName(status.Signal())}
	}
	return &wire.Exit{Code: status.ExitStatus()}
}
