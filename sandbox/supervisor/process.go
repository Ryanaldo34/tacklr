package supervisor

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/ryanaldo34/tacklr/sandbox/internal/wire"
)

// The vsock ports that the guest connects to on the host: the control
// frames, and the 9P2000.L server of /workspace.
const (
	controlPort   = 1024
	workspacePort = 564
)

// guestCID is the guest's vsock address. Each VM has its own vsock
// device, so every guest has the same one.
const guestCID = 3

// workerInit is the worker's path in the root file system image.
const workerInit = "/sbin/tacklr-sandbox-worker"

// bootArgs are the guest kernel's arguments before init: the serial
// console (the VM's log), a reboot that ends Firecracker, and a reboot
// one second after a panic, such as the end of init.
const bootArgs = "console=ttyS0 reboot=k panic=1 pci=off"

// vmmUID and vmmGID are the IDs of Firecracker inside its user namespace
// (nobody). They are not 0, so Firecracker has no capabilities in its
// namespaces after exec.
const (
	vmmUID = 65534
	vmmGID = 65534
)

// vmmNamespaces are the namespaces of each Firecracker process. The
// network namespace has only a loopback device that is down, and the VM
// has no network device.
const vmmNamespaces = syscall.CLONE_NEWUSER | syscall.CLONE_NEWPID | syscall.CLONE_NEWNS |
	syscall.CLONE_NEWNET | syscall.CLONE_NEWIPC | syscall.CLONE_NEWUTS | syscall.CLONE_NEWCGROUP

// machine is what each VM of a supervisor runs.
type machine struct {
	firecracker string
	kernel      string
	rootfs      string
	vcpus       int
	memoryMiB   int
	init        []string
	log         io.Writer
}

// vmConfig is Firecracker's configuration file (--config-file).
type vmConfig struct {
	BootSource struct {
		KernelImagePath string `json:"kernel_image_path"`
		BootArgs        string `json:"boot_args"`
	} `json:"boot-source"`
	Drives  []vmDrive `json:"drives"`
	Machine struct {
		VCPUCount  int `json:"vcpu_count"`
		MemSizeMiB int `json:"mem_size_mib"`
	} `json:"machine-config"`
	Vsock struct {
		GuestCID int    `json:"guest_cid"`
		UDSPath  string `json:"uds_path"`
	} `json:"vsock"`
}

type vmDrive struct {
	DriveID      string `json:"drive_id"`
	PathOnHost   string `json:"path_on_host"`
	IsRootDevice bool   `json:"is_root_device"`
	IsReadOnly   bool   `json:"is_read_only"`
}

// bootArgs is the kernel command line: bootArgs, then init and its
// arguments. An argument with a space is quoted.
func (m machine) bootArgs() (string, error) {
	command := m.init
	if len(command) == 0 {
		command = []string{workerInit}
	}
	args := []string{bootArgs, "init=" + command[0]}
	if len(command) > 1 {
		args = append(args, "--")
	}
	for _, arg := range command[1:] {
		if strings.ContainsAny(arg, "\"\n") {
			return "", fmt.Errorf("supervisor: the init argument %q has a quote or a newline", arg)
		}
		if strings.Contains(arg, " ") {
			arg = `"` + arg + `"`
		}
		args = append(args, arg)
	}
	return strings.Join(args, " "), nil
}

// process is one running VM: its Firecracker process, and the guest's
// connections to the host.
type process struct {
	cmd       *exec.Cmd
	dir       string
	control   net.Listener
	workspace net.Listener
	// conn is the guest's control connection, once it connects.
	conn net.Conn
	// exited closes after the process has ended and is reaped. state is set
	// before it closes.
	exited chan struct{}
	state  *os.ProcessState
}

// startProcess boots a VM of m. Firecracker runs with no API socket, from
// a configuration file in a new directory, in new namespaces, with an
// empty environment. Its stdout (its log and the guest's console) goes to
// m.log. The guest connects to the listeners of the VM's vsock ports.
func startProcess(m machine) (p *process, err error) {
	args, err := m.bootArgs()
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "tacklr-vm-")
	if err != nil {
		return nil, fmt.Errorf("supervisor: the VM directory: %w", err)
	}
	p = &process{dir: dir, exited: make(chan struct{})}
	defer func() {
		if err != nil {
			p.closeListeners()
			_ = os.RemoveAll(dir)
		}
	}()
	vsock := filepath.Join(dir, "v.sock")
	if p.control, err = net.Listen("unix", fmt.Sprintf("%s_%d", vsock, controlPort)); err != nil {
		return nil, fmt.Errorf("supervisor: the control socket: %w", err)
	}
	if p.workspace, err = net.Listen("unix", fmt.Sprintf("%s_%d", vsock, workspacePort)); err != nil {
		return nil, fmt.Errorf("supervisor: the workspace socket: %w", err)
	}
	var config vmConfig
	config.BootSource.KernelImagePath = m.kernel
	config.BootSource.BootArgs = args
	config.Drives = []vmDrive{{DriveID: "root", PathOnHost: m.rootfs, IsRootDevice: true, IsReadOnly: true}}
	config.Machine.VCPUCount = m.vcpus
	config.Machine.MemSizeMiB = m.memoryMiB
	config.Vsock.GuestCID = guestCID
	config.Vsock.UDSPath = vsock
	encoded, _ := json.Marshal(config)
	configPath := filepath.Join(dir, "vm.json")
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		return nil, fmt.Errorf("supervisor: the VM configuration: %w", err)
	}
	log := m.log
	if log == nil {
		log = io.Discard
	}
	p.cmd = &exec.Cmd{
		Path:   m.firecracker,
		Args:   []string{m.firecracker, "--no-api", "--config-file", configPath, "--level", "Warning"},
		Env:    []string{},
		Dir:    dir,
		Stdout: log,
		Stderr: log,
		SysProcAttr: &syscall.SysProcAttr{
			Cloneflags:                 vmmNamespaces,
			UidMappings:                []syscall.SysProcIDMap{{ContainerID: vmmUID, HostID: os.Getuid(), Size: 1}},
			GidMappings:                []syscall.SysProcIDMap{{ContainerID: vmmGID, HostID: os.Getgid(), Size: 1}},
			GidMappingsEnableSetgroups: false,
			Setsid:                     true,
			Pdeathsig:                  syscall.SIGKILL,
		},
	}
	if err := p.cmd.Start(); err != nil {
		return nil, fmt.Errorf("supervisor: start Firecracker: %w", err)
	}
	go func() {
		_ = p.cmd.Wait()
		p.state = p.cmd.ProcessState
		close(p.exited)
	}()
	return p, nil
}

// accept waits for the guest to connect to l, once, and stops listening.
// It fails when the VM ends first, or after timeout.
func (p *process) accept(l net.Listener, timeout time.Duration) (net.Conn, error) {
	accepted := make(chan struct{})
	defer close(accepted)
	go func() {
		select {
		case <-p.exited:
		case <-time.After(timeout):
		case <-accepted:
		}
		_ = l.Close()
	}()
	return l.Accept()
}

// kill sends SIGKILL to Firecracker. It is the first process of its pid
// namespace, so the whole namespace, and the VM, ends with it.
func (p *process) kill() {
	_ = p.cmd.Process.Kill()
}

// closeListeners stops listening for the guest.
func (p *process) closeListeners() {
	for _, l := range []net.Listener{p.control, p.workspace} {
		if l != nil {
			_ = l.Close()
		}
	}
}

// close kills the VM, waits until Firecracker is reaped, and removes the
// VM's directory.
func (p *process) close() {
	p.kill()
	<-p.exited
	p.closeListeners()
	if p.conn != nil {
		_ = p.conn.Close()
	}
	_ = os.RemoveAll(p.dir)
}

// exit is how the process ended. Call it after exited closes.
func (p *process) exit() *wire.Exit {
	status, _ := p.state.Sys().(syscall.WaitStatus)
	if status.Signaled() {
		return &wire.Exit{Code: -1, Signal: unix.SignalName(status.Signal())}
	}
	return &wire.Exit{Code: status.ExitStatus()}
}
