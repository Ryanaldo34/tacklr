// Command tacklr-sandbox-worker is the init of a sandbox microVM: it
// mounts the guest's file systems and /workspace over 9P2000.L, serves the
// supervisor on the vsock control port (package worker), and powers the
// VM off. It runs only as the first process of a VM, so the supervisor's
// tests, which boot real VMs, cover it.
package main

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/ryanaldo34/tacklr/sandbox/worker"
)

// The host's vsock address and ports: the control frames, and the 9P2000.L
// server of /workspace.
const (
	hostCID       = 2
	controlPort   = 1024
	workspacePort = 564
)

// The agent user and group that scripts run as, and its home (a tmpfs).
const (
	agentUID = 1000
	agentGID = 1000
	home     = "/home/agent"
)

// msize is the largest 9P message: one read or write of up to about 512 KiB.
const msize = 512 << 10

func main() {
	if err := boot(); err != nil {
		fmt.Fprintln(os.Stderr, "tacklr-sandbox-worker:", err)
	}
	unix.Sync()
	_ = unix.Reboot(unix.LINUX_REBOOT_CMD_RESTART)
	select {}
}

func boot() error {
	if err := mountSystem(); err != nil {
		return err
	}
	workspace, err := connect(workspacePort)
	if err != nil {
		return fmt.Errorf("connect the workspace: %w", err)
	}
	defer workspace.Close()
	options := fmt.Sprintf("trans=fd,rfdno=%d,wfdno=%d,version=9p2000.L,msize=%d,cache=mmap", workspace.Fd(), workspace.Fd(), msize)
	if err := mount("tacklr", "/workspace", "9p", unix.MS_NOSUID|unix.MS_NODEV, options); err != nil {
		return err
	}
	control, err := connect(controlPort)
	if err != nil {
		return fmt.Errorf("connect the control port: %w", err)
	}
	defer control.Close()
	return worker.Serve(control, worker.Runner{
		Bash:       "/bin/bash",
		Dir:        "/workspace",
		Credential: &syscall.Credential{Uid: agentUID, Gid: agentGID, Groups: []uint32{}},
		Env:        []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=" + home, "USER=agent", "LANG=C.UTF-8"},
	})
}

// mountSystem mounts the guest's own file systems on the read-only root:
// proc, sysfs, and tmpfs for /tmp, /run, /dev/shm and the agent's home.
// The kernel has mounted devtmpfs on /dev.
func mountSystem() error {
	flags := uintptr(unix.MS_NOSUID | unix.MS_NODEV)
	if err := os.MkdirAll("/dev/shm", 0o750); err != nil {
		return err
	}
	for _, m := range []struct {
		kind, target, options string
		flags                 uintptr
	}{
		{"proc", "/proc", "", flags | unix.MS_NOEXEC},
		{"sysfs", "/sys", "", flags | unix.MS_NOEXEC},
		{"tmpfs", "/tmp", "mode=1777", flags},
		{"tmpfs", "/run", "mode=0755", flags},
		{"tmpfs", "/dev/shm", "mode=1777", flags},
		{"tmpfs", home, fmt.Sprintf("mode=0755,uid=%d,gid=%d", agentUID, agentGID), flags},
	} {
		if err := mount(m.kind, m.target, m.kind, m.flags, m.options); err != nil {
			return err
		}
	}
	return unix.Sethostname([]byte("sandbox"))
}

func mount(source, target, kind string, flags uintptr, options string) error {
	if err := unix.Mount(source, target, kind, flags, options); err != nil {
		return fmt.Errorf("mount %s: %w", target, err)
	}
	return nil
}

// connect connects to port of the host over vsock.
func connect(port uint32) (*os.File, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.Connect(fd, &unix.SockaddrVM{CID: hostCID, Port: port}); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return os.NewFile(uintptr(fd), fmt.Sprintf("vsock:%d", port)), nil
}
