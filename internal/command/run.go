// Package command contains the host command execution mechanism. Harness tool
// registration and permission policy remain in the root package.
package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
)

const (
	outputCap      = 1 << 20
	jailSetupExit  = 125
	jailFailMarker = "tacklr-jail-setup-failed"
)

// Run executes command in the projected VFS directory and returns a bounded,
// structured stdout/stderr result. Non-zero process exits are successful
// outcomes and a cancelled context is an error. A jail that cannot start
// panics: run_command must never run outside it.
//
// The shell is jailed to dir with a user+mount+pid namespace so it cannot see
// sibling session mounts, and host system directories are read-only. The
// shell gets only shellEnv, never the worker's environment.
func Run(ctx context.Context, dir, command string) (string, error) {
	cmd := jailCommand(ctx, dir, command)
	cmd.Env = shellEnv()
	cmd.Stdin = bytes.NewReader(nil)
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	budget := outputBudget{remaining: outputCap}
	stdout := budget.writer()
	stderr := budget.writer()
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	exit := 0
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			if exitError.ExitCode() == jailSetupExit && strings.Contains(stderr.buf.String(), jailFailMarker) {
				panic("run_command: session jail requires Linux user namespaces: " + strings.TrimSpace(stderr.buf.String()))
			}
			exit = exitError.ExitCode()
		} else {
			panic("run_command: session jail requires Linux user namespaces: " + err.Error())
		}
	}
	return formatResult(exit, budget.truncated, stdout, stderr), nil
}

func formatResult(exit int, truncated bool, stdout, stderr *budgetWriter) string {
	var builder strings.Builder
	extra := 0
	if truncated {
		extra = len("output truncated\n")
	}
	builder.Grow(64 + extra + stdout.buf.Len() + stderr.buf.Len())
	fmt.Fprintf(&builder, "exit=%d truncated=%v\n", exit, truncated)
	if truncated {
		builder.WriteString("output truncated\n")
	}
	builder.WriteString("--- stdout ---\n")
	out := stdout.buf.Bytes()
	builder.Write(out)
	if len(out) > 0 && out[len(out)-1] != '\n' {
		builder.WriteByte('\n')
	}
	builder.WriteString("--- stderr ---\n")
	builder.Write(stderr.buf.Bytes())
	return builder.String()
}

type outputBudget struct {
	mu        sync.Mutex
	remaining int
	truncated bool
}

type budgetWriter struct {
	budget *outputBudget
	buf    bytes.Buffer
}

func (b *outputBudget) writer() *budgetWriter {
	return &budgetWriter{budget: b}
}

func (w *budgetWriter) Write(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	w.budget.mu.Lock()
	defer w.budget.mu.Unlock()
	if w.budget.remaining <= 0 {
		w.budget.truncated = true
		return len(value), nil
	}
	take := min(len(value), w.budget.remaining)
	if take < len(value) {
		w.budget.truncated = true
	}
	_, _ = w.buf.Write(value[:take])
	w.budget.remaining -= take
	return len(value), nil
}

// Bootstrap runs in a fresh user+mount+pid namespace (SysProcAttr). Bind
// HostDir at /session and host /bin /lib /usr /dev so /bin/sh still runs, then
// exec chroot so pid 1 is inside the jail. Exit 125 with jailFailMarker if the
// jail cannot be built. The agent command's exit status is preserved.
//
// /bin, /lib, /lib64 and /usr are remounted read-only: a worker running as
// root would otherwise let the agent replace host programs (git, sh) that the
// worker itself runs later. mount (util-linux and busybox) keeps the flags the
// host mount already has, which a user namespace may not clear. They are bound
// recursively: a user namespace refuses a plain bind of a mount that has
// locked submounts (a host with a mount under /usr, such as /usr/lib/modules).
const jailScript = `
set -e
die() { echo ` + jailFailMarker + ` >&2; exit 125; }
ro() { mount -o remount,bind,ro "$1" || die; }
host=$1
cmd=$2
mount --make-rprivate / || die
jail=$(mktemp -d /tmp/tacklr-jail-XXXXXX) || die
mkdir -p "$jail/session" "$jail/bin" "$jail/lib" "$jail/usr" "$jail/dev" "$jail/tmp" "$jail/etc" || die
mount --bind "$host" "$jail/session" || die
mount --rbind /bin "$jail/bin" && ro "$jail/bin" || die
if [ -d /lib ]; then mount --rbind /lib "$jail/lib" && ro "$jail/lib" || die; fi
if [ -d /lib64 ]; then mkdir -p "$jail/lib64" && mount --rbind /lib64 "$jail/lib64" && ro "$jail/lib64" || die; fi
if [ -d /usr ]; then mount --rbind /usr "$jail/usr" && ro "$jail/usr" || die; fi
if [ -d /dev ]; then mount --rbind /dev "$jail/dev" || die; fi
PATH=/bin:/usr/bin:/sbin:/usr/sbin
chroot_bin=$(command -v chroot) || die
export PATH=/bin:/usr/bin
exec "$chroot_bin" "$jail" /bin/sh -c 'cd /session && eval "$1"' run_command "$cmd" || die
`

var jailOnce sync.Once

func userNSAvailable() bool {
	dir, err := os.MkdirTemp("", "tacklr-jail-probe-")
	if err != nil {
		return false
	}
	defer os.RemoveAll(dir)
	cmd := exec.Command("/bin/sh", "-c", `mount --make-rprivate / && mount --bind "$1" "$1"`, "probe", dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:                 syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS | syscall.CLONE_NEWPID,
		GidMappingsEnableSetgroups: false,
		UidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	return cmd.Run() == nil
}

func RequireJail() {
	jailOnce.Do(func() {
		if !userNSAvailable() {
			panic("run_command: session jail requires Linux user namespaces")
		}
	})
}

// shellEnv is the whole environment of a jailed shell. Nothing comes from the
// worker process, whose environment can hold cloud credentials and tokens.
// HOME and TMPDIR are the jail's private /tmp.
func shellEnv() []string {
	return []string{"PATH=/bin:/usr/bin", "HOME=/tmp", "TMPDIR=/tmp", "LANG=C.UTF-8"}
}

func jailCommand(ctx context.Context, dir, command string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", jailScript, "run_command", dir, command)
	cmd.Dir = os.TempDir()
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:                    true,
		Pdeathsig:                  syscall.SIGKILL,
		Cloneflags:                 syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS | syscall.CLONE_NEWPID,
		GidMappingsEnableSetgroups: false,
		UidMappings: []syscall.SysProcIDMap{{
			ContainerID: 0,
			HostID:      os.Getuid(),
			Size:        1,
		}},
		GidMappings: []syscall.SysProcIDMap{{
			ContainerID: 0,
			HostID:      os.Getgid(),
			Size:        1,
		}},
	}
	return cmd
}
