//go:build linux

package command

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

// Bootstrap runs in a fresh user+mount+pid namespace (SysProcAttr). Bind
// HostDir at /session and host /bin /lib /usr /dev so /bin/sh still runs, then
// exec chroot so pid 1 is inside the jail. Exit 125 with jailFailMarker if the
// jail cannot be built. The agent command's exit status is preserved.
const jailScript = `
set -e
die() { echo ` + jailFailMarker + ` >&2; exit 125; }
host=$1
cmd=$2
mount --make-rprivate / || die
jail=$(mktemp -d /tmp/tacklr-jail-XXXXXX) || die
mkdir -p "$jail/session" "$jail/bin" "$jail/lib" "$jail/usr" "$jail/dev" "$jail/tmp" "$jail/etc" || die
mount --bind "$host" "$jail/session" || die
mount --bind /bin "$jail/bin" || die
if [ -d /lib ]; then mount --bind /lib "$jail/lib" || die; fi
if [ -d /lib64 ]; then mkdir -p "$jail/lib64" && mount --bind /lib64 "$jail/lib64" || die; fi
if [ -d /usr ]; then mount --bind /usr "$jail/usr" || die; fi
if [ -d /dev ]; then mount --rbind /dev "$jail/dev" || die; fi
PATH=/bin:/usr/bin:/sbin:/usr/sbin
chroot_bin=$(command -v chroot) || die
export PATH=/bin:/usr/bin
exec "$chroot_bin" "$jail" /bin/sh -c 'cd /session && eval "$1"' run_command "$cmd" || die
`

const jailRequired = true

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
