//! The sandbox layers, each one tested alone in a child process.
//!
//! A sandbox cannot be removed from a process, so each case starts this test
//! binary again as a child (`jail probe <name> [arg]`). The child applies
//! one layer and then does an operation that the layer must stop. The child
//! exits with `EXPECTED` when the layer stops the operation, or when the shell
//! gives the expected output under the whole jail. Seccomp kills the
//! child with `SIGSYS`.
//!
//! The children cannot write coverage data after Landlock applies, so this
//! file adds no coverage of `jail`. The Go tests of `sandbox/supervisor` run
//! the whole sandbox in the real worker binary.

use std::fs::File;
use std::io::ErrorKind;
use std::net::{TcpListener, TcpStream};
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::{SocketAddr, UnixListener, UnixStream};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, ExitStatus, exit};

use std::path::Path;
use std::sync::{Arc, Mutex};

use tacklr_sandbox_worker::jail;
use tacklr_sandbox_worker::shell::{Output, Packages, Shell, Stream};
use virtual_fs::mem_fs;

const PROBE: &str = "probe";
const EXPECTED: i32 = 0;
const UNEXPECTED: i32 = 1;

/// A case name and the check that runs its probe.
type Case = (&'static str, fn() -> bool);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some(PROBE) {
        let code = probe(&args[2], args.get(3).map(String::as_str));
        // SAFETY: _exit ends the process at once. It skips the exit handlers,
        // such as the coverage writer, whose system calls the jail refuses.
        unsafe { libc::_exit(code) };
    }
    let cases: [Case; 8] = [
        ("landlock denies reading and writing host files", files_case),
        ("landlock denies tcp connections", tcp_case),
        (
            "landlock denies abstract unix sockets made outside the sandbox",
            abstract_unix_case,
        ),
        (
            "landlock denies signals to processes outside the sandbox",
            signal_case,
        ),
        (
            "seccomp kills the process on a system call outside the list",
            seccomp_case,
        ),
        (
            "seccomp kills the process when it maps executable memory",
            || seccomp_kills("exec-memory"),
        ),
        (
            "seccomp kills the process when it makes a namespace",
            || seccomp_kills("namespace"),
        ),
        ("the shell runs a pipeline under the whole jail", shell_case),
    ];
    let mut failed = 0;
    for (name, case) in cases {
        let passed = case();
        println!("test {name} ... {}", if passed { "ok" } else { "FAILED" });
        failed += usize::from(!passed);
    }
    if failed > 0 {
        println!("{failed} sandbox case(s) failed");
        exit(1);
    }
}

fn run_probe(name: &str, arg: Option<&str>) -> ExitStatus {
    let mut command = Command::new(std::env::current_exe().expect("test binary path"));
    command.args([PROBE, name]);
    command.args(arg);
    command.status().expect("start probe")
}

fn files_case() -> bool {
    let dir = std::env::temp_dir();
    run_probe("files", dir.to_str()).code() == Some(EXPECTED)
}

fn tcp_case() -> bool {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
    let address = listener.local_addr().expect("address").to_string();
    run_probe("tcp", Some(&address)).code() == Some(EXPECTED)
}

fn abstract_unix_case() -> bool {
    let name = format!("tacklr-sandbox-test-{}", std::process::id());
    let address = SocketAddr::from_abstract_name(&name).expect("abstract address");
    let _listener = UnixListener::bind_addr(&address).expect("listen");
    run_probe("abstract-unix", Some(&name)).code() == Some(EXPECTED)
}

fn signal_case() -> bool {
    run_probe("signal", None).code() == Some(EXPECTED)
}

fn shell_case() -> bool {
    run_probe("shell", None).code() == Some(EXPECTED)
}

fn seccomp_case() -> bool {
    seccomp_kills("seccomp")
}

fn seccomp_kills(probe: &str) -> bool {
    run_probe(probe, None).signal() == Some(libc::SIGSYS)
}

/// Runs in the child: applies one layer, then does the operation the layer
/// must stop. Returns the exit code.
fn probe(name: &str, arg: Option<&str>) -> i32 {
    if name == "shell" {
        let packages = Packages::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("packages"))
            .expect("load packages");
        jail::enter(2).expect("enter the jail");
        let shell = Shell::start(packages).expect("start the shell");
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let sink = stdout.clone();
        let output: Output = Arc::new(move |stream, bytes| {
            if stream == Stream::Stdout {
                sink.lock().unwrap().extend_from_slice(bytes);
            }
            Ok(())
        });
        let code = shell
            .run(
                "printf 'b\\na\\n' | sort | tr a-z A-Z > out.txt && sleep 0.1 && cat out.txt",
                Arc::new(mem_fs::FileSystem::default()),
                output,
            )
            .expect("run");
        let output = String::from_utf8(stdout.lock().unwrap().clone()).expect("utf-8");
        return if code == 0 && output == "A\nB\n" {
            EXPECTED
        } else {
            UNEXPECTED
        };
    }
    if name == "exec-memory" {
        jail::install_syscall_filter().expect("install the seccomp filter");
        // SAFETY: an anonymous mapping takes no memory of the caller. The
        // filter does not allow PROT_EXEC, so the kernel kills the process.
        unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                4096,
                libc::PROT_READ | libc::PROT_EXEC,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        return UNEXPECTED;
    }
    if name == "namespace" {
        jail::install_syscall_filter().expect("install the seccomp filter");
        // SAFETY: the clone flags make a thread in a new user namespace, which
        // the filter does not allow, so the kernel kills the process first.
        unsafe {
            libc::syscall(
                libc::SYS_clone,
                libc::CLONE_THREAD | libc::CLONE_SIGHAND | libc::CLONE_VM | libc::CLONE_NEWUSER,
                0,
                0,
                0,
                0,
            )
        };
        return UNEXPECTED;
    }
    if name == "seccomp" {
        jail::install_syscall_filter().expect("install the seccomp filter");
        // SAFETY: getppid takes no arguments. The filter does not allow it,
        // so the kernel kills the process here.
        unsafe { libc::syscall(libc::SYS_getppid) };
        return UNEXPECTED;
    }
    jail::restrict_filesystem_and_network().expect("apply Landlock");
    let denied = match name {
        "files" => {
            let read = File::open("/etc/hostname");
            let write = File::create(std::path::Path::new(arg.unwrap()).join("tacklr-probe"));
            is_denied(read.err()) && is_denied(write.err())
        }
        "tcp" => is_denied(TcpStream::connect(arg.unwrap()).err()),
        "abstract-unix" => {
            let address = SocketAddr::from_abstract_name(arg.unwrap()).expect("address");
            is_denied(UnixStream::connect_addr(&address).err())
        }
        "signal" => {
            // SAFETY: kill takes no pointers. Signal 0 checks permission only.
            let result = unsafe { libc::kill(libc::getppid(), 0) };
            result == -1 && is_denied(Some(std::io::Error::last_os_error()))
        }
        _ => panic!("unknown probe {name}"),
    };
    if denied { EXPECTED } else { UNEXPECTED }
}

fn is_denied(error: Option<std::io::Error>) -> bool {
    error.is_some_and(|error| error.kind() == ErrorKind::PermissionDenied)
}
