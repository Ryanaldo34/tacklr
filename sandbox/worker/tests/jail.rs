//! The sandbox layers, each one tested alone in a child process.
//!
//! A sandbox cannot be removed from a process, so each case starts this test
//! binary again as a child (`jail probe <name> [arg]`). The child applies
//! one layer and then does an operation that the layer must stop. The child
//! exits with `DENIED` when the layer stops the operation. Seccomp kills the
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

use tacklr_sandbox_worker::jail;

const PROBE: &str = "probe";
const DENIED: i32 = 0;
const ALLOWED: i32 = 1;

/// A case name and the check that runs its probe.
type Case = (&'static str, fn() -> bool);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some(PROBE) {
        exit(probe(&args[2], args.get(3).map(String::as_str)));
    }
    let cases: [Case; 5] = [
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
    run_probe("files", dir.to_str()).code() == Some(DENIED)
}

fn tcp_case() -> bool {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
    let address = listener.local_addr().expect("address").to_string();
    run_probe("tcp", Some(&address)).code() == Some(DENIED)
}

fn abstract_unix_case() -> bool {
    let name = format!("tacklr-sandbox-test-{}", std::process::id());
    let address = SocketAddr::from_abstract_name(&name).expect("abstract address");
    let _listener = UnixListener::bind_addr(&address).expect("listen");
    run_probe("abstract-unix", Some(&name)).code() == Some(DENIED)
}

fn signal_case() -> bool {
    run_probe("signal", None).code() == Some(DENIED)
}

fn seccomp_case() -> bool {
    run_probe("seccomp", None).signal() == Some(libc::SIGSYS)
}

/// Runs in the child: applies one layer, then does the operation the layer
/// must stop. Returns the exit code.
fn probe(name: &str, arg: Option<&str>) -> i32 {
    if name == "seccomp" {
        jail::install_syscall_filter().expect("install the seccomp filter");
        // SAFETY: getppid takes no arguments. The filter does not allow it,
        // so the kernel kills the process here.
        unsafe { libc::syscall(libc::SYS_getppid) };
        return ALLOWED;
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
    if denied { DENIED } else { ALLOWED }
}

fn is_denied(error: Option<std::io::Error>) -> bool {
    error.is_some_and(|error| error.kind() == ErrorKind::PermissionDenied)
}
