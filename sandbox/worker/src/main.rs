//! The sandbox worker binary: `tacklr-sandbox-worker <packages dir>`. The
//! supervisor starts it with its socket as fd 3. See the library
//! documentation for the startup order.

use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::ExitCode;

use tacklr_sandbox_worker::shell::{Packages, Shell};
use tacklr_sandbox_worker::{jail, serve};

/// The fd of the worker's end of the socket pair.
const SOCKET_FD: RawFd = 3;

/// The exit code when the worker cannot load its packages, apply its
/// sandbox, or start its runtime.
const SETUP_FAILED: u8 = 125;

fn main() -> ExitCode {
    // SAFETY: the supervisor gives the worker its socket as fd 3, and
    // nothing else in this process owns fd 3.
    let mut socket = UnixStream::from(unsafe { OwnedFd::from_raw_fd(SOCKET_FD) });
    let Some(dir) = std::env::args_os().nth(1).map(PathBuf::from) else {
        return ExitCode::from(SETUP_FAILED);
    };
    let Ok(packages) = Packages::load(&dir) else {
        return ExitCode::from(SETUP_FAILED);
    };
    let Ok(enforcement) = jail::enter(SOCKET_FD) else {
        return ExitCode::from(SETUP_FAILED);
    };
    let Ok(shell) = Shell::start(packages) else {
        return ExitCode::from(SETUP_FAILED);
    };
    match serve::serve(&mut socket, enforcement, &shell) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}
