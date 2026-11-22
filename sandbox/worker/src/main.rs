//! The sandbox worker binary. The supervisor starts it with its socket as
//! fd 3. See the library documentation for the startup order.

use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::process::ExitCode;

use tacklr_sandbox_worker::{jail, serve};

/// The fd of the worker's end of the socket pair.
const SOCKET_FD: RawFd = 3;

/// The exit code when the worker cannot apply its sandbox.
const SANDBOX_FAILED: u8 = 125;

fn main() -> ExitCode {
    // SAFETY: the supervisor gives the worker its socket as fd 3, and
    // nothing else in this process owns fd 3.
    let mut socket = UnixStream::from(unsafe { OwnedFd::from_raw_fd(SOCKET_FD) });
    let Ok(enforcement) = jail::enter(SOCKET_FD) else {
        return ExitCode::from(SANDBOX_FAILED);
    };
    match serve::serve(&mut socket, enforcement) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}
