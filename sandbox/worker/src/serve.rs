//! The worker's request loop.

use std::io;
use std::os::unix::net::UnixStream;

use crate::frame;
use crate::message::{Enforcement, Reply, Request};

/// Sends `ready` with `enforcement`, then answers requests on `socket` until
/// the supervisor sends `stop` or closes the socket.
///
/// # Errors
///
/// Returns an error if the socket fails or the supervisor sends a frame that
/// is not a valid request.
pub fn serve(socket: &mut UnixStream, enforcement: Enforcement) -> io::Result<()> {
    frame::write(socket, &Reply::Ready { enforcement })?;
    while let Some(request) = frame::read(socket)? {
        match request {
            Request::Ping { id } => frame::write(socket, &Reply::Pong { id })?,
            Request::Stop => return Ok(()),
        }
    }
    Ok(())
}
