//! The worker's request loop.

use std::io;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

use virtual_fs::{FileSystem, mem_fs};

use crate::frame::{self, KIND_STDERR, KIND_STDOUT};
use crate::message::{Enforcement, Reply, Request};
use crate::shell::{Output, Shell, Stream};

/// Sends `ready` with `enforcement`, then answers requests on `socket` until
/// the supervisor sends `stop` or closes the socket. Runs go one at a time,
/// in one workspace that lives as long as the worker.
///
/// # Errors
///
/// Returns an error if the socket fails or the supervisor sends a frame that
/// is not a valid request.
pub fn serve(socket: &mut UnixStream, enforcement: Enforcement, shell: &Shell) -> io::Result<()> {
    let workspace: Arc<dyn FileSystem + Send + Sync> = Arc::new(mem_fs::FileSystem::default());
    frame::write(socket, &Reply::Ready { enforcement })?;
    while let Some(request) = frame::read(socket)? {
        match request {
            Request::Ping { id } => frame::write(socket, &Reply::Pong { id })?,
            Request::Stop => return Ok(()),
            Request::Run { id, script } => {
                let (code, error) = run(socket, shell, &script, workspace.clone())?;
                frame::write(socket, &Reply::Done { id, code, error })?;
            }
        }
    }
    Ok(())
}

/// Runs `script` and streams its stdout and stderr to `socket` as they come.
/// Returns the exit code, or -1 and why the runtime could not run it.
fn run(
    socket: &UnixStream,
    shell: &Shell,
    script: &str,
    workspace: Arc<dyn FileSystem + Send + Sync>,
) -> io::Result<(i32, Option<String>)> {
    let writer = Mutex::new(socket.try_clone()?);
    let output: Output = Arc::new(move |stream, bytes| {
        let kind = match stream {
            Stream::Stdout => KIND_STDOUT,
            Stream::Stderr => KIND_STDERR,
        };
        frame::write_data(&mut *writer.lock().expect("socket writer"), kind, bytes)
    });
    let result = shell.run(script, workspace, output);
    Ok(result.map_or_else(
        |error| (-1, Some(format!("{error:#}"))),
        |code| (code, None),
    ))
}
