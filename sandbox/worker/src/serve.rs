//! The worker's request loop.

use std::io;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::mpsc::{Sender, channel};
use std::thread;

use crate::frame::{self, Incoming, KIND_STDERR, KIND_STDOUT};
use crate::message::{Enforcement, Reply, Request};
use crate::remote_fs::{Link, RemoteFs};
use crate::shell::{Output, Shell, Stream};

/// Sends `ready` with `enforcement`, then answers requests on `socket` until
/// the supervisor sends `stop` or closes the socket. Runs go one at a time.
/// A run's workspace is the client's: its file requests go over `socket`
/// while it runs.
///
/// # Errors
///
/// Returns an error if the socket fails or the supervisor sends a frame that
/// is not a valid request or file reply.
pub fn serve(socket: &mut UnixStream, enforcement: Enforcement, shell: &Shell) -> io::Result<()> {
    let link = Link::new(socket.try_clone()?);
    link.reply(&Reply::Ready { enforcement })?;
    let (requests, incoming) = channel();
    let reader = socket.try_clone()?;
    let replies = link.clone();
    thread::spawn(move || read(reader, &replies, &requests));
    let workspace = Arc::new(RemoteFs(link.clone()));
    for request in incoming {
        match request? {
            Request::Ping { id } => link.reply(&Reply::Pong { id })?,
            Request::Stop => return Ok(()),
            Request::Run { id, script } => {
                let output: Output = {
                    let link = link.clone();
                    Arc::new(move |stream, bytes| {
                        let kind = match stream {
                            Stream::Stdout => KIND_STDOUT,
                            Stream::Stderr => KIND_STDERR,
                        };
                        link.output(kind, bytes)
                    })
                };
                let (code, error) = shell.run(&script, workspace.clone(), output).map_or_else(
                    |error| (-1, Some(format!("{error:#}"))),
                    |code| (code, None),
                );
                link.reply(&Reply::Done { id, code, error })?;
            }
        }
    }
    Ok(())
}

/// Reads the supervisor's frames: requests go to `requests`, file replies to
/// `link`. A frame that is not valid goes to `requests` as an error. When the
/// socket ends, `link` fails the file requests that wait.
fn read(mut socket: UnixStream, link: &Link, requests: &Sender<io::Result<Request>>) {
    loop {
        match frame::read(&mut socket) {
            Ok(Some(Incoming::Request(request))) => {
                let _ = requests.send(Ok(request));
            }
            Ok(Some(Incoming::Fs(reply, data))) => link.deliver(reply, data),
            Ok(None) => break,
            Err(error) => {
                let _ = requests.send(Err(error));
                break;
            }
        }
    }
    link.close();
}
