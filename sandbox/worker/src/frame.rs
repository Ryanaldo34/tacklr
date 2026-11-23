//! Frames on the worker's socket.
//!
//! A frame is a 5-byte header and a body. The header is the body length as a
//! big-endian `u32`, then one kind byte: control (a JSON body), or the
//! stdout or stderr bytes of a run. The Go side (`sandbox/internal/wire`)
//! uses the same format.

use std::io::{self, ErrorKind, Read, Write};

use crate::message::{FsReply, FsRequest, Reply, Request};

/// The largest body a frame may have.
pub const MAX_BODY: usize = 1 << 20;

/// The kind byte of a frame with a JSON control message.
pub const KIND_CONTROL: u8 = 1;

/// The kind byte of a frame with stdout bytes of the current run.
pub const KIND_STDOUT: u8 = 2;

/// The kind byte of a frame with stderr bytes of the current run.
pub const KIND_STDERR: u8 = 3;

/// The kind byte of a file request or reply: the length of its JSON as a
/// big-endian `u32`, the JSON, then its bytes.
pub const KIND_FS: u8 = 4;

/// A frame from the supervisor.
#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    /// A request.
    Request(Request),
    /// A reply to a file request, and its bytes.
    Fs(FsReply, Vec<u8>),
}

const HEADER: usize = 5;

/// Writes `reply` as one control frame, in one write.
///
/// # Errors
///
/// Returns the error of the writer.
///
/// # Panics
///
/// Never: a [`Reply`] has only strings and integers, so it always encodes,
/// and it is far below 4 GiB.
///
/// The stdout and stderr of a run go in [`write_data`] frames.
pub fn write(writer: &mut dyn Write, reply: &Reply) -> io::Result<()> {
    let body = serde_json::to_vec(reply).expect("a reply always encodes");
    write_frame(writer, KIND_CONTROL, &body)
}

/// Writes `bytes` as frames of `kind`, each at most [`MAX_BODY`].
///
/// # Errors
///
/// Returns the error of the writer.
pub fn write_data(writer: &mut dyn Write, kind: u8, bytes: &[u8]) -> io::Result<()> {
    for chunk in bytes.chunks(MAX_BODY) {
        write_frame(writer, kind, chunk)?;
    }
    Ok(())
}

/// Writes `request` and `data` as one file frame, in one write.
///
/// # Errors
///
/// Returns the error of the writer.
///
/// # Panics
///
/// Never for a request whose data fits in a frame: an [`FsRequest`] always
/// encodes.
pub fn write_fs(writer: &mut dyn Write, request: &FsRequest, data: &[u8]) -> io::Result<()> {
    let json = serde_json::to_vec(request).expect("a file request always encodes");
    let length = u32::try_from(json.len()).expect("a file request is far below 4 GiB");
    let mut body = Vec::with_capacity(4 + json.len() + data.len());
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(&json);
    body.extend_from_slice(data);
    write_frame(writer, KIND_FS, &body)
}

fn write_frame(writer: &mut dyn Write, kind: u8, body: &[u8]) -> io::Result<()> {
    let length = u32::try_from(body.len()).expect("a frame body is at most MAX_BODY");
    let mut frame = Vec::with_capacity(HEADER + body.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.push(kind);
    frame.extend_from_slice(body);
    writer.write_all(&frame)
}

/// Reads one frame from the supervisor: a request, or a reply to a file
/// request with its bytes.
///
/// Returns `None` when the stream ends before a full header.
///
/// # Errors
///
/// Returns an error if the reader fails, the body is larger than
/// [`MAX_BODY`], the kind is not control or file, the body ends early, or
/// the body is not a valid request or reply.
pub fn read(reader: &mut dyn Read) -> io::Result<Option<Incoming>> {
    let mut header = [0u8; HEADER];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    if length > MAX_BODY {
        return Err(invalid("the frame body is too large"));
    }
    if header[4] != KIND_CONTROL && header[4] != KIND_FS {
        return Err(invalid("the frame kind is not known"));
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    if header[4] == KIND_CONTROL {
        return Ok(Some(Incoming::Request(serde_json::from_slice(&body)?)));
    }
    let json = body
        .get(..4)
        .map(|prefix| u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize)
        .filter(|json| 4 + json <= body.len())
        .ok_or_else(|| invalid("the file frame is too short"))?;
    let reply = serde_json::from_slice(&body[4..4 + json])?;
    Ok(Some(Incoming::Fs(reply, body[4 + json..].to_vec())))
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}
