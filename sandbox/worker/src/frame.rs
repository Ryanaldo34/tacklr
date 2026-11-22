//! Frames on the worker's socket.
//!
//! A frame is a 5-byte header and a body. The header is the body length as a
//! big-endian `u32`, then one kind byte. The only kind today is control: a
//! JSON body. The Go side (`sandbox/internal/wire`) uses the same format.

use std::io::{self, ErrorKind, Read, Write};

use crate::message::{Reply, Request};

/// The largest body a frame may have.
pub const MAX_BODY: usize = 1 << 20;

/// The kind byte of a frame with a JSON control message.
pub const KIND_CONTROL: u8 = 1;

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
pub fn write(writer: &mut dyn Write, reply: &Reply) -> io::Result<()> {
    let body = serde_json::to_vec(reply).expect("a reply always encodes");
    let length = u32::try_from(body.len()).expect("a reply is far below 4 GiB");
    let mut frame = Vec::with_capacity(HEADER + body.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.push(KIND_CONTROL);
    frame.extend_from_slice(&body);
    writer.write_all(&frame)
}

/// Reads one control frame and decodes its request.
///
/// Returns `None` when the stream ends before a full header.
///
/// # Errors
///
/// Returns an error if the reader fails, the body is larger than
/// [`MAX_BODY`], the kind is not [`KIND_CONTROL`], the body ends early, or the
/// body is not a valid request.
pub fn read(reader: &mut dyn Read) -> io::Result<Option<Request>> {
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
    if header[4] != KIND_CONTROL {
        return Err(invalid("the frame kind is not known"));
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    Ok(Some(serde_json::from_slice(&body)?))
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}
