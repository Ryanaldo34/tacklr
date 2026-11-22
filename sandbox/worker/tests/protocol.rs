//! The worker's request loop and frame format, over a real Unix socket pair.

use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::thread;

use serde_json::{Value, json};
use tacklr_sandbox_worker::frame::{self, KIND_CONTROL, MAX_BODY};
use tacklr_sandbox_worker::message::Enforcement;
use tacklr_sandbox_worker::serve::serve;

const ENFORCEMENT: Enforcement = Enforcement { landlock_abi: 6 };

fn start() -> (UnixStream, thread::JoinHandle<io::Result<()>>) {
    let (mut worker, supervisor) = UnixStream::pair().expect("socket pair");
    let handle = thread::spawn(move || serve(&mut worker, ENFORCEMENT));
    (supervisor, handle)
}

/// Reads one reply as the supervisor does: a control frame with a JSON body.
fn read_value(socket: &mut UnixStream) -> Value {
    let mut header = [0u8; 5];
    socket.read_exact(&mut header).expect("read header");
    assert_eq!(header[4], KIND_CONTROL);
    let mut body =
        vec![0u8; u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize];
    socket.read_exact(&mut body).expect("read body");
    serde_json::from_slice(&body).expect("decode reply")
}

/// Writes a request as the supervisor does: one control frame.
fn write_request(socket: &mut UnixStream, request: &Value) {
    let body = serde_json::to_vec(request).expect("encode request");
    write_raw(
        socket,
        u32::try_from(body.len()).unwrap(),
        KIND_CONTROL,
        &body,
    );
}

fn write_raw(socket: &mut UnixStream, length: u32, kind: u8, body: &[u8]) {
    let mut bytes = length.to_be_bytes().to_vec();
    bytes.push(kind);
    bytes.extend_from_slice(body);
    socket.write_all(&bytes).expect("write raw frame");
}

#[test]
fn worker_reports_ready_answers_pings_and_exits_on_stop() {
    let (mut supervisor, handle) = start();

    let ready = read_value(&mut supervisor);
    write_request(&mut supervisor, &json!({"type": "ping", "id": 7}));
    let pong = read_value(&mut supervisor);
    write_request(&mut supervisor, &json!({"type": "stop"}));

    assert_eq!(
        ready,
        json!({"type": "ready", "enforcement": {"landlock_abi": 6}})
    );
    assert_eq!(pong, json!({"type": "pong", "id": 7}));
    handle.join().expect("join").expect("serve ends cleanly");
}

#[test]
fn worker_exits_cleanly_when_the_supervisor_closes_the_socket() {
    let (mut supervisor, handle) = start();
    read_value(&mut supervisor);

    drop(supervisor);

    handle.join().expect("join").expect("serve ends cleanly");
}

#[test]
fn worker_refuses_a_request_it_does_not_know() {
    let (mut supervisor, handle) = start();
    read_value(&mut supervisor);

    write_request(&mut supervisor, &json!({"type": "run"}));

    let error = handle.join().expect("join").expect_err("serve fails");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn worker_refuses_a_frame_larger_than_the_limit() {
    let (mut supervisor, handle) = start();
    read_value(&mut supervisor);

    write_raw(
        &mut supervisor,
        u32::try_from(MAX_BODY + 1).unwrap(),
        KIND_CONTROL,
        b"",
    );

    let error = handle.join().expect("join").expect_err("serve fails");
    assert_eq!(error.to_string(), "the frame body is too large");
}

#[test]
fn worker_refuses_a_frame_of_unknown_kind() {
    let (mut supervisor, handle) = start();
    read_value(&mut supervisor);

    write_raw(&mut supervisor, 2, 9, b"{}");

    let error = handle.join().expect("join").expect_err("serve fails");
    assert_eq!(error.to_string(), "the frame kind is not known");
}

#[test]
fn worker_refuses_a_frame_whose_body_ends_early() {
    let (mut supervisor, handle) = start();
    read_value(&mut supervisor);

    write_raw(&mut supervisor, 10, KIND_CONTROL, b"{}");
    drop(supervisor);

    let error = handle.join().expect("join").expect_err("serve fails");
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
}

/// A reader whose device fails, to reach the read error of the frame header.
struct BrokenReader;

impl Read for BrokenReader {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::from(ErrorKind::ConnectionReset))
    }
}

#[test]
fn frame_read_returns_the_error_of_the_reader() {
    let error = frame::read(&mut BrokenReader).expect_err("read fails");

    assert_eq!(error.kind(), ErrorKind::ConnectionReset);
}

#[test]
fn worker_fails_when_the_supervisor_is_gone_before_ready() {
    let (mut worker, supervisor) = UnixStream::pair().expect("socket pair");
    drop(supervisor);

    let error = serve(&mut worker, ENFORCEMENT).expect_err("serve fails");

    assert_eq!(error.kind(), ErrorKind::BrokenPipe);
}
