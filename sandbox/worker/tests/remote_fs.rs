//! The workspace file system of a run, against the client's side of the
//! protocol over a real Unix socket pair.

use std::io::{Read, SeekFrom};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;

use tacklr_sandbox_worker::frame::{self, Incoming, KIND_FS};
use tacklr_sandbox_worker::message::FsReply;
use tacklr_sandbox_worker::remote_fs::{Link, RemoteFs};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use virtual_fs::{FileSystem, FsError};

mod common;
use common::Workspace;

/// A client that serves a workspace to a link.
struct Client {
    link: Arc<Link>,
    socket: UnixStream,
    served: thread::JoinHandle<Workspace>,
}

impl Client {
    /// Ends the link's socket and returns the client's workspace.
    fn end(self) -> Workspace {
        self.socket.shutdown(Shutdown::Both).expect("shutdown");
        self.served.join().expect("client")
    }
}

fn connect(workspace: Workspace) -> Client {
    let (worker, mut client) = UnixStream::pair().expect("socket pair");
    let socket = worker.try_clone().expect("clone");
    let link = Link::new(worker.try_clone().expect("clone"));
    let replies = link.clone();
    let mut reader = worker;
    thread::spawn(move || {
        while let Ok(Some(Incoming::Fs(reply, data))) = frame::read(&mut reader) {
            replies.deliver(reply, data);
        }
    });
    let served = thread::spawn(move || {
        let mut workspace = workspace;
        let mut header = [0u8; 5];
        while client.read_exact(&mut header).is_ok() {
            assert_eq!(header[4], KIND_FS);
            let mut body = vec![
                0u8;
                u32::from_be_bytes([header[0], header[1], header[2], header[3]])
                    as usize
            ];
            client.read_exact(&mut body).expect("body");
            workspace.answer(&mut client, &body);
        }
        workspace
    });
    Client {
        link,
        socket,
        served,
    }
}

/// Runs `work` on a runtime worker thread, where a file request may block.
fn on_runtime<T: Send + 'static>(work: impl Future<Output = T> + Send + 'static) -> T {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(runtime.spawn(work)).expect("task")
}

#[test]
fn remote_file_seeks_reads_resizes_and_removes_through_the_client() {
    let mut workspace = Workspace::default();
    workspace
        .files
        .insert("/a.txt".into(), b"hello world".to_vec());
    let client = connect(workspace);
    let fs = RemoteFs(client.link.clone());

    let (tail, size, ready, times) = on_runtime(async move {
        let mut file = fs
            .new_open_options()
            .read(true)
            .write(true)
            .open(Path::new("/a.txt"))
            .expect("open");
        file.seek(SeekFrom::End(-5)).await.expect("seek end");
        let mut tail = String::new();
        file.read_to_string(&mut tail).await.expect("read");
        file.seek(SeekFrom::Start(0)).await.expect("seek start");
        let mut head = [0u8; 4];
        file.read_exact(&mut head[..2]).await.expect("read ahead");
        file.read_exact(&mut head[2..])
            .await
            .expect("read from the read-ahead");
        assert_eq!(&head, b"hell");
        file.seek(SeekFrom::End(0)).await.expect("seek end again");
        file.seek(SeekFrom::Current(-5)).await.expect("seek back");
        let past_start = file.seek(SeekFrom::Current(-100)).await;
        let ready = (
            std::future::poll_fn(|cx| Pin::new(&mut *file).poll_read_ready(cx))
                .await
                .expect("read ready"),
            std::future::poll_fn(|cx| Pin::new(&mut *file).poll_write_ready(cx))
                .await
                .expect("write ready"),
        );
        file.set_len(5).expect("set len");
        file.flush().await.expect("flush");
        file.shutdown().await.expect("shutdown");
        let times = (
            file.last_accessed(),
            file.last_modified(),
            file.created_time(),
        );
        let size = file.size();
        file.unlink().expect("unlink");
        assert_eq!(
            past_start.expect_err("seek before the start").kind(),
            std::io::ErrorKind::InvalidInput
        );
        (tail, size, ready, times)
    });

    assert_eq!(tail, "world");
    assert_eq!(size, 5);
    assert_eq!(ready, (5, frame::MAX_BODY / 2));
    assert_eq!(times, (0, 0, 0));
    assert!(!client.end().files.contains_key("/a.txt"));
}

#[test]
fn remote_fs_makes_lists_and_removes_directories_through_the_client() {
    let client = connect(Workspace::default());
    let fs = RemoteFs(client.link.clone());

    let (listed, kind, link_error) = on_runtime(async move {
        fs.create_dir(Path::new("/d")).expect("mkdir");
        let listed: Vec<_> = fs
            .read_dir(Path::new("/"))
            .expect("read dir")
            .map(|entry| entry.expect("entry").path)
            .collect();
        let kind = fs.symlink_metadata(Path::new("/d")).expect("stat").is_dir();
        fs.remove_dir(Path::new("/d")).expect("rmdir");
        (
            listed,
            kind,
            fs.readlink(Path::new("/d")).expect_err("readlink"),
        )
    });

    assert_eq!(listed, [Path::new("/d")]);
    assert!(kind);
    assert_eq!(link_error, FsError::InvalidInput);
    assert!(client.end().dirs.is_empty());
}

#[test]
fn remote_fs_reports_the_clients_error_and_a_closed_link() {
    let client = connect(Workspace::default());
    let fs = RemoteFs(client.link.clone());
    let closed = client.link.clone();

    let (missing, after_close) = on_runtime(async move {
        let missing = fs.metadata(Path::new("/missing")).expect_err("missing");
        closed.deliver(
            FsReply {
                id: 999,
                ..FsReply::default()
            },
            Vec::new(),
        );
        closed.close();
        (
            missing,
            fs.metadata(Path::new("/missing")).expect_err("closed"),
        )
    });

    assert_eq!(missing, FsError::EntryNotFound);
    assert_eq!(after_close, FsError::NotConnected);
    client.end();
}
