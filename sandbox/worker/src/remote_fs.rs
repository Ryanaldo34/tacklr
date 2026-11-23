//! The workspace of a run: a file system whose files live with the client.
//!
//! Each call is one file request over the worker's socket, and waits for the
//! client's reply. Reads are ranged, with read-ahead; writes go to the
//! client as they come. The client keeps the open files and writes their
//! changes back when the worker closes them.

use std::collections::HashMap;
use std::io::{self, SeekFrom};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use virtual_fs::{
    DirEntry, FileOpener, FileSystem, FileType, FsError, Metadata, OpenOptions, OpenOptionsConfig,
    ReadDir, VirtualFile,
};

use crate::frame;
use crate::message::{FsOp, FsReply, FsRequest, Reply};

/// The most bytes a read asks for at once: the next reads of the file come
/// from this buffer.
const READ_AHEAD: u64 = 256 << 10;

/// A file request that waits for its reply and bytes.
type Waiting = SyncSender<(FsReply, Vec<u8>)>;

/// The worker's socket as the runs use it: one writer for every frame, and
/// the file requests that wait for their reply.
#[derive(Debug)]
pub struct Link {
    writer: Mutex<UnixStream>,
    pending: Mutex<Option<HashMap<u64, Waiting>>>,
    next: AtomicU64,
}

impl Link {
    /// A link that writes to `writer`.
    #[must_use]
    pub fn new(writer: UnixStream) -> Arc<Self> {
        Arc::new(Self {
            writer: Mutex::new(writer),
            pending: Mutex::new(Some(HashMap::new())),
            next: AtomicU64::new(0),
        })
    }

    /// Writes a control reply.
    ///
    /// # Errors
    ///
    /// Returns the error of the socket.
    ///
    /// # Panics
    ///
    /// Panics if a thread panicked while it held the link's lock.
    pub fn reply(&self, reply: &Reply) -> io::Result<()> {
        frame::write(&mut *self.writer.lock().expect("socket writer"), reply)
    }

    /// Writes output bytes as frames of `kind`.
    ///
    /// # Errors
    ///
    /// Returns the error of the socket.
    ///
    /// # Panics
    ///
    /// Panics if a thread panicked while it held the link's lock.
    pub fn output(&self, kind: u8, bytes: &[u8]) -> io::Result<()> {
        frame::write_data(
            &mut *self.writer.lock().expect("socket writer"),
            kind,
            bytes,
        )
    }

    /// Gives a file reply to the request that waits for it. A reply that no
    /// request waits for is dropped.
    ///
    /// # Panics
    ///
    /// Panics if a thread panicked while it held the link's lock.
    pub fn deliver(&self, reply: FsReply, data: Vec<u8>) {
        let waiting = self
            .pending
            .lock()
            .expect("pending")
            .as_mut()
            .and_then(|pending| pending.remove(&reply.id));
        if let Some(waiting) = waiting {
            let _ = waiting.send((reply, data));
        }
    }

    /// Fails every request that waits, and every later one: the socket has
    /// ended.
    ///
    /// # Panics
    ///
    /// Panics if a thread panicked while it held the link's lock.
    pub fn close(&self) {
        self.pending.lock().expect("pending").take();
    }

    fn call(&self, op: FsOp, data: &[u8]) -> Result<(FsReply, Vec<u8>), FsError> {
        let id = self.next.fetch_add(1, Ordering::SeqCst) + 1;
        let (sender, receiver) = sync_channel(1);
        self.pending
            .lock()
            .expect("pending")
            .as_mut()
            .ok_or(FsError::NotConnected)?
            .insert(id, sender);
        let request = FsRequest { kind: "fs", id, op };
        frame::write_fs(
            &mut *self.writer.lock().expect("socket writer"),
            &request,
            data,
        )
        .map_err(|_| FsError::NotConnected)?;
        let (reply, data) =
            tokio::task::block_in_place(|| receiver.recv()).map_err(|_| FsError::NotConnected)?;
        match reply.error.as_deref() {
            None => Ok((reply, data)),
            Some(errno) => Err(fs_error(errno)),
        }
    }
}

/// The file system errors of the errno names a client replies with. Any
/// other name is an I/O error.
const ERRNOS: [(&str, FsError); 9] = [
    ("ENOENT", FsError::EntryNotFound),
    ("EACCES", FsError::PermissionDenied),
    ("EPERM", FsError::PermissionDenied),
    ("EROFS", FsError::PermissionDenied),
    ("EEXIST", FsError::AlreadyExists),
    ("EISDIR", FsError::NotAFile),
    ("ENOTDIR", FsError::BaseNotDirectory),
    ("EINVAL", FsError::InvalidInput),
    ("EFBIG", FsError::InvalidInput),
];

fn fs_error(errno: &str) -> FsError {
    ERRNOS
        .iter()
        .find_map(|(name, error)| (*name == errno).then_some(*error))
        .unwrap_or(FsError::IOError)
}

fn metadata(reply: &FsReply) -> Metadata {
    Metadata {
        ft: FileType {
            dir: reply.is_dir,
            file: !reply.is_dir,
            ..FileType::default()
        },
        accessed: reply.modified,
        created: reply.modified,
        modified: reply.modified,
        len: reply.size,
    }
}

fn text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// The file system of a run's workspace.
#[derive(Debug, Clone)]
pub struct RemoteFs(pub Arc<Link>);

impl FileSystem for RemoteFs {
    fn readlink(&self, _path: &Path) -> virtual_fs::Result<PathBuf> {
        Err(FsError::InvalidInput)
    }

    fn read_dir(&self, path: &Path) -> virtual_fs::Result<ReadDir> {
        let (reply, _) = self.0.call(FsOp::ReadDir { path: text(path) }, &[])?;
        let entries = reply
            .entries
            .into_iter()
            .map(|entry| DirEntry {
                path: path.join(&entry.name),
                metadata: Ok(metadata(&FsReply {
                    is_dir: entry.is_dir,
                    ..FsReply::default()
                })),
            })
            .collect();
        Ok(ReadDir::new(entries))
    }

    fn create_dir(&self, path: &Path) -> virtual_fs::Result<()> {
        self.0.call(FsOp::Mkdir { path: text(path) }, &[]).map(drop)
    }

    fn remove_dir(&self, path: &Path) -> virtual_fs::Result<()> {
        self.remove_file(path)
    }

    fn rename<'a>(
        &'a self,
        from: &'a Path,
        to: &'a Path,
    ) -> Pin<Box<dyn Future<Output = virtual_fs::Result<()>> + Send + 'a>> {
        let op = FsOp::Rename {
            from: text(from),
            to: text(to),
        };
        Box::pin(async move { self.0.call(op, &[]).map(drop) })
    }

    fn metadata(&self, path: &Path) -> virtual_fs::Result<Metadata> {
        let (reply, _) = self.0.call(FsOp::Stat { path: text(path) }, &[])?;
        Ok(metadata(&reply))
    }

    fn symlink_metadata(&self, path: &Path) -> virtual_fs::Result<Metadata> {
        self.metadata(path)
    }

    fn remove_file(&self, path: &Path) -> virtual_fs::Result<()> {
        self.0
            .call(FsOp::Remove { path: text(path) }, &[])
            .map(drop)
    }

    fn new_open_options(&self) -> OpenOptions<'_> {
        OpenOptions::new(self)
    }
}

impl FileOpener for RemoteFs {
    fn open(
        &self,
        path: &Path,
        conf: &OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn VirtualFile + Send + Sync + 'static>> {
        let op = FsOp::Open {
            path: text(path),
            write: conf.write,
            create: conf.create || conf.create_new,
            truncate: conf.truncate,
            append: conf.append,
        };
        let (reply, _) = self.0.call(op, &[])?;
        Ok(Box::new(RemoteFile {
            link: self.0.clone(),
            path: path.to_path_buf(),
            handle: reply.handle,
            size: reply.size,
            position: 0,
            cached: (0, Vec::new()),
        }))
    }
}

/// An open file of a run's workspace.
#[derive(Debug)]
struct RemoteFile {
    link: Arc<Link>,
    path: PathBuf,
    handle: u64,
    size: u64,
    position: u64,
    cached: (u64, Vec<u8>),
}

impl RemoteFile {
    fn read_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let (start, bytes) = &self.cached;
        let end = *start + bytes.len() as u64;
        if offset < *start || offset >= end {
            let op = FsOp::Read {
                handle: self.handle,
                offset,
                length: READ_AHEAD.max(length as u64),
            };
            let (_, data) = self.link.call(op, &[])?;
            self.cached = (offset, data);
        }
        let (start, bytes) = &self.cached;
        let from = usize::try_from(offset - start).expect("the cached range is in memory");
        Ok(bytes[from..(from + length).min(bytes.len())].to_vec())
    }
}

impl Drop for RemoteFile {
    fn drop(&mut self) {
        let _ = self.link.call(
            FsOp::Close {
                handle: self.handle,
            },
            &[],
        );
    }
}

impl VirtualFile for RemoteFile {
    fn last_accessed(&self) -> u64 {
        0
    }

    fn last_modified(&self) -> u64 {
        0
    }

    fn created_time(&self) -> u64 {
        0
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn set_len(&mut self, new_size: u64) -> virtual_fs::Result<()> {
        let op = FsOp::Truncate {
            handle: self.handle,
            size: new_size,
        };
        self.link.call(op, &[])?;
        self.size = new_size;
        self.cached = (0, Vec::new());
        Ok(())
    }

    fn unlink(&mut self) -> virtual_fs::Result<()> {
        self.link
            .call(
                FsOp::Remove {
                    path: text(&self.path),
                },
                &[],
            )
            .map(drop)
    }

    fn poll_read_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(
            usize::try_from(self.size.saturating_sub(self.position)).unwrap_or(usize::MAX)
        ))
    }

    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(frame::MAX_BODY / 2))
    }
}

impl AsyncRead for RemoteFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let position = self.position;
        let bytes = self.read_at(position, buf.remaining())?;
        self.position += bytes.len() as u64;
        buf.put_slice(&bytes);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for RemoteFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let take = buf.len().min(frame::MAX_BODY / 2);
        let op = FsOp::Write {
            handle: self.handle,
            offset: self.position,
        };
        let (reply, _) = self.link.call(op, &buf[..take])?;
        self.position += take as u64;
        self.size = reply.size;
        self.cached = (0, Vec::new());
        Poll::Ready(Ok(take))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for RemoteFile {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let next = match position {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(delta) => self.size.checked_add_signed(delta),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
        };
        self.position = next.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}
