//! The messages between the supervisor and the worker.
//!
//! Each message is a JSON object with a `type` field. The Go side
//! (`sandbox/internal/wire`) has the same messages.

use serde::{Deserialize, Serialize};

/// A message from the supervisor to the worker.
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    /// Asks the worker to answer with a [`Reply::Pong`] with the same ID.
    Ping {
        /// The ID the worker puts on its pong.
        id: u64,
    },
    /// Asks the worker to exit with code 0.
    Stop,
    /// Asks the worker to run a bash script in its workspace. The worker
    /// sends the script's output in stdout and stderr frames, then
    /// [`Reply::Done`] with the same ID.
    Run {
        /// The ID the worker puts on its done reply.
        id: u64,
        /// The script, run with `bash -c`.
        script: String,
    },
}

/// A message from the worker to the supervisor.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    /// The first message: the sandbox is applied.
    Ready {
        /// What the worker enforces.
        enforcement: Enforcement,
    },
    /// The answer to a [`Request::Ping`].
    Pong {
        /// The ID of the ping.
        id: u64,
    },
    /// The end of a [`Request::Run`], after all its output.
    Done {
        /// The ID of the run.
        id: u64,
        /// The script's exit code, or -1 when `error` is set.
        code: i32,
        /// Why the runtime could not run the script, if it could not.
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

/// What the worker enforces on itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Enforcement {
    /// The Landlock ABI version the ruleset uses on this kernel. It is
    /// [`jail::MIN_LANDLOCK_ABI`](crate::jail::MIN_LANDLOCK_ABI) or more.
    pub landlock_abi: i32,
}

/// A file request from the worker to the client that serves its workspace,
/// during a run. It travels in a [`KIND_FS`](crate::frame::KIND_FS) frame,
/// with the bytes of a write after it.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct FsRequest {
    /// Always `fs`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// The ID the client puts on its reply.
    pub id: u64,
    /// The operation.
    #[serde(flatten)]
    pub op: FsOp,
}

/// A file operation. Paths are below the workspace root, such as `/work/a.txt`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FsOp {
    /// The file or directory at `path`.
    Stat {
        /// The path.
        path: String,
    },
    /// The entries of the directory at `path`.
    ReadDir {
        /// The path.
        path: String,
    },
    /// Makes the directory at `path`.
    Mkdir {
        /// The path.
        path: String,
    },
    /// Removes the file or empty directory at `path`.
    Remove {
        /// The path.
        path: String,
    },
    /// Moves a file.
    Rename {
        /// The path of the file.
        from: String,
        /// The new path.
        to: String,
    },
    /// Opens the file at `path`; the reply has its handle and size.
    Open {
        /// The path.
        path: String,
        /// Open for writing.
        write: bool,
        /// Make the file if it does not exist.
        create: bool,
        /// Empty the file.
        truncate: bool,
        /// Write at the end.
        append: bool,
    },
    /// Reads up to `length` bytes at `offset`; the bytes follow the reply.
    Read {
        /// The handle of an open file.
        handle: u64,
        /// The offset.
        offset: u64,
        /// The most bytes to read.
        length: u64,
    },
    /// Writes the bytes after the request at `offset`.
    Write {
        /// The handle of an open file.
        handle: u64,
        /// The offset.
        offset: u64,
    },
    /// Sets the size of an open file.
    Truncate {
        /// The handle of an open file.
        handle: u64,
        /// The new size.
        size: u64,
    },
    /// Writes an open file's changes back and closes it.
    Close {
        /// The handle of an open file.
        handle: u64,
    },
}

/// The client's reply to an [`FsRequest`].
#[derive(Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FsReply {
    /// Always `fs_reply`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The ID of the request.
    pub id: u64,
    /// The errno name (`ENOENT`, `EROFS`, ...) when the request failed.
    #[serde(default)]
    pub error: Option<String>,
    /// The entry is a directory (stat).
    #[serde(default)]
    pub is_dir: bool,
    /// The size of the file (stat, open, write, truncate).
    #[serde(default)]
    pub size: u64,
    /// The modification time in nanoseconds since the epoch (stat).
    #[serde(default)]
    pub modified: u64,
    /// The directory entries (`read_dir`).
    #[serde(default)]
    pub entries: Vec<FsEntry>,
    /// The handle of the opened file (open).
    #[serde(default)]
    pub handle: u64,
}

/// One entry of a directory.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FsEntry {
    /// The entry's name.
    pub name: String,
    /// The entry is a directory.
    pub is_dir: bool,
}
