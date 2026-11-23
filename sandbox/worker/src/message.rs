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
