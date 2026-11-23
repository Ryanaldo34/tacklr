//! The Tacklr sandbox worker: one process per sandbox, started by the Go
//! supervisor (`sandbox/supervisor`).
//!
//! The supervisor starts the worker in new user, pid, mount, network, IPC,
//! UTS and cgroup namespaces, with an empty environment and one end of a
//! Unix socket pair as fd 3. The worker then:
//!
//! 1. Closes every other inherited fd above 3 ([`jail::enter`]).
//! 2. Applies a Landlock ruleset with no allowed paths and no allowed ports,
//!    and scopes abstract Unix sockets and signals to its own domain.
//! 3. Installs a seccomp allow-list. A system call outside the list kills
//!    the process with `SIGSYS`.
//! 4. Sends `ready` with its [`message::Enforcement`] report and answers the
//!    supervisor's requests ([`serve::serve`]).
//!
//! If the worker cannot apply the sandbox, it exits with code 125 and does
//! not serve.

pub mod frame;
pub mod jail;
pub mod message;
pub mod remote_fs;
pub mod serve;
pub mod shell;
