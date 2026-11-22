//! The sandbox the worker applies to itself.
//!
//! The supervisor already put the worker in new namespaces. This module adds
//! the layers that the worker applies to itself, in this order:
//!
//! 1. Close the inherited fds that the worker must not keep.
//! 2. Landlock: no filesystem access, no TCP bind or connect, and no abstract
//!    Unix socket or signal outside the worker's own Landlock domain.
//! 3. seccomp: an allow-list of system calls. Any other system call kills the
//!    process with `SIGSYS`.
//!
//! Both Landlock and seccomp set `PR_SET_NO_NEW_PRIVS` before they apply.
//! Call [`enter`] while the process has only one thread, so that every thread
//! it starts later gets the same filter.

use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::os::fd::RawFd;

use landlock::{
    ABI, Access, AccessFs, AccessNet, CompatLevel, Compatible, Ruleset, RulesetAttr, Scope,
};
use seccompiler::{BpfProgram, SeccompAction, SeccompFilter};

use crate::message::Enforcement;

/// The oldest Landlock ABI the worker accepts (Linux 6.12). On an older
/// kernel, [`enter`] fails and the worker does not start.
pub const MIN_LANDLOCK_ABI: ABI = ABI::V6;

/// The newest Landlock ABI the worker knows. The worker also handles the
/// access rights of the ABIs after [`MIN_LANDLOCK_ABI`], up to this one, when
/// the kernel has them.
const MAX_LANDLOCK_ABI: ABI = ABI::V10;

/// The system calls the worker may make after [`enter`]: socket I/O, memory
/// allocation, the futex of the Rust runtime, signal return and exit. Every
/// other system call kills the process.
const ALLOWED_SYSCALLS: &[libc::c_long] = &[
    libc::SYS_read,
    libc::SYS_write,
    libc::SYS_recvfrom,
    libc::SYS_sendto,
    libc::SYS_close,
    libc::SYS_brk,
    libc::SYS_mmap,
    libc::SYS_munmap,
    libc::SYS_mremap,
    libc::SYS_madvise,
    libc::SYS_futex,
    libc::SYS_rt_sigreturn,
    libc::SYS_sigaltstack,
    libc::SYS_exit,
    libc::SYS_exit_group,
];

/// An error that stopped the worker from applying its sandbox.
pub type JailError = Box<dyn Error + Send + Sync>;

/// Applies the whole sandbox to the calling process: closes every fd above
/// `last_kept_fd`, then applies Landlock, then seccomp.
///
/// # Errors
///
/// Returns an error if one of the layers cannot be applied. The process must
/// then exit and do no work.
pub fn enter(last_kept_fd: RawFd) -> Result<Enforcement, JailError> {
    close_fds_after(last_kept_fd)?;
    let landlock_abi = restrict_filesystem_and_network()?;
    install_syscall_filter()?;
    Ok(Enforcement { landlock_abi })
}

/// Applies the Landlock ruleset to the calling thread: no filesystem access,
/// no TCP bind or connect, and abstract Unix sockets and signals scoped to
/// the worker's own Landlock domain. Returns the Landlock ABI that the
/// ruleset uses on this kernel.
///
/// # Errors
///
/// Returns an error if the kernel does not have [`MIN_LANDLOCK_ABI`], or if
/// a Landlock system call fails.
pub fn restrict_filesystem_and_network() -> Result<i32, JailError> {
    let status = Ruleset::default()
        .set_compatibility(CompatLevel::HardRequirement)
        .handle_access(AccessFs::from_all(MIN_LANDLOCK_ABI))?
        .handle_access(AccessNet::from_all(MIN_LANDLOCK_ABI))?
        .scope(Scope::from_all(MIN_LANDLOCK_ABI))?
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(MAX_LANDLOCK_ABI))?
        .handle_access(AccessNet::from_all(MAX_LANDLOCK_ABI))?
        .create()?
        .restrict_self()?;
    Ok(ABI::from(status.landlock) as i32)
}

/// Installs the seccomp allow-list ([`ALLOWED_SYSCALLS`]) on the calling
/// thread. A system call outside the list kills the whole process with
/// `SIGSYS`.
///
/// # Errors
///
/// Returns an error if the CPU architecture is not supported or the kernel
/// refuses the filter.
pub fn install_syscall_filter() -> Result<(), JailError> {
    let rules = ALLOWED_SYSCALLS
        .iter()
        .map(|&syscall| (syscall, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let filter = SeccompFilter::new(
        rules,
        SeccompAction::KillProcess,
        SeccompAction::Allow,
        std::env::consts::ARCH.try_into()?,
    )?;
    let program: BpfProgram = filter.try_into()?;
    seccompiler::apply_filter(&program)?;
    Ok(())
}

fn close_fds_after(last_kept_fd: RawFd) -> io::Result<()> {
    let first = libc::c_uint::try_from(last_kept_fd + 1)
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: close_range takes no pointers. The caller keeps the fds up to
    // last_kept_fd, and nothing in the process owns a higher fd at startup.
    let result = unsafe { libc::close_range(first, libc::c_uint::MAX, 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
