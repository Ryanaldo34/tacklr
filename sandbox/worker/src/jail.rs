//! The sandbox the worker applies to itself.
//!
//! The supervisor already put the worker in new namespaces. This module adds
//! the layers that the worker applies to itself, in this order:
//!
//! 1. Close the inherited fds that the worker must not keep.
//! 2. Landlock: no filesystem access, no TCP bind or connect, and no abstract
//!    Unix socket or signal outside the worker's own Landlock domain.
//! 3. seccomp: an allow-list of system calls. Any other system call kills the
//!    process with `SIGSYS`. No call may map executable memory, start a
//!    process or a namespace, or open a file.
//!
//! Both Landlock and seccomp set `PR_SET_NO_NEW_PRIVS` before they apply.
//! Landlock and seccomp apply to the calling thread and to the threads it
//! starts later, so call [`enter`] while the process has only one thread.

use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::os::fd::RawFd;

use landlock::{
    ABI, Access, AccessFs, AccessNet, CompatLevel, Compatible, Ruleset, RulesetAttr, Scope,
};
use seccompiler::{
    BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
    SeccompRule,
};

use crate::message::Enforcement;

/// The oldest Landlock ABI the worker accepts (Linux 6.12). On an older
/// kernel, [`enter`] fails and the worker does not start.
pub const MIN_LANDLOCK_ABI: ABI = ABI::V6;

/// The newest Landlock ABI the worker knows. The worker also handles the
/// access rights of the ABIs after [`MIN_LANDLOCK_ABI`], up to this one, when
/// the kernel has them.
const MAX_LANDLOCK_ABI: ABI = ABI::V10;

/// The system calls the worker may make after [`enter`] with any
/// arguments: socket I/O, memory, the futexes, threads and signals of the
/// async runtime and of Wasmer's trap handlers, and exit.
/// [`syscall_rules`] adds the calls allowed only with some arguments.
const ALLOWED_SYSCALLS: &[libc::c_long] = &[
    libc::SYS_read,
    libc::SYS_write,
    libc::SYS_recvfrom,
    libc::SYS_sendto,
    libc::SYS_close,
    libc::SYS_brk,
    libc::SYS_munmap,
    libc::SYS_mremap,
    libc::SYS_madvise,
    libc::SYS_futex,
    libc::SYS_getrandom,
    libc::SYS_gettid,
    libc::SYS_sched_yield,
    libc::SYS_sched_getaffinity,
    libc::SYS_set_robust_list,
    libc::SYS_rseq,
    libc::SYS_rt_sigaction,
    libc::SYS_rt_sigprocmask,
    libc::SYS_rt_sigreturn,
    libc::SYS_sigaltstack,
    libc::SYS_exit,
    libc::SYS_exit_group,
];

/// The namespace flags of `clone`. A thread of the worker never makes one.
const NAMESPACE_FLAGS: u64 = (libc::CLONE_NEWUSER
    | libc::CLONE_NEWNS
    | libc::CLONE_NEWPID
    | libc::CLONE_NEWNET
    | libc::CLONE_NEWIPC
    | libc::CLONE_NEWUTS
    | libc::CLONE_NEWCGROUP) as u64;

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

/// Installs the seccomp filters on the calling thread. Two filters make
/// `openat` fail with `EACCES`, as Landlock does, and `clone3` with
/// `ENOSYS`, so that threads start with `clone`, whose flags the allow-list
/// can check. The allow-list of [`syscall_rules`] goes last, because each
/// install first sets `PR_SET_NO_NEW_PRIVS`, which the allow-list refuses.
/// Any system call outside it kills the whole process with `SIGSYS`.
///
/// # Errors
///
/// Returns an error if the CPU architecture is not supported or the kernel
/// refuses a filter.
pub fn install_syscall_filter() -> Result<(), JailError> {
    let arch = std::env::consts::ARCH.try_into()?;
    let filters = [
        SeccompFilter::new(
            [(libc::SYS_openat, Vec::new())].into(),
            SeccompAction::Allow,
            SeccompAction::Errno(libc::EACCES as u32),
            arch,
        )?,
        SeccompFilter::new(
            [(libc::SYS_clone3, Vec::new())].into(),
            SeccompAction::Allow,
            SeccompAction::Errno(libc::ENOSYS as u32),
            arch,
        )?,
        SeccompFilter::new(
            syscall_rules()?,
            SeccompAction::KillProcess,
            SeccompAction::Allow,
            arch,
        )?,
    ];
    for filter in filters {
        let program: BpfProgram = filter.try_into()?;
        seccompiler::apply_filter(&program)?;
    }
    Ok(())
}

/// The allow-list: [`ALLOWED_SYSCALLS`] with any arguments, plus `mmap` and
/// `mprotect` without `PROT_EXEC`, `clone` of a thread without namespace
/// flags, `prctl` to name a thread, `fcntl` to read fd flags or duplicate
/// the socket, and `openat` and `clone3`, which the errno filters refuse.
fn syscall_rules() -> Result<BTreeMap<i64, Vec<SeccompRule>>, JailError> {
    let arg = |index: u8, op: SeccompCmpOp, value: u64| {
        SeccompCondition::new(index, SeccompCmpArgLen::Qword, op, value)
    };
    let no_exec = SeccompRule::new(vec![arg(
        2,
        SeccompCmpOp::MaskedEq(libc::PROT_EXEC as u64),
        0,
    )?])?;
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = ALLOWED_SYSCALLS
        .iter()
        .chain([libc::SYS_openat, libc::SYS_clone3].iter())
        .map(|&syscall| (syscall, Vec::new()))
        .collect();
    rules.insert(libc::SYS_mmap, vec![no_exec.clone()]);
    rules.insert(libc::SYS_mprotect, vec![no_exec]);
    rules.insert(
        libc::SYS_clone,
        vec![SeccompRule::new(vec![
            arg(
                0,
                SeccompCmpOp::MaskedEq(libc::CLONE_THREAD as u64),
                libc::CLONE_THREAD as u64,
            )?,
            arg(0, SeccompCmpOp::MaskedEq(NAMESPACE_FLAGS), 0)?,
        ])?],
    );
    rules.insert(
        libc::SYS_prctl,
        vec![SeccompRule::new(vec![arg(
            0,
            SeccompCmpOp::Eq,
            libc::PR_SET_NAME as u64,
        )?])?],
    );
    rules.insert(
        libc::SYS_fcntl,
        vec![
            SeccompRule::new(vec![arg(1, SeccompCmpOp::Eq, libc::F_GETFD as u64)?])?,
            SeccompRule::new(vec![arg(
                1,
                SeccompCmpOp::Eq,
                libc::F_DUPFD_CLOEXEC as u64,
            )?])?,
        ],
    );
    Ok(rules)
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
