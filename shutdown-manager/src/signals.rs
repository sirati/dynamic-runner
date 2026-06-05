//! Single concern: register OS signal handlers that flip a
//! [`ShutdownFlag`] AND record which signal arrived from which sender,
//! so the manager can report *why* it tore down and *who* asked.
//!
//! Both SIGTERM and SIGCONT funnel to the same flag. SIGCONT is used
//! because SLURM's `--signal` can deliver it (some operators prefer
//! SIGCONT over SIGTERM to avoid clashing with workload signal
//! handlers); accepting both lets the wrapper script choose.
//!
//! ## Why `SA_SIGINFO` (and not `signal_hook::low_level::register`)
//!
//! The diagnostic this module owns is "who killed us": on a SLURM
//! TIMEOUT/scancel the SIGTERM comes from `slurmstepd`; from the
//! wrapper/coordinator it comes from the wrapper PID; an OOM-kill comes
//! from the kernel (`si_pid == 0`). Distinguishing them requires the
//! `siginfo_t.si_pid` the kernel fills in for the handler, which is only
//! available when the handler is installed with `SA_SIGINFO`.
//! `signal_hook::low_level::register` hands back a bare `FnMut()` with no
//! siginfo (and the extended-siginfo feature is deliberately disabled to
//! keep the static binary minimal), so this module installs the handler
//! directly via `libc::sigaction` with `SA_SIGINFO`.
//!
//! ## Async-signal-safety
//!
//! The handler runs in signal context, so it does ONLY async-signal-safe
//! work: it reads `siginfo_t.si_pid` and stores the signal number, the
//! sender PID, and a "captured" marker into process-global atomics, then
//! sets the [`ShutdownFlag`] (an atomic store). No allocation, no `/proc`
//! reads, no locks. Resolving the sender PID to a binary name + command
//! line touches `/proc` and allocates, so it happens OUTSIDE the handler
//! in [`describe_last_signal`], called from the poll path once the flag
//! is observed.

use crate::shutdown_flag::ShutdownFlag;
use std::io;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

/// The flag the handler sets. Installed once via [`install`]; the
/// `extern "C"` handler reaches it here because it cannot carry a
/// captured environment. One shutdown manager per process ⇒ one flag.
static SHUTDOWN_FLAG: OnceLock<ShutdownFlag> = OnceLock::new();

/// Last acted-on signal's source, recorded by the handler and read by
/// [`describe_last_signal`]. `signo`/`sender_pid` are only meaningful
/// once `captured` is true. Process-global because the `extern "C"`
/// handler has no state to thread these through.
static LAST_SIGNO: AtomicI32 = AtomicI32::new(0);
static LAST_SENDER_PID: AtomicI32 = AtomicI32::new(0);
static SOURCE_CAPTURED: AtomicBool = AtomicBool::new(false);

/// Install SIGTERM + SIGCONT handlers that record the source and set
/// `flag`.
///
/// Returns Err if `sigaction(2)` fails for either signal. On success the
/// handlers stay active for the process lifetime; we don't bother
/// unregistering because the binary exits immediately after the
/// shutdown sequence.
pub fn install(flag: &ShutdownFlag) -> io::Result<()> {
    // Store the flag globally so the parameterless `extern "C"` handler
    // can reach it. `set` only fails if called twice — install is called
    // exactly once from `main`, so a second call is a programming error
    // we surface rather than silently ignore.
    if SHUTDOWN_FLAG.set(flag.clone()).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "signals::install called more than once",
        ));
    }
    install_for(libc::SIGTERM)?;
    install_for(libc::SIGCONT)?;
    Ok(())
}

/// Install the `SA_SIGINFO` handler for one signal number.
fn install_for(signum: i32) -> io::Result<()> {
    // SAFETY: zero-initialised `sigaction` is a valid empty action; we
    // then fill the SA_SIGINFO handler + flags before passing it to the
    // kernel. `std::mem::zeroed` is the documented idiom for this
    // C-struct-with-opaque-fields type.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    // `sa_sigaction` is a `sighandler_t` (a `usize`); the kernel
    // dispatches the three-arg form because `SA_SIGINFO` is set below.
    // Cast through a fn-pointer-as-thin-pointer first per the lint.
    action.sa_sigaction = handler as *const () as usize;
    // SA_SIGINFO: deliver the three-arg form so `siginfo_t.si_pid` is
    // populated. SA_RESTART: restart interruptible syscalls rather than
    // failing them with EINTR — the poll loop sleeps/execs around these
    // handlers and must not see spurious EINTR.
    action.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
    // Empty mask: we don't block other signals while in the handler; the
    // handler is trivially short (atomic stores only).
    // SAFETY: sigemptyset writes only into the provided sigset_t.
    unsafe {
        libc::sigemptyset(&mut action.sa_mask);
    }
    // SAFETY: `sigaction` installs `action` for `signum`; passing a null
    // old-action pointer discards the previous disposition (we never
    // restore it — the process exits after shutdown). `action` outlives
    // the call.
    let rc = unsafe { libc::sigaction(signum, &action, std::ptr::null_mut()) };
    match rc {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// The `SA_SIGINFO` signal handler. Async-signal-safe: it reads
/// `siginfo_t.si_pid` and performs atomic stores only.
///
/// `_context` is the `ucontext_t*` the kernel passes as the third arg;
/// unused, but the signature must match `void (*)(int, siginfo_t*,
/// void*)` for `SA_SIGINFO`.
extern "C" fn handler(signo: i32, info: *mut libc::siginfo_t, _context: *mut libc::c_void) {
    // SAFETY: under SA_SIGINFO the kernel guarantees `info` points at a
    // valid `siginfo_t` for the duration of the handler. `si_pid()`
    // reads the sender PID the kernel filled in (0 for kernel-originated
    // signals such as the OOM-killer). A null `info` cannot occur under
    // SA_SIGINFO, but we guard defensively since deref is unsafe.
    let sender_pid: i32 = if info.is_null() {
        -1
    } else {
        unsafe { (*info).si_pid() }
    };
    // Record signo + sender BEFORE marking captured, so a reader that
    // sees `captured == true` always sees the matching signo/pid
    // (Release/Acquire pairing on the marker below).
    LAST_SIGNO.store(signo, Ordering::Relaxed);
    LAST_SENDER_PID.store(sender_pid, Ordering::Relaxed);
    SOURCE_CAPTURED.store(true, Ordering::Release);
    // Set the shutdown flag last; the poll loop wakes on it and then
    // resolves the source via `describe_last_signal`.
    if let Some(flag) = SHUTDOWN_FLAG.get() {
        flag.set();
    }
}

/// Human-readable name for a signal number (the ones this manager
/// installs, plus the common terminating signals an operator might see).
/// Falls back to the bare number so an unexpected signal still prints
/// something useful.
fn signal_name(signo: i32) -> String {
    match signo {
        libc::SIGTERM => "SIGTERM".to_string(),
        libc::SIGCONT => "SIGCONT".to_string(),
        libc::SIGINT => "SIGINT".to_string(),
        libc::SIGHUP => "SIGHUP".to_string(),
        libc::SIGQUIT => "SIGQUIT".to_string(),
        libc::SIGKILL => "SIGKILL".to_string(),
        other => format!("signal {}", other),
    }
}

/// Resolve a sender PID to a one-line description: the signal name, the
/// sender PID, and — when resolvable — the sender's binary name plus its
/// full NUL-joined command line.
///
/// `reason` is the caller's statement of *why* the manager is tearing
/// down (e.g. "shutdown flag set by an incoming signal"); it is appended
/// so every emitted line carries WHAT + WHY uniformly.
///
/// Returns `None` when no signal source was ever captured (the
/// SIGNAL_SHUTDOWN branch was reached for a non-signal reason — wrapper
/// PID gone — so there is no sender to report).
///
/// `proc_root` is the filesystem root to resolve `/proc/<pid>/{comm,
/// cmdline}` under; production passes `/proc`, tests inject a fixture
/// dir. Kept a parameter (not hard-coded) so the resolution is unit-
/// testable against a fabricated `/proc` tree.
pub fn describe_last_signal(reason: &str) -> Option<String> {
    describe_last_signal_in("/proc", reason)
}

/// Testable core of [`describe_last_signal`] with an injectable `/proc`
/// root. See that function for the public contract.
pub fn describe_last_signal_in(proc_root: &str, reason: &str) -> Option<String> {
    // Acquire pairs with the handler's Release store on the marker: once
    // we observe `captured`, the signo/pid stores are visible too.
    if !SOURCE_CAPTURED.load(Ordering::Acquire) {
        return None;
    }
    let signo = LAST_SIGNO.load(Ordering::Relaxed);
    let sender_pid = LAST_SENDER_PID.load(Ordering::Relaxed);
    Some(format_signal_source(proc_root, signo, sender_pid, reason))
}

/// Pure formatter: turn (signal, sender pid, reason) into the operator
/// line, resolving the sender from `proc_root`. Split out so it can be
/// unit-tested directly over a known pid (e.g. the test process itself)
/// without going through the global handler state.
pub fn format_signal_source(
    proc_root: &str,
    signo: i32,
    sender_pid: i32,
    reason: &str,
) -> String {
    let name = signal_name(signo);
    let sender = describe_sender(proc_root, sender_pid);
    format!(
        "received {} from {} -> initiating teardown because {}",
        name, sender, reason
    )
}

/// Describe the sender PID. Handles the three cases the operator cares
/// about:
///   * `si_pid == 0` — kernel-originated (OOM-killer, or a kernel signal
///     with no userspace sender);
///   * a resolvable PID — `pid=<n> (<comm>: "<full cmdline>")`;
///   * a PID whose `/proc` entry is gone or unreadable (the sender
///     already exited, or it lives in another PID namespace / is
///     unreadable) — `pid=<n> (unresolved: <why>)`.
fn describe_sender(proc_root: &str, sender_pid: i32) -> String {
    if sender_pid == 0 {
        return "kernel (pid=0; e.g. OOM-killer or a kernel-sent signal)".to_string();
    }
    if sender_pid < 0 {
        // -1 is our own "siginfo unavailable" sentinel from the handler.
        return "unknown sender (siginfo unavailable)".to_string();
    }
    let comm = read_comm(proc_root, sender_pid);
    let cmdline = read_cmdline(proc_root, sender_pid);
    match (comm, cmdline) {
        (Some(c), Some(cmd)) => format!("pid={} ({}: {:?})", sender_pid, c, cmd),
        (Some(c), None) => format!("pid={} ({})", sender_pid, c),
        (None, Some(cmd)) => format!("pid={} (cmdline: {:?})", sender_pid, cmd),
        (None, None) => format!(
            "pid={} (unresolved: /proc entry gone — sender already exited \
             or not visible to this process)",
            sender_pid
        ),
    }
}

/// Read `/proc/<pid>/comm` (the binary's short name) trimmed of its
/// trailing newline. `None` when the file is missing/unreadable.
fn read_comm(proc_root: &str, pid: i32) -> Option<String> {
    let raw = std::fs::read_to_string(format!("{}/{}/comm", proc_root, pid)).ok()?;
    let trimmed = raw.trim_end_matches('\n');
    match trimmed.is_empty() {
        true => None,
        false => Some(trimmed.to_string()),
    }
}

/// Read `/proc/<pid>/cmdline` (the full argv, NUL-separated on disk) and
/// re-join the args with spaces for a human-readable single line. The
/// kernel terminates the buffer with a trailing NUL, so we drop empty
/// trailing fields. `None` when the file is missing or empty (a kernel
/// thread has an empty cmdline).
fn read_cmdline(proc_root: &str, pid: i32) -> Option<String> {
    let raw = std::fs::read(format!("{}/{}/cmdline", proc_root, pid)).ok()?;
    if raw.is_empty() {
        return None;
    }
    let joined = raw
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect::<Vec<_>>()
        .join(" ");
    match joined.is_empty() {
        true => None,
        false => Some(joined),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `format_signal_source` resolves a KNOWN-live pid (the test
    /// process itself) to its comm + full cmdline. This is the core
    /// signal-source helper exercised over a real `/proc` entry,
    /// proving the pid → comm/cmdline resolution wires up.
    #[test]
    fn format_resolves_self_pid_comm_and_cmdline() {
        let me = std::process::id() as i32;
        let line = format_signal_source(
            "/proc",
            libc::SIGTERM,
            me,
            "shutdown flag set by an incoming signal",
        );
        assert!(line.contains("received SIGTERM"), "line: {}", line);
        assert!(line.contains(&format!("pid={}", me)), "line: {}", line);
        // The test binary's comm contains the crate name fragment; at a
        // minimum the (<comm>: "<cmdline>") shape must be present.
        assert!(
            line.contains(": \""),
            "expected a resolved (comm: \"cmdline\") shape; line: {}",
            line
        );
        assert!(
            line.contains("initiating teardown because shutdown flag set"),
            "WHY clause must be present; line: {}",
            line
        );
    }

    /// `si_pid == 0` is the kernel/OOM-killer case: it must be reported
    /// as such, never resolved against `/proc/0`.
    #[test]
    fn format_reports_kernel_for_pid_zero() {
        let line =
            format_signal_source("/proc", libc::SIGTERM, 0, "shutdown flag set by an incoming signal");
        assert!(line.contains("kernel (pid=0"), "line: {}", line);
        assert!(line.contains("OOM-killer"), "line: {}", line);
    }

    /// A dead / never-existed pid must resolve gracefully to an
    /// "unresolved" note rather than panicking or claiming a comm.
    #[test]
    fn format_handles_unresolvable_pid_gracefully() {
        // i32::MAX is above kernel.pid_max on every supported kernel, so
        // /proc/<pid> never exists.
        let line = format_signal_source(
            "/proc",
            libc::SIGCONT,
            i32::MAX,
            "shutdown flag set by an incoming signal",
        );
        assert!(line.contains("received SIGCONT"), "line: {}", line);
        assert!(line.contains(&format!("pid={}", i32::MAX)), "line: {}", line);
        assert!(line.contains("unresolved"), "line: {}", line);
    }

    /// comm + cmdline resolution against a fabricated `/proc` tree:
    /// proves the NUL-join (cmdline) and newline-trim (comm) parsing,
    /// independent of any real process.
    #[test]
    fn resolves_against_injected_proc_tree() {
        let dir = tempfile::tempdir().unwrap();
        let proc = dir.path().join("proc");
        let pid_dir = proc.join("4242");
        std::fs::create_dir_all(&pid_dir).unwrap();
        std::fs::write(pid_dir.join("comm"), b"slurmstepd\n").unwrap();
        // cmdline is NUL-separated with a trailing NUL, exactly as the
        // kernel exposes it.
        std::fs::write(
            pid_dir.join("cmdline"),
            b"slurmstepd: [153731.batch]\0",
        )
        .unwrap();

        let line = format_signal_source(
            proc.to_str().unwrap(),
            libc::SIGTERM,
            4242,
            "SLURM delivered the job-step SIGTERM",
        );
        assert_eq!(
            line,
            "received SIGTERM from pid=4242 (slurmstepd: \"slurmstepd: [153731.batch]\") \
             -> initiating teardown because SLURM delivered the job-step SIGTERM",
            "full sample line mismatch: {}",
            line
        );
    }

    /// Multi-arg cmdline re-joins the NUL-separated argv with spaces.
    #[test]
    fn cmdline_multiarg_joins_with_spaces() {
        let dir = tempfile::tempdir().unwrap();
        let proc = dir.path().join("proc");
        let pid_dir = proc.join("7");
        std::fs::create_dir_all(&pid_dir).unwrap();
        std::fs::write(pid_dir.join("comm"), b"dynrunner-slur\n").unwrap();
        std::fs::write(
            pid_dir.join("cmdline"),
            b"/opt/dynrunner-slurm-wrapper\0--secondary-id\0sec-0\0",
        )
        .unwrap();
        let line = format_signal_source(
            proc.to_str().unwrap(),
            libc::SIGCONT,
            7,
            "wrapper forwarded SIGCONT",
        );
        assert!(
            line.contains("(dynrunner-slur: \"/opt/dynrunner-slurm-wrapper --secondary-id sec-0\")"),
            "line: {}",
            line
        );
    }

    /// A pid with comm present but an empty cmdline (kernel thread) still
    /// reports the comm and does not fabricate a cmdline.
    #[test]
    fn empty_cmdline_falls_back_to_comm_only() {
        let dir = tempfile::tempdir().unwrap();
        let proc = dir.path().join("proc");
        let pid_dir = proc.join("9");
        std::fs::create_dir_all(&pid_dir).unwrap();
        std::fs::write(pid_dir.join("comm"), b"kthreadd\n").unwrap();
        std::fs::write(pid_dir.join("cmdline"), b"").unwrap();
        let line =
            format_signal_source(proc.to_str().unwrap(), libc::SIGTERM, 9, "kernel-thread sent signal");
        assert!(line.contains("pid=9 (kthreadd)"), "line: {}", line);
        assert!(!line.contains(": \""), "must not fabricate a cmdline; line: {}", line);
    }
}
