//! Single concern: signal provenance via signalfd(2) — the headline new
//! capability. Block the catchable signal set, read each delivery, and
//! log {monotonic+wall ts, signo, ssi_pid, ssi_uid, ssi_code
//! (SI_USER/SI_KERNEL/SI_TKILL/...), comm(ssi_pid), cmdline(ssi_pid)}.
//!
//! # Public API (for Phase 2 integration)
//!
//! ```ignore
//! let mut monitor = signals::install()?;   // call BEFORE spawning any child
//! // ... spawn child, run relay ...
//! let term = monitor.recv_terminating().await; // resolves on first SIGTERM/INT/HUP/QUIT
//! // term: TerminatingSignal { signo, signame, sender_pid, sender_uid, si_code, comm, cmdline }
//! // begin Phase 2 teardown using term.* for the shutdown log line.
//! ```
//!
//! - [`block_signals`] blocks the monitored set process-wide via
//!   `sigprocmask`. It MUST run in SYNC `fn main()` BEFORE the tokio runtime
//!   is built — otherwise a signal delivered to a worker/blocking-pool
//!   thread that has not yet inherited the block triggers the default
//!   disposition and the signalfd never sees it.
//! - [`start_monitor`] (async; call AFTER the runtime exists and AFTER
//!   `block_signals`) creates the signalfd over the already-blocked set and
//!   spawns the provenance-logging task.
//! - [`child_mask_reset`] is the single owner of the child signal-mask
//!   reset (`SIG_SETMASK` to empty via `pre_exec`). Children inherit the
//!   blocked mask across fork+exec; every child that must receive normal
//!   signal disposition (shutdown manager, podman/conmon + container PID 1,
//!   relay `bash -c`, image-load `bash -c`) applies it at its spawn site.
//! - [`SignalMonitor::recv_terminating`] is the single async notification
//!   surface: it resolves with the provenance of the FIRST terminating
//!   signal observed (SIGTERM/SIGINT/SIGHUP/SIGQUIT). All signals (incl.
//!   non-terminating forensic ones) keep being logged regardless.
//! - The monitor task runs for the lifetime of the process; the
//!   [`SignalMonitor`] owns its `JoinHandle`.

use std::path::Path;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nix::sys::signal::{SigSet, Signal, SigmaskHow, sigprocmask};
use nix::sys::signalfd::SignalFd;
use tokio::sync::mpsc;

/// Process-lifetime monotonic anchor. The first call fixes "t0"; every
/// logged record reports nanos elapsed since it, which is a stable,
/// comparable monotonic stamp (raw `Instant` has no portable absolute
/// serialization).
fn mono_anchor() -> Instant {
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    *ANCHOR.get_or_init(Instant::now)
}

/// Provenance of a terminating signal, handed to `main` so Phase 2 can
/// log a precise shutdown cause and begin teardown.
#[derive(Debug, Clone)]
pub struct TerminatingSignal {
    /// Numeric signal number (`ssi_signo`).
    pub signo: u32,
    /// Symbolic name, e.g. `"SIGTERM"`, or `"SIG<n>"` if unknown.
    pub signame: String,
    /// PID of the sending process (`ssi_pid`); 0 if the kernel sent it.
    pub sender_pid: u32,
    /// Real UID of the sender (`ssi_uid`).
    pub sender_uid: u32,
    /// Decoded `ssi_code` label (SI_USER/SI_KERNEL/...).
    pub si_code: String,
    /// `/proc/<pid>/comm` of the sender, or `<unknown>`.
    pub comm: String,
    /// `/proc/<pid>/cmdline` of the sender (space-joined), or `<unknown>`.
    pub cmdline: String,
}

/// Owns the signalfd monitor task and the channel that delivers the first
/// terminating-signal provenance to `main`.
pub struct SignalMonitor {
    handle: tokio::task::JoinHandle<()>,
    term_rx: mpsc::Receiver<TerminatingSignal>,
}

impl SignalMonitor {
    /// Resolves with the provenance of the FIRST terminating signal
    /// (SIGTERM/SIGINT/SIGHUP/SIGQUIT) observed by the monitor. If the
    /// monitor task has already exited without one (it never does in
    /// normal operation), this returns a synthetic `<monitor-gone>`
    /// record so callers can still make forward progress on teardown.
    pub async fn recv_terminating(&mut self) -> TerminatingSignal {
        self.term_rx.recv().await.unwrap_or_else(|| TerminatingSignal {
            signo: 0,
            signame: "<monitor-gone>".to_string(),
            sender_pid: 0,
            sender_uid: 0,
            si_code: "<none>".to_string(),
            comm: "<unknown>".to_string(),
            cmdline: "<unknown>".to_string(),
        })
    }

    /// Abort the monitor task. Intended for shutdown after teardown is
    /// complete; the blocked-signal mask itself is not restored (the
    /// process is exiting).
    pub fn shutdown(&self) {
        self.handle.abort();
    }

    /// Test-only constructor: a monitor backed by a caller-fed channel with
    /// NO signalfd thread. Lets the lifecycle's select!/teardown routing be
    /// tested deterministically by injecting a synthetic `TerminatingSignal`,
    /// without raising a real process-directed signal (which is unreliable
    /// once the test harness is multithreaded — see the C1 note: `sigprocmask`
    /// only reliably blocks the calling thread, so a process-directed signal
    /// can hit an unblocked harness thread and trip the default disposition).
    /// Real signalfd delivery is covered by Phase 5 on the cluster.
    #[cfg(test)]
    pub fn for_test() -> (SignalMonitor, mpsc::Sender<TerminatingSignal>) {
        let (tx, rx) = mpsc::channel::<TerminatingSignal>(8);
        // A parked, never-resolving task stands in for the monitor loop so the
        // JoinHandle is valid; recv_terminating reads only the injected channel.
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        (
            SignalMonitor {
                handle,
                term_rx: rx,
            },
            tx,
        )
    }
}

/// The catchable signals to monitor. SIGTERM/INT/HUP/QUIT are terminating
/// (the bash reference trapped TERM/HUP/INT); USR1/USR2/CONT are added for
/// forensic breadth. SIGKILL/SIGSTOP are deliberately absent (uncatchable).
const MONITORED: &[Signal] = &[
    Signal::SIGTERM,
    Signal::SIGINT,
    Signal::SIGHUP,
    Signal::SIGQUIT,
    Signal::SIGUSR1,
    Signal::SIGUSR2,
    Signal::SIGCONT,
];

/// Signals whose arrival must notify `main` to begin teardown.
fn is_terminating(sig: Signal) -> bool {
    matches!(
        sig,
        Signal::SIGTERM | Signal::SIGINT | Signal::SIGHUP | Signal::SIGQUIT
    )
}

/// Symbolic name for a raw signo, falling back to `SIG<n>`.
fn signame(signo: u32) -> String {
    match Signal::try_from(signo as i32) {
        Ok(sig) => sig.as_str().to_string(),
        Err(_) => format!("SIG{signo}"),
    }
}

/// The monitored set as a `SigSet`. Single builder so `block_signals` and
/// `start_monitor` operate over exactly the same set.
fn monitored_set() -> SigSet {
    let mut set = SigSet::empty();
    for &sig in MONITORED {
        set.add(sig);
    }
    set
}

/// Block the monitored signal set process-wide via `sigprocmask`. MUST run
/// in SYNC `fn main()` BEFORE the tokio runtime is built and BEFORE any
/// thread is spawned, so every later thread (tokio workers + blocking pool)
/// inherits the block and the signalfd is the sole consumer of deliveries.
/// Pure mask side effect — no fd, no task; pair with [`start_monitor`].
pub fn block_signals() -> std::io::Result<()> {
    // Fix the monotonic anchor at block time (process start).
    let _ = mono_anchor();
    let set = monitored_set();
    // Block process-wide so deliveries are queued (to the later signalfd)
    // rather than acted on by default handlers.
    sigprocmask(SigmaskHow::SIG_BLOCK, Some(&set), None).map_err(std::io::Error::from)
}

/// Create the signalfd over the (already-blocked) monitored set and spawn
/// the provenance-logging monitor task. MUST run AFTER [`block_signals`]
/// AND after the tokio runtime exists (it `spawn_blocking`s the loop). The
/// set is rebuilt identically here; it is already blocked process-wide, so
/// the signalfd is the sole consumer of every monitored delivery.
pub fn start_monitor() -> std::io::Result<SignalMonitor> {
    let set = monitored_set();
    let sfd = SignalFd::new(&set).map_err(std::io::Error::from)?;

    // Bounded channel: a handful of slots is plenty — only the FIRST
    // terminating signal matters to `main`, the rest are best-effort.
    let (term_tx, term_rx) = mpsc::channel::<TerminatingSignal>(8);

    let handle = tokio::task::spawn_blocking(move || {
        monitor_loop(sfd, term_tx, Path::new("/proc"));
    });

    Ok(SignalMonitor { handle, term_rx })
}

/// Single owner of the child signal-mask reset. Children inherit the
/// blocked mask across fork+exec (execve preserves the signal mask), which
/// would break the shutdown manager (no SIGCONT nudge), podman/conmon +
/// container PID 1 (no SIGTERM graceful stop), and the relay's per-command
/// `bash -c` children. Register this on EVERY child spawn site so the child
/// starts with an empty mask (`SIG_SETMASK` to empty) right before exec.
///
/// Async (`tokio::process::Command`) and sync (`std::process::Command`)
/// both expose `pre_exec` via `CommandExt`; the closure runs in the forked
/// child between fork and exec, where only async-signal-safe calls are
/// permitted — `sigprocmask(2)` qualifies.
///
/// SAFETY: the `pre_exec` closure performs only an async-signal-safe
/// `sigprocmask` syscall; it allocates nothing and touches no shared state.
pub fn child_pre_exec() -> impl FnMut() -> std::io::Result<()> + Send + Sync + 'static {
    || {
        let empty = SigSet::empty();
        sigprocmask(SigmaskHow::SIG_SETMASK, Some(&empty), None)
            .map_err(std::io::Error::from)
    }
}

/// Blocking read loop over the signalfd. For each delivery it logs a
/// structured provenance record and, on a terminating signal, forwards
/// the provenance to `main` (best-effort: a full/closed channel is fine
/// since only the first terminating signal is acted upon).
fn monitor_loop(sfd: SignalFd, term_tx: mpsc::Sender<TerminatingSignal>, proc_base: &Path) {
    loop {
        match sfd.read_signal() {
            Ok(Some(siginfo)) => {
                let prov = build_provenance(&siginfo, proc_base);
                log_provenance(&siginfo, &prov);

                if let Ok(sig) = Signal::try_from(siginfo.ssi_signo as i32) {
                    if is_terminating(sig) {
                        // Best-effort: ignore send errors (channel full or
                        // receiver dropped — main already learned/exited).
                        let _ = term_tx.try_send(prov);
                    }
                }
            }
            // SignalFd in blocking mode never returns Ok(None) (that path
            // is for SFD_NONBLOCK, which we do not set); retry defensively.
            Ok(None) => continue,
            Err(nix::errno::Errno::EINTR) => continue,
            Err(err) => {
                tracing::error!(error = %err, "signalfd read failed; monitor exiting");
                return;
            }
        }
    }
}

/// Resolve the full provenance record for one delivered siginfo.
fn build_provenance(
    siginfo: &nix::sys::signalfd::siginfo,
    proc_base: &Path,
) -> TerminatingSignal {
    let pid = siginfo.ssi_pid;
    TerminatingSignal {
        signo: siginfo.ssi_signo,
        signame: signame(siginfo.ssi_signo),
        sender_pid: pid,
        sender_uid: siginfo.ssi_uid,
        si_code: decode_si_code(siginfo.ssi_code),
        comm: read_comm(proc_base, pid).unwrap_or_else(|| "<unknown>".to_string()),
        cmdline: read_cmdline(proc_base, pid).unwrap_or_else(|| "<unknown>".to_string()),
    }
}

/// Emit the structured provenance log line for one delivery.
fn log_provenance(siginfo: &nix::sys::signalfd::siginfo, prov: &TerminatingSignal) {
    let wall_unix_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let monotonic_ns = mono_anchor().elapsed().as_nanos();
    let terminating = Signal::try_from(siginfo.ssi_signo as i32)
        .map(is_terminating)
        .unwrap_or(false);

    tracing::warn!(
        wall_unix_ns = %wall_unix_ns,
        monotonic_ns = %monotonic_ns,
        signo = siginfo.ssi_signo,
        signame = %prov.signame,
        sender_pid = prov.sender_pid,
        sender_uid = prov.sender_uid,
        si_code = %prov.si_code,
        terminating = terminating,
        comm = %prov.comm,
        cmdline = %prov.cmdline,
        "signal received (provenance)"
    );
}

/// Decode `ssi_code` to a stable label. PURE / testable.
fn decode_si_code(code: i32) -> String {
    match code {
        libc::SI_USER => "SI_USER".to_string(),
        libc::SI_KERNEL => "SI_KERNEL".to_string(),
        libc::SI_TKILL => "SI_TKILL".to_string(),
        libc::SI_QUEUE => "SI_QUEUE".to_string(),
        other => format!("SI_OTHER({other})"),
    }
}

/// Read `/proc/<pid>/comm` (single line, trimmed). PURE w.r.t. base dir.
fn read_comm(proc_base: &Path, pid: u32) -> Option<String> {
    let path = proc_base.join(pid.to_string()).join("comm");
    let raw = std::fs::read_to_string(path).ok()?;
    let trimmed = raw.trim_end_matches('\n').to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// Read `/proc/<pid>/cmdline`: NUL-separated args, trailing empty dropped,
/// joined with spaces. PURE w.r.t. base dir.
fn read_cmdline(proc_base: &Path, pid: u32) -> Option<String> {
    let path = proc_base.join(pid.to_string()).join("cmdline");
    let bytes = std::fs::read(path).ok()?;
    if bytes.is_empty() {
        return None;
    }
    let joined = bytes
        .split(|&b| b == 0)
        .filter(|seg| !seg.is_empty())
        .map(|seg| String::from_utf8_lossy(seg).into_owned())
        .collect::<Vec<_>>()
        .join(" ");
    if joined.is_empty() {
        None
    } else {
        Some(joined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn decode_si_code_known_and_unknown() {
        assert_eq!(decode_si_code(libc::SI_USER), "SI_USER");
        assert_eq!(decode_si_code(libc::SI_KERNEL), "SI_KERNEL");
        assert_eq!(decode_si_code(libc::SI_TKILL), "SI_TKILL");
        assert_eq!(decode_si_code(libc::SI_QUEUE), "SI_QUEUE");
        let weird = 12345;
        assert_eq!(decode_si_code(weird), format!("SI_OTHER({weird})"));
    }

    fn write_proc(base: &Path, pid: u32, comm: Option<&[u8]>, cmdline: Option<&[u8]>) {
        let dir = base.join(pid.to_string());
        fs::create_dir_all(&dir).unwrap();
        if let Some(c) = comm {
            fs::write(dir.join("comm"), c).unwrap();
        }
        if let Some(c) = cmdline {
            fs::write(dir.join("cmdline"), c).unwrap();
        }
    }

    #[test]
    fn read_comm_present_trims_newline() {
        let tmp = tempfile::tempdir().unwrap();
        write_proc(tmp.path(), 42, Some(b"bash\n"), None);
        assert_eq!(read_comm(tmp.path(), 42), Some("bash".to_string()));
    }

    #[test]
    fn read_comm_missing_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(read_comm(tmp.path(), 99), None);
    }

    #[test]
    fn read_comm_empty_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        write_proc(tmp.path(), 7, Some(b"\n"), None);
        assert_eq!(read_comm(tmp.path(), 7), None);
    }

    #[test]
    fn read_cmdline_nul_separated_joined() {
        let tmp = tempfile::tempdir().unwrap();
        write_proc(tmp.path(), 100, None, Some(b"bash\0-c\0echo hi\0"));
        assert_eq!(
            read_cmdline(tmp.path(), 100),
            Some("bash -c echo hi".to_string())
        );
    }

    #[test]
    fn read_cmdline_missing_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(read_cmdline(tmp.path(), 5), None);
    }

    #[test]
    fn read_cmdline_empty_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        write_proc(tmp.path(), 6, None, Some(b""));
        assert_eq!(read_cmdline(tmp.path(), 6), None);
    }

    #[test]
    fn read_cmdline_only_nuls_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        write_proc(tmp.path(), 8, None, Some(b"\0\0"));
        assert_eq!(read_cmdline(tmp.path(), 8), None);
    }

    #[test]
    fn signame_known_and_unknown() {
        assert_eq!(signame(libc::SIGTERM as u32), "SIGTERM");
        assert_eq!(signame(4242), "SIG4242");
    }

    // NOTE: the optional end-to-end test (install(); raise SIGUSR1; observe
    // the monitor) is deliberately omitted — it is not reliable in this
    // harness. The monitor task is a `spawn_blocking` loop parked in a
    // BLOCKING `SignalFd::read_signal()` syscall that has no cancellation
    // point. `JoinHandle::abort()` does not interrupt an in-flight blocking
    // closure, and the tokio test runtime hangs on drop waiting for that
    // blocking thread to finish, so the test process never exits (observed:
    // >60s then SIGKILL). It also mutates the process-wide signal mask,
    // which would leak into sibling tests in the same binary. The pure
    // helpers (decode_si_code, signame, read_comm, read_cmdline) carry the
    // testable logic; the signalfd plumbing is exercised by Phase 5 on the
    // real cluster.
}
