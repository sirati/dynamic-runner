//! Single concern: observe and act on a host PID by its number —
//! liveness probing (`kill(pid, 0)`) and signal delivery
//! (`kill(pid, signal)`). Both are the same syscall behind the same
//! permission model; keeping them in one module means every host-PID
//! operation the reaper performs has exactly one owner.
//!
//! The poll loop in [`crate::poll_loop`] uses [`ProcessProbe::is_alive`]
//! as a third wake input alongside the shutdown flag and the
//! container-idle counter. When the wrapper script that spawned the
//! shutdown manager is reaped by SLURM proctrack before its signal
//! trap can forward `systemctl --user kill`, the manager would
//! otherwise sit forever (orphan conmons keep the container "present"
//! from podman's POV, so the idle branch never trips). Observing
//! wrapper liveness lets the manager fall through to SIGNAL_SHUTDOWN
//! on its own.
//!
//! The same poll loop uses [`ProcessProbe::signal`] + [`ProcessProbe::is_alive`]
//! to reap the captured container workload PID directly — independent
//! of podman's container record. Once podman loses the record (`--rm`
//! cleanup, or a premature `rm -af`), a name-based `podman kill`
//! no-ops; signalling the host PID does not, so the reaper can still
//! finish the job and verify the workload actually terminated.
//!
//! ## Boundary
//!
//! `ProcessProbe::is_alive(pid) -> bool` and
//! `ProcessProbe::signal(pid, signal) -> bool`. Callers know nothing
//! about how aliveness is determined or how the signal is delivered;
//! the probe knows nothing about state machines or container records.
//! Production uses [`KillProbe`]; tests use a scripted mock in
//! [`crate::testing`].
//!
//! ## PID-reuse caveat
//!
//! The kernel may reuse a PID once it is freed. If the wrapper exits
//! and the kernel hands its PID to an unrelated process before the
//! shutdown manager's next poll tick, `kill(pid, 0)` will report the
//! reused process as alive. In that pathological case cleanup is
//! delayed by however long the reuser stays up — never skipped.
//! Catching this would require also matching `/proc/<pid>/starttime`
//! at manager startup, which more than doubles the probe's surface
//! for a vanishingly rare race; we accept the trade-off.

/// Probe interface so the poll loop is testable without a real PID
/// space. Production impl uses `kill(pid, 0)`; test impl returns
/// scripted booleans.
pub trait ProcessProbe {
    /// Return `true` iff a process with this PID exists and is
    /// visible to the calling UID. `false` on `ESRCH` (no such
    /// process) and `EPERM` (exists but unsignalable — treated as
    /// "we don't see it for cleanup-decision purposes"). Other
    /// errnos are unexpected; impls best-effort log and return
    /// `false` so the cleanup path is not deadlocked.
    fn is_alive(&self, pid: u32) -> bool;

    /// Deliver `signal` to the process with this PID. Returns `true`
    /// iff the kernel accepted the request (`kill` returned 0).
    /// `false` on any error — most commonly `ESRCH` (the process is
    /// already gone, which the reaper treats as success-by-other-means
    /// and confirms via [`Self::is_alive`]).
    ///
    /// This is the precise, single-PID counterpart to a broad
    /// `pkill`: the reaper only ever signals the one workload PID it
    /// captured from podman while the container record existed — never
    /// a name/pattern match that could hit an unrelated process.
    fn signal(&self, pid: u32, signal: i32) -> bool;
}

/// Production probe: `kill(pid, 0)` via libc FFI.
///
/// `signal(0)` is the POSIX idiom — the kernel validates the target
/// PID's existence and the caller's permission to signal it without
/// delivering anything. Cheaper than reading `/proc/<pid>/status`
/// and does not allocate.
#[derive(Debug, Default, Clone, Copy)]
pub struct KillProbe;

impl ProcessProbe for KillProbe {
    fn is_alive(&self, pid: u32) -> bool {
        // SAFETY: `libc::kill(pid, 0)` is a syscall that delivers no
        // signal (signal 0 is the existence check per POSIX). It only
        // reads kernel state; no userspace memory is touched on this
        // side. The cast `pid as i32` is safe because Linux's
        // `kernel.pid_max` is bounded at 2^22 and fits in i32.
        let rc = unsafe { libc::kill(pid as i32, 0) };
        if rc == 0 {
            return true;
        }
        // SAFETY: `__errno_location` returns a pointer to a
        // thread-local i32 set by the most recent failing libc call.
        // POSIX-standard errno access.
        let errno = unsafe { *libc::__errno_location() };
        match errno {
            // No such process — definitively gone.
            libc::ESRCH => false,
            // Process exists but we lack permission to signal it.
            // For the wrapper-monitor use case both processes run as
            // the same UID, so EPERM should not occur. If it
            // somehow does, conservative is to behave as "gone" —
            // mis-classifying a permission-restricted parent as gone
            // triggers cleanup earlier than necessary but never
            // orphans, while the inverse would deadlock the loop.
            libc::EPERM => false,
            other => {
                eprintln!(
                    "[shutdown-mgr] KillProbe::is_alive: unexpected errno {} for pid {}",
                    other, pid
                );
                false
            }
        }
    }

    fn signal(&self, pid: u32, signal: i32) -> bool {
        // SAFETY: `libc::kill(pid, signal)` is a syscall that delivers
        // `signal` to the target PID. It touches no userspace memory on
        // this side. The cast `pid as i32` is safe because Linux's
        // `kernel.pid_max` is bounded at 2^22 and fits in i32.
        let rc = unsafe { libc::kill(pid as i32, signal) };
        rc == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Self-process must always be alive — confirms the FFI is
    /// wired and the success path returns `true`.
    #[test]
    fn kill_probe_sees_self_alive() {
        let me = std::process::id();
        assert!(KillProbe.is_alive(me));
    }

    /// A maximally-large PID is overwhelmingly likely to be absent
    /// (Linux's `kernel.pid_max` caps at 2^22 by default; u32::MAX
    /// is far above that). Confirms ESRCH path returns `false`.
    #[test]
    fn kill_probe_reports_absent_for_unused_pid() {
        // `kernel.pid_max` default is 4_194_304; pick a value above
        // that to guarantee ESRCH. Using a very high u32 avoids the
        // narrow race where some future kernel raises pid_max.
        assert!(!KillProbe.is_alive(u32::MAX - 1));
    }

    /// `signal(self, 0)` is `kill(self, 0)` — the existence probe.
    /// Self always exists and is signalable by its own UID, so the
    /// kernel accepts the request. Confirms the success path returns
    /// `true` without delivering a real signal (signal 0 delivers
    /// nothing).
    #[test]
    fn kill_probe_signal_zero_to_self_succeeds() {
        let me = std::process::id();
        assert!(KillProbe.signal(me, 0));
    }

    /// Signalling an absent PID fails (ESRCH) — `signal` returns
    /// `false`. The reaper treats this as "already gone" and confirms
    /// via `is_alive`; it never escalates against a PID the kernel
    /// says does not exist.
    #[test]
    fn kill_probe_signal_to_absent_pid_fails() {
        assert!(!KillProbe.signal(u32::MAX - 1, libc::SIGTERM));
    }
}
