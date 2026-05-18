//! Single concern: probe whether a given PID is currently a live
//! process visible to this process's UID.
//!
//! The poll loop in [`crate::poll_loop`] uses this as a third wake
//! input alongside the shutdown flag and the container-idle counter.
//! When the wrapper script that spawned the shutdown manager is
//! reaped by SLURM proctrack before its signal trap can forward
//! `systemctl --user kill`, the manager would otherwise sit forever
//! (orphan conmons keep the container "present" from podman's POV,
//! so the idle branch never trips). Observing wrapper liveness lets
//! the manager fall through to SIGNAL_SHUTDOWN on its own.
//!
//! ## Boundary
//!
//! `ProcessProbe::is_alive(pid) -> bool`. Callers know nothing about
//! how aliveness is determined; the probe knows nothing about state
//! machines. Production uses [`KillProbe`]; tests use a scripted mock
//! in [`crate::testing`].
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
}
