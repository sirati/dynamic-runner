//! Single concern: the per-job scratch-root LIVENESS marker — an
//! advisory file lock (`<rndtmp>/wrapper.lock`) the wrapper HOLDS for
//! its whole life, and which the pre-flight orphan sweep PROBES to
//! distinguish a live sibling job's scratch root from a true orphan.
//!
//! Why this exists (asm-dataset run_20260611_115429): the pre-flight
//! sweep classified ANY `/tmp/*/storage` root owned by the current
//! user as an orphan. That held when at most one secondary job ever
//! ran per node at a time — but the member-respawn pipeline submits a
//! REPLACEMENT sbatch job while the flapped-but-alive original job is
//! still running, and SLURM may land it on a node that still hosts a
//! LIVE secondary. The replacement's pre-flight then stop/`rm -af`-ed
//! the live container's storage root, ripping the rootfs out from
//! under the still-running secondary process: its already-mapped pages
//! kept it alive, but every NEW path lookup failed — respawned workers
//! died with exec ENOENT, `libc.so.6: cannot open shared object file`,
//! and `nix-store` missing from PATH (three escalating views of one
//! gutted mount tree).
//!
//! The lock is the canonical "is the owner alive?" primitive for this:
//! the kernel releases it automatically when the wrapper process exits
//! — however it exits (clean return, SIGKILL, kernel OOM, node-local
//! slurmd sweep) — so a TRUE orphan (the original incident this sweep
//! was built for: job killed without teardown, conmon-supervised
//! container left running) probes as NOT live and is still swept. No
//! PID files (PID reuse), no squeue dependency (compute-node access),
//! no staleness windows.
//!
//! API surface (the only thing `main`/`preflight` see):
//! - [`ScratchLock::acquire`] — wrapper side: create + exclusively lock
//!   the marker; hold the guard for the process's life.
//! - [`is_live`] — sweep side: probe whether some live process holds
//!   the marker lock for a scratch root.

use std::fs::File;
use std::io;
use std::path::Path;

/// Basename of the liveness marker inside a per-job scratch root
/// (`/tmp/<name_prefix>-<suffix>/wrapper.lock`). One constant, two
/// consumers (acquire + probe) — the path shape cannot drift.
pub const LOCK_BASENAME: &str = "wrapper.lock";

/// RAII guard for the wrapper's own scratch-root liveness lock. The
/// exclusive lock is held as long as this value (its `File`) lives;
/// the kernel releases it when the process exits, however it exits.
#[derive(Debug)]
pub struct ScratchLock {
    _file: File,
}

/// Wrapper side: create `<scratch_root>/wrapper.lock` and take the
/// exclusive advisory lock, marking this scratch root LIVE for every
/// concurrent pre-flight sweep on the node. Call right after the
/// scratch tree is created and hold the returned guard for the whole
/// run.
///
/// Errors are the caller's to log-and-proceed: failing to mark
/// liveness must never gate the container launch (it merely leaves
/// this job as exposed as a pre-fix one).
pub fn acquire(scratch_root: &Path) -> io::Result<ScratchLock> {
    let file = File::create(scratch_root.join(LOCK_BASENAME))?;
    // `try_lock` (not blocking `lock`): the only possible holder is a
    // duplicate wrapper on OUR OWN scratch root, which cannot happen
    // (the root embeds this job's random suffix) — but if it somehow
    // did, blocking forever at startup would be strictly worse than
    // surfacing the error.
    file.try_lock().map_err(|e| match e {
        std::fs::TryLockError::Error(err) => err,
        std::fs::TryLockError::WouldBlock => io::Error::new(
            io::ErrorKind::WouldBlock,
            "scratch-root liveness lock already held by another process",
        ),
    })?;
    Ok(ScratchLock { _file: file })
}

/// Sweep side: does a LIVE process hold the liveness lock for
/// `scratch_root`?
///
/// - Marker missing (pre-fix wrapper, or any non-job `/tmp` dir) →
///   `false`: the sweep proceeds exactly as before this fix — true
///   orphans keep being cleaned.
/// - Marker present and lockable → the owner is dead; `false` (the
///   probe lock is dropped immediately).
/// - Marker present and held (`WouldBlock`) → a live wrapper owns this
///   scratch root; `true` — the sweep MUST skip it.
/// - Marker present but unreadable/unlockable for any other reason →
///   `true` (fail SAFE: never tear down a root we cannot prove dead).
pub fn is_live(scratch_root: &Path) -> bool {
    let path = scratch_root.join(LOCK_BASENAME);
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return false,
        // Unreadable marker (permissions, I/O error): we cannot prove
        // the owner dead — treat as live rather than risk gutting a
        // running job's rootfs.
        Err(_) => return true,
    };
    match file.try_lock() {
        // Lock acquired: no live owner. The probe lock releases when
        // `file` drops at the end of this scope.
        Ok(()) => false,
        Err(std::fs::TryLockError::WouldBlock) => true,
        // Indeterminate: fail safe (see above).
        Err(std::fs::TryLockError::Error(_)) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No marker → not live (the pre-fix-wrapper / true-orphan shape:
    /// the sweep must keep cleaning these).
    #[test]
    fn missing_marker_is_not_live() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!is_live(tmp.path()));
    }

    /// A held lock marks the root live; dropping the guard (the owner
    /// exiting) makes the SAME root probe dead again.
    #[test]
    fn held_lock_is_live_until_guard_drops() {
        let tmp = tempfile::tempdir().unwrap();
        let guard = acquire(tmp.path()).expect("acquire");
        assert!(
            is_live(tmp.path()),
            "a held wrapper.lock must probe as live"
        );
        drop(guard);
        assert!(
            !is_live(tmp.path()),
            "a released wrapper.lock must probe as dead (orphan)"
        );
    }

    /// An unlocked marker file (owner died) probes dead — the orphan
    /// sweep proceeds.
    #[test]
    fn unlocked_marker_is_not_live() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(LOCK_BASENAME), b"").unwrap();
        assert!(!is_live(tmp.path()));
    }
}
