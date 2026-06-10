//! Single concern: CHECK whether the run user's logind linger is enabled,
//! so the wrapper can HONOR it (proceed) and WARN if it is not.
//!
//! THE CHAIN (proven, krater17): `Linger=no` + the submitter's only login
//! session (the persistent `ssh -J -R -N` reverse tunnel) drops → systemd
//! reaps `user@<uid>.service` → SIGTERMs the rootless-podman scope →
//! signal-proxy → container PID1 → fan-kill. `loginctl enable-linger` is the
//! direct cure: it keeps `user@.service` running past last-session-close so
//! the chain never fires.
//!
//! WHO ENABLES LINGER (the owner's setup-side design): the SUBMITTER's setup
//! enables linger over its ssh to each compute node at tunnel-build time —
//! that ssh carries a `pam_systemd` logind session, which the enable needs.
//! The wrapper does NOT enable linger and its role is ONLY to CHECK + HONOR
//! the state the setup established, WARNing (a safety net) if it reads as
//! not set. The wrapper performs NO enable / disable / restore.
//!
//! WHY A FILE STAT, NOT `loginctl` (proven, krater 2026-06-10): in the
//! sbatch/slurmstepd context `loginctl` cannot reach logind AT ALL — not
//! just `enable-linger` (ENXIO) but `show-user` too. A bus-dependent check
//! therefore reads "not lingering" even when linger IS on (observed: all 40
//! nodes pre-set `Linger=yes` with `/var/lib/systemd/linger/<user>` present,
//! and the loginctl-based check still reported not-enabled on every
//! secondary). The persistent linger marker `/var/lib/systemd/linger/<user>`
//! is what `enable-linger` writes and what systemd itself reads — a plain
//! `stat(2)`, zero bus dependency, truthful in any execution context.
//!
//! Boundary: callers get one primitive — [`check_linger`] (the boolean
//! current state) — and know nothing about logind, the linger store path,
//! or how the run user is resolved. The path construction + existence test
//! ([`marker_exists`]) takes the store directory as a parameter so it is
//! unit-testable against a temp directory.

use std::path::Path;

/// systemd's persistent linger store: `loginctl enable-linger <user>`
/// creates `<LINGER_DIR>/<user>` (an empty root-owned marker file) and
/// `disable-linger` removes it. logind reads this directory at startup to
/// decide which `user@<uid>.service` instances persist past session close.
const LINGER_DIR: &str = "/var/lib/systemd/linger";

/// CHECK: is the run user currently lingering? A bus-free `stat(2)` on the
/// persistent linger marker (see module doc for why NOT `loginctl`). Any
/// failure to resolve the run user's name is treated as `false` (NOT
/// confirmed lingering): the caller then WARNs (the setup should have
/// enabled it) but PROCEEDS — linger is a resilience property, never a
/// launch gate.
pub fn check_linger() -> bool {
    match resolve_run_user() {
        Some(user) => marker_exists(Path::new(LINGER_DIR), &user),
        None => false,
    }
}

/// Does the persistent linger marker for `user` exist under `dir`?
/// Split out from [`check_linger`] so the existence semantics are testable
/// against a temp directory instead of the real `/var/lib/systemd/linger`.
fn marker_exists(dir: &Path, user: &str) -> bool {
    dir.join(user).exists()
}

/// Resolve the run user by passwd name (the linger marker is keyed by user
/// NAME, exactly as `loginctl enable-linger <user>` writes it). When the
/// euid has no passwd entry (unusual, but possible under nss quirks),
/// returns `None` and the check degrades to "not confirmed lingering".
fn resolve_run_user() -> Option<String> {
    match nix::unistd::User::from_uid(nix::unistd::geteuid()) {
        Ok(Some(u)) => Some(u.name),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Marker present → lingering; absent → not. The check is a pure
    /// existence test on `<dir>/<user>` — no bus, no subprocess.
    #[test]
    fn marker_existence_is_the_check() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!marker_exists(tmp.path(), "kruppb"), "no marker → false");
        std::fs::write(tmp.path().join("kruppb"), b"").unwrap();
        assert!(marker_exists(tmp.path(), "kruppb"), "marker → true");
        // Another user's marker must not satisfy the check.
        assert!(!marker_exists(tmp.path(), "otheruser"));
    }

    /// A nonexistent store directory (no systemd linger dir at all) reads
    /// as not-lingering — the WARN-and-proceed path, never an error.
    #[test]
    fn missing_store_dir_is_not_lingering() {
        assert!(!marker_exists(
            Path::new("/nonexistent-linger-store-dir"),
            "kruppb"
        ));
    }
}
