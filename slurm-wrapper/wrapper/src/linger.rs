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
//! WHY NOT passwd-ONLY USER RESOLUTION (proven, krater 2026-06-10, run
//! ebfdf790): this wrapper is a STATIC MUSL binary, and musl's `getpwuid`
//! has NO NSS — it reads `/etc/passwd` only, so an LDAP/sssd-provisioned
//! cluster user (the normal HPC case) is INVISIBLE to it. The bus-free
//! file-stat check then failed on every node not because the marker was
//! missing (it was provably present) but because the run user's NAME could
//! not be resolved. The resolution therefore falls back to the environment:
//! `SLURM_JOB_USER` (set by slurmstepd for every batch step, cross-checked
//! against `SLURM_JOB_UID` when present), then `USER` / `LOGNAME`. Scanning
//! `/var/lib/systemd/linger/` for "the euid's marker" instead is NOT
//! possible: markers are empty root-owned files keyed by NAME, so
//! attributing one to the euid would itself need the name lookup that just
//! failed.
//!
//! Boundary: callers get one primitive — [`check_linger`] (the typed
//! current state, [`LingerCheck`]) — and know nothing about logind, the
//! linger store path, or how the run user is resolved. The path
//! construction + existence test ([`marker_exists`]) and the resolution
//! chain ([`resolve_run_user_from`]) take their inputs as parameters so
//! they are unit-testable against a temp directory / synthetic env.

use std::path::Path;

/// systemd's persistent linger store: `loginctl enable-linger <user>`
/// creates `<LINGER_DIR>/<user>` (an empty root-owned marker file) and
/// `disable-linger` removes it. logind reads this directory at startup to
/// decide which `user@<uid>.service` instances persist past session close.
const LINGER_DIR: &str = "/var/lib/systemd/linger";

/// The typed outcome of [`check_linger`], so the caller's log line can
/// name WHICH of the two distinct not-confirmed causes applies — a
/// present-but-unattributable marker (user resolution failed) is an
/// environment defect with its own remediation, not a missing enable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LingerCheck {
    /// The run user's linger marker exists — linger is on.
    Enabled { user: String },
    /// The run user resolved but no marker exists — the setup-side
    /// enable did not (yet) land for this user.
    NotEnabled { user: String },
    /// The run user's NAME could not be resolved by any source (passwd
    /// — which under static musl sees only `/etc/passwd`, never
    /// LDAP/sssd — nor `SLURM_JOB_USER` / `USER` / `LOGNAME`), so the
    /// marker cannot be checked at all.
    UserUnresolved,
}

/// CHECK: is the run user currently lingering? A bus-free `stat(2)` on the
/// persistent linger marker (see module doc for why NOT `loginctl`). Both
/// non-`Enabled` outcomes are WARN-and-PROCEED for the caller: linger is a
/// resilience property, never a launch gate.
pub fn check_linger() -> LingerCheck {
    match resolve_run_user() {
        Some(user) => {
            if marker_exists(Path::new(LINGER_DIR), &user) {
                LingerCheck::Enabled { user }
            } else {
                LingerCheck::NotEnabled { user }
            }
        }
        None => LingerCheck::UserUnresolved,
    }
}

/// Does the persistent linger marker for `user` exist under `dir`?
/// Split out from [`check_linger`] so the existence semantics are testable
/// against a temp directory instead of the real `/var/lib/systemd/linger`.
fn marker_exists(dir: &Path, user: &str) -> bool {
    dir.join(user).exists()
}

/// Resolve the run user by passwd name first, then by environment (the
/// linger marker is keyed by user NAME, exactly as `loginctl
/// enable-linger <user>` writes it). Production shell over
/// [`resolve_run_user_from`] wiring in the real euid, passwd lookup, and
/// process env.
fn resolve_run_user() -> Option<String> {
    let euid = nix::unistd::geteuid();
    let passwd_user = match nix::unistd::User::from_uid(euid) {
        Ok(Some(u)) => Some(u.name),
        _ => None,
    };
    resolve_run_user_from(passwd_user, euid.as_raw(), |key| {
        std::env::var(key).ok().filter(|v| !v.is_empty())
    })
}

/// The user-name resolution chain, parametric over its inputs so it is
/// unit-testable without touching the real passwd/env:
///
/// 1. `passwd_user` — the euid's passwd entry. Authoritative when present,
///    but under static musl (`getpwuid` reads only `/etc/passwd`, no NSS)
///    it is `None` for every LDAP/sssd-provisioned cluster user.
/// 2. `SLURM_JOB_USER` — set by slurmstepd in every batch-step env, naming
///    the submitting (= running) user. Cross-checked against
///    `SLURM_JOB_UID` when that is present and parsable: a mismatch with
///    the actual euid means the env does not describe THIS process, so the
///    name is rejected rather than trusted.
/// 3. `USER`, then `LOGNAME` — the conventional login-name variables,
///    last because they are inherited unverified.
fn resolve_run_user_from(
    passwd_user: Option<String>,
    euid: u32,
    env: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    if let Some(user) = passwd_user {
        return Some(user);
    }
    if let Some(user) = env("SLURM_JOB_USER") {
        let uid_matches = match env("SLURM_JOB_UID").map(|v| v.parse::<u32>()) {
            Some(Ok(uid)) => uid == euid,
            // Unparsable SLURM_JOB_UID: the pair is inconsistent, reject.
            Some(Err(_)) => false,
            // No SLURM_JOB_UID at all: accept the name (slurmstepd sets
            // both, but a missing cross-check beats no fallback).
            None => true,
        };
        if uid_matches {
            return Some(user);
        }
    }
    env("USER").or_else(|| env("LOGNAME"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    /// A passwd entry is authoritative — env is not even consulted.
    #[test]
    fn passwd_entry_wins() {
        let user = resolve_run_user_from(
            Some("pwuser".into()),
            1000,
            env_of(&[("SLURM_JOB_USER", "slurmuser"), ("USER", "envuser")]),
        );
        assert_eq!(user.as_deref(), Some("pwuser"));
    }

    /// THE PRODUCTION FAILURE (krater, static musl + LDAP user): no passwd
    /// entry, but slurmstepd's `SLURM_JOB_USER`/`SLURM_JOB_UID` pair names
    /// the run user — the fallback must resolve it.
    #[test]
    fn slurm_job_user_resolves_when_passwd_is_blind() {
        let user = resolve_run_user_from(
            None,
            1000,
            env_of(&[("SLURM_JOB_USER", "kruppb"), ("SLURM_JOB_UID", "1000")]),
        );
        assert_eq!(user.as_deref(), Some("kruppb"));
    }

    /// `SLURM_JOB_UID` present but NOT the euid → the env describes a
    /// different process; the name is rejected and resolution falls
    /// through to `USER`/`LOGNAME`.
    #[test]
    fn slurm_job_user_rejected_on_uid_mismatch() {
        let env = env_of(&[("SLURM_JOB_USER", "otheruser"), ("SLURM_JOB_UID", "2000")]);
        assert_eq!(resolve_run_user_from(None, 1000, &env), None);
        let env = env_of(&[
            ("SLURM_JOB_USER", "otheruser"),
            ("SLURM_JOB_UID", "2000"),
            ("USER", "envuser"),
        ]);
        assert_eq!(
            resolve_run_user_from(None, 1000, &env).as_deref(),
            Some("envuser"),
        );
    }

    /// Unparsable `SLURM_JOB_UID` is an inconsistent pair → reject, same
    /// as a mismatch.
    #[test]
    fn slurm_job_user_rejected_on_unparsable_uid() {
        let env = env_of(&[("SLURM_JOB_USER", "kruppb"), ("SLURM_JOB_UID", "junk")]);
        assert_eq!(resolve_run_user_from(None, 1000, &env), None);
    }

    /// `SLURM_JOB_USER` without any `SLURM_JOB_UID` is accepted (no
    /// cross-check available, and the fallback existing at all is the
    /// point).
    #[test]
    fn slurm_job_user_accepted_without_uid() {
        let env = env_of(&[("SLURM_JOB_USER", "kruppb")]);
        assert_eq!(
            resolve_run_user_from(None, 1000, &env).as_deref(),
            Some("kruppb"),
        );
    }

    /// `USER` then `LOGNAME` close the chain; nothing set → `None`
    /// (→ [`LingerCheck::UserUnresolved`]).
    #[test]
    fn user_then_logname_then_unresolved() {
        let env = env_of(&[("USER", "envuser"), ("LOGNAME", "loguser")]);
        assert_eq!(
            resolve_run_user_from(None, 1000, &env).as_deref(),
            Some("envuser"),
        );
        let env = env_of(&[("LOGNAME", "loguser")]);
        assert_eq!(
            resolve_run_user_from(None, 1000, &env).as_deref(),
            Some("loguser"),
        );
        assert_eq!(resolve_run_user_from(None, 1000, env_of(&[])), None);
    }
}
