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
//! The wrapper does NOT enable linger: its sbatch/slurmstepd context has NO
//! logind session, so `loginctl enable-linger` ENXIOs there ("No such device
//! or address"). The wrapper's role is therefore ONLY to CHECK + HONOR the
//! state the setup established, and to WARN (a safety net) if it reads as not
//! set — surfacing a setup-enable that silently failed. The wrapper performs
//! NO enable / disable / restore; the setup owns the whole linger lifecycle.
//!
//! Boundary: callers get one primitive — [`check_linger`] (the boolean
//! current state) — and know nothing about `loginctl`, logind, the system
//! bus, or how the run user is resolved. The decision parse ([`classify`])
//! is a PURE function of the `show-user --value -p Linger` output so it is
//! unit-testable independent of the process spawn (the `loginctl` invocation
//! needs a live logind and is not unit-testable).
//!
//! BOUNDED: the `loginctl` shell-out runs under
//! [`dynrunner_reap::bounded_command::run_bounded`] — the SAME bound the
//! in-band reap uses — so a wedged logind degrades to "could not confirm"
//! rather than stranding the wrapper before the container launch.

use crate::bin_resolve::which;
use std::process::Command;
use std::time::Duration;

use dynrunner_reap::bounded_command::{BoundedOutcome, run_bounded};
use dynrunner_reap::clock::RealClock;

/// Wall-clock bound for the `loginctl` round-trip. The system-bus logind
/// call is near-instant when healthy; bound it SHORT so a wedged logind
/// degrades to "could not confirm" rather than stalling the launch path.
const LOGINCTL_BUDGET: Duration = Duration::from_secs(5);

/// The login-session decoupling status parsed from a single
/// `loginctl show-user <user> --property=Linger --value` read: `yes` /
/// `no`, or `Unknown` when the value is empty/unparsable (e.g. the user has
/// no logind record yet).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LingerCheck {
    On,
    Off,
    Unknown,
}

/// PURE: classify the trimmed stdout of `loginctl show-user --value -p
/// Linger`. `--value` prints the bare property value, so the expected
/// payload is exactly `yes` or `no` (one line). Anything else (empty, an
/// error line, a future-systemd value) is [`LingerCheck::Unknown`] so the
/// caller treats it as "not confirmed on" rather than guessing.
fn classify(check_output: &str) -> LingerCheck {
    match check_output.trim() {
        "yes" => LingerCheck::On,
        "no" => LingerCheck::Off,
        _ => LingerCheck::Unknown,
    }
}

/// CHECK: is the run user currently lingering? Reads
/// `loginctl show-user --value -p Linger`. Any non-`yes` answer — `no`, an
/// empty/unparsable value, an absent `loginctl`, or a spawn/exit/timeout
/// error — is treated as `false` (NOT confirmed lingering): the caller then
/// WARNs (the setup should have enabled it) but PROCEEDS, since linger is a
/// resilience property, never a launch gate. The wrapper does NOT enable it
/// — the setup-side ssh owns that (see module doc).
pub fn check_linger() -> bool {
    let Some(loginctl) = which("loginctl") else {
        return false;
    };
    let user = resolve_run_user();
    match run_show_user(&loginctl, user.as_deref()) {
        Ok(check) => classify(&check) == LingerCheck::On,
        Err(_) => false,
    }
}

/// Resolve the run user by passwd name where possible. When the euid has no
/// passwd entry (unusual, but possible under nss quirks), returns `None` and
/// the caller falls back to the bare self-targeting `loginctl` form
/// (`show-user` with no positional arg defaults to the caller's own user).
fn resolve_run_user() -> Option<String> {
    match nix::unistd::User::from_uid(nix::unistd::geteuid()) {
        Ok(Some(u)) => Some(u.name),
        _ => None,
    }
}

/// `loginctl show-user <user> --property=Linger --value` → trimmed stdout.
/// System bus; cheap. `Err(reason)` on spawn failure, non-zero exit, OR a
/// logind hang past [`LOGINCTL_BUDGET`]. With no `user` the positional arg
/// is omitted (self).
fn run_show_user(loginctl: &str, user: Option<&str>) -> Result<String, String> {
    let mut cmd = Command::new(loginctl);
    cmd.arg("show-user");
    if let Some(u) = user {
        cmd.arg(u);
    }
    cmd.arg("--property=Linger").arg("--value");
    // want_stdout=true: we need the `yes`/`no` value back.
    match run_bounded(cmd, LOGINCTL_BUDGET, &RealClock, true) {
        BoundedOutcome::Exited {
            success: true,
            stdout,
        } => Ok(String::from_utf8_lossy(&stdout).trim().to_string()),
        outcome => Err(describe_failure(&outcome)),
    }
}

/// Single-line failure reason for a non-success [`BoundedOutcome`]. stderr is
/// nulled by `run_bounded` (the bounded path treats it as best-effort
/// silent), so the reason is the exit/timeout/spawn classification — enough
/// for the caller's WARNING. (Never called with `Exited { success: true }`.)
fn describe_failure(outcome: &BoundedOutcome) -> String {
    match outcome {
        BoundedOutcome::Exited { success: false, .. } => "non-zero exit".to_string(),
        BoundedOutcome::TimedOut => format!(
            "timed out after {}s (logind unresponsive); killed",
            LOGINCTL_BUDGET.as_secs()
        ),
        BoundedOutcome::SpawnError(e) => format!("spawn error: {e}"),
        BoundedOutcome::Exited { success: true, .. } => "ok".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_yes_is_on() {
        assert_eq!(classify("yes"), LingerCheck::On);
        // `--value` output may carry a trailing newline from loginctl.
        assert_eq!(classify("yes\n"), LingerCheck::On);
        assert_eq!(classify("  yes  "), LingerCheck::On);
    }

    #[test]
    fn classify_no_is_off() {
        assert_eq!(classify("no"), LingerCheck::Off);
        assert_eq!(classify("no\n"), LingerCheck::Off);
    }

    #[test]
    fn classify_empty_or_garbage_is_unknown() {
        // Empty (no logind record yet) and anything unexpected must NOT be
        // read as on — the caller WARNs on Unknown rather than assuming the
        // setup enabled linger.
        assert_eq!(classify(""), LingerCheck::Unknown);
        assert_eq!(classify("\n"), LingerCheck::Unknown);
        assert_eq!(classify("Linger=yes"), LingerCheck::Unknown);
        assert_eq!(classify("maybe"), LingerCheck::Unknown);
    }

    /// Decision logic: only `yes` is read as lingering; `no`/Unknown must
    /// NOT be — `check_linger` keys the honor-vs-warn branch off exactly
    /// this `On` test. Verified without spawning loginctl.
    ///
    /// (The loginctl-ABSENT path — `which("loginctl") == None` → `false` —
    /// is deliberately NOT exercised here by clobbering the process-global
    /// `PATH`: this binary's sibling tests spawn `bash`/`podman` via PATH,
    /// so emptying it from one test races those spawns to a spurious
    /// ENOENT. The None-on-absent behaviour belongs to and is tested in
    /// `bin_resolve::which`; here it is a single early return on `which`.)
    #[test]
    fn only_yes_is_lingering() {
        assert_eq!(classify("yes"), LingerCheck::On, "yes -> lingering");
        assert_ne!(classify("no"), LingerCheck::On, "no -> not lingering");
        assert_ne!(classify(""), LingerCheck::On, "unknown -> not lingering");
    }
}
