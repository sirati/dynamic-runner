//! Single concern: decouple the secondary's rootless-podman workers from
//! the submitter's login session, so a dropped `ssh -J -R -N` (the only
//! login session) does not reap `user@<uid>.service` and fan-kill the
//! podman scope.
//!
//! THE CHAIN (proven, krater17): `Linger=no` + the submitter's only login
//! session drops → systemd reaps `user@<uid>.service` → SIGTERMs the podman
//! scope → signal-proxy → container PID1 → fan-kill. `loginctl
//! enable-linger` is the direct cure: it talks to `org.freedesktop.login1`
//! on the SYSTEM bus (`/run/dbus/system_bus_socket`), reachable from the
//! SLURM job INDEPENDENT of the unreachable `user@<uid>` bus. Linger keeps
//! `user@.service` running past last-session-close so the `default.target`
//! stop that reaped the scope never fires — the chain is broken at origin.
//!
//! Boundary: callers get a [`LingerState`] enum and know nothing about
//! `loginctl`, logind, the system bus, or how the run user is resolved. The
//! decision logic ([`classify`]) is a PURE function of the `show-user
//! --value -p Linger` output so it is unit-testable independent of the
//! process spawn (the `loginctl` invocation needs a live logind and is not
//! unit-testable).

use crate::bin_resolve::which;
use std::process::{Command, Stdio};

/// Outcome of the launch-time linger self-heal. The call site maps this to
/// proceed (`AlreadyOn`/`Enabled`) or fail-fast (`Failed`); it never sees
/// the `loginctl` mechanics behind the decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LingerState {
    /// Linger was already enabled for the run user — workers are decoupled
    /// from the login session already; nothing to do.
    AlreadyOn,
    /// Linger was off and this wrapper enabled it (and re-verified it took).
    Enabled,
    /// Linger could not be confirmed on (loginctl absent, the enable was
    /// denied, or the re-verify still read `no`). Workers are NOT decoupled.
    Failed { reason: String },
}

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

/// Ensure the run user has linger enabled so the workers survive the
/// submitter login-session close. Resolve the run user, CHECK current
/// linger, ENABLE if off, then RE-VERIFY the enable took.
///
/// Best-effort PATH resolution of `loginctl` via [`which`]; an absent
/// `loginctl` is a [`LingerState::Failed`] (the wrapper cannot decouple the
/// workers without it). The actual logind round-trip is not unit-testable;
/// the parse/decision logic lives in [`classify`].
pub fn ensure_linger() -> LingerState {
    let Some(loginctl) = which("loginctl") else {
        return LingerState::Failed {
            reason: "loginctl not found on PATH; cannot enable linger".to_string(),
        };
    };

    // Resolve the run user by name where possible — `enable-linger <user>`
    // is explicit and matches the operator's `loginctl enable-linger <user>`
    // muscle memory. When the euid has no passwd entry (unusual, but
    // possible under nss quirks), fall back to the bare self-targeting form
    // (`show-user`/`enable-linger` with no positional arg default to the
    // caller's own user).
    let user = match nix::unistd::User::from_uid(nix::unistd::geteuid()) {
        Ok(Some(u)) => Some(u.name),
        _ => None,
    };

    // (1) CHECK current linger.
    match run_show_user(&loginctl, user.as_deref()) {
        Ok(check) => match classify(&check) {
            LingerCheck::On => return LingerState::AlreadyOn,
            LingerCheck::Off | LingerCheck::Unknown => { /* fall through to enable */ }
        },
        Err(reason) => {
            return LingerState::Failed {
                reason: format!("could not read current linger state: {reason}"),
            };
        }
    }

    // (2) ENABLE.
    if let Err(reason) = run_enable_linger(&loginctl, user.as_deref()) {
        return LingerState::Failed {
            reason: format!("enable-linger failed: {reason}"),
        };
    }

    // (3) RE-VERIFY the enable actually took (a non-zero-exit enable would
    // have been caught above, but polkit can also accept the call and leave
    // the state unchanged on some configs; the re-read is the ground truth).
    match run_show_user(&loginctl, user.as_deref()) {
        Ok(check) => match classify(&check) {
            LingerCheck::On => LingerState::Enabled,
            LingerCheck::Off | LingerCheck::Unknown => LingerState::Failed {
                reason: format!(
                    "enable-linger ran but re-verify still reads not-on (value: {:?})",
                    check.trim()
                ),
            },
        },
        Err(reason) => LingerState::Failed {
            reason: format!("could not re-verify linger after enable: {reason}"),
        },
    }
}

/// `loginctl show-user <user> --property=Linger --value` → trimmed stdout.
/// System bus; cheap. `Err(reason)` on spawn failure or a non-zero exit.
/// With no `user` the positional arg is omitted (self).
fn run_show_user(loginctl: &str, user: Option<&str>) -> Result<String, String> {
    let mut cmd = Command::new(loginctl);
    cmd.arg("show-user");
    if let Some(u) = user {
        cmd.arg(u);
    }
    cmd.arg("--property=Linger").arg("--value");
    run_capture(cmd)
}

/// `loginctl enable-linger <user>` (self when `user` is `None`). System bus.
/// `Err(reason)` on spawn failure or a non-zero exit.
fn run_enable_linger(loginctl: &str, user: Option<&str>) -> Result<(), String> {
    let mut cmd = Command::new(loginctl);
    cmd.arg("enable-linger");
    if let Some(u) = user {
        cmd.arg(u);
    }
    run_capture(cmd).map(|_| ())
}

/// Run a `loginctl` command capturing stdout. Returns the trimmed stdout on
/// success, or a single-line failure reason (including captured stderr) on
/// spawn error / non-zero exit.
fn run_capture(mut cmd: Command) -> Result<String, String> {
    let out = cmd
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("spawn failed: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "exited {:?}: {}",
            out.status.code(),
            stderr.trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
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
        // read as on — the caller treats Unknown as "needs enable / not
        // confirmed", never as success.
        assert_eq!(classify(""), LingerCheck::Unknown);
        assert_eq!(classify("\n"), LingerCheck::Unknown);
        assert_eq!(classify("Linger=yes"), LingerCheck::Unknown);
        assert_eq!(classify("maybe"), LingerCheck::Unknown);
    }

    /// Decision logic: `yes` short-circuits to AlreadyOn (no enable needed);
    /// `no`/Unknown must NOT be classified as on (they fall through to the
    /// enable path). This is the AlreadyOn-vs-needs-enable branch that
    /// `ensure_linger` keys off, verified without spawning loginctl.
    #[test]
    fn decision_already_on_vs_needs_enable() {
        assert_eq!(classify("yes"), LingerCheck::On, "on -> AlreadyOn");
        assert_ne!(classify("no"), LingerCheck::On, "off must enable");
        assert_ne!(classify(""), LingerCheck::On, "unknown must enable");
    }

    /// Fail-fast reachability: with `loginctl` absent from PATH,
    /// `ensure_linger` must return `Failed` (the call site exits non-zero
    /// before launching the container). We exercise the real entrypoint
    /// with an empty PATH so `which("loginctl")` resolves to `None`,
    /// guaranteeing the Failed branch independent of the host's logind.
    #[test]
    fn ensure_linger_fails_fast_when_loginctl_absent() {
        // PATH is process-global; serialize against PATH-sensitive siblings
        // is unnecessary here because no other test in this module reads
        // PATH, and `which` reads it fresh each call.
        let saved = std::env::var_os("PATH");
        std::env::set_var("PATH", "");
        let state = ensure_linger();
        match saved {
            Some(p) => std::env::set_var("PATH", p),
            None => std::env::remove_var("PATH"),
        }
        match state {
            LingerState::Failed { reason } => {
                assert!(
                    reason.contains("loginctl"),
                    "reason should name loginctl, got {reason:?}"
                );
            }
            other => panic!("expected Failed when loginctl absent, got {other:?}"),
        }
    }

    /// The reason string is plumbed through the Failed variant so the call
    /// site's `tracing::error!` can surface it. Confirms the variant shape
    /// the call site matches on.
    #[test]
    fn failed_carries_reason() {
        let s = LingerState::Failed {
            reason: "boom".to_string(),
        };
        assert_eq!(
            s,
            LingerState::Failed {
                reason: "boom".to_string()
            }
        );
    }
}
