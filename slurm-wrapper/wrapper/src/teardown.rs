//! Single concern: wrapper teardown.
//!
//! Two parts, both teardown:
//!
//!   * [`forward_shutdown_nudge`] — wake the retained out-of-cgroup
//!     shutdown manager (SIGCONT) so it re-evaluates idle-shutdown. This
//!     is now a pure last-resort nudge, NOT the primary reap path.
//!
//!   * [`reap_container_inband`] — the BOUNDED SYNCHRONOUS in-band reap
//!     of the container's conmon + workload PIDs, performed by the wrapper
//!     itself inside the `KillWait` window on a terminating signal, BEFORE
//!     it returns to SLURM. This is delegation-INDEPENDENT (it kills the
//!     orphan unconditionally via `kill(2)` on captured host PIDs, no
//!     cgroup/delegation needed) and self-bounded (~15s total) so it
//!     finishes before the cgroup SIGKILL sweep and the `slurm_*.out` log
//!     shows the full `stop → SIGTERM → force-kill → confirmed gone`
//!     sequence. It reuses the shared [`dynrunner_reap`] reap
//!     state-machine — the SAME one the out-of-cgroup manager runs — so
//!     there is no second, private copy of the reap.

use std::process::Command;
use std::time::Duration;

use dynrunner_reap::bounded_command::{run_bounded, BoundedOutcome};
use dynrunner_reap::clock::RealClock;
use dynrunner_reap::process_probe::{KillProbe, ProcessProbe};
use dynrunner_reap::reap::{reap_pids, ReapGraces, ReapStatus, ReapTarget};

use crate::dirs::Layout;
use crate::shutdown_spawn::ShutdownMode;
use crate::LOG_TARGET;

/// Bounded grace budget for the in-band reap. Sized to fit comfortably
/// inside a stock `KillWait` (~30s) and the framework's own
/// `signal_lead_seconds`: graceful container stop, then a short SIGTERM
/// grace, then a short SIGKILL grace. Self-bounded — never reads
/// `KillWait` (operator policy the wrapper does not control), so the
/// `signal_lead_seconds`/`KillWait` inversion can never strand a
/// half-finished escalation. Total ≈ g1 + g2 + g3 = 15s.
const STOP_GRACE: Duration = Duration::from_secs(10);
const SIGTERM_GRACE: Duration = Duration::from_secs(3);
const SIGKILL_GRACE: Duration = Duration::from_secs(2);

/// Wall-clock bound for the metadata `podman inspect` calls (PID capture).
/// A read of the container record is near-instant when healthy; bound it
/// SHORT so a podman wedged on NFS-backed storage cannot gate the kill(2)
/// reap. The reap is podman-/delegation-INDEPENDENT and is the hard
/// backstop — if inspect cannot answer, we proceed with whatever PIDs we
/// captured (possibly none) rather than block until SLURM's `KillWait`
/// SIGKILLs the wrapper mid-teardown.
const INSPECT_BUDGET: Duration = Duration::from_secs(5);

/// Wall-clock bound for `podman stop -t <STOP_GRACE>`. Podman's own `-t`
/// already bounds the graceful wait to [`STOP_GRACE`] before it SIGKILLs
/// the container, but the `podman` PROCESS itself can still wedge (NFS
/// storage lock, hung conmon). Bound at `STOP_GRACE` + headroom so a
/// healthy stop completes, while a wedged podman is killed and we fall
/// straight through to the identity-checked kill(2) reap — the stop is a
/// best-effort courtesy, the reap is the guarantee.
const STOP_BUDGET: Duration = Duration::from_secs(15);

/// Wall-clock bound for `podman rm -f` (handle destruction). Best-effort
/// cleanup AFTER the reap has already confirmed the PIDs gone; bounded so
/// a wedged podman cannot strand the wrapper at the very end of teardown.
const RM_BUDGET: Duration = Duration::from_secs(5);

/// Mirror generate.rs:306-310: `systemctl --user kill --signal=SIGCONT
/// <unit>` for systemd mode, no-op for `None`.
///
/// SIGCONT cannot be blocked or ignored and does not terminate the
/// target; it only wakes the manager's poll loop so it re-evaluates
/// idle-shutdown. All errors are swallowed, mirroring the bash
/// `2>/dev/null || true`: a missing unit means the manager is already
/// gone, which is not the wrapper's concern. (The `setsid` mode was
/// removed with its broken fallback — see `shutdown_spawn`.)
pub fn forward_shutdown_nudge(mode: &ShutdownMode) {
    match mode {
        ShutdownMode::Systemd { unit } => {
            // Mirror `systemctl --user kill --signal=SIGCONT <unit>
            // 2>/dev/null || true`: ignore spawn/exit failures.
            let _ = Command::new("systemctl")
                .args(["--user", "kill", "--signal=SIGCONT", unit])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
        ShutdownMode::None => {}
    }
}

/// Bounded SYNCHRONOUS in-band reap of the container's conmon + workload
/// PIDs, run on a terminating signal before the wrapper returns to SLURM.
///
/// Sequence (each step logged, so `slurm_*.out` carries the full
/// grace→force-kill narrative):
///   1. resolve conmon PID + workload PID (+ start times) from
///      `podman inspect` while the record still exists;
///   2. `podman stop -t <STOP_GRACE>` — graceful container stop, bounded;
///   3. if a captured PID is still alive: identity-checked
///      SIGTERM → `SIGTERM_GRACE` → SIGKILL → `SIGKILL_GRACE` on conmon
///      AND the workload, via the shared reap state-machine;
///   4. `podman rm -f` — destroy the handle.
///
/// SYNC (blocking) by design: it runs `thread::sleep` graces and is the
/// terminal teardown step. The caller drives it via `spawn_blocking` so
/// the async runtime stays responsive. Returns the aggregate
/// [`ReapStatus`] for the forensic log.
pub fn reap_container_inband(podman: &str, layout: &Layout) -> ReapStatus {
    let probe = KillProbe;
    let name = layout.container_name.as_str();

    // 1. Resolve the host PIDs while the container record still exists.
    let conmon = inspect_pid(podman, layout, name, "{{.State.ConmonPid}}")
        .map(|pid| ReapTarget::new(pid, probe.start_time(pid)));
    let workload = inspect_pid(podman, layout, name, "{{.State.Pid}}")
        .map(|pid| ReapTarget::new(pid, probe.start_time(pid)));
    tracing::warn!(
        target: LOG_TARGET,
        container = %name,
        conmon_pid = conmon.map(|t| t.pid),
        workload_pid = workload.map(|t| t.pid),
        "in-band reap: resolved container PIDs; beginning graceful stop"
    );

    // 2. Graceful container stop, bounded.
    let stopped = podman_stop(podman, layout, name, STOP_GRACE.as_secs() as u32);
    tracing::info!(
        target: LOG_TARGET,
        container = %name,
        ok = stopped,
        grace_secs = STOP_GRACE.as_secs(),
        "in-band reap: podman stop returned"
    );

    // 3. Identity-checked SIGTERM→grace→SIGKILL of conmon AND the
    // workload directly — the delegation-independent kill. conmon first
    // (the supervisor that survives a missed cgroup sweep), then the
    // workload.
    let targets: Vec<ReapTarget> = [conmon, workload].into_iter().flatten().collect();
    let graces = ReapGraces {
        sigterm_grace: SIGTERM_GRACE,
        sigkill_grace: SIGKILL_GRACE,
    };
    let mut log = |msg: &str| tracing::warn!(target: LOG_TARGET, "in-band reap: {msg}");
    let reap = reap_pids(&probe, &RealClock, &targets, graces, &mut log);

    // 4. Destroy the podman handle. `--rm` may already have cleaned up if
    // the container stopped, so this is best-effort.
    let removed = podman_rm_force(podman, layout, name);
    match reap {
        ReapStatus::OrphanSurvives => tracing::error!(
            target: LOG_TARGET,
            container = %name,
            "in-band reap: a captured PID survived SIGKILL grace — orphan may persist"
        ),
        ReapStatus::ConfirmedGone | ReapStatus::NotApplicable => tracing::info!(
            target: LOG_TARGET,
            container = %name,
            rm_ok = removed,
            "in-band reap: container confirmed gone; force-kill sequence complete"
        ),
    }
    reap
}

/// `podman --root .. --runroot .. inspect --format <fmt> <name>` → host
/// PID, or `None` when the record is gone / the field is 0 / unparsable /
/// or podman wedged past [`INSPECT_BUDGET`]. Mirrors the shutdown-manager's
/// `inspect_pid`; both run the SAME invocation shape against the
/// per-secondary storage root. BOUNDED: a podman stuck on NFS storage must
/// not gate the kill(2) reap, so a timeout degrades to `None` (no captured
/// PID for this slot) — the reap still runs on whatever was captured.
fn inspect_pid(podman: &str, layout: &Layout, name: &str, format: &str) -> Option<u32> {
    let mut cmd = podman_base(podman, layout);
    cmd.arg("inspect").arg("--format").arg(format).arg(name);
    let BoundedOutcome::Exited {
        success: true,
        stdout,
    } = run_bounded(cmd, INSPECT_BUDGET, &RealClock, true)
    else {
        return None;
    };
    let pid = String::from_utf8_lossy(&stdout)
        .trim()
        .lines()
        .next()?
        .trim()
        .parse::<u32>()
        .ok()?;
    match pid {
        0 => None,
        _ => Some(pid),
    }
}

/// `podman stop -t <grace> <name>` — graceful stop, best-effort bool.
/// BOUNDED at [`STOP_BUDGET`]: a wedged podman is SIGKILLed and reported
/// `false`, never blocking the kill(2) reap that follows.
fn podman_stop(podman: &str, layout: &Layout, name: &str, grace_secs: u32) -> bool {
    let mut cmd = podman_base(podman, layout);
    cmd.arg("stop").arg("-t").arg(grace_secs.to_string()).arg(name);
    run_silent_bounded(cmd, STOP_BUDGET)
}

/// `podman rm -f <name>` — force-remove the handle, best-effort bool.
/// BOUNDED at [`RM_BUDGET`]: a wedged podman cannot strand teardown at the
/// final cleanup step.
fn podman_rm_force(podman: &str, layout: &Layout, name: &str) -> bool {
    let mut cmd = podman_base(podman, layout);
    cmd.arg("rm").arg("-f").arg(name);
    run_silent_bounded(cmd, RM_BUDGET)
}

/// Build a `podman` invocation with the per-secondary storage prefix +
/// cgroupfs manager — the SAME prefix `podman_run` uses, so the inspect /
/// stop / rm see the same storage root the container was launched under.
fn podman_base(podman: &str, layout: &Layout) -> Command {
    let mut c = Command::new(podman);
    c.arg("--root")
        .arg(&layout.podman_storage)
        .arg("--runroot")
        .arg(&layout.podman_run)
        .arg("--cgroup-manager=cgroupfs")
        // Per-child XDG (C3): podman's rootless storage cookie lives in
        // $XDG_RUNTIME_DIR; point it at the same per-secondary run root
        // the launch used, never the wrapper's inherited value.
        .env("XDG_RUNTIME_DIR", &layout.podman_run);
    c
}

/// Run a command under a wall-clock bound, swallowing all output and
/// returning whether it exited 0. A timeout (the command was SIGKILLed) or
/// a spawn error reports `false` — best-effort: on the teardown critical
/// path the kill(2) reap is the guarantee, so a wedged or failed podman
/// degrades to `false` and never blocks.
fn run_silent_bounded(cmd: Command, budget: Duration) -> bool {
    matches!(
        run_bounded(cmd, budget, &RealClock, false),
        BoundedOutcome::Exited { success: true, .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_mode_is_noop() {
        forward_shutdown_nudge(&ShutdownMode::None);
    }

    /// With no live container record, `podman inspect` reports nothing,
    /// so the in-band reap captures no targets and reports
    /// `NotApplicable` — the no-orphan happy path. Uses a fake podman
    /// that prints empty inspect output and exits 0 for stop/rm.
    #[test]
    fn reap_inband_no_record_is_not_applicable() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = tempfile::tempdir().unwrap();
        // Fake podman: `inspect` exits non-zero (no record), everything
        // else exits 0.
        let podman = tmp.path().join("fake-podman");
        let mut f = std::fs::File::create(&podman).unwrap();
        writeln!(
            f,
            "#!/usr/bin/env bash\nfor a in \"$@\"; do if [ \"$a\" = inspect ]; then exit 1; fi; done\nexit 0"
        )
        .unwrap();
        drop(f);
        std::fs::set_permissions(&podman, std::fs::Permissions::from_mode(0o755)).unwrap();

        let layout = test_layout(tmp.path());
        let status = reap_container_inband(&podman.to_string_lossy(), &layout);
        assert_eq!(
            status,
            ReapStatus::NotApplicable,
            "no captured PID → nothing to reap → NotApplicable"
        );
    }

    fn test_layout(root: &std::path::Path) -> Layout {
        let podman_run = root.join("run");
        std::fs::create_dir_all(&podman_run).unwrap();
        Layout {
            rndtmp: root.to_path_buf(),
            container_name: "asm-test-0".to_string(),
            src_tmp: root.join("src"),
            out_tmp: root.join("out"),
            log_tmp: root.join("log"),
            podman_storage: root.join("storage"),
            podman_run,
            socket_dir: root.join("sockets"),
            cmd_socket: root.join("sockets/cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-test".to_string(),
            shutdown_log_dir: root.join("log-network/sec-0"),
            shutdown_log_path: root.join("log-network/sec-0/shutdown-manager.log"),
            shutdown_pid_file: root.join("shutdown-manager.pid"),
            local_image: root.join("image.tar"),
            image_cache_root: root.join("imgcache"),
        }
    }
}
