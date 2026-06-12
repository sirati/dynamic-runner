//! SLURM pipeline orchestration.
//!
//! Single concern: sequencing the SLURM-mode launch — gateway connect,
//! image build / transfer, source-binary upload, SLURM job submission,
//! optional reverse-tunnel setup, hand-off to the primary coordinator,
//! and the strictly-ordered teardown.
//!
//! This module owns ONLY the orchestration. It deliberately does NOT
//! own:
//!
//! * gateway transport (`dynrunner-gateway`),
//! * SLURM job management (`SlurmJobManager` in this crate),
//! * the SLURM preparation phase (a sibling concern; the canonical
//!   implementation currently lives in
//!   `python/dynamic_runner/packaging/preparation.py` and will move
//!   to `dynrunner-slurm::preparation` in a follow-up unit),
//! * the primary coordinator (`dynrunner-manager-distributed`).
//!
//! ## Status
//!
//! The pure-Rust orchestrator currently ships as a structural
//! skeleton: the [`PipelineSteps`] trait declares the step
//! boundaries, [`CleanupSteps`] declares the teardown boundaries,
//! and [`PipelineGuard`] enforces the strict teardown order
//! (`preparation.cleanup()` → `gateway.disconnect()` → targeted
//! `pkill`) as a `Drop` impl. The synchronous orchestration body
//! lives in the PyO3 layer (`crates/dynrunner-pyo3/src/slurm/pipeline.rs`)
//! where it can call the existing Python facade
//! (`SlurmPreparation`, `SshGateway`, `SlurmJobManager`) by their
//! public names — names that remain stable across the thin-shim
//! migration of those types from Python to Rust.
//!
//! Once Rust counterparts of `SshGateway`, `SlurmPreparation`, and
//! `SlurmJobManager` land, this module gains a working `run()` that
//! composes them directly, and the PyO3 layer reduces to pure
//! type-marshalling around a call to that pure-Rust `run()`.
//!
//! ## Pkill regex invariant
//!
//! The graceful gateway-master shutdown MUST run before the
//! teardown's targeted sweep (`ssh.*-R [0-9]+:localhost`), so the
//! sweep does not catch the master's own
//! `-R 0.0.0.0:<port>:localhost` forwards. The `0.0.0.0:` prefix is
//! the load-bearing differentiator: per-secondary tunnels use
//! `-R <port>:localhost...` (no prefix) and the regex matches; the
//! master uses `-R 0.0.0.0:<port>:localhost...` and the regex
//! deliberately does NOT match.
//!
//! ## Parentage scoping invariant
//!
//! Every residual sweep is additionally scoped by PARENTAGE
//! ([`TunnelSweepScope`]): the run teardown may only signal THIS
//! process's own children, and the pipeline-start stray cleanup may
//! only signal `init`-orphans. A pattern-only (uid-global) kill is
//! forbidden — it destroyed concurrent runs' verified tunnels
//! (run_20260611_221215 / run_20260611_161034: every secondary then
//! dialed a vanished worker-side listener for the whole bring-up
//! window and 0/11 welcomed).

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;
use tracing;

/// Boundary trait: the pipeline orchestrator's three external concerns.
///
/// Each method is a single async step in the orchestration; the
/// implementation chooses which underlying type owns the step (Rust
/// crate or Python facade). The orchestration sequence in [`run`] is
/// invariant; only the *implementations* of these steps differ across
/// the migration.
pub trait PipelineSteps {
    /// Submit SLURM jobs and (in reverse-connection mode) wait for
    /// secondaries to publish their connection-info files.
    ///
    /// Maps to Python `SlurmPreparation.prepare(...)`. Owns image
    /// build + transfer, sbatch submission, and the reverse-tunnel
    /// state machine. Returns once all secondaries are reachable
    /// (forward-mode: SLURM has accepted the jobs; reverse-mode:
    /// every per-secondary `ssh -N -R` is verified live).
    fn prepare<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>>;

    /// Push the consumer's `--source` tree to the gateway's srcbins
    /// dir. Caller (the orchestrator) decides WHEN to invoke this
    /// based on `uses_file_based_items` / `--source-already-staged`.
    ///
    /// Maps to Python `SlurmJobManager.upload_source_binaries(...)`.
    fn upload_source_binaries<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>>;

    /// Hand control to the Rust primary coordinator. Returns when
    /// the run is finished (success or failure surfaced via the
    /// coordinator's own state, not the `Result`).
    ///
    /// Maps to Python `_drive_rust_primary(...)`.
    fn drive_primary<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>>;
}

/// Strict teardown sequence enforced via `Drop`.
///
/// Cleanup ordering: preparation cleanup FIRST (tracked per-secondary
/// tunnels), THEN graceful gateway-master shutdown (`ssh -O exit`),
/// THEN the targeted pkill belt-and-suspenders for any per-secondary
/// tunnel that escaped tracking. See the module-level docs for why
/// disconnect must precede pkill.
///
/// `Drop` runs the cleanup synchronously; for async cleanup steps the
/// caller is expected to either:
/// * call [`PipelineGuard::finish`] explicitly inside an async context
///   (preferred), or
/// * provide a `tokio::runtime::Handle` so `Drop` can `block_on`
///   (fallback for panic paths).
pub struct PipelineGuard<C: CleanupSteps> {
    steps: Option<C>,
}

impl<C: CleanupSteps> PipelineGuard<C> {
    pub fn new(steps: C) -> Self {
        Self { steps: Some(steps) }
    }

    /// Explicit teardown. Preferred over relying on `Drop` so async
    /// cleanup runs in the caller's runtime context.
    pub async fn finish(mut self) {
        if let Some(steps) = self.steps.take() {
            run_cleanup(steps).await;
        }
    }
}

impl<C: CleanupSteps> Drop for PipelineGuard<C> {
    fn drop(&mut self) {
        if let Some(steps) = self.steps.take() {
            // Best-effort sync teardown; this only fires on panic
            // paths where the async `finish()` couldn't complete.
            steps.cleanup_sync();
        }
    }
}

async fn run_cleanup<C: CleanupSteps>(steps: C) {
    // Step 1: per-secondary tunnel cleanup.
    if let Err(e) = steps.cleanup_preparation().await {
        tracing::warn!(error = %e, "preparation.cleanup() failed");
    }
    // Step 2: graceful gateway-master shutdown. MUST happen before
    // the targeted pkill so the master's own forwards exit cleanly
    // via `ssh -O exit` rather than getting caught by SIGTERM.
    if let Err(e) = steps.disconnect_gateway().await {
        tracing::warn!(error = %e, "gateway.disconnect() failed");
    }
    // Step 3: targeted pkill belt-and-suspenders.
    if let Err(e) = steps.pkill_residual_tunnels().await {
        tracing::warn!(error = %e, "residual-tunnel pkill failed");
    }
}

/// Boundary trait: the three teardown steps, in order. A separate
/// trait from [`PipelineSteps`] because cleanup runs unconditionally
/// (try/finally) while `PipelineSteps` runs only on the happy path,
/// and the impls usually share state but not ownership.
pub trait CleanupSteps: Send + 'static {
    fn cleanup_preparation<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>>;

    fn disconnect_gateway<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>>;

    fn pkill_residual_tunnels<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>>;

    /// Synchronous fallback for the `Drop` panic path.
    fn cleanup_sync(self);
}

/// Which reverse-tunnel processes a residual sweep is allowed to kill.
///
/// The sweep is the belt-and-suspenders layer UNDER the precise owners
/// (`kill_on_drop` + the tunnel registry teardown + the kernel
/// `PR_SET_PDEATHSIG` linkage), so it must never reach FURTHER than
/// they do. The pre-fix sweeps were uid-global `pkill -f` hammers: any
/// dynrunner pipeline finishing (an e2e suite, a stale previous
/// dispatch finally tearing down, an observer probe) killed EVERY
/// `ssh -R` reverse tunnel the user owned on the submitter host —
/// including a CONCURRENT production run's freshly verified tunnels
/// (run_20260611_221215: all 11 worker-side listeners vanished within
/// seconds of verification and every secondary dialed
/// connection-refused for the whole bring-up window; same shape in
/// run_20260611_161034). Scoping by parentage makes cross-run kills
/// structurally impossible: another live run's tunnels are children of
/// THAT run's pid — never `self` and never `init`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunnelSweepScope {
    /// Only processes reparented to `init` (ppid 1) — stragglers whose
    /// spawning dispatch died without teardown (pre-PDEATHSIG builds,
    /// or a SIGKILLed parent on a platform where the death signal was
    /// refused). Safe to run at pipeline START: a live run's tunnels
    /// are children of that live run, not of `init`. (Residual: a
    /// straggler reparented to a user-session subreaper instead of
    /// `init` is missed here — PDEATHSIG owns that case on current
    /// builds.)
    Orphans,
    /// Only THIS process's own children — the run-teardown sweep for
    /// tunnels that escaped the registry's tracked cleanup.
    OwnChildren,
}

/// Match pattern per scope. The teardown (own-children) pattern stays
/// the tight per-secondary shape (`-R <port>:localhost...`), which
/// deliberately does NOT match the master's `-R 0.0.0.0:<port>:...` —
/// see the module-level pkill-regex-invariant section. The orphan
/// sweep matches broadly (any `-R`-carrying ssh toward localhost):
/// everything it can reach is parentless by definition, so a dead
/// run's master is a legitimate target too.
fn sweep_pattern(scope: TunnelSweepScope) -> &'static str {
    match scope {
        TunnelSweepScope::Orphans => r"ssh.*-R.*localhost",
        TunnelSweepScope::OwnChildren => r"ssh.*-R [0-9]+:localhost",
    }
}

/// PURE: which of the `(pid, ppid)` candidates the sweep may signal
/// under `scope`, given the sweeping process's own pid. The scoping
/// rule in one place, unit-testable without processes.
fn sweep_targets(candidates: &[(u32, u32)], self_pid: u32, scope: TunnelSweepScope) -> Vec<u32> {
    candidates
        .iter()
        .filter(|(_, ppid)| match scope {
            TunnelSweepScope::Orphans => *ppid == 1,
            TunnelSweepScope::OwnChildren => *ppid == self_pid,
        })
        .map(|(pid, _)| *pid)
        .collect()
}

/// Parent pid of `pid` from `/proc/<pid>/status`, `None` if the
/// process vanished or the field is unreadable (a gone candidate is
/// simply not swept — never an error).
fn read_ppid(pid: u32) -> Option<u32> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find_map(|l| l.strip_prefix("PPid:"))
        .and_then(|v| v.trim().parse().ok())
}

/// Sweep residual reverse-tunnel processes, scoped by parentage.
///
/// Enumerates candidates with `pgrep -u <uid> -f <pattern>`, filters
/// them through [`sweep_targets`] (own children for the run teardown,
/// `init`-orphans for the pipeline-start stray cleanup), and SIGTERMs
/// exactly that set — the same clean shutdown ssh receives on the
/// orderly teardown ladder. A process of ANOTHER live run (child of
/// that run's pid) is structurally out of every scope.
///
/// Exposed at module level (rather than inlined into a single
/// `CleanupSteps` impl) so the future pure-Rust preparation impl can
/// call the same primitive — single source of truth for the kill
/// pattern + scoping when preparation gains its own reverse-tunnel
/// spawning logic.
pub async fn sweep_residual_reverse_tunnels(
    uid: u32,
    scope: TunnelSweepScope,
) -> std::io::Result<()> {
    let output = tokio::process::Command::new("pgrep")
        .arg("-u")
        .arg(uid.to_string())
        .arg("-f")
        .arg(sweep_pattern(scope))
        .stderr(std::process::Stdio::null())
        .output()
        .await?;
    // pgrep exits 1 when nothing matched — no stragglers, success.
    if !output.status.success() && output.status.code() != Some(1) {
        tracing::warn!(status = ?output.status, "pgrep returned unexpected status");
        return Ok(());
    }
    let candidates: Vec<(u32, u32)> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|l| l.trim().parse::<u32>().ok())
        .filter_map(|pid| read_ppid(pid).map(|ppid| (pid, ppid)))
        .collect();
    let targets = sweep_targets(&candidates, std::process::id(), scope);
    if targets.is_empty() {
        return Ok(());
    }
    tracing::info!(
        ?scope,
        pids = ?targets,
        "sweeping residual reverse-tunnel processes (parentage-scoped)"
    );
    for pid in targets {
        // Best-effort: a target that exited between enumeration and
        // signal is the success case (nothing left to clean).
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("preparation failed: {0}")]
    Preparation(String),
    #[error("source upload failed: {0}")]
    SourceUpload(String),
    #[error("primary coordinator failed: {0}")]
    Primary(String),
    #[error("gateway error: {0}")]
    Gateway(#[from] dynrunner_gateway::traits::GatewayError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// The parentage-scoping rule (run_20260611_221215 replay, pure
    /// half): a candidate that is ANOTHER live process's child — e.g. a
    /// concurrent production run's verified tunnel — is out of EVERY
    /// scope; the teardown scope takes only self's children; the
    /// startup scope takes only `init`-orphans.
    #[test]
    fn sweep_scoping_never_targets_another_runs_children() {
        let self_pid = 4242;
        let candidates = [
            (100, self_pid), // our own straggler        → OwnChildren
            (200, 1),        // dead run's orphan        → Orphans
            (300, 9999),     // CONCURRENT run's tunnel  → never
        ];
        assert_eq!(
            sweep_targets(&candidates, self_pid, TunnelSweepScope::OwnChildren),
            vec![100],
            "teardown sweep must take exactly self's children"
        );
        assert_eq!(
            sweep_targets(&candidates, self_pid, TunnelSweepScope::Orphans),
            vec![200],
            "startup sweep must take exactly init-orphans"
        );
        for scope in [TunnelSweepScope::OwnChildren, TunnelSweepScope::Orphans] {
            assert!(
                !sweep_targets(&candidates, self_pid, scope).contains(&300),
                "a concurrent live run's tunnel (child of pid 9999) must \
                 never be swept under {scope:?}"
            );
        }
    }

    /// Live-process half of the replay: spawn one decoy "tunnel" whose
    /// cmdline matches the teardown pattern as OUR child, and one as a
    /// (still-live) intermediary's child — the concurrent-run shape.
    /// The OwnChildren sweep must kill ours and spare the other.
    #[tokio::test]
    async fn own_children_sweep_spares_other_processes_tunnels() {
        use std::os::unix::process::CommandExt as _;

        // Our own child, argv0 spoofed to the per-secondary tunnel
        // shape. The decoy body is bash (argv0-insensitive — coreutils'
        // `sleep` is a multicall binary that dispatches on argv[0]).
        // The `-c` body is a COMPOUND command so bash stays resident
        // (a single simple command is exec-optimized, replacing the
        // spoofed cmdline with the plain `sleep`).
        let mut own = std::process::Command::new("bash");
        own.arg0("ssh -N -R 54321:localhost:1 sweep-decoy-own");
        own.args(["-c", "sleep 30; exit 0"]);
        let mut own = own.spawn().expect("spawn own decoy");

        // A concurrent "run": a live intermediary bash whose CHILD
        // matches the pattern (ppid = bash, not us, not init).
        // The port digits ride an env var so the INTERMEDIARY's own
        // cmdline does not itself match the sweep pattern (only the
        // exec'd child's runtime-expanded argv0 does).
        let mut other = std::process::Command::new("bash")
            .arg("-c")
            .arg(
                "(exec -a \"ssh -N -R ${P}:localhost:1 sweep-decoy-other\" \
                 bash -c 'sleep 30; exit 0') & wait",
            )
            .env("P", "54322")
            .spawn()
            .expect("spawn other-run decoy");

        // Wait until pgrep sees both decoys (spawn + exec are async).
        let uid = unsafe { libc::getuid() };
        for _ in 0..50 {
            let out = tokio::process::Command::new("pgrep")
                .args(["-u", &uid.to_string(), "-f", "sweep-decoy-"])
                .output()
                .await
                .expect("pgrep");
            if String::from_utf8_lossy(&out.stdout).lines().count() >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        sweep_residual_reverse_tunnels(uid, TunnelSweepScope::OwnChildren)
            .await
            .expect("sweep");

        // Our child dies (SIGTERM); poll briefly for the reap.
        let mut own_dead = false;
        for _ in 0..50 {
            if own.try_wait().expect("try_wait own").is_some() {
                own_dead = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(own_dead, "the sweep must SIGTERM this process's own decoy tunnel");

        // The other "run's" tunnel survives: its intermediary is still
        // waiting on it.
        assert!(
            other.try_wait().expect("try_wait other").is_none(),
            "the sweep must NOT touch a tunnel owned by another live process"
        );
        let _ = other.kill();
        let _ = other.wait();
    }

    /// Verifies the cleanup order is preparation → disconnect → pkill,
    /// regardless of which step (if any) errors out. Each step records
    /// its own number on a shared vec; the assertion is on the vec
    /// shape after `run_cleanup` returns.
    #[tokio::test]
    async fn cleanup_runs_in_documented_order() {
        struct Recorder {
            calls: Arc<Mutex<Vec<u8>>>,
        }
        impl CleanupSteps for Recorder {
            fn cleanup_preparation<'a>(
                &'a self,
            ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
                let calls = self.calls.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push(1);
                    Ok(())
                })
            }
            fn disconnect_gateway<'a>(
                &'a self,
            ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
                let calls = self.calls.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push(2);
                    Ok(())
                })
            }
            fn pkill_residual_tunnels<'a>(
                &'a self,
            ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
                let calls = self.calls.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push(3);
                    Ok(())
                })
            }
            fn cleanup_sync(self) {}
        }

        let calls: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Recorder {
            calls: calls.clone(),
        };
        run_cleanup(recorder).await;
        assert_eq!(*calls.lock().unwrap(), vec![1u8, 2, 3]);
    }

    /// Even when each step errors, all three still run (try/finally
    /// semantics — earlier failures must not skip later cleanup).
    #[tokio::test]
    async fn cleanup_continues_on_error() {
        struct AlwaysErrors {
            calls: Arc<Mutex<Vec<u8>>>,
        }
        impl CleanupSteps for AlwaysErrors {
            fn cleanup_preparation<'a>(
                &'a self,
            ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
                let calls = self.calls.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push(1);
                    Err(PipelineError::Preparation("simulated".into()))
                })
            }
            fn disconnect_gateway<'a>(
                &'a self,
            ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
                let calls = self.calls.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push(2);
                    Err(PipelineError::Other("simulated".into()))
                })
            }
            fn pkill_residual_tunnels<'a>(
                &'a self,
            ) -> Pin<Box<dyn Future<Output = Result<(), PipelineError>> + Send + 'a>> {
                let calls = self.calls.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push(3);
                    Err(PipelineError::Other("simulated".into()))
                })
            }
            fn cleanup_sync(self) {}
        }

        let calls: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = AlwaysErrors {
            calls: calls.clone(),
        };
        run_cleanup(recorder).await;
        assert_eq!(*calls.lock().unwrap(), vec![1u8, 2, 3]);
    }
}
