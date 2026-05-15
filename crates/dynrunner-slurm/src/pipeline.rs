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
//! teardown's targeted pkill (`ssh.*-R [0-9]+:localhost`), so the
//! pkill does not catch the master's own
//! `-R 0.0.0.0:<port>:localhost` forwards. The `0.0.0.0:` prefix is
//! the load-bearing differentiator: per-secondary tunnels use
//! `-R <port>:localhost...` (no prefix) and the regex matches; the
//! master uses `-R 0.0.0.0:<port>:localhost...` and the regex
//! deliberately does NOT match.

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

/// Run the residual-tunnel pkill via `tokio::process::Command`.
///
/// Pattern matches per-secondary reverse tunnels
/// (`ssh ... -R <port>:localhost...`) but deliberately NOT the master
/// (`ssh ... -R 0.0.0.0:<port>:localhost...`). See the module-level
/// pkill-regex-invariant section for the full rationale.
///
/// Exposed at module level (rather than inlined into a single
/// `CleanupSteps` impl) so the future pure-Rust preparation impl can
/// call the same primitive — single source of truth for the kill
/// regex when preparation gains its own reverse-tunnel spawning logic.
/// Scale-aware default for the per-secondary setup-deadline (seconds).
///
/// Single source of truth for the formula
/// `max(60, num_secondaries * 15)` that the SLURM pipeline applies
/// when the operator did not pass `--slurm-setup-deadline-secs`.
///
/// Why a scale-aware default at all: the secondary's setup phase
/// (welcome + cert exchange + wait-for-setup) is bounded by
/// `SecondaryConfig.setup_deadline`. The primary today waits for ALL
/// secondaries to connect before broadcasting the setup-bootstrap;
/// when a slow cluster takes >60s to schedule + boot every node, the
/// first-arriving secondaries hit the (default 60s) deadline and
/// cold-exit BEFORE the late ones have connected, by which point the
/// primary's broadcast lands on closed channels and the cluster
/// collapses with `channel closed`. The architectural fix (primary
/// fires on quorum + late-arrivers use `join_running_cluster`) is a
/// separate follow-up; this knob is the operator-facing band-aid.
///
/// Why `num_secondaries * 15`: empirically, LMU Krater (the load-
/// bearing repro at 2026-05) saw ~15s between consecutive sbatch-
/// dispatched secondaries reaching the primary; the linear scaling
/// keeps `--jobs 1..4` at the historical 60s floor (no behaviour
/// change for small runs) while letting `--jobs 15` ramp to 225s
/// and `--jobs 32` to 480s. The formula is a heuristic, not a
/// derivation — operators on faster or slower clusters override
/// with `--slurm-setup-deadline-secs N`.
///
/// `explicit_override = Some(n)` always wins regardless of
/// `num_secondaries`; the formula only applies when the operator
/// left the knob unset.
pub fn compute_setup_deadline_secs(explicit_override: Option<u64>, num_secondaries: u32) -> u64 {
    if let Some(n) = explicit_override {
        return n;
    }
    let scaled = u64::from(num_secondaries).saturating_mul(15);
    scaled.max(60)
}

pub async fn pkill_residual_reverse_tunnels(uid: u32) -> std::io::Result<()> {
    let status = tokio::process::Command::new("pkill")
        .arg("-u")
        .arg(uid.to_string())
        .arg("-f")
        .arg(r"ssh.*-R [0-9]+:localhost")
        .stderr(std::process::Stdio::null())
        .status()
        .await?;
    // pkill exits 1 when no process matched — that's the success
    // case here (no stragglers to clean up).
    if !status.success() && status.code() != Some(1) {
        tracing::warn!(?status, "pkill returned unexpected status");
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
        let recorder = Recorder { calls: calls.clone() };
        run_cleanup(recorder).await;
        assert_eq!(*calls.lock().unwrap(), vec![1u8, 2, 3]);
    }

    /// The scale-aware setup-deadline default keeps the historical
    /// 60s floor for small runs (`--jobs 1..4`) — matching the
    /// pre-2026-05 behaviour so existing operators see no diff.
    #[test]
    fn setup_deadline_default_keeps_60s_floor_for_small_jobs() {
        assert_eq!(compute_setup_deadline_secs(None, 1), 60);
        assert_eq!(compute_setup_deadline_secs(None, 2), 60);
        assert_eq!(compute_setup_deadline_secs(None, 3), 60);
        assert_eq!(compute_setup_deadline_secs(None, 4), 60);
    }

    /// At `num_secondaries == 4` the formula's linear term equals
    /// the floor (4 * 15 == 60); from 5+ secondaries the linear
    /// term dominates. Documented examples from the task brief:
    /// 15 -> 225, 32 -> 480.
    #[test]
    fn setup_deadline_scales_linearly_above_floor() {
        assert_eq!(compute_setup_deadline_secs(None, 5), 75);
        assert_eq!(compute_setup_deadline_secs(None, 15), 225);
        assert_eq!(compute_setup_deadline_secs(None, 32), 480);
    }

    /// An explicit override always wins, regardless of whether the
    /// scaled formula would produce a smaller OR larger value. This
    /// is the load-bearing escape hatch for clusters with their own
    /// scheduling characteristics.
    #[test]
    fn setup_deadline_explicit_override_wins() {
        // Override below the floor: operator knows their cluster
        // boots fast; we respect that.
        assert_eq!(compute_setup_deadline_secs(Some(30), 1), 30);
        // Override below the scaled value: operator knows their
        // cluster boots fast even at scale.
        assert_eq!(compute_setup_deadline_secs(Some(120), 15), 120);
        // Override above the scaled value: operator knows their
        // cluster boots slow; we respect that.
        assert_eq!(compute_setup_deadline_secs(Some(600), 32), 600);
    }

    /// `num_secondaries = 0` is a defensive bound. The SLURM
    /// pipeline never actually invokes with zero (run.py validates
    /// `--jobs` defaults to 1), but the function must not panic.
    /// The floor takes over.
    #[test]
    fn setup_deadline_zero_secondaries_yields_floor() {
        assert_eq!(compute_setup_deadline_secs(None, 0), 60);
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
        let recorder = AlwaysErrors { calls: calls.clone() };
        run_cleanup(recorder).await;
        assert_eq!(*calls.lock().unwrap(), vec![1u8, 2, 3]);
    }
}
