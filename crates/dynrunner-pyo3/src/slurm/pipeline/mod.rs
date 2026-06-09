//! `_native.run_slurm_pipeline` — PyO3 entry point for SLURM mode.
//!
//! Ports `python/dynamic_runner/packaging/pipeline.py::run_slurm_pipeline`
//! step-for-step. Each step calls the public Python facade on the
//! `dynamic_runner` module by its stable public name; thin-shim
//! migration of the underlying types (gateway, job_manager,
//! preparation) leaves those names intact, so this orchestrator does
//! not need to be edited as those types switch from pure-Python to
//! pyclass-wrapped Rust.
//!
//! ## Why orchestrate at the PyO3 layer (not pure Rust)?
//!
//! `run_slurm_pipeline` composes the gateway, the podman packaging,
//! the job manager, the slurm preparation phase, and the
//! `RustPrimaryCoordinator`, plus the `TaskDefinition` Protocol and
//! the `argparse.Namespace` / `TaskDeploymentSpec` payloads. Several
//! of those types currently exist only on the Python side (their
//! Rust counterparts are landing in sibling migration units).
//! Orchestrating at the PyO3 layer lets us land the orchestration
//! itself now — faithful sequence, correct teardown ordering
//! enforced as Rust code — without blocking on those Rust types.
//!
//! See `crates/dynrunner-slurm/src/pipeline.rs` for the structural
//! skeleton of the future pure-Rust orchestrator (boundary trait,
//! cleanup-ordering invariant, shared pkill primitive). When the
//! Rust gateway / preparation / job_manager types land, the body
//! here reduces to constructing them and calling that pure-Rust
//! composition.

use pyo3::prelude::*;

/// `bool(getattr(obj, name, None))` — handles missing-attr +
/// None-attr the same way Python does. Centralises the
/// `getattr-then-truthy` pattern used at the gating sites for
/// `--source-already-staged` (and any future similarly-shaped CLI
/// flag).
pub(super) fn attr_truthy(obj: &Bound<'_, PyAny>, name: &str) -> bool {
    obj.getattr(name)
        .ok()
        .map(|v| !v.is_none() && v.is_truthy().unwrap_or(false))
        .unwrap_or(false)
}

/// Gate for invoking `job_manager.upload_source_binaries`.
///
/// Fires when the dispatcher discovered binaries (`!binaries_empty`)
/// and we are not in pre-staged mode (`!source_already_staged`).
///
/// Deliberately does NOT consult the task-class `uses_file_based_items`
/// flag. Upload-stageability is a PER-ITEM property — "does this
/// binary resolve to a real file under `--source`?" — and the per-item
/// authority is `upload_source_binaries`' own walk, which strip-prefixes
/// each binary against `source_root` and skips any item that is
/// out-of-tree OR has no backing file on disk (a computed/producer item —
/// a `uses_file_based_items=False` task discovers items it PRODUCES, with
/// nothing to upload; the task-class flag cannot tell it apart from a
/// mixed composite, so the walk decides per item rather than stat+scp'ing
/// a path that does not exist)
/// (see `dynrunner-slurm/src/job_manager/images.rs`; the primary's
/// `compute_initial_staging_entries` applies the same OUT-OF-TREE
/// predicate via the shared `resolve_against_root` merge, but stays STRICT
/// on the existence axis — it is reached only by file-based tasks, whose
/// files exist, so a genuinely-missing source surfaces there). A
/// pure-producer task whose binaries are all computed (no backing file)
/// therefore uploads nothing even with this gate true; a mixed composite
/// (real-file items + opaque sentinels spawned later, never present in
/// `binaries` at submit time) correctly uploads its real files.
///
/// `uses_file_based_items` remains the authority for the StageFile gate
/// and `resolve_for_dispatch` (dispatch-time resolution), where the
/// opaque sentinels genuinely must resolve via the bind-mount — those
/// readers keep the task-class flag.
pub(super) fn should_upload_source_binaries(
    binaries_empty: bool,
    source_already_staged: bool,
) -> bool {
    !binaries_empty && !source_already_staged
}

/// Drop-guard that runs the strict teardown order
/// (armed setup-abort scancel → `tunnel_manager.cleanup()` →
/// `gateway.disconnect()` → tightened `pkill`) on scope exit. Modeled
/// on Python's `try/finally` block in `pipeline.py::run_slurm_pipeline`.
/// The order is invariant — see the `pkill_residual_reverse_tunnels`
/// doc in `dynrunner-slurm` for why disconnect MUST precede pkill, and
/// the scancel step below for why it must precede disconnect.
///
/// * Holds `Py<PyAny>` references to the live `tunnel_manager` (a
///   `RustSlurmPreparation` pyclass; only present in reverse-connection
///   mode), `gateway`, and (once sbatch has submitted) the
///   `job_manager` instances. `Option<...>` shape so an early-failure
///   path can construct the guard with what it has so far and the
///   `Drop` skips the missing steps.
/// * Each step is best-effort: a failure logs but does not abort the
///   remaining steps. Same semantics as Python's `try/finally` chain
///   where the gateway disconnect runs even if preparation cleanup
///   raised.
///
/// ## Setup-abort job rollback (arm/disarm)
///
/// Between sbatch-submit and the coordinator genuinely owning the run
/// there is a window of setup work — `upload_source_binaries`,
/// coordinator construction, the consumer's `on_run_start` hook — any
/// step of which can fail. A failure there used to leave the
/// already-submitted SLURM jobs orphaned: the secondaries dialed their
/// now-torn-down reverse tunnels forever ("Connection refused") and
/// stranded the fleet, forcing the operator to `scancel` manually.
///
/// The guard closes that window. [`Self::arm_job_cancel`] is called the
/// instant `run_preparation` returns (jobs submitted, `job_ids`
/// populated); [`Self::disarm_job_cancel`] is called the instant the
/// run is handed to `coord.run()` (the coordinator now owns the
/// lifecycle — its teardown, not this setup guard, governs the jobs
/// from here, and that runtime path is a separate, owner-gated
/// concern). While armed, the `Drop` scancels via the job manager's
/// `cancel_all_jobs`, which targets ONLY this run's own tracked
/// `job_ids` — never a broad `scancel` pattern that could reach a
/// co-tenant's jobs on a shared host. A healthy fleet is therefore
/// NEVER scancelled: by the time `coord.run()` runs (success path), the
/// guard is disarmed, and a normal return drops a disarmed guard. Only
/// an abort *before* the hand-off finds the guard still armed.
///
/// The scancel runs FIRST in `Drop` — before tunnel cleanup and the
/// gateway disconnect — because `cancel_all_jobs` issues `scancel` over
/// the still-live gateway; tearing the gateway down first would leave
/// nothing to issue the command through.
pub(super) struct CleanupGuard {
    tunnel_manager: Option<Py<PyAny>>,
    gateway: Option<Py<PyAny>>,
    /// The SLURM job manager whose tracked `job_ids` get scancelled on
    /// an armed setup-abort. `None` until [`Self::arm_job_cancel`]
    /// installs it after sbatch submission; remote-podman (no sbatch)
    /// never installs one.
    job_manager: Option<Py<PyAny>>,
    /// `true` only in the setup window between sbatch-submit and the
    /// `coord.run()` hand-off. Gates the scancel step in `Drop`.
    job_cancel_armed: bool,
}

impl CleanupGuard {
    pub(super) fn new(gateway: Py<PyAny>) -> Self {
        Self {
            tunnel_manager: None,
            gateway: Some(gateway),
            job_manager: None,
            job_cancel_armed: false,
        }
    }

    pub(super) fn set_tunnel_manager(&mut self, tunnel_manager: Py<PyAny>) {
        self.tunnel_manager = Some(tunnel_manager);
    }

    /// Arm setup-abort job rollback. Called right after sbatch
    /// submission (`run_preparation` returns): from here until
    /// [`Self::disarm_job_cancel`], any abort drops an armed guard that
    /// scancels `job_manager`'s tracked job ids. Holds the
    /// `job_manager` so `Drop` can call `cancel_all_jobs` on it.
    pub(super) fn arm_job_cancel(&mut self, job_manager: Py<PyAny>) {
        self.job_manager = Some(job_manager);
        self.job_cancel_armed = true;
    }

    /// Disarm setup-abort job rollback. Called the instant the run is
    /// handed to `coord.run()` — the coordinator now owns the job
    /// lifecycle, so a later failure is a runtime concern (separately,
    /// owner-gated) rather than a setup abort. After this a `Drop` (on
    /// success OR on a runtime error) leaves the submitted jobs alone.
    pub(super) fn disarm_job_cancel(&mut self) {
        self.job_cancel_armed = false;
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        Python::attach(|py| {
            // Step 0: armed setup-abort job rollback. Runs BEFORE the
            // gateway disconnect below because `cancel_all_jobs` issues
            // `scancel` over the still-live gateway. Targets only this
            // run's own tracked `job_ids` (the job manager's
            // `cancel_all_jobs` drains its tracked list — never a broad
            // pattern). Disarmed once the coordinator owns the run, so a
            // healthy fleet is never reached here.
            if self.job_cancel_armed
                && let Some(jm) = self.job_manager.take()
                && let Err(e) = jm.bind(py).call_method0("cancel_all_jobs")
            {
                tracing::warn!(error = ?e, "setup-abort job_manager.cancel_all_jobs() failed");
            }
            // Step 1: per-secondary tunnel cleanup (tracked in the
            // RustSlurmPreparation tunnel manager). Only present if
            // reverse-connection mode constructed one — non-reverse
            // runs skip this step.
            if let Some(prep) = self.tunnel_manager.take()
                && let Err(e) = prep.bind(py).call_method0("cleanup")
            {
                tracing::warn!(error = ?e, "tunnel_manager.cleanup() failed");
            }
            // Step 2: graceful gateway-master shutdown FIRST. This
            // takes the master and all its `-R` forwardings down via
            // `ssh -O exit`. Must happen BEFORE the targeted pkill
            // below — otherwise pkill SIGTERMs the master before its
            // graceful exit completes and we get spurious "Control
            // socket connect: No such file or directory" warnings.
            if let Some(gw) = self.gateway.take()
                && let Err(e) = gw.bind(py).call_method0("disconnect")
            {
                tracing::warn!(error = ?e, "gateway.disconnect() failed");
            }
            // Step 3: targeted pkill for any per-secondary reverse
            // tunnel that escaped `preparation.cleanup()` tracking.
            // Pattern specifically matches `-R <port>:localhost...`
            // (preparation's shape); the master used
            // `-R 0.0.0.0:<port>:localhost...` so the regex
            // deliberately does NOT race the master shutdown above.
            if let Err(e) = pkill_residual_tunnels(py) {
                tracing::warn!(error = ?e, "residual-tunnel pkill failed");
            }
        });
    }
}

/// FFI for `getuid(2)`. Avoids pulling in a direct `libc` dep just
/// for one syscall — the `nix` crate already in the workspace
/// doesn't expose `getuid` in the slurm crate's feature set.
fn current_uid() -> u32 {
    unsafe extern "C" {
        fn getuid() -> u32;
    }
    unsafe { getuid() }
}

/// Sync bridge for an async pkill. Builds a single-shot
/// current-thread tokio runtime, releases the GIL, runs `op`, and
/// reattaches. Single source of truth for the runtime-construction
/// boilerplate shared by the two pkill phases.
fn block_on_detached<F, R>(py: Python<'_>, op: F) -> PyResult<R>
where
    F: FnOnce(u32) -> std::pin::Pin<Box<dyn std::future::Future<Output = PyResult<R>> + Send>>
        + Send,
    R: Send,
{
    py.detach(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("tokio runtime: {e}"))
            })?;
        rt.block_on(op(current_uid()))
    })
}

/// `pkill -u <uid> -f 'ssh.*-R [0-9]+:localhost'`.
///
/// Routed through `dynrunner_slurm::pipeline::pkill_residual_reverse_tunnels`
/// so a future pure-Rust preparation port (L2.F) calling the same
/// function gets the c399f5a-tightened regex by construction.
fn pkill_residual_tunnels(py: Python<'_>) -> PyResult<()> {
    block_on_detached(py, |uid| {
        Box::pin(async move {
            dynrunner_slurm::pipeline::pkill_residual_reverse_tunnels(uid)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("pkill: {e}")))
        })
    })
}

/// `pkill -u <uid> -f 'ssh.*-R.*localhost'`. Broad-pattern
/// leftover-cleanup before any new ssh master is started — there
/// is nothing yet to protect at this point in the lifecycle, so
/// the pattern is intentionally broader than the post-run
/// teardown's tightened pattern.
pub(super) fn pkill_leftover_tunnels(py: Python<'_>) -> PyResult<()> {
    block_on_detached(py, |uid| {
        Box::pin(async move {
            let _ = tokio::process::Command::new("pkill")
                .arg("-u")
                .arg(uid.to_string())
                .arg("-f")
                .arg(r"ssh.*-R.*localhost")
                .stderr(std::process::Stdio::null())
                .status()
                .await;
            Ok(())
        })
    })
}

#[cfg(test)]
mod tests {
    use super::should_upload_source_binaries;

    /// Mode-1 mixed-composite shape: the dispatcher discovered real-file
    /// binaries (`binaries_empty=false`) and we are NOT pre-staged
    /// (`source_already_staged=false`). The gate MUST fire — this is the
    /// shape the task-class `uses_file_based_items=False` conjunct used
    /// to wrongly suppress (the per-item upload walk is the real
    /// stageability authority).
    #[test]
    fn gate_fires_for_mode1_mixed_composite() {
        assert!(should_upload_source_binaries(
            /* binaries_empty */ false, /* source_already_staged */ false,
        ));
    }

    /// Revert-check: the pre-fix gate ANDed in `uses_file_based_items`.
    /// For the mixed-composite shape that flag is False, so the OLD gate
    /// (`!empty && uses_file_based_items && !staged`) did NOT fire. This
    /// reproduces that old boolean to prove the bug was real and the
    /// conjunct removal is what unblocks the upload.
    #[test]
    fn revert_check_old_conjunct_suppressed_mode1() {
        let binaries_empty = false;
        let source_already_staged = false;
        let uses_file_based_items = false; // task-class flag for a mixed composite
        let old_gate = !binaries_empty && uses_file_based_items && !source_already_staged;
        assert!(
            !old_gate,
            "old task-class-gated condition wrongly suppressed mode-1 upload"
        );
        // The new per-item gate ignores the flag and fires.
        assert!(should_upload_source_binaries(
            binaries_empty,
            source_already_staged
        ));
    }

    /// Mode-2 (pre-staged) short-circuit is preserved: regardless of
    /// discovered binaries, `source_already_staged=true` means no upload.
    #[test]
    fn gate_short_circuits_in_pre_staged_mode() {
        assert!(!should_upload_source_binaries(
            /* binaries_empty */ false, /* source_already_staged */ true,
        ));
    }

    /// No discovered binaries → nothing to upload, gate is false.
    /// (The pure-opaque case where the dispatcher discovered zero items.)
    #[test]
    fn gate_false_with_no_binaries() {
        assert!(!should_upload_source_binaries(true, false));
        assert!(!should_upload_source_binaries(true, true));
    }
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod cleanup_guard_tests {
    //! Pins the setup-abort job-rollback contract on [`CleanupGuard`]:
    //!   1. ARMED (sbatch submitted, run not yet handed off) → `Drop`
    //!      scancels via `job_manager.cancel_all_jobs`, targeting only
    //!      this run's own tracked `job_ids`.
    //!   2. DISARMED (the run reached `coord.run()`) → `Drop` leaves the
    //!      submitted jobs alone: a healthy fleet is never scancelled.
    //!   3. NEVER ARMED (no sbatch — e.g. remote-podman, or a failure
    //!      before submission) → `Drop` never touches a job manager.
    //!
    //! Revert-check: case (1) fails if the arm/disarm + Drop step-0 is
    //! removed — an un-armed guard never calls `cancel_all_jobs`, so the
    //! orphaned-fleet defect resurfaces.
    //!
    //! Tests require an embedded CPython interpreter; gated behind the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python cleanup_guard`
    use super::CleanupGuard;
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList, PyModule};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static MODULE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Compile a one-off module exposing recording `Gateway` and
    /// `JobManager` doubles plus the run's submitted `job_ids`.
    ///
    /// * `JobManager.cancel_all_jobs()` snapshots its own `job_ids` into
    ///   the module-level `cancelled` list and clears the tracked ids —
    ///   the exact shape of the real Python shim (drain the tracked
    ///   list, target only this run's own jobs). Tests assert on
    ///   `cancelled` to prove the scancel hit precisely these ids.
    /// * `Gateway.disconnect()` is a no-op recorder so the guard's
    ///   later teardown steps don't blow up the test.
    ///
    /// Returns `(gateway, job_manager, globals)` so tests can drop the
    /// guard and then inspect `cancelled`.
    fn make_doubles(job_ids: &[&str]) -> (Py<PyAny>, Py<PyAny>, Py<PyAny>) {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_cleanup_guard_{nonce}");
        let file_name = format!("{module_name}.py");
        let ids_literal = job_ids
            .iter()
            .map(|s| format!("{s:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        let body = format!(
            "cancelled = []\n\
             disconnected = []\n\
             class Gateway:\n    \
                 def disconnect(self):\n        \
                     disconnected.append(True)\n\
             class JobManager:\n    \
                 def __init__(self):\n        \
                     self.job_ids = [{ids_literal}]\n    \
                 def cancel_all_jobs(self):\n        \
                     cancelled.extend(self.job_ids)\n        \
                     self.job_ids = []\n",
        );
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(body).unwrap().as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .expect("compile mock cleanup-guard module");
            let gateway = module.getattr("Gateway").unwrap().call0().unwrap().unbind();
            let job_manager = module
                .getattr("JobManager")
                .unwrap()
                .call0()
                .unwrap()
                .unbind();
            let globals = module.dict().unbind().into_any();
            (gateway, job_manager, globals)
        })
    }

    /// Read the module-level `cancelled` list as a `Vec<String>`.
    fn cancelled_ids(py: Python<'_>, globals: &Py<PyAny>) -> Vec<String> {
        let g = globals.bind(py).cast::<PyDict>().unwrap();
        let cancelled = g.get_item("cancelled").unwrap().unwrap();
        cancelled
            .cast::<PyList>()
            .unwrap()
            .iter()
            .map(|v| v.extract::<String>().unwrap())
            .collect()
    }

    /// ARMED: a setup-phase abort (guard dropped while armed) scancels
    /// exactly this run's tracked job ids. This is the orphaned-fleet
    /// defect's regression test — and the revert-check: without the
    /// arm/disarm + Drop step-0, `cancel_all_jobs` is never called and
    /// `cancelled` stays empty, failing this assertion.
    #[test]
    fn armed_setup_abort_scancels_tracked_jobs() {
        let (gateway, job_manager, globals) = make_doubles(&["101", "102", "103", "104"]);
        Python::attach(|py| {
            let mut guard = CleanupGuard::new(gateway.clone_ref(py));
            guard.arm_job_cancel(job_manager.clone_ref(py));
            // Simulate the setup-phase abort: drop the still-armed guard.
            drop(guard);

            assert_eq!(
                cancelled_ids(py, &globals),
                vec!["101", "102", "103", "104"],
                "armed guard must scancel exactly this run's tracked job ids",
            );
        });
    }

    /// DISARMED: once the run is handed to `coord.run()`, the guard is
    /// disarmed and a `Drop` (success path OR runtime failure) leaves
    /// the running fleet untouched.
    #[test]
    fn disarmed_handoff_leaves_fleet_running() {
        let (gateway, job_manager, globals) = make_doubles(&["201", "202"]);
        Python::attach(|py| {
            let mut guard = CleanupGuard::new(gateway.clone_ref(py));
            guard.arm_job_cancel(job_manager.clone_ref(py));
            // Hand-off point reached: coordinator owns the run.
            guard.disarm_job_cancel();
            drop(guard);

            assert!(
                cancelled_ids(py, &globals).is_empty(),
                "a disarmed guard must NEVER scancel a healthy fleet",
            );
        });
    }

    /// NEVER ARMED: a guard that never saw a job manager (no sbatch yet,
    /// or remote-podman) drops without touching any job manager.
    #[test]
    fn never_armed_does_not_scancel() {
        let (gateway, _job_manager, globals) = make_doubles(&["301"]);
        Python::attach(|py| {
            let guard = CleanupGuard::new(gateway.clone_ref(py));
            drop(guard);

            assert!(
                cancelled_ids(py, &globals).is_empty(),
                "a guard with no armed job manager must not scancel",
            );
        });
    }
}

mod drive_rust;
mod image_build;
mod preparation;
mod respawn;
mod run_pipeline;
mod run_remote_podman;

pub(crate) use preparation::run_preparation_py;
pub(crate) use run_pipeline::run_slurm_pipeline;
pub(crate) use run_remote_podman::run_remote_podman_pipeline;
