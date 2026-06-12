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
/// Between the first sbatch submission and the coordinator genuinely
/// owning the run there is a window of setup work — the rest of the
/// submit-loop, reverse-tunnel establishment, `upload_source_binaries`,
/// coordinator construction, the consumer's `on_run_start` hook — any
/// step of which can fail. A failure there used to leave the
/// already-submitted SLURM jobs orphaned: the secondaries dialed their
/// now-torn-down reverse tunnels forever ("Connection refused") and
/// stranded the fleet, forcing the operator to `scancel` manually.
///
/// The guard closes that window. [`Self::arm_job_cancel`] is called
/// BEFORE `run_preparation` begins (the sbatch submit-loop and the
/// reverse-tunnel setup both run inside it — arming only on its return
/// left a tunnel-establishment failure orphaning the whole
/// just-submitted cohort); [`Self::disarm_job_cancel`] is called only
/// after `coord.run()` RETURNS SUCCESSFULLY (see below for why not at
/// the `run()`-entry boundary). While armed, the `Drop` scancels via
/// the job manager's `cancel_all_jobs`, which drains ONLY this run's
/// own tracked `job_ids` AT DROP TIME — never a broad `scancel` pattern
/// that could reach a co-tenant's jobs on a shared host. Pre-submission
/// arming is therefore safe by construction (no ids tracked → the drop
/// is a no-op) and a mid-submit-loop abort cancels exactly the
/// already-submitted subset.
///
/// ## Why disarm on `coord.run()` SUCCESS, not at `run()` entry
///
/// The hand-off to fleet self-termination is proven ONLY by the run
/// reaching a verdict-broadcast terminal — the welcomed fleet observes
/// the replicated CRDT terminal (RunComplete / graceful drain) and
/// exits on its own, so the SLURM allocation self-terminates. At the
/// pyo3 boundary that terminal-reached proof IS a SUCCESSFUL
/// `coord.run()` return: every `Ok` terminal is such a wind-down. A
/// `run()` raise is always an abnormal terminal where the fleet did NOT
/// self-terminate — most acutely `RunError::BringUpFailed` (0/N
/// welcomes inside the quorum-proceed window), which assembles no fleet
/// and broadcasts no verdict, leaving the just-submitted secondaries
/// stranded in setup with SLURM holding the whole cohort. Disarming at
/// `run()` ENTRY (the old point) dropped those ids on the floor —
/// asm-tokenizer run_161034 exited non-zero on BringUpFailed with all
/// 11 sbatch jobs left RUNNING. Keeping the guard armed across `run()`
/// and disarming only after a confirmed-`Ok` return fixes that: a
/// healthy fleet still rides a disarmed-guard drop (the `Ok` path
/// disarms), while every raise leaves the guard armed so the
/// `drop(guard)` scancels exactly this run's tracked ids. There is no
/// `run()` raise where the fleet is healthy-and-mid-work — a healthy
/// run returns `Ok` — so this never scancels a self-terminating fleet.
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

    /// Arm setup-abort job rollback. Called BEFORE `run_preparation`
    /// (which owns the sbatch submit-loop): from here until
    /// [`Self::disarm_job_cancel`], any abort drops an armed guard that
    /// scancels `job_manager`'s tracked job ids — the ids tracked AT
    /// DROP time, so pre-submission arming is a no-op and a mid-setup
    /// abort cancels exactly the already-submitted subset. Holds the
    /// `job_manager` so `Drop` can call `cancel_all_jobs` on it.
    pub(super) fn arm_job_cancel(&mut self, job_manager: Py<PyAny>) {
        self.job_manager = Some(job_manager);
        self.job_cancel_armed = true;
    }

    /// Disarm job rollback. Called only after `coord.run()` RETURNS
    /// SUCCESSFULLY — the proof that the run reached a verdict-broadcast
    /// terminal and the welcomed fleet is winding itself down on the
    /// observed CRDT terminal, so the SLURM allocation self-terminates.
    /// After this a `Drop` leaves the submitted jobs alone. A `run()`
    /// raise (BringUpFailed, cluster-collapse strand, pre-phase abort,
    /// …) is reached BEFORE this call, so it drops a STILL-ARMED guard
    /// and scancels this run's tracked ids — the fleet never
    /// self-terminated. See the struct doc's "Why disarm on `coord.run()`
    /// SUCCESS" section.
    pub(super) fn disarm_job_cancel(&mut self) {
        self.job_cancel_armed = false;
    }

    /// Apply the run-boundary hand-off rule: disarm IFF `coord.run()`
    /// returned successfully. This is the single source of truth for
    /// "when does the setup guard hand the cohort off to fleet
    /// self-termination" — `run_ok` is the verdict-broadcast proof.
    ///
    /// Encapsulating the rule (rather than an inline
    /// `run_outcome?; disarm_job_cancel()`) makes the ORDERING explicit
    /// and impossible to regress: the bug it fixes was a disarm sequenced
    /// BEFORE `run()`, which dropped the still-allocated cohort on a
    /// bring-up fatal. A `false` here is a no-op — the guard stays armed
    /// so its `Drop` scancels the stranded ids. Mirrors the in-process
    /// `fire_on_run_end(task, run_outcome.is_ok())` discriminator the
    /// caller already computes for the consumer hook.
    pub(super) fn disarm_on_run_success(&mut self, run_ok: bool) {
        if run_ok {
            self.disarm_job_cancel();
        }
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

/// Run-teardown residual sweep: SIGTERM matching `ssh -R` tunnels that
/// are THIS process's own children (escaped the registry's tracked
/// cleanup). Routed through
/// `dynrunner_slurm::pipeline::sweep_residual_reverse_tunnels` so a
/// future pure-Rust preparation port (L2.F) calling the same function
/// gets the parentage scoping by construction — a uid-global pkill
/// here killed CONCURRENT runs' tunnels (run_20260611_221215).
fn pkill_residual_tunnels(py: Python<'_>) -> PyResult<()> {
    block_on_detached(py, |uid| {
        Box::pin(async move {
            dynrunner_slurm::pipeline::sweep_residual_reverse_tunnels(
                uid,
                dynrunner_slurm::pipeline::TunnelSweepScope::OwnChildren,
            )
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("tunnel sweep: {e}")))
        })
    })
}

/// Pipeline-start stray cleanup: SIGTERM matching `ssh -R` tunnel
/// processes that are ORPHANS (reparented to `init` — their spawning
/// dispatch died without teardown). Never touches a live run's
/// tunnels: those are children of that run's still-alive pid. The old
/// "nothing yet to protect" uid-global pkill was wrong exactly there —
/// it protected nothing of THIS run but destroyed every CONCURRENT
/// run's verified tunnels at dispatch start.
pub(super) fn pkill_leftover_tunnels(py: Python<'_>) -> PyResult<()> {
    block_on_detached(py, |uid| {
        Box::pin(async move {
            dynrunner_slurm::pipeline::sweep_residual_reverse_tunnels(
                uid,
                dynrunner_slurm::pipeline::TunnelSweepScope::Orphans,
            )
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("tunnel sweep: {e}")))
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
    //! Pins the job-rollback contract on [`CleanupGuard`]:
    //!   1. ARMED (sbatch submitted, run not yet handed off) → `Drop`
    //!      scancels via `job_manager.cancel_all_jobs`, targeting only
    //!      this run's own tracked `job_ids`.
    //!   2. DISARMED (an explicit `disarm_job_cancel`) → `Drop` leaves the
    //!      submitted jobs alone: a self-terminating fleet is never
    //!      scancelled.
    //!   3. NEVER ARMED (no sbatch — e.g. remote-podman, or a failure
    //!      before submission) → `Drop` never touches a job manager.
    //!
    //! Run-boundary hand-off rule (`disarm_on_run_success`), pinned by
    //! [`run_success_disarms_fleet_self_terminates`] and
    //! [`bringup_fatal_after_preparation_scancels_cohort`]:
    //!   4. `coord.run()` returned Ok → disarm → `Drop` no-cancel (the
    //!      verdict-broadcast fleet self-terminates).
    //!   5. `coord.run()` RAISED (BringUpFailed-shaped, after preparation
    //!      succeeded) → guard stays ARMED → `Drop` scancels the cohort.
    //!      This is the run_161034 orphan repro: RED if the disarm is
    //!      sequenced UNCONDITIONALLY (the old pre-`run()` disarm).
    //!
    //! Revert-check: case (1) fails if the arm/disarm + Drop step-0 is
    //! removed — an un-armed guard never calls `cancel_all_jobs`, so the
    //! orphaned-fleet defect resurfaces. Case (5) fails if the disarm
    //! moves back ahead of (or off the success-conditional of) `run()`.
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

    /// DISARMED: an explicit `disarm_job_cancel` (the hand-off to fleet
    /// self-termination) means a subsequent `Drop` leaves the running
    /// fleet untouched.
    #[test]
    fn disarmed_handoff_leaves_fleet_running() {
        let (gateway, job_manager, globals) = make_doubles(&["201", "202"]);
        Python::attach(|py| {
            let mut guard = CleanupGuard::new(gateway.clone_ref(py));
            guard.arm_job_cancel(job_manager.clone_ref(py));
            // Hand-off point reached: the run wound down with a verdict.
            guard.disarm_job_cancel();
            drop(guard);

            assert!(
                cancelled_ids(py, &globals).is_empty(),
                "a disarmed guard must NEVER scancel a self-terminating fleet",
            );
        });
    }

    /// EARLY ARM (pre-submission): the guard is armed BEFORE
    /// `run_preparation` runs the sbatch submit-loop, so at arm time
    /// the job manager tracks ZERO ids. `cancel_all_jobs` drains the
    /// ids tracked AT DROP time — an abort after some submissions
    /// must scancel exactly the already-submitted subset, not the
    /// (empty) arm-time snapshot. This pins the contract that makes
    /// the pre-preparation arm point correct: a tunnel-establishment
    /// failure inside `run_preparation` (the asm-dataset
    /// run_20260611 orphaned-cohort shape) now rolls the cohort back.
    #[test]
    fn early_armed_abort_scancels_jobs_submitted_after_arming() {
        let (gateway, job_manager, globals) = make_doubles(&[]);
        Python::attach(|py| {
            let mut guard = CleanupGuard::new(gateway.clone_ref(py));
            // Arm BEFORE any submission — zero tracked ids.
            guard.arm_job_cancel(job_manager.clone_ref(py));
            // Simulate the submit-loop populating tracked ids after
            // the arm (the real job manager appends on each sbatch).
            let jm = job_manager.bind(py);
            for id in ["401", "402", "403"] {
                jm.getattr("job_ids")
                    .unwrap()
                    .call_method1("append", (id,))
                    .unwrap();
            }
            // Abort mid-setup (e.g. ssh tunnel establishment failed).
            drop(guard);

            assert_eq!(
                cancelled_ids(py, &globals),
                vec!["401", "402", "403"],
                "an early-armed guard must scancel the ids tracked at DROP time",
            );
        });
    }

    /// EARLY ARM, abort before any submission: an armed guard whose
    /// job manager tracks no ids drops as a harmless no-op — arming
    /// before `run_preparation` can never invent cancellations.
    #[test]
    fn early_armed_abort_before_submission_is_noop() {
        let (gateway, job_manager, globals) = make_doubles(&[]);
        Python::attach(|py| {
            let mut guard = CleanupGuard::new(gateway.clone_ref(py));
            guard.arm_job_cancel(job_manager.clone_ref(py));
            drop(guard);

            assert!(
                cancelled_ids(py, &globals).is_empty(),
                "no submissions → nothing to scancel",
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

    /// RUN-BOUNDARY, SUCCESS: preparation succeeded, the cohort is armed,
    /// and `coord.run()` returned Ok (a verdict-broadcast terminal — the
    /// welcomed fleet observes the CRDT terminal and exits on its own).
    /// `disarm_on_run_success(true)` hands the cohort off, so the
    /// subsequent `Drop` leaves the self-terminating jobs alone. Pins the
    /// no-spurious-cancel side of the rule.
    #[test]
    fn run_success_disarms_fleet_self_terminates() {
        let (gateway, job_manager, globals) = make_doubles(&["501", "502"]);
        Python::attach(|py| {
            let mut guard = CleanupGuard::new(gateway.clone_ref(py));
            // Armed across `run()` (preparation succeeded, cohort live).
            guard.arm_job_cancel(job_manager.clone_ref(py));
            // `coord.run()` returned Ok → the run-boundary hand-off rule
            // disarms (mirrors `drive_rust_primary`'s
            // `guard.disarm_on_run_success(run_outcome.is_ok())`).
            guard.disarm_on_run_success(true);
            drop(guard);

            assert!(
                cancelled_ids(py, &globals).is_empty(),
                "a run that reached a verdict-broadcast terminal must NOT \
                 be scancelled — the fleet self-terminates",
            );
        });
    }

    /// RUN-BOUNDARY, BRING-UP FATAL: the run_161034 orphan repro. The
    /// cohort is armed, preparation SUCCEEDED, and then `coord.run()`
    /// RAISED a BringUpFailed-shaped fatal (0/N welcomes — no fleet
    /// assembled, no verdict broadcast, secondaries stranded in setup
    /// with SLURM holding the whole allocation). The run-boundary
    /// hand-off rule sees `run_ok=false`, so it leaves the guard ARMED;
    /// the `Drop` then scancels exactly this run's submitted ids.
    ///
    /// RED against the old code, which disarmed UNCONDITIONALLY ahead of
    /// `run()` — that path left the guard disarmed, dropped, and the 11
    /// sbatch jobs RUNNING. (Reproduced inline below as `old_disarm`.)
    #[test]
    fn bringup_fatal_after_preparation_scancels_cohort() {
        let (gateway, job_manager, globals) = make_doubles(&["601", "602", "603"]);
        Python::attach(|py| {
            let mut guard = CleanupGuard::new(gateway.clone_ref(py));
            // Preparation succeeded → cohort submitted + armed.
            guard.arm_job_cancel(job_manager.clone_ref(py));
            // `coord.run()` raised BringUpFailed: run_ok=false. The rule
            // is a no-op (stays armed); the `?`-propagated raise short-
            // circuits to the `Drop` below.
            guard.disarm_on_run_success(false);
            drop(guard);

            assert_eq!(
                cancelled_ids(py, &globals),
                vec!["601", "602", "603"],
                "a post-preparation bring-up fatal must scancel the \
                 stranded cohort — not leave it RUNNING",
            );
        });

        // Revert-check: the OLD ordering disarmed unconditionally BEFORE
        // `run()`. Reproduce that boolean to prove the bug was real — an
        // unconditional disarm followed by a raise drops a disarmed guard
        // and scancels NOTHING.
        let (gateway, job_manager, globals) = make_doubles(&["601", "602", "603"]);
        Python::attach(|py| {
            let mut guard = CleanupGuard::new(gateway.clone_ref(py));
            guard.arm_job_cancel(job_manager.clone_ref(py));
            // OLD behaviour: disarm at `run()` ENTRY, irrespective of how
            // `run()` ends.
            guard.disarm_job_cancel();
            // `run()` then raised BringUpFailed → drop a DISARMED guard.
            drop(guard);

            assert!(
                cancelled_ids(py, &globals).is_empty(),
                "the OLD unconditional pre-run disarm orphaned the cohort \
                 on a bring-up fatal (this is the bug the fix closes)",
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
