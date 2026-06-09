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
/// each binary against `source_root` and skips any out-of-tree item
/// (see `dynrunner-slurm/src/job_manager/images.rs`; the primary's
/// `compute_initial_staging_entries` applies the same predicate,
/// aligned in the shared-`resolve_against_root` merge). A pure-opaque
/// task whose binaries are all out-of-tree therefore uploads nothing
/// even with this gate true; a mixed composite (real-file items +
/// opaque sentinels spawned later, never present in `binaries` at
/// submit time) correctly uploads its real files.
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
/// (`tunnel_manager.cleanup()` → `gateway.disconnect()` → tightened
/// `pkill`) on scope exit. Modeled on Python's `try/finally` block
/// in `pipeline.py::run_slurm_pipeline`. The order is invariant —
/// see the `pkill_residual_reverse_tunnels` doc in `dynrunner-slurm`
/// for why disconnect MUST precede pkill.
///
/// * Holds `Py<PyAny>` references to the live `tunnel_manager` (a
///   `RustSlurmPreparation` pyclass; only present in reverse-connection
///   mode) and `gateway` instances. `Option<...>` shape so an
///   early-failure path can construct the guard with what it has so
///   far and the `Drop` skips the missing steps.
/// * Each step is best-effort: a failure logs but does not abort the
///   remaining steps. Same semantics as Python's `try/finally` chain
///   where the gateway disconnect runs even if preparation cleanup
///   raised.
pub(super) struct CleanupGuard {
    tunnel_manager: Option<Py<PyAny>>,
    gateway: Option<Py<PyAny>>,
}

impl CleanupGuard {
    pub(super) fn new(gateway: Py<PyAny>) -> Self {
        Self {
            tunnel_manager: None,
            gateway: Some(gateway),
        }
    }

    pub(super) fn set_tunnel_manager(&mut self, tunnel_manager: Py<PyAny>) {
        self.tunnel_manager = Some(tunnel_manager);
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        Python::attach(|py| {
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

mod drive_rust;
mod image_build;
mod preparation;
mod respawn;
mod run_pipeline;
mod run_remote_podman;

pub(crate) use preparation::run_preparation_py;
pub(crate) use run_pipeline::run_slurm_pipeline;
pub(crate) use run_remote_podman::run_remote_podman_pipeline;
