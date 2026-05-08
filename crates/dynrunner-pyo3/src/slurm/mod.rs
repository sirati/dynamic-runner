//! PyO3 bindings for the SLURM-mode pipeline.
//!
//! Single concern: expose the SLURM pipeline orchestration to Python
//! as `_native.run_slurm_pipeline`. The orchestration logic itself
//! ports the canonical Python `run_slurm_pipeline` step-for-step;
//! step *implementations* delegate to the existing Python facade
//! (`dynamic_runner.packaging.{gateway, podman, job_manager,
//! preparation}`) by their public names. Those names are stable
//! across the L2.A-F migration via the thin-shim convention, so this
//! orchestrator does not need to be edited when the underlying
//! types switch from pure-Python to pyclass-wrapped Rust.

pub(crate) mod pipeline;
