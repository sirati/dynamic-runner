//! PyO3 bindings for `dynrunner-slurm`.
//!
//! Owns the Python surface for the SLURM job-manager lifecycle. The
//! Python-side thin shim (`dynamic_runner.packaging.job_manager`)
//! delegates to `RustSlurmJobManager` for the primitives the Rust
//! `dynrunner_slurm::SlurmJobManager` already supports: directory
//! preparation, individual job cancel, status query. Methods that
//! still own consumer-visible behaviour in Python — `submit_job`,
//! `upload_source_binaries`, `build_and_transfer_images`, the bash
//! wrapper-script generators — stay Python until their dedicated
//! migration units (L1.7 / L1.8 / L1.9 / L2.E) land and reconcile
//! the Python ↔ Rust semantic gaps. At that point this module's
//! method surface grows accordingly with no additional bridging
//! work in the thin shim.

pub(crate) mod job_manager;
pub(crate) mod py_gateway;

pub(crate) use job_manager::PyRustSlurmJobManager;
