//! SLURM wrapper-script generator (single source of truth).
//!
//! Single concern: render the bash wrapper that runs on a SLURM
//! compute node. This is the canonical generator; the Python
//! `dynamic_runner.packaging.job_manager` module thin-shims into it
//! via the PyO3 binding (see `crates/dynrunner-pyo3/src/slurm/`).
//!
//! Inputs are **fully-resolved strings** by the caller: tilde
//! expansion against the gateway's remote home, image-tar basename,
//! load command (`podman load < ...` template with substitutions
//! already done — except the `$VAR` references which the bash
//! interpreter resolves), etc. The generator does no path-resolution
//! of its own; the Python caller's only job is to pre-resolve those
//! strings from its objects (`PodmanImageMetadata`, `PodmanPackaging`,
//! `SlurmConfig`, `TaskDeploymentSpec`).
//!
//! Layout:
//! - [`config`] — public types: `WrapperScriptConfig`,
//!   `ConnectionMode`, `WRAPPER_SRC_NETWORK_CONTAINER_PATH` const.
//! - [`generate`] — the secondary-mode generator
//!   `generate_wrapper_script` (sits above the 300-line target; the
//!   bash body it emits is one cohesive script).
//! - [`test_generate`] — the image-validation generator
//!   `generate_test_wrapper_script` + `TestWrapperScriptConfig`.
//! - [`quote`] — inline `bash_quote` + `rand_hex8` helpers.
//! - [`tests`] — module-internal tests, split per concern into
//!   sub-files (`standard_mode`, `reverse_mode`, `argv_quoting`,
//!   `cleanup`, `test_wrapper`, `syntax_and_quote`).

mod config;
mod generate;
mod quote;
mod test_generate;
#[cfg(test)]
mod tests;

pub use config::{ConnectionMode, WRAPPER_SRC_NETWORK_CONTAINER_PATH, WrapperScriptConfig};
pub use generate::generate_wrapper_script;
pub use test_generate::{TestWrapperScriptConfig, generate_test_wrapper_script};
