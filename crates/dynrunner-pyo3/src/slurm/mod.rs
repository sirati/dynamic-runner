//! PyO3 bindings for the SLURM wrapper-script generator.
//!
//! Single concern: surface the Rust `generate_wrapper_script` and
//! `generate_test_wrapper_script` from `dynrunner-slurm` to Python.
//! The Python caller pre-resolves its own object graph (gateway
//! tilde-expansion, `PodmanPackaging.get_load_command`, etc.) into
//! flat strings; this module is a thin extract-types-and-call shim.

pub mod wrapper_script;
