//! PyO3 bindings for `dynrunner-slurm`.

pub mod wrapper_script;
pub(crate) mod preparation;
pub(crate) mod job_manager;
pub(crate) mod py_gateway;

pub(crate) use job_manager::PyRustSlurmJobManager;
