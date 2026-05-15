//! Rust-side extractor for `dynamic_runner.SubprocessSpec` — the data-
//! only contract a Python `spawn_secondary` callback returns to the
//! Rust `RustPrimaryCoordinator`.
//!
//! Before this module existed, the callback returned a live
//! `subprocess.Popen` and Rust merely held the `Py<PyAny>` handle so it
//! could later call `.kill()` / `.wait()` back through Python. That
//! split the lifecycle across the language boundary: Python owned the
//! `Popen` object's destruction (GC could collect it mid-run), Rust
//! held only a borrowed reference, and the actual `wait()` syscall was
//! a re-entrant PyO3 method call. The
//! `feedback_features_in_rust_python_is_bridge` rule forbids that:
//! lifecycle/loops belong in Rust, Python is a CLI/config bridge.
//!
//! `SubprocessSpec` is the bridge: Python assembles the argv (mode-
//! specific entry-point string assembly is legitimate CLI concern —
//! the spawned process IS Python, and `deployment.secondary_module`
//! lives in the consumer's `TaskDeploymentSpec`) and hands it back as
//! data. Rust then calls `std::process::Command::new(...).spawn()` and
//! owns the resulting `std::process::Child` for the lifetime of the
//! coordinator.
//!
//! `None` as the callback return (not a `SubprocessSpec`) is the SLURM
//! `_slurm_already_spawned` signal: "the wrapper script and sbatch
//! launched the secondary already; do not spawn anything here, and
//! own no `Child` for it." Rust treats that branch as a no-op
//! pass-through.
//!
//! The Python `SubprocessSpec` lives in
//! `python/dynamic_runner/subprocess_spec.py` as a plain
//! `@dataclass(frozen=True)`. Keeping it pure-Python lets the
//! `tests/test_spawn_secondary.py` unit tests run without the maturin
//! wheel built; the trade-off is that the contract here is duck-typed
//! (`getattr` on `argv` / `env`) rather than a typed PyO3 extract.

use std::collections::HashMap;

use pyo3::prelude::*;

/// Owned Rust mirror of `dynamic_runner.SubprocessSpec`. Constructed
/// by extracting `argv` (required, list of str) and `env` (optional,
/// dict[str, str] or `None`) from any Python object that satisfies
/// the shape.
#[derive(Clone, Debug)]
pub(crate) struct SubprocessSpec {
    /// argv[0] is the executable path; argv[1..] are arguments.
    /// Matches the `subprocess.Popen([...])` shape Python callers
    /// already build.
    pub argv: Vec<String>,
    /// Optional environment override. `None` means "inherit the
    /// primary's environment" (Rust `Command::spawn` default).
    /// `Some(map)` REPLACES the environment (the Python equivalent of
    /// `Popen(..., env=env)` where the test seeds `os.environ.copy()`
    /// then mutates a few keys).
    pub env: Option<HashMap<String, String>>,
}

impl SubprocessSpec {
    /// Extract from a Python `SubprocessSpec`-shaped object. Raises
    /// `TypeError` if `argv` is missing or not iterable of `str`;
    /// `env`, when present, must be `None` or a `dict[str, str]`.
    pub(crate) fn from_pyany(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let argv: Vec<String> = obj
            .getattr("argv")
            .map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(
                    "spawn_secondary callback return: object has no `argv` attribute \
                     (expected `dynamic_runner.SubprocessSpec` or `None`)",
                )
            })?
            .extract()
            .map_err(|e| {
                pyo3::exceptions::PyTypeError::new_err(format!(
                    "spawn_secondary callback return: `argv` is not a list[str]: {e}"
                ))
            })?;
        if argv.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "spawn_secondary callback return: `argv` must contain at least one \
                 element (the executable path)",
            ));
        }
        let env: Option<HashMap<String, String>> = match obj.getattr("env") {
            Ok(v) if v.is_none() => None,
            Ok(v) => Some(v.extract().map_err(|e| {
                pyo3::exceptions::PyTypeError::new_err(format!(
                    "spawn_secondary callback return: `env` is not a dict[str, str]: {e}"
                ))
            })?),
            // `env` attribute absent (older callback shape) — treat as
            // None (inherit parent environment). Symmetric with the
            // `subprocess.Popen(cmd)` default.
            Err(_) => None,
        };
        Ok(Self { argv, env })
    }

    /// Spawn this spec as a `std::process::Child`, transferring full
    /// lifecycle ownership to Rust. The caller stores the returned
    /// `Child` and is responsible for kill/wait at end of run.
    pub(crate) fn spawn(&self) -> std::io::Result<std::process::Child> {
        let mut cmd = std::process::Command::new(&self.argv[0]);
        cmd.args(&self.argv[1..]);
        if let Some(env) = &self.env {
            cmd.env_clear();
            cmd.envs(env);
        }
        cmd.spawn()
    }
}
