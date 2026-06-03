//! PyO3 façade for the secondary-respawn policy.
//!
//! Single concern: carry the four CLI knobs
//! (`--respawn-policy`, `--respawn-max-per-secondary`,
//! `--respawn-max-total`, `--respawn-cooldown`) from Python into the
//! Rust-side [`RespawnBudget`] consumed by
//! [`PrimaryCoordinator::enable_respawn`]. No spawner construction
//! happens here — the spawner is built separately by the deployment
//! layer (multi-process or SLURM) and handed to the coordinator
//! alongside this policy.
//!
//! Module boundary:
//!
//! - From Python: instantiate `PyRespawnPolicy.disabled()` or
//!   `PyRespawnPolicy.on_secondary_death(max_per_secondary,
//!   max_total, cooldown_secs)` and pass it as a constructor kwarg
//!   to `RustPrimaryCoordinator`.
//! - From Rust: [`PyRespawnPolicy::to_budget`] returns
//!   `Option<RespawnBudget>` — `Some` when the policy is enabled,
//!   `None` when disabled. The caller branches once on the option
//!   and either calls `enable_respawn` or leaves the coordinator's
//!   respawn fields at their default `None`. CCD-5: no hot-site `if
//!   policy_enabled` checks downstream.

use pyo3::prelude::*;

use dynrunner_manager_distributed::primary::respawn::RespawnBudget;

/// Python-facing respawn policy. Exposed as a pyclass with two
/// constructors mirroring the CLI choices for `--respawn-policy`.
///
/// `Clone` because the coordinator constructor takes ownership of a
/// copy: the policy is read once at `run()` start to materialise the
/// inner [`RespawnBudget`], and the Python-side instance may outlive
/// that read.
#[pyclass(name = "RespawnPolicy", from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct PyRespawnPolicy {
    inner: PolicyKind,
}

#[derive(Clone, Debug)]
enum PolicyKind {
    Disabled,
    OnSecondaryDeath { budget: RespawnBudget },
}

#[pymethods]
impl PyRespawnPolicy {
    /// Constructor for the `--respawn-policy=disabled` arm. No
    /// per-knob defaults are read; the resulting policy maps to
    /// `Option<RespawnBudget>::None` and the coordinator's
    /// respawn pipeline stays unwired.
    #[staticmethod]
    fn disabled() -> Self {
        Self {
            inner: PolicyKind::Disabled,
        }
    }

    /// Constructor for the `--respawn-policy=on-secondary-death` arm.
    /// All three knobs are positional + required so the Python caller
    /// must thread the CLI defaults (`3`, `10`, `30.0`) explicitly —
    /// surfacing them at the call site keeps the wiring auditable.
    #[staticmethod]
    fn on_secondary_death(max_per_secondary: u32, max_total: u32, cooldown_secs: f64) -> Self {
        Self {
            inner: PolicyKind::OnSecondaryDeath {
                budget: RespawnBudget {
                    max_per_secondary,
                    max_total,
                    cooldown: std::time::Duration::from_secs_f64(cooldown_secs),
                },
            },
        }
    }

    /// True iff the policy is `OnSecondaryDeath { .. }`. Used by
    /// Python tests / debug logging; the Rust-side wiring consults
    /// [`Self::to_budget`] which returns the typed option.
    #[getter]
    fn enabled(&self) -> bool {
        matches!(self.inner, PolicyKind::OnSecondaryDeath { .. })
    }
}

impl PyRespawnPolicy {
    /// Translate the Python-facing policy into the Rust-side
    /// [`RespawnBudget`] (or `None` for disabled). Called by
    /// `PyPrimaryCoordinator::run` at coordinator-construction time.
    pub(crate) fn to_budget(&self) -> Option<RespawnBudget> {
        match &self.inner {
            PolicyKind::Disabled => None,
            PolicyKind::OnSecondaryDeath { budget } => Some(budget.clone()),
        }
    }

    /// Rust-side constructor for the disabled arm. Mirrors the
    /// Python-facing `PyRespawnPolicy.disabled()` staticmethod so
    /// callers that build the policy entirely in Rust (e.g.
    /// `PyPrimaryCoordinator::new`'s default when the kwarg is
    /// omitted) don't need the GIL.
    pub(crate) fn rust_disabled() -> Self {
        Self {
            inner: PolicyKind::Disabled,
        }
    }
}
