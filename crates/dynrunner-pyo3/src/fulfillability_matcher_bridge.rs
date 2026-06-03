//! PyO3 bridge: adapt a Python `matcher(task_view, holdings) -> bool`
//! callable into a [`FulfillabilityMatcher`] the operational loop can
//! call.
//!
//! Single concern of this file: convert one Rust
//! `(hash, &TaskInfo, reason, &holdings)` tuple into one Python call,
//! under the GIL, and translate the Python return into a Rust `bool` —
//! swallowing every `PyErr` path to `tracing::warn` so the matcher
//! pipeline never tears down on a buggy / raising Python matcher.
//! Nothing about WHICH manager owns the matcher, HOW the manager
//! threads the kwarg through, or WHEN `set_fulfillability_matcher`
//! runs lives here — those concerns belong to the manager pyclass
//! files and are uniformly thin (single line each).
//!
//! Matcher signature (Python side):
//!
//! ```python
//! def matcher(
//!     failed_task_info: dynamic_runner.TaskInfoView,
//!     holdings: dict[str, set[str]],
//! ) -> bool: ...
//! ```
//!
//! Error / exception handling:
//!   - `PyErr` from the call surfaces a `tracing::warn` and the
//!     matcher returns `false` for that task. Other tasks in the
//!     same batch are unaffected (per-task isolation lives on the
//!     pipeline side via `catch_unwind`).
//!   - Non-boolean Python return values are extracted via
//!     `Py::is_truthy` so a numeric / string return collapses to
//!     the expected boolean shape rather than raising.
//!   - `pyo3::panic::PanicException` propagates as a Rust panic the
//!     pipeline's `catch_unwind` isolates.

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PySet};

use dynrunner_core::{RunnerIdentifier, TaskInfo};
use dynrunner_manager_distributed::fulfillability_matcher::FulfillabilityMatcher;

use crate::pytypes::PyTaskInfoView;

/// Adapter that holds an unbound Python callable and dispatches each
/// `should_reinject` call to it. `Send` is satisfied by `Py<PyAny>`'s
/// contract; the trait does not require `Sync` (the operational loop
/// owns the matcher single-threaded).
pub(crate) struct PyFulfillabilityMatcher {
    /// The Python callable. Held as an unbound `Py<PyAny>` so the
    /// adapter outlives any single `Python<'py>` lifetime; each
    /// `should_reinject` re-binds under a fresh GIL acquisition.
    matcher: Py<PyAny>,
}

impl PyFulfillabilityMatcher {
    /// Build a bridge from a Python callable.
    ///
    /// Boxed at the call site (returned as
    /// `Box<dyn FulfillabilityMatcher<RunnerIdentifier>>`) so the
    /// manager-distributed registration API consumes a uniform
    /// trait-object shape and the caller doesn't need to spell out
    /// the concrete type. Returning `Box<dyn ...>` instead of `Self`
    /// is the load-bearing API contract; clippy's "new should return
    /// Self" doesn't fit a constructor whose whole purpose is to
    /// hand callers an erased trait object.
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(matcher: Py<PyAny>) -> Box<dyn FulfillabilityMatcher<RunnerIdentifier>> {
        Box::new(Self { matcher })
    }
}

impl FulfillabilityMatcher<RunnerIdentifier> for PyFulfillabilityMatcher {
    fn should_reinject(
        &self,
        hash: &str,
        task: &TaskInfo<RunnerIdentifier>,
        reason: &str,
        holdings: &HashMap<String, HashSet<String>>,
    ) -> bool {
        // GIL acquisition crosses the runtime boundary. Per-call
        // cost is one attach + view-construct + dict-build + call.
        // The pipeline's burst-coalescing keeps the call rate
        // bounded by the number of Unfulfillable tasks, not the
        // holdings-update event volume.
        let outcome: PyResult<bool> = Python::attach(|py| {
            let view = PyTaskInfoView::from_task(hash, task, reason);
            let view_obj = Py::new(py, view)?;
            // Build the holdings dict: peer_id (str) → set[str].
            // PySet is the closest match to Python's set semantics;
            // a frozenset would also work but mutability is the
            // matcher's call and the framework owns the dict only
            // for the duration of the call anyway.
            let holdings_dict = PyDict::new(py);
            for (peer_id, outpaths) in holdings {
                let set = PySet::empty(py)?;
                for outpath in outpaths {
                    set.add(outpath)?;
                }
                holdings_dict.set_item(peer_id, set)?;
            }
            let result = self.matcher.bind(py).call1((view_obj, holdings_dict))?;
            // Accept any truthy Python return — explicit `bool`
            // extraction would refuse a numeric / list return that
            // the matcher implementation might use as a shorthand
            // "yes" / "no" signal. `is_truthy` matches Python's
            // own `if matcher(...)` semantics.
            result.is_truthy()
        });
        match outcome {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    target: "dynrunner_pyo3_fulfillability_matcher",
                    task_hash = %hash,
                    error = %e,
                    "Python fulfillability matcher raised; treating as \
                     false and continuing with the batch",
                );
                false
            }
        }
    }
}
