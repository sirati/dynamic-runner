//! Read-only Python view of a `TaskState::Unfulfillable` entry,
//! passed to consumer-installed fulfillability matchers.

use pyo3::prelude::*;

use dynrunner_core::TaskInfo;

/// Read-only Python view of a `TaskState::Unfulfillable` entry,
/// passed to the consumer-installed fulfillability matcher predicate.
///
/// Single concern: the minimum data the matcher needs to decide
/// whether the cluster's current holdings cover the task's
/// requirements. Not a full `PyTaskInfo` clone — the matcher's
/// observed contract is just "what hash, what on-disk identifier
/// path, what was the cluster's reason for failing it" — so only
/// `hash`, `path`, and `reason` cross the FFI boundary. Anything
/// the matcher needs beyond that belongs in `payload` and is
/// addressed by a future view extension, not by widening this type
/// today.
///
/// Read-only at the Python surface: every field is a `#[pyo3(get)]`
/// without a setter; the Python side cannot mutate the underlying
/// CRDT through this view.
#[pyclass(name = "TaskInfoView", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyTaskInfoView {
    /// Content-hash of the failed task. Same key the rest of the
    /// CRDT addressable surface uses (`ClusterState::task_state(hash)`,
    /// `PrimaryHandle::reinject_task(hash)`).
    #[pyo3(get)]
    pub(crate) hash: String,
    /// Wire-supplied identifier path (`TaskInfo.path`). The matcher's
    /// usual decision input — paired with the cluster's holdings map,
    /// the matcher checks whether any peer now advertises the
    /// resource the task expected. Rendered through `to_string_lossy`
    /// at construction so the Python side gets a plain `str`.
    #[pyo3(get)]
    pub(crate) path: String,
    /// The `TaskState::Unfulfillable.reason` body — the operator-
    /// resolvable-failure reason the runtime recorded at apply time.
    /// String, not a structured type, because the reason is wire-
    /// originated free-form text (the `BoundedString<2048>` cap is
    /// the wire codec's concern, not the in-memory view's).
    #[pyo3(get)]
    pub(crate) reason: String,
}

impl PyTaskInfoView {
    /// Build a view from a borrowed `TaskInfo<I>` + the surrounding
    /// `(hash, reason)` pair. Generic over `I` because the bridge is
    /// called from the operational loop where the identifier type is
    /// the coordinator's `I` parameter; the view itself erases `I`
    /// (only `path` is rendered, and that's an `I`-free string).
    pub(crate) fn from_task<I>(hash: &str, task: &TaskInfo<I>, reason: &str) -> Self {
        Self {
            hash: hash.to_string(),
            path: task.path.to_string_lossy().into_owned(),
            reason: reason.to_string(),
        }
    }
}
