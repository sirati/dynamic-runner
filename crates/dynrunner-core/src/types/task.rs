//! [`TaskInfo`] — the one scheduling unit handed to the runtime — and the
//! [`TaskInput`] alias used by older call-sites.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::identifiers::{AffinityId, PhaseId, TypeId};
use super::resource::SoftPreferredSecondaries;

/// One scheduling unit handed to the runtime: an identifier, an on-disk
/// payload (`path` + `size`), the phase/type tag that decides where it
/// dispatches, an optional affinity hint, and an opaque per-item payload.
///
/// Generic over the identifier type `I` so different task definitions
/// can use different identifier structures.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct TaskInfo<I> {
    pub path: PathBuf,
    pub size: u64,
    pub identifier: I,
    /// Which phase declared by `TaskDefinition.get_phases()` this item belongs to.
    /// Items only dispatch when their phase is active.
    pub phase_id: PhaseId,
    /// Which task type within the phase. Selects the worker module + memory estimator.
    pub type_id: TypeId,
    /// Optional soft worker-pinning class. Items with the same `Some(id)` prefer
    /// the same worker for kernel page-cache reuse; `None` joins the free pool.
    pub affinity_id: Option<AffinityId>,
    /// Opaque per-item data passed through to the worker. The framework never
    /// inspects this; consumers can stash JSON-serializable metadata here.
    pub payload: serde_json::Value,
    /// Optional consumer-supplied task identifier. Other tasks reference
    /// this id from their `task_depends_on` to express a "wait for that
    /// task to complete before dispatching me" ordering constraint.
    /// `None` means the task cannot itself be referenced as a
    /// prerequisite (anonymous task); it may still have its own
    /// `task_depends_on` entries pointing at named tasks. Consumers
    /// SHOULD pick stable, readable ids
    /// (e.g. `"toolchain__aarch64__clang15"`) so the corresponding
    /// dependent tasks can reference them without re-deriving a hash.
    /// Validated for uniqueness across the run at
    /// `PendingPool::extend` time.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Task ids of prerequisite tasks that must terminate (success
    /// OR permanent failure) before this task is eligible for
    /// dispatch. Default `Vec::new()` means "no per-task ordering
    /// constraint; eligibility is governed solely by the phase
    /// state machine".
    ///
    /// Dependencies are CROSS-PHASE-VALID — a task in a later phase
    /// can depend on a task in an earlier phase; the phase barrier
    /// already enforces the earlier phase completes first, so a
    /// cross-phase entry just becomes a tighter (per-task) constraint.
    /// The common use case is INTRA-PHASE: e.g. variant builds
    /// depending on their corresponding toolchain build, with both
    /// in the same phase, lets the scheduler dispatch variants
    /// continuously as toolchains drain instead of barriering on
    /// the whole phase.
    ///
    /// Validated at `PendingPool::extend`: every referenced id must
    /// correspond to a task in the run, otherwise
    /// `PendingPoolError::UnknownTaskDep` is returned. The dep
    /// graph is also cycle-checked.
    ///
    /// Cascade-failure semantics: when a prerequisite task fails
    /// permanently (Recoverable retry budget exhausted, or
    /// NonRecoverable / OOM), every dependent task is marked failed
    /// transitively with a synthetic upstream-failed error rather
    /// than waiting forever for a satisfaction that will never come.
    #[serde(default)]
    pub task_depends_on: Vec<String>,
    /// Soft hint of preferred secondaries (by peer name) for this task.
    /// Empty == no preference (free pool); the scheduler is free to
    /// pick any secondary. See [`SoftPreferredSecondaries`] for the
    /// soft-vs-strict semantic boundary. `#[serde(default)]` keeps
    /// the wire backward-compatible with peers that don't emit the
    /// field; `skip_serializing_if = "…is_empty"` keeps the wire
    /// quiet for the common empty case so a rolling upgrade is
    /// indistinguishable on the wire.
    #[serde(default, skip_serializing_if = "SoftPreferredSecondaries::is_empty")]
    pub preferred_secondaries: SoftPreferredSecondaries,
    /// Local-only on-disk location, set by the secondary after
    /// resolving `path` through its extraction cache / pre-staged
    /// shared mount. `None` means "the worker should open `path`
    /// against its configured source dir as before". `Some(p)`
    /// means "open `p` directly; `path` remains the wire-supplied
    /// identifier used for output-tree mirroring and bookkeeping".
    ///
    /// Never crosses the cluster wire — the primary↔secondary
    /// `DistributedBinaryInfo` round-trip resets it to `None` on
    /// receive (`#[serde(skip)]`). It exists to decouple two
    /// concerns that pre-fix collided in `path`: the wire-supplied
    /// identifier (consumer-visible, used for output layout) and
    /// the local on-disk file location (host-specific, set by the
    /// secondary's resolver). Mutating `path` to the resolved
    /// location made consumers see absolute extraction-cache paths
    /// in `task.relative_path`, which broke
    /// `output_dir / Path(relative_path).parent` mirroring (Python
    /// drops the left side when the right is absolute).
    #[serde(skip)]
    pub resolved_path: Option<PathBuf>,
}

pub type TaskInput<I> = TaskInfo<I>;
