//! [`TaskInfo`] — the one scheduling unit handed to the runtime — and the
//! [`TaskInput`] alias used by older call-sites.
//!
//! Also hosts [`TaskDep`], the dep-graph edge primitive. Co-located here
//! because dependencies are a `TaskInfo` concern (the field, the
//! validation rules, and the cycle-checker are all reached via
//! `TaskInfo.task_depends_on`).

use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize};

use super::identifiers::{AffinityId, PhaseId, TypeId};
use super::resource::SoftPreferredSecondaries;

/// One scheduling unit handed to the runtime: an identifier, an on-disk
/// payload (`path` + `size`), the phase/type tag that decides where it
/// dispatches, an optional affinity hint, and an opaque per-item payload.
///
/// Generic over the identifier type `I` so different task definitions
/// can use different identifier structures.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
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
    /// Stable consumer-supplied task identifier. REQUIRED — every
    /// task carries a non-empty id. Validated at the Python→Rust
    /// boundary (`crate::pytypes::extract_binaries` + `PyTaskInfo`
    /// construction) and again for uniqueness inside the run at
    /// `PendingPool::extend` time. Producers that omit the id or
    /// supply an empty string fail loudly at registration; the
    /// silent-skip path that used to mask producer-side bugs is
    /// gone.
    ///
    /// Other tasks reference this id from their `task_depends_on`
    /// to express a "wait for that task to complete before
    /// dispatching me" ordering constraint. Used by the memprofile
    /// sampler for per-task file naming, by the retry tracker for
    /// attempt-counting, and by the failure reporter to group
    /// results by task identity. The framework treats it
    /// opaquely; consumers compose whatever identity scheme makes
    /// sense for their domain (asm-tokenizer uses slash-separated
    /// paths like `nping/x86/clang/9/Os`). Pick stable, readable
    /// ids so the corresponding dependent tasks can reference them
    /// without re-deriving a hash.
    pub task_id: String,
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
    pub task_depends_on: Vec<TaskDep>,
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
    /// Monotone version of the `preferred_secondaries` metadata, stamped
    /// by the originating primary on each
    /// `TaskPreferredSecondariesUpdated` mutation. Lives at the
    /// `TaskInfo` level (not per-`TaskState`-variant) because the
    /// preferred update mutates `preferred_secondaries` in place on
    /// EVERY variant (incl. `Completed`/`Pending`) under a fixed ledger
    /// key — the enclosing variant's assignment/terminal version would
    /// be the wrong home (a preferred-update on a `Completed` task must
    /// not be incoherent). Two concurrent preferred-updates converge on
    /// the higher `preferred_version`. NOT folded into
    /// `compute_task_hash` (the hash recipe is `{phase_id, path,
    /// identifier}`), so a preferred update never changes the ledger
    /// key. `#[serde(default)]` keeps the wire backward-compatible with
    /// peers that predate the field (missing field decodes as the
    /// `(0, 0)` strict minimum).
    #[serde(default)]
    pub preferred_version: super::version::TaskVersion,
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

/// One edge in the per-task dep graph: the full `(phase_id, task_id)`
/// identity of the prerequisite, plus a per-edge opt-in to receive the
/// predecessor's transitive ancestors' outputs (not just the direct
/// predecessor's).
///
/// Identity is the FULL `(phase_id, task_id)` — every dependency names
/// its phase explicitly. The same `task_id` in two different phases is
/// a DISTINCT prerequisite; there is no implicit same-phase default at
/// runtime. New deps always carry an explicit `phase_id`; the only
/// producer of a phase-less dep is a legacy persisted snapshot, which
/// the migration shim ([`TaskDep::is_unphased`] +
/// [`TaskDep::fill_phase`]) reconciles on restore.
///
/// `inherit_outputs = false` (the default, and the only shape legacy
/// `Vec<String>` payloads decode to) means "wait for this task; read
/// only its own outputs".
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskDep {
    pub task_id: String,
    /// The prerequisite's phase. Part of the dependency's full
    /// `(phase_id, task_id)` identity. A legacy un-phased dep decodes
    /// with the migration sentinel (an empty `PhaseId`); the snapshot
    /// migration shim replaces that sentinel with the enclosing task's
    /// phase on restore. A new dep always carries a real phase, so it
    /// is never the sentinel and is left untouched by the shim.
    pub phase_id: PhaseId,
    #[serde(default)]
    pub inherit_outputs: bool,
}

impl TaskDep {
    /// Whether this dep is a legacy un-phased entry (carries the
    /// migration sentinel: an empty `PhaseId`). True ONLY for deps
    /// decoded from a legacy snapshot that predates the
    /// `(phase_id, task_id)` identity. A new dep always names its
    /// phase, so this is always `false` for runtime-originated deps —
    /// which is why the migration shim leaves new deps unaffected.
    pub fn is_unphased(&self) -> bool {
        self.phase_id.as_str().is_empty()
    }

    /// Migration-ONLY: fill the enclosing task's phase into a legacy
    /// un-phased dep. A no-op for any dep that already names its phase
    /// (the common runtime case), so applying it to a snapshot that
    /// mixes legacy and new deps touches only the legacy entries. This
    /// is NEVER a runtime default — it runs exclusively on the
    /// snapshot-restore path, where the enclosing task's `phase_id` is
    /// the unambiguous source of the missing phase (a legacy snapshot
    /// only ever expressed same-phase deps implicitly).
    pub fn fill_phase(&mut self, enclosing: &PhaseId) {
        if self.is_unphased() {
            self.phase_id = enclosing.clone();
        }
    }
}

// Untagged wire shape: a single `Vec<TaskDep>` JSON array may mix bare
// strings (legacy, un-phased) and full structs. serde's derive can't
// express "accept either shape" on a single struct, so the canonical
// idiom is to deserialise into a private untagged enum and then map.
// Without this decoder, every existing snapshot / ledger / wire fixture
// that serialises `task_depends_on` as `["foo", "bar"]` would fail to
// load.
//
// A legacy bare-string or a legacy struct without `phase_id` decodes to
// the migration sentinel (an empty `PhaseId`). New senders always emit
// the explicit `phase_id`, so they decode to a real phase. The decode
// is intentionally lenient about a missing phase so a LEGACY PERSISTED
// SNAPSHOT loads — runtime correctness is enforced by the migration
// shim (snapshot restore) and by the fact that no new sender ever omits
// the phase under the coordinated-restart deployment model. The
// sentinel is never a runtime default: a phase-less dep that reaches
// dispatch/hash without the shim resolves no task and surfaces loudly.
#[derive(Deserialize)]
#[serde(untagged)]
enum TaskDepWire {
    Bare(String),
    Full {
        task_id: String,
        #[serde(default)]
        phase_id: PhaseId,
        #[serde(default)]
        inherit_outputs: bool,
    },
}

impl<'de> Deserialize<'de> for TaskDep {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match TaskDepWire::deserialize(d)? {
            TaskDepWire::Bare(task_id) => TaskDep {
                task_id,
                phase_id: PhaseId::default(),
                inherit_outputs: false,
            },
            TaskDepWire::Full {
                task_id,
                phase_id,
                inherit_outputs,
            } => TaskDep {
                task_id,
                phase_id,
                inherit_outputs,
            },
        })
    }
}

#[cfg(test)]
mod task_dep_tests {
    use super::*;

    #[test]
    fn task_dep_bare_string_decodes_to_migration_sentinel() {
        // A legacy bare-string dep carries no phase; it decodes to the
        // migration sentinel (empty PhaseId) so a legacy snapshot loads.
        let dep: TaskDep = serde_json::from_str("\"foo\"").expect("bare string");
        assert_eq!(
            dep,
            TaskDep {
                task_id: "foo".to_string(),
                phase_id: PhaseId::default(),
                inherit_outputs: false,
            }
        );
        assert!(dep.is_unphased());
    }

    #[test]
    fn task_dep_struct_round_trips_phase_and_inherit() {
        // The canonical new shape carries the full (phase_id, task_id)
        // identity. Round-trip it on the wire.
        let dep = TaskDep {
            task_id: "foo".to_string(),
            phase_id: PhaseId::from("phase-A"),
            inherit_outputs: true,
        };
        let json = serde_json::to_value(&dep).expect("to_value");
        assert_eq!(json["task_id"], "foo");
        assert_eq!(json["phase_id"], "phase-A");
        assert_eq!(json["inherit_outputs"], true);
        let back: TaskDep = serde_json::from_value(json).expect("round-trip");
        assert_eq!(back, dep);
        assert!(!back.is_unphased());
    }

    #[test]
    fn task_dep_legacy_struct_without_phase_is_unphased() {
        // A legacy struct that predates phased identity omits phase_id;
        // it decodes to the sentinel, not a runtime default.
        let dep: TaskDep = serde_json::from_str("{\"task_id\":\"foo\",\"inherit_outputs\":true}")
            .expect("legacy struct");
        assert_eq!(
            dep,
            TaskDep {
                task_id: "foo".to_string(),
                phase_id: PhaseId::default(),
                inherit_outputs: true,
            }
        );
        assert!(dep.is_unphased());
    }

    #[test]
    fn task_dep_struct_default_inherit_outputs_false() {
        // The `inherit_outputs` key may be omitted from the struct shape.
        let dep: TaskDep = serde_json::from_str("{\"task_id\":\"foo\",\"phase_id\":\"p\"}")
            .expect("struct without flag");
        assert_eq!(
            dep,
            TaskDep {
                task_id: "foo".to_string(),
                phase_id: PhaseId::from("p"),
                inherit_outputs: false,
            }
        );
    }

    #[test]
    fn vec_task_dep_mixed_array_decodes() {
        let v: Vec<TaskDep> = serde_json::from_str(
            "[\"a\", {\"task_id\":\"b\",\"phase_id\":\"p1\",\"inherit_outputs\":true}]",
        )
        .expect("mixed array");
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].task_id, "a");
        assert!(v[0].is_unphased());
        assert!(!v[0].inherit_outputs);
        assert_eq!(v[1].task_id, "b");
        assert_eq!(v[1].phase_id, PhaseId::from("p1"));
        assert!(v[1].inherit_outputs);
    }

    #[test]
    fn fill_phase_migrates_sentinel_but_leaves_new_dep_untouched() {
        // The migration shim primitive: a sentinel dep takes the
        // enclosing phase; an explicit dep is unaffected.
        let mut legacy = TaskDep {
            task_id: "x".into(),
            phase_id: PhaseId::default(),
            inherit_outputs: false,
        };
        legacy.fill_phase(&PhaseId::from("enclosing"));
        assert_eq!(legacy.phase_id, PhaseId::from("enclosing"));

        let mut explicit = TaskDep {
            task_id: "y".into(),
            phase_id: PhaseId::from("other"),
            inherit_outputs: false,
        };
        explicit.fill_phase(&PhaseId::from("enclosing"));
        assert_eq!(
            explicit.phase_id,
            PhaseId::from("other"),
            "new dep untouched"
        );
    }
}
