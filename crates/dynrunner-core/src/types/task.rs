//! [`TaskInfo`] — the one scheduling unit handed to the runtime — and the
//! [`TaskInput`] alias used by older call-sites.
//!
//! Also hosts [`TaskDep`], the dep-graph edge primitive. Kept here
//! because dependencies are a `TaskInfo` concern (the field, the
//! validation rules, and the cycle-checker are all reached via
//! `TaskInfo.task_depends_on`).

use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize};

use super::identifiers::{AffinityId, PhaseId, TypeId};
use super::resource::SoftPreferredSecondaries;

/// The KIND of a task — the first-class behavioral classification that
/// decides, at four seams only (scheduling, reassignment-on-death,
/// dependency-resolution, counting), whether a task is ordinary worker
/// WORK, a framework SETUP primitive, or a per-secondary SECONDARY-AFFINE
/// gate.
///
/// A `Setup` task is NEVER worker-assignable: it is executed IN-PROCESS
/// by its affinity member (the source-owning member) — the executor
/// lands in a later phase; in this primitive a `Setup` task simply sits
/// in the ledger until that executor exists. It is NON-reassignable: an
/// executor death drives it to a terminal-unrecoverable state, never a
/// requeue. A SUCCEEDED `Setup` task satisfies a dependent's `TaskDep`
/// (so build tasks can gate on a setup task overlapping) and is counted
/// in its OWN `setup_succeeded` bucket — never the `succeeded` bucket.
///
/// A `SecondaryAffine` task is the per-secondary import primitive (#497):
/// it is NEITHER worker-assignable NOR reassignable, the primary NEVER
/// executes it, and it is NEVER counted in any success/fail bucket. It is
/// a schedulability GATE only — its execution is once-per-secondary,
/// tracked node-locally OFF the CRDT. The primary considers it
/// dependency-satisfied when its OWN deps are done (a ready-not-executed
/// transition); the once-per-secondary local run lands in later phases.
/// Like `Setup` it is non-worker-assignable/non-reassignable, but unlike
/// `Setup` it is NOT the common `Work` case — so it MUST be serialized on
/// the wire (see `is_work`).
///
/// `Default` is `Work`, and the field is `#[serde(default)]` on
/// `TaskInfo`, so a frame from a peer that predates this field decodes
/// as `Work` — the wire stays backward-compatible. NOT folded into
/// `compute_task_hash` (the recipe is `{phase_id, path, identifier}`),
/// so marking a task `Setup` (or `SecondaryAffine`) never changes its
/// ledger key.
///
/// The classification drives behavior ONLY through this enum's
/// predicate methods at the four seams; no consumer of the field spells
/// a bare `if kind == Setup` — they ask `is_worker_assignable()` /
/// `is_reassignable()` / `is_setup()` / `is_secondary_affine()` so the
/// kind→behavior mapping has a single owner (this type).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskKind {
    /// Ordinary worker work: dispatched to a worker, reassignable on the
    /// holder's death, completion counts in `succeeded`. The default —
    /// every existing task, and every wire frame that predates the
    /// `kind` field, is `Work`.
    #[default]
    Work,
    /// A framework setup primitive: never worker-assigned, executed
    /// in-process by its affinity member, non-reassignable (death →
    /// terminal unrecoverable), depend-able, and counted in the separate
    /// `setup_succeeded` bucket on success.
    Setup,
    /// A per-secondary import primitive (#497): never worker-assigned,
    /// non-reassignable, NEVER executed by the primary and NEVER counted
    /// in any bucket. A schedulability GATE whose dependents unblock when
    /// its OWN deps resolve (ready-not-executed); its actual execution is
    /// once-per-secondary and tracked node-locally, off the CRDT. The
    /// ready-resolution, the `QueuedAfterLocalDependency` work-task state,
    /// and the once-per-secondary local executor land in later phases —
    /// in this primitive the kind exists so it can be declared, routed
    /// past the worker-dispatch view, and serialized on the wire.
    SecondaryAffine,
    /// A per-secondary SPECULATIVE-PREP primitive (#638): a phase-AGNOSTIC,
    /// idle-filler "eager prep" task that runs once-per-secondary as the LAST
    /// dispatch resort (only when an idle worker has nothing else: an empty
    /// pool view, an empty affine queue, and a failed idle-steal). Like
    /// [`Self::SecondaryAffine`]
    /// it is NEITHER worker-assignable NOR reassignable, the primary NEVER
    /// executes it, and it is NEVER counted in any success/fail bucket. Unlike
    /// `SecondaryAffine` it is NOT a schedulability gate — it has NO dependents,
    /// so it never enters the pool's phase buckets at all; its readiness is
    /// purely the per-secondary 2-bit cell it shares with affine on the cell
    /// substrate. It MUST NOT hold a phase open (`counts_for_phase_drain` is
    /// false) and MUST be serialized on the wire (like `SecondaryAffine`, a
    /// peer that dropped it from the wire would lose the prep declaration).
    SecondaryEagerPrep,
}

/// The COUNTING category of a task — the mutually-exclusive partition a
/// tally/reporting concern uses to bucket a ledger entry WITHOUT peeking
/// at the [`TaskKind`] discriminant itself. Derived ONLY via
/// [`TaskKind::count_category`], so the kind→category mapping has a
/// single owner (this enum + that method). See `count_category` for the
/// per-category contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCountCategory {
    /// Ordinary worker work — the generic per-state buckets.
    Work,
    /// A framework setup task — its OWN setup-prefixed per-state buckets.
    Setup,
    /// A per-secondary import GATE token — one flat count, no per-state
    /// subdivision (phase-uncounted; readiness is the per-secondary
    /// bitvector, not a global state).
    SecondaryAffine,
    /// A per-secondary SPECULATIVE-PREP token (#638) — one flat count, no
    /// per-state subdivision (phase-uncounted; readiness is the per-secondary
    /// cell, not a global state). The eager-prep twin of
    /// [`Self::SecondaryAffine`]'s flat count.
    SecondaryEagerPrep,
}

impl TaskKind {
    /// Whether a task of this kind may be dispatched to a WORKER. Only
    /// `Work` is worker-assignable; a `Setup` task is executed in-process
    /// by its affinity member and a `SecondaryAffine` task runs
    /// once-per-secondary node-locally — neither must ever enter a
    /// worker-dispatch view. The scheduling seam (`PendingPool`) reads
    /// this.
    pub fn is_worker_assignable(self) -> bool {
        matches!(self, TaskKind::Work)
    }

    /// Whether a QUEUED task of this kind holds its phase open against the
    /// drain transition — i.e. it is real work the phase must wait to
    /// consume from its bucket before it can drain. `Work` (dispatched to a
    /// worker) and `Setup` (consumed from the bucket by its in-process
    /// executor) BOTH count: while either sits queued, its phase has
    /// outstanding bucket work. A `SecondaryAffine` task does NOT count: under
    /// Model B it is a non-worker-assignable LEDGER TOKEN — the
    /// placement-readiness signal kept in the bucket whose per-secondary runs
    /// are driven off-queue by the affine scheduler + bitvector, never
    /// consumed from the bucket on a phase-drain path. Counting it would pin
    /// `queued_count > 0` forever and the phase would never drain. The
    /// drain-side counterpart of [`Self::is_worker_assignable`]: the latter
    /// gates the worker VIEW, this gates the drain COUNT. The scheduling seam
    /// (`PendingPool::queued_count`) reads this.
    pub fn counts_for_phase_drain(self) -> bool {
        !matches!(
            self,
            TaskKind::SecondaryAffine | TaskKind::SecondaryEagerPrep
        )
    }

    /// Whether an in-flight task of this kind may be REASSIGNED (requeued
    /// to `Pending`) when its holder dies. `Work` is reassignable —
    /// another worker picks it up. A `Setup` task is NOT: its executor is
    /// its source-owning member, so an executor death is unrecoverable
    /// (the task goes terminal, dependents cascade). A `SecondaryAffine`
    /// task is likewise not worker-reassignable here — it is never a
    /// dispatched worker task at all (its once-per-secondary execution is
    /// tracked node-locally). The death seam (the primary's in-flight
    /// recovery) reads this.
    pub fn is_reassignable(self) -> bool {
        matches!(self, TaskKind::Work)
    }

    /// Whether this is a `Setup` task. The plain discriminant query for
    /// the few call sites that classify by kind without a behavioral
    /// predicate (e.g. the PyO3 boundary mapping an `is_setup` bool). A
    /// `SecondaryAffine` task is NOT a setup task — it has its own
    /// [`Self::is_secondary_affine`] discriminant and a distinct
    /// (per-secondary, off-CRDT) execution model.
    pub fn is_setup(self) -> bool {
        matches!(self, TaskKind::Setup)
    }

    /// Whether a def of this kind needs a PER-SECONDARY completion CELL on the
    /// shared cell substrate — `true` for both [`Self::SecondaryAffine`] (the
    /// import gate) and [`Self::SecondaryEagerPrep`] (the idle filler), `false`
    /// for ordinary `Work`/`Setup`. The KIND-BLIND seam the cell-id registration
    /// pass reads to decide which originated `TaskAdded` gets a CRDT-agreed dense
    /// cell-id reserved + a paired `SecondaryCellRegistered` injected, so one
    /// origination pass serves every cell-bearing kind (no per-kind pass).
    pub fn has_secondary_cell(self) -> bool {
        matches!(
            self,
            TaskKind::SecondaryAffine | TaskKind::SecondaryEagerPrep
        )
    }

    /// Whether this is a `SecondaryAffine` task (#497) — the per-secondary
    /// import gate. The plain discriminant query for the call sites that
    /// classify by kind without a behavioral predicate (the later phases'
    /// ready-resolution originator, the secondary's local-dep gate, and
    /// the PyO3 boundary mapping an `is_secondary_affine` bool).
    pub fn is_secondary_affine(self) -> bool {
        matches!(self, TaskKind::SecondaryAffine)
    }

    /// Whether this is a `SecondaryEagerPrep` task (#638) — the per-secondary
    /// speculative idle-filler. The plain discriminant query for the call
    /// sites that classify by kind without a behavioral predicate (the
    /// eager-prep dispatch leaf, the pool intake divert, and the PyO3 boundary
    /// mapping an `is_secondary_eager_prep` bool). A `SecondaryAffine` task is
    /// NOT an eager-prep task — they share the per-secondary CELL substrate but
    /// are distinct kinds with distinct dispatch precedence.
    pub fn is_secondary_eager_prep(self) -> bool {
        matches!(self, TaskKind::SecondaryEagerPrep)
    }

    /// The COUNTING category this kind belongs to — the single modular
    /// projection a tally/reporting concern dispatches on so it never
    /// spells a bare `if kind == Setup` (the antipattern the four-seam
    /// design forbids). The three categories are mutually exclusive and
    /// exhaust the kinds, mirroring the counting seam's contract:
    ///
    ///   - [`TaskCountCategory::Work`] — ordinary worker work; counted in
    ///     the generic per-STATE buckets.
    ///   - [`TaskCountCategory::Setup`] — a framework setup task; counted
    ///     in its OWN setup-prefixed per-STATE buckets, EXCLUDED from the
    ///     generic ones, so an operator sees setup progress without it
    ///     inflating the work tally.
    ///   - [`TaskCountCategory::SecondaryAffine`] — a per-secondary import
    ///     GATE token; it is phase-uncounted (its readiness is the
    ///     per-secondary 2-bit bitvector, not a global pending/done state),
    ///     so it is reported as ONE flat count with NO state subdivision
    ///     and EXCLUDED from every per-state bucket.
    ///
    /// The counting seam ([`StateCounts`](../../../dynrunner_manager_distributed/cluster_state/struct.StateCounts.html))
    /// is the sole consumer: it routes each ledger entry by THIS category,
    /// so adding/retiring a kind moves the partition in ONE place.
    pub fn count_category(self) -> TaskCountCategory {
        match self {
            TaskKind::Work => TaskCountCategory::Work,
            TaskKind::Setup => TaskCountCategory::Setup,
            TaskKind::SecondaryAffine => TaskCountCategory::SecondaryAffine,
            TaskKind::SecondaryEagerPrep => TaskCountCategory::SecondaryEagerPrep,
        }
    }

    /// Whether this is the common `Work` case — the predicate behind the
    /// `#[serde(skip_serializing_if = …)]` attribute on `TaskInfo.kind`
    /// (serde hands it a `&T`). Keeps the default `Work` kind off the
    /// wire so a rolling upgrade is indistinguishable for ordinary tasks.
    ///
    /// Deliberately `matches!(self, Work)`, NOT a delegation to
    /// [`Self::is_worker_assignable`]: a `SecondaryAffine` task is also
    /// non-worker-assignable, but it is NOT the common `Work` case and
    /// MUST be serialized (a peer that drops it from the wire would lose
    /// the gate). Only the exact `Work` discriminant may be skipped.
    fn is_work(&self) -> bool {
        matches!(self, TaskKind::Work)
    }
}

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
    /// First-class behavioral classification — ordinary worker [`TaskKind::Work`]
    /// (the default) or the framework [`TaskKind::Setup`] primitive.
    /// Drives behavior at the four kind-seams only (scheduling /
    /// reassignment / dependency-resolution / counting) via
    /// [`TaskKind`]'s predicate methods. `#[serde(default)]` keeps the
    /// wire backward-compatible (a frame without the field decodes as
    /// `Work`); `skip_serializing_if` keeps the common `Work` case quiet
    /// on the wire so a rolling upgrade is indistinguishable for ordinary
    /// tasks. NOT part of `compute_task_hash`, so marking a task `Setup`
    /// never changes its ledger key.
    #[serde(default, skip_serializing_if = "TaskKind::is_work")]
    pub kind: TaskKind,
    /// EXECUTOR-affinity member for a [`TaskKind::Setup`] task: the peer
    /// id of the source-owning member that runs this setup task IN-PROCESS
    /// (zero-worker). For framework auto-staging this is the submitter /
    /// observer; for a consumer setup task it is the consumer-specified
    /// member (e.g. a compute node for `--build-compilers`). The primary's
    /// setup selector targets exactly this member; the member's in-process
    /// executor runs the task and originates its terminal.
    ///
    /// `None` on a [`TaskKind::Work`] task (the overwhelmingly-common case)
    /// and on a `Setup` task whose affinity defaults to the primary itself
    /// (the selector reads `None` as "run on the primary"). NOT part of
    /// `compute_task_hash` (the recipe is `{phase_id, path, identifier}`),
    /// so setting an executor affinity never changes the ledger key — it is
    /// a routing concern, exactly like `preferred_secondaries`.
    /// `#[serde(default)]` keeps the wire backward-compatible (a frame
    /// without the field decodes as `None`); `skip_serializing_if` keeps the
    /// common `None` case off the wire so a rolling upgrade is
    /// indistinguishable for ordinary tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_affinity: Option<String>,
    /// Upload-file reference for a [`TaskKind::Setup`] task whose action
    /// is "upload this file to the cluster" (#336 P1). `Some` ⇒ the
    /// in-process executor on [`Self::setup_affinity`] performs the upload
    /// via the registered upload-action callback and originates
    /// `SetupCompleted` on success (the dependent work task that
    /// `task_depends_on` this setup task then unblocks). `None` ⇒ the
    /// no-upload-needed case (the pre-staged / mode-2 gate): the setup
    /// task's action is the unchanged no-op success — exactly the #489
    /// primitive behaviour.
    ///
    /// Only a `Setup` task ever carries `Some`; a `Work` task is always
    /// `None`. NOT part of `compute_task_hash` (the recipe is
    /// `{phase_id, path, identifier}`), so attaching a file ref never
    /// changes the ledger key — it is action metadata, exactly like
    /// `setup_affinity` is routing metadata. `#[serde(default)]` keeps the
    /// wire backward-compatible (a frame from a peer that predates this
    /// field decodes as `None` — the no-op gate); `skip_serializing_if`
    /// keeps the common `None` case off the wire so a rolling upgrade is
    /// indistinguishable for every non-upload task.
    ///
    /// BOXED so the (rare) upload payload does not bloat the common
    /// `TaskInfo` (and thus `ClusterMutation::TaskAdded`, which embeds it) —
    /// `Option<Box<_>>` is one pointer. Serde treats `Box<T>` transparently,
    /// so the wire shape is identical to an inline `UploadFileRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_file: Option<Box<UploadFileRef>>,
    /// Files a [`TaskKind::Work`] task needs UPLOADED to the cluster before
    /// it can run (#336 P2). DISTINCT from [`Self::upload_file`]: that is the
    /// file a SETUP task's own upload action transfers; THIS is the set of
    /// files a WORK task declares as prerequisites. The framework's
    /// files-attach transform turns each unique `(source, dest)` across the
    /// whole batch into EXACTLY ONE upload setup task (deduped — a file shared
    /// by N work tasks produces one upload, not N) and wires each work task's
    /// `task_depends_on` to the setup tasks for ITS files, so the work task
    /// dispatches only after every one of its files has uploaded.
    ///
    /// `None` (the overwhelmingly-common case) ⇒ no attached files; the task
    /// behaves exactly as today (no upload setup tasks, the bulk-walk / #489
    /// no-op gate path unchanged). Only a `Work` task ever carries entries;
    /// the derived upload setup tasks carry their file on `upload_file`
    /// instead. NOT part of `compute_task_hash` (the recipe is `{phase_id,
    /// path, identifier}`), so declaring required files never changes the
    /// ledger key — it is attach metadata, exactly like `upload_file` is
    /// action metadata.
    ///
    /// Stored as `Option<Box<[UploadFileRef]>>` so the (rare) attach payload
    /// does not bloat the common `TaskInfo` (and thus
    /// `ClusterMutation::TaskAdded`, which embeds it) — the empty case is a
    /// `None` (one machine word, niche-optimized), and a boxed slice keeps the
    /// non-empty case to a fat pointer rather than a `Vec`'s three words inline
    /// on the struct. (Mirrors why `upload_file` is `Option<Box<_>>`.) Serde
    /// treats `Box<[T]>` transparently as an array and `skip_serializing_if`
    /// flattens the `None` empty case away, so the wire shape is an optional
    /// array of `UploadFileRef`. `#[serde(default)]` keeps the wire
    /// backward-compatible (a frame from a peer that predates this field
    /// decodes as `None`); a rolling upgrade is indistinguishable for every
    /// task that declares no files. Use [`Self::required_files`] for a flat
    /// `&[UploadFileRef]` view that erases the `Option`/`Box`, and
    /// [`required_files_storage`] to build this shape from a flat list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_files: Option<Box<[UploadFileRef]>>,
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

impl<I> TaskInfo<I> {
    /// A flat `&[UploadFileRef]` view of [`Self::required_files`] (#336 P2),
    /// erasing the `Option`/`Box` indirection the storage uses to keep the
    /// common (no-files) `TaskInfo` small. `None` ⇒ the empty slice, so every
    /// caller iterates a slice without minding the storage shape. This is the
    /// SINGLE read seam for the attach payload — the framework's files-attach
    /// transform and every consumer of the declared files go through here.
    pub fn required_files(&self) -> &[UploadFileRef] {
        self.required_files.as_deref().unwrap_or(&[])
    }
}

/// Build the `Option<Box<[…]>>` storage shape for [`TaskInfo::required_files`]
/// from a flat list (#336 P2) — the SINGLE write seam, so no construction site
/// spells the `Option`/`Box` wrapping. An empty list normalizes to `None` (the
/// common case; the empty payload never bloats the wire or the
/// `ClusterMutation::TaskAdded` enum). A free function (not a `TaskInfo<I>`
/// associated fn) because the shaping is independent of the identifier type —
/// callers must not have to spell `I` to wrap a file list.
pub fn required_files_storage(files: Vec<UploadFileRef>) -> Option<Box<[UploadFileRef]>> {
    if files.is_empty() {
        None
    } else {
        Some(files.into_boxed_slice())
    }
}

/// The framework-owned cluster MOUNT ROOT an upload lands under (#644).
///
/// A consumer selects WHICH of the framework's bind-mount roots an
/// attached file is uploaded into; it never spells a host path (the
/// framework owns the host→container mount mapping). This is the mount
/// SELECTOR primitive — a closed set of the roots the framework
/// publishes, not a free path.
///
/// * [`UploadRoot::Source`] (the default) — the gateway srcbins dir, the
///   SAME bind-mount root the bulk source walk populates, surfaced in a
///   secondary container as `/app/src-network`. The pre-#644 behaviour:
///   every upload landed here.
/// * [`UploadRoot::Output`] — the shared output mount
///   (`SlurmConfig::get_output_dir`), surfaced as `/app/out-network`,
///   where the consumer's affine import gates read.
///
/// Wire-compatible: `#[serde(default)]` (⇒ [`UploadRoot::Source`]) so an
/// `UploadFileRef` serialized by a peer / snapshot that predates this
/// field decodes unchanged, and the common (`Source`) case never widens
/// the wire when paired with `skip_serializing_if` on the field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum UploadRoot {
    /// The gateway srcbins root (`/app/src-network`). The default, and the
    /// only root any pre-#644 upload used.
    #[default]
    Source,
    /// The shared output root (`/app/out-network`).
    Output,
}

/// The file a [`TaskKind::Setup`] upload-action task uploads to the
/// cluster (#336 P1). Deliberately MINIMAL for P1: a `source` path on
/// the source-owning member (the submitter / observer that physically
/// holds the file) plus an optional explicit `dest` and a framework
/// mount-root selector. P2 owns how discovery / consumers ATTACH refs to
/// tasks; this type is only the per-file payload the registered upload
/// callback receives.
///
/// `dest = None` ⇒ the upload callback derives the destination from
/// `source` the same way the bulk-walk does (strip-prefix under the
/// chosen root). `dest = Some(p)` ⇒ upload to exactly `p` relative to the
/// chosen root — the explicit-placement case a consumer-spawned
/// file-setup-task uses for a shared resource that does not live under
/// `--source`.
///
/// `root` selects WHICH framework mount the `dest` (or derived
/// destination) is relative to; see [`UploadRoot`]. Defaults to
/// [`UploadRoot::Source`] (the pre-#644 behaviour).
///
/// Wire-compatible: `#[serde(default)]` on `dest` AND `root` so a future
/// field addition stays additive, and the whole type rides on `TaskInfo`
/// only when present (`skip_serializing_if` on the optional fields), so a
/// peer that predates #336 / #644 never sees them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadFileRef {
    /// On-disk location of the file to upload, on the source-owning
    /// affinity member. Absolute, or relative to the member's configured
    /// source root (the upload callback resolves it the same way the
    /// bulk-walk's per-file path is resolved).
    pub source: PathBuf,
    /// Explicit cluster-side destination for the upload. `None` ⇒ the
    /// callback derives it from `source` (strip-prefix under the chosen
    /// `root`), matching the bulk-walk's placement. `Some` ⇒ upload
    /// verbatim to this path under `root` (the consumer-spawned
    /// shared-resource case).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dest: Option<PathBuf>,
    /// The framework mount root the upload lands under. Defaults to
    /// [`UploadRoot::Source`] (the srcbins root every pre-#644 upload
    /// used); a consumer sets [`UploadRoot::Output`] to land in the shared
    /// output mount instead. `#[serde(default)]` keeps an old serialized
    /// form (no `root`) decoding to `Source`; `skip_serializing_if` keeps
    /// the default off the wire so a `Source` upload is byte-identical to
    /// the pre-#644 frame.
    #[serde(default, skip_serializing_if = "is_default_root")]
    pub root: UploadRoot,
}

/// Whether a [`UploadRoot`] is the serialization default ([`UploadRoot::Source`]).
/// Used by `skip_serializing_if` so a default-root `UploadFileRef` serializes
/// byte-identically to a pre-#644 frame (no `root` key), keeping the common
/// case off the wire and old peers/snapshots compatible.
fn is_default_root(root: &UploadRoot) -> bool {
    *root == UploadRoot::default()
}

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
///
/// RESOLVED def-id ([`Self::def_id`], L5): the compact, CRDT-agreed
/// store index of the PREREQUISITE task's content. The originating
/// primary resolves each dep's `(phase_id, task_id)` identity to the
/// prereq's `TaskDefId` at `TaskAdded` origination — AFTER the whole
/// batch's defs are reserved, so an intra-batch forward-ref resolves —
/// and stamps it here. The receiver's def-store fill reads this directly
/// (no `(phase_id, task_id)` re-resolution needed, so it is forward-ref-
/// safe regardless of in-batch delivery order) and stores the compact
/// `TaskDepRef` on the frozen def, dropping the heap `(phase_id, task_id)`
/// strings. The string identity stays on this `TaskDep` for the consumers
/// that key by it (the dispatch wire, the secondary affine gate, the
/// predecessor-outputs walk); the def-store rebuilds it from the ref via
/// `resolve(def_id)` for the frozen-def read seams. `None` is the
/// un-resolved shape: a legacy/un-stamped dep, or any local-apply path
/// that does not route through the broadcast stamp — the def-store fill
/// then falls back to identity resolution.
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
    /// The PREREQUISITE's resolved, CRDT-agreed def-store id (L5). Stamped
    /// by the originating primary at `TaskAdded` origination (the receiver
    /// stores it as the compact `TaskDepRef` on the frozen def). `None` for
    /// a legacy/un-stamped dep or a non-broadcast local-apply dep; the
    /// def-store fill falls back to `(phase_id, task_id)` identity
    /// resolution in that case. `#[serde(default)]` decodes a pre-field
    /// sender's frame to `None` (wire-safe), and `skip_serializing_if`
    /// keeps the un-resolved shape off the wire so a legacy/dispatch frame
    /// is byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub def_id: Option<u32>,
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
        #[serde(default)]
        def_id: Option<u32>,
    },
}

impl<'de> Deserialize<'de> for TaskDep {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match TaskDepWire::deserialize(d)? {
            TaskDepWire::Bare(task_id) => TaskDep {
                task_id,
                phase_id: PhaseId::default(),
                inherit_outputs: false,
                def_id: None,
            },
            TaskDepWire::Full {
                task_id,
                phase_id,
                inherit_outputs,
                def_id,
            } => TaskDep {
                task_id,
                phase_id,
                inherit_outputs,
                def_id,
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
                def_id: None,
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
            def_id: None,
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
                def_id: None,
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
                def_id: None,
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
    fn task_dep_resolved_def_id_round_trips_and_is_skipped_when_none() {
        // L5: a stamped dep carries the prereq's resolved def-id on the
        // wire; an un-stamped dep keeps it off the wire (legacy-compatible).
        let stamped = TaskDep {
            task_id: "foo".to_string(),
            phase_id: PhaseId::from("p"),
            inherit_outputs: true,
            def_id: Some(7),
        };
        let json = serde_json::to_value(&stamped).expect("to_value");
        assert_eq!(json["def_id"], 7);
        let back: TaskDep = serde_json::from_value(json).expect("round-trip");
        assert_eq!(back, stamped);

        let unstamped = TaskDep {
            task_id: "foo".to_string(),
            phase_id: PhaseId::from("p"),
            inherit_outputs: false,
            def_id: None,
        };
        let json = serde_json::to_value(&unstamped).expect("to_value");
        assert!(
            json.get("def_id").is_none(),
            "an un-stamped dep keeps def_id off the wire"
        );
        // A legacy frame (def_id absent) decodes to None.
        let legacy: TaskDep =
            serde_json::from_str("{\"task_id\":\"foo\",\"phase_id\":\"p\"}").expect("legacy");
        assert_eq!(legacy.def_id, None);
    }

    #[test]
    fn fill_phase_migrates_sentinel_but_leaves_new_dep_untouched() {
        // The migration shim primitive: a sentinel dep takes the
        // enclosing phase; an explicit dep is unaffected.
        let mut legacy = TaskDep {
            task_id: "x".into(),
            phase_id: PhaseId::default(),
            inherit_outputs: false,
            def_id: None,
        };
        legacy.fill_phase(&PhaseId::from("enclosing"));
        assert_eq!(legacy.phase_id, PhaseId::from("enclosing"));

        let mut explicit = TaskDep {
            task_id: "y".into(),
            phase_id: PhaseId::from("other"),
            inherit_outputs: false,
            def_id: None,
        };
        explicit.fill_phase(&PhaseId::from("enclosing"));
        assert_eq!(
            explicit.phase_id,
            PhaseId::from("other"),
            "new dep untouched"
        );
    }
}

#[cfg(test)]
mod upload_file_ref_serde_tests {
    //! #644 wire/snapshot back-compat for the new `root` field on
    //! [`UploadFileRef`]. `UploadFileRef` rides the replicated CRDT
    //! (`FrozenTaskCore::required_files` / `upload_file`, both
    //! `#[derive(Serialize, Deserialize)]` with `pub` fields for the
    //! def-transfer wire), so a peer / snapshot that predates this field MUST
    //! decode unchanged.
    use super::*;

    #[test]
    fn old_form_without_root_decodes_as_source() {
        // A pre-#644 serialized form (no `root` key) decodes with the default
        // root — keeping an old peer's / snapshot's frame compatible.
        let old: UploadFileRef =
            serde_json::from_str(r#"{"source":"/src/a"}"#).expect("decode pre-#644 form");
        assert_eq!(old.source, PathBuf::from("/src/a"));
        assert_eq!(old.dest, None);
        assert_eq!(
            old.root,
            UploadRoot::Source,
            "missing `root` must default to Source (wire back-compat)"
        );

        // The pre-#644 form WITH an explicit dest still decodes the same way.
        let old_dest: UploadFileRef =
            serde_json::from_str(r#"{"source":"/src/b","dest":"/dst/b"}"#)
                .expect("decode pre-#644 form with dest");
        assert_eq!(old_dest.dest, Some(PathBuf::from("/dst/b")));
        assert_eq!(old_dest.root, UploadRoot::Source);
    }

    #[test]
    fn default_root_is_omitted_from_the_wire() {
        // A `Source`-root ref serializes byte-identically to a pre-#644 frame
        // (no `root` key) — the common case never widens the wire and an old
        // peer never sees a field it cannot decode.
        let src_ref = UploadFileRef {
            source: PathBuf::from("/src/a"),
            dest: None,
            root: UploadRoot::Source,
        };
        let json = serde_json::to_value(&src_ref).expect("serialize");
        assert!(
            json.get("root").is_none(),
            "the default Source root must be skipped on the wire; got {json}"
        );

        // A non-default `Output` root IS serialized (so a #644-aware peer
        // reconstructs it) and round-trips.
        let out_ref = UploadFileRef {
            source: PathBuf::from("/src/a"),
            dest: None,
            root: UploadRoot::Output,
        };
        let json = serde_json::to_value(&out_ref).expect("serialize output");
        assert!(
            json.get("root").is_some(),
            "a non-default Output root must ride the wire; got {json}"
        );
        let back: UploadFileRef = serde_json::from_value(json).expect("round-trip");
        assert_eq!(back, out_ref, "Output root round-trips");
    }
}

#[cfg(test)]
mod task_kind_tests {
    use super::*;

    /// Build a minimal `TaskInfo` with the given kind so the kind-seam
    /// predicates and the wire shape can be exercised without dragging in
    /// the scheduler. Mirrors the `mk` helper in `task_hash.rs` tests.
    fn mk_task(kind: TaskKind) -> TaskInfo<String> {
        TaskInfo {
            path: PathBuf::from("/bin/x"),
            size: 1,
            identifier: "id".to_string(),
            phase_id: PhaseId::from("phase-A"),
            type_id: TypeId::from("t"),
            kind,
            setup_affinity: None,
            upload_file: None,
            required_files: None,
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: "task-1".to_string(),
            task_depends_on: Vec::new(),
            preferred_secondaries: SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            resolved_path: None,
        }
    }

    #[test]
    fn secondary_affine_predicates() {
        // SecondaryAffine is NEITHER worker-assignable, reassignable, nor a
        // setup task — it is its own discriminant. The primary never
        // dispatches it to a worker, never requeues it on death, and never
        // routes it through the setup executor; its only seam in this phase
        // is its own `is_secondary_affine` query.
        let k = TaskKind::SecondaryAffine;
        assert!(!k.is_worker_assignable(), "never worker-assignable");
        assert!(!k.is_reassignable(), "never reassignable");
        assert!(!k.is_setup(), "not a setup task");
        assert!(k.is_secondary_affine(), "is its own kind");

        // The other kinds answer the new discriminant false.
        assert!(!TaskKind::Work.is_secondary_affine());
        assert!(!TaskKind::Setup.is_secondary_affine());
        assert!(!TaskKind::SecondaryEagerPrep.is_secondary_affine());
    }

    #[test]
    fn secondary_eager_prep_predicates() {
        // SecondaryEagerPrep MIRRORS SecondaryAffine at the kind seams: NEITHER
        // worker-assignable, reassignable, nor a setup task; it does NOT count
        // for phase drain (CRITICAL — it must never block a phase transition);
        // and it is its own discriminant. The primary never dispatches it to a
        // worker, never requeues it on death, and never routes it through the
        // setup executor.
        let k = TaskKind::SecondaryEagerPrep;
        assert!(!k.is_worker_assignable(), "never worker-assignable");
        assert!(!k.is_reassignable(), "never reassignable");
        assert!(!k.is_setup(), "not a setup task");
        assert!(!k.is_secondary_affine(), "distinct from affine");
        assert!(k.is_secondary_eager_prep(), "is its own kind");
        assert!(
            !k.counts_for_phase_drain(),
            "must NOT hold a phase open — phase-agnostic idle filler"
        );

        // The other kinds answer the new discriminant false.
        assert!(!TaskKind::Work.is_secondary_eager_prep());
        assert!(!TaskKind::Setup.is_secondary_eager_prep());
        assert!(!TaskKind::SecondaryAffine.is_secondary_eager_prep());

        // Its counting category is its own flat-count partition.
        assert_eq!(k.count_category(), TaskCountCategory::SecondaryEagerPrep);
    }

    #[test]
    fn secondary_affine_round_trips_on_the_wire() {
        // Unlike the default `Work` kind (skipped to keep a rolling
        // upgrade quiet), a SecondaryAffine task MUST appear on the wire —
        // dropping it would lose the gate. Assert the `kind` field is
        // PRESENT in the JSON and decodes back to SecondaryAffine.
        let task = mk_task(TaskKind::SecondaryAffine);
        let value = serde_json::to_value(&task).expect("serialize");
        assert_eq!(
            value["kind"], "SecondaryAffine",
            "SecondaryAffine kind must be serialized, not skipped"
        );
        let back: TaskInfo<String> = serde_json::from_value(value).expect("round-trip");
        assert_eq!(back.kind, TaskKind::SecondaryAffine);

        // The common `Work` case stays OFF the wire (skip_serializing_if),
        // so a legacy frame with no `kind` field still decodes to `Work`.
        let work = mk_task(TaskKind::Work);
        let work_value = serde_json::to_value(&work).expect("serialize work");
        assert!(
            work_value.get("kind").is_none(),
            "the default Work kind is skipped on the wire"
        );
        let legacy_back: TaskInfo<String> =
            serde_json::from_value(work_value).expect("legacy round-trip");
        assert_eq!(
            legacy_back.kind,
            TaskKind::Work,
            "a frame without a kind field decodes to Work"
        );
    }

    #[test]
    fn secondary_eager_prep_round_trips_on_the_wire() {
        // Like SecondaryAffine (and unlike the skipped default `Work`), a
        // SecondaryEagerPrep task MUST appear on the wire — dropping it would
        // lose the prep declaration. Assert the `kind` field is PRESENT in the
        // JSON and decodes back to SecondaryEagerPrep.
        let task = mk_task(TaskKind::SecondaryEagerPrep);
        let value = serde_json::to_value(&task).expect("serialize");
        assert_eq!(
            value["kind"], "SecondaryEagerPrep",
            "SecondaryEagerPrep kind must be serialized, not skipped"
        );
        let back: TaskInfo<String> = serde_json::from_value(value).expect("round-trip");
        assert_eq!(back.kind, TaskKind::SecondaryEagerPrep);
    }
}
