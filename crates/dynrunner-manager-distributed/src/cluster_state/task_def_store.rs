//! Frozen task-definition store: the content-addressed, replicated
//! registry of the IMMUTABLE core of every task's [`TaskInfo`].
//!
//! Single concern: WHERE a task's frozen definition lives and how a
//! content hash maps to a compact [`TaskDefId`]. A `TaskInfo` carries
//! both immutable identity (path, identifier, phase/type tags, payload,
//! dep edges, …) and a small mutable tail the runtime rewrites in place
//! (`preferred_secondaries`, `preferred_version`, `resolved_path`). This
//! store holds ONLY the frozen core, deduplicated by the same content
//! hash the task ledger keys on ([`compute_task_hash`]): two tasks that
//! hash equal share one [`Arc<FrozenTaskDef>`], and the small recurring
//! `Arc<str>` ids (phase/type) are interned so equal ids share one
//! allocation across the whole store.
//!
//! The store is REPLICATED state, like `tasks` — every node holds the
//! same set of frozen defs (a content-addressed registry converges by
//! construction: equal content yields equal hash yields the same id).
//! It is NOT folded into the anti-entropy digest: a def's content is
//! already implied by the `tasks` fold through the content-based join
//! key, so folding the index would double-count and diverge.
//!
//! The originate/apply wiring is LIVE: the broadcast choke point stamps a
//! primary-allocated `TaskDefId` onto every `TaskAdded`, and the apply path
//! interns at that wire id ([`TaskDefStore::intern_at`] /
//! `ClusterState::intern_task_def_at`), while dep edges resolve back to their
//! `(phase_id, task_id)` identity through [`TaskDefStore::resolve`] /
//! `resolve_dep_refs` on every replica. A handful of read surfaces
//! (`id_for_hash`, `cell_hash_for_id`) are not yet wired to a production
//! caller and carry `#[allow(dead_code)]` until their leaf lands — they are
//! real and tested, just not yet called outside `#[cfg(test)]`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use dynrunner_core::{
    AffinityId, PhaseId, SoftPreferredSecondaries, TaskDep, TaskInfo, TaskKind, TaskVersion,
    TypeId, UploadFileRef,
};
use serde::{Deserialize, Serialize};

/// Compact, monotonically-minted handle to a [`FrozenTaskDef`] in a
/// [`TaskDefStore`]. The numeric value is the def's index in the store's
/// dense `defs` vector, so resolution is an O(1) slot read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct TaskDefId(pub u32);

impl TaskDefId {
    /// Sentinel for an UN-interned [`FrozenTaskDef`] (the one produced by
    /// [`FrozenTaskDef::from_task_info`] before it reaches a store): the
    /// intern step ([`TaskDefStore::intern`] / [`TaskDefStore::intern_at`])
    /// STAMPS the real id over this before the def is stored, so a stored
    /// or observed def never carries it. `u32::MAX` is safe: the
    /// monotone allocator never mints it in any realistic run, and the
    /// bijection-enforced `intern_at` would reject it as an id-rebind if a
    /// wire ever carried it as a real slot.
    pub(crate) const UNBOUND: TaskDefId = TaskDefId(u32::MAX);
}

/// Compact, monotonically-minted handle for a PER-SECONDARY cell-bearing task
/// DEFINITION — a SIBLING id space to [`TaskDefId`], minted ONLY for defs that
/// need a per-secondary completion cell (today [`TaskKind::SecondaryAffine`] and
/// [`TaskKind::SecondaryEagerPrep`]) so the value stays DENSE (the per-secondary
/// cell bitvector indexes a cell by it; a sparse def-id space would waste two
/// bits per non-cell def). KIND-BLIND: both cell-bearing kinds share this one
/// dense id space. Allocated the SAME way as `TaskDefId` (primary-side
/// `alloc_for_cell_hash` reservation → wire-agreed `intern_cell_at`
/// placement, hash↔id bijection-enforced, failover resume floor), so it is
/// CRDT-agreed + failover-stable + snapshot-portable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct SecondaryCellId(pub u32);

/// One dep-graph edge on a [`FrozenTaskDef`] (L5): the compact
/// `TaskDefId` of the PREREQUISITE task's def, plus the per-edge
/// `inherit_outputs` opt-in carried verbatim from the source
/// [`dynrunner_core::TaskDep`].
///
/// The COMPACT replacement for the string-identity [`dynrunner_core::TaskDep`]
/// on the frozen core: a `TaskDep` carries `task_id: String` + `phase_id:
/// PhaseId` (≈ 2 heap allocations per edge), whereas a `TaskDepRef` is a
/// `u32` + a `bool`. The string `(phase_id, task_id)` identity the dep
/// CONSUMERS key by (the dispatch wire, the secondary affine gate, the
/// predecessor-outputs walk) is rebuilt on demand from the prereq's def via
/// [`TaskDefStore::resolve`] — sound post-#603/L6a, where a def_id is
/// globally-numerically-stable AND snapshot-portable, so `resolve(def_id)`
/// works on every replica across snapshot / restore / failover.
///
/// `inherit_outputs` is NOT dropped (the bare-`u32` shape would be lossy —
/// it is the per-edge flag that drives the transitive-ancestor output walk
/// in `predecessor_outputs`): it rides on every ref so the rebuilt
/// `TaskDep` reproduces the source edge faithfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TaskDepRef {
    /// The prerequisite task's def-store id. Resolves to the prereq def via
    /// [`TaskDefStore::resolve`] on every replica (the #603/L6a portability
    /// guarantee), from which the `(phase_id, task_id)` identity the dep
    /// consumers key by is rebuilt.
    pub(crate) def_id: TaskDefId,
    /// The per-edge transitive-ancestor output opt-in, carried verbatim
    /// from the source [`dynrunner_core::TaskDep::inherit_outputs`] so the
    /// rebuilt dep reproduces the edge faithfully (CL-A3 — the ref is not
    /// lossy).
    pub(crate) inherit_outputs: bool,
}

/// The FROZEN core of a [`TaskInfo`]: the 13 immutable fields that make
/// up a task's identity + dispatch recipe, EXCLUDING the 3 mutable tail
/// fields the runtime rewrites in place (`preferred_secondaries`,
/// `preferred_version`, `resolved_path`).
///
/// Generic over the identifier type `I` for the same reason `TaskInfo`
/// is. The serde bound mirrors `TaskInfo`'s so the def round-trips on a
/// future def-transfer wire.
///
/// SELF-DESCRIBING id: the `def_id` field carries this def's store id so
/// the inline serialization (a `TaskState` ships its `Arc<FrozenTaskDef>`
/// by value in the snapshot) PERSISTS the assigned id. A restoring replica
/// rebuilds the store's id↔def + hash↔id maps from the carried id
/// ([`TaskDefStore::intern_at`]) — the snapshot ships no separate def-store
/// wire field. The id is STAMPED at intern time (the single slot-write both
/// fill paths share), never set by the un-interned [`Self::from_task_info`]
/// splitter; a pre-intern value is a sentinel ([`TaskDefId::UNBOUND`]) the
/// intern step always overwrites before the def is stored or observed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
pub struct FrozenTaskDef<I> {
    /// This def's store id, STAMPED at intern time so the inline
    /// serialization is self-describing (see the type doc). [`TaskDefId::UNBOUND`]
    /// on an un-interned def produced by [`Self::from_task_info`]; the
    /// intern step overwrites it before storage, so a stored/observed def
    /// always carries its real id. `pub(crate)` (not `pub` like the content
    /// fields): the id is a crate-internal store handle, while the content
    /// fields are `pub` for the def-transfer wire — and [`TaskDefId`] is
    /// itself crate-private, so a `pub` field would leak a private type.
    pub(crate) def_id: TaskDefId,
    /// This def's affine-id IFF it is a [`TaskKind::SecondaryAffine`] def —
    /// `None` for every ordinary (Work/Setup) def. STAMPED at the wire-agreed
    /// affine intern ([`TaskDefStore::intern_cell_at`]) so the inline
    /// serialization is self-describing exactly like `def_id`: a restoring
    /// replica re-anchors the affine-id↔hash binding from the carried value
    /// ([`super::ClusterState::register_restored_def`]), so the affine bitvector
    /// cell index survives snapshot/restore/failover with no separate wire
    /// field. `None` on an un-interned def (the [`Self::from_task_info`]
    /// splitter has no store to allocate against) and on every non-affine def.
    pub(crate) affine_id: Option<SecondaryCellId>,
    pub path: PathBuf,
    pub size: u64,
    pub identifier: I,
    pub phase_id: PhaseId,
    pub type_id: TypeId,
    pub kind: TaskKind,
    pub setup_affinity: Option<String>,
    pub upload_file: Option<Box<UploadFileRef>>,
    pub required_files: Option<Box<[UploadFileRef]>>,
    pub affinity_id: Option<AffinityId>,
    pub payload: serde_json::Value,
    pub task_id: String,
    /// Dep-graph edges as COMPACT def-id refs (L5), NOT the string-identity
    /// [`TaskDep`]. Each ref names the prerequisite's stable def_id +
    /// the per-edge `inherit_outputs`; the `(phase_id, task_id)` identity
    /// the consumers key by is rebuilt on demand via the def store
    /// ([`super::ClusterState::resolve_dep_refs`] / the read seams). Filled
    /// at intern from the incoming [`TaskInfo::task_depends_on`] — the
    /// un-interned [`Self::from_task_info`] splitter carves the string deps
    /// OUT (it has no store to resolve against), and the intern step
    /// resolves them in. So a def produced by `from_task_info` carries an
    /// EMPTY `task_depends_on` until the store fills it; a stored/observed
    /// def always carries its resolved refs.
    pub(crate) task_depends_on: Vec<TaskDepRef>,
}

impl<I> FrozenTaskDef<I> {
    /// Split a [`TaskInfo`] into its frozen core + the 3 mutable tail
    /// values the runtime owns + the string-identity dep list (L5). The
    /// destructure names EVERY `TaskInfo` field with NO `..` rest, so a
    /// future `TaskInfo` field is a COMPILE ERROR here until the developer
    /// classifies it frozen-vs-mutable.
    ///
    /// The frozen core's `task_depends_on` (now `Vec<TaskDepRef>`) is left
    /// EMPTY: the splitter has no store to resolve `(phase_id, task_id)` →
    /// `TaskDefId` against, so it carves the string `Vec<TaskDep>` OUT as a
    /// fourth return value. The intern step ([`super::ClusterState`]'s
    /// `intern_task_def*`) resolves them into the def's refs against its
    /// store. Mirrors how the mutable tail is carved out — the splitter
    /// owns the structural split, the store owns the dep resolution.
    pub(crate) fn from_task_info(
        t: TaskInfo<I>,
    ) -> (
        FrozenTaskDef<I>,
        SoftPreferredSecondaries,
        TaskVersion,
        Option<PathBuf>,
        Vec<TaskDep>,
    ) {
        let TaskInfo {
            path,
            size,
            identifier,
            phase_id,
            type_id,
            kind,
            setup_affinity,
            upload_file,
            required_files,
            affinity_id,
            payload,
            task_id,
            task_depends_on,
            // ── mutable tail: returned separately, NOT part of the frozen core ──
            preferred_secondaries,
            preferred_version,
            resolved_path,
        } = t;
        (
            FrozenTaskDef {
                // UN-interned: the intern step stamps the real id over this
                // sentinel before the def is stored or observed.
                def_id: TaskDefId::UNBOUND,
                // Un-allocated: the affine intern step stamps the real
                // affine-id (only for a SecondaryAffine def) before storage.
                affine_id: None,
                path,
                size,
                identifier,
                phase_id,
                type_id,
                kind,
                setup_affinity,
                upload_file,
                required_files,
                affinity_id,
                payload,
                task_id,
                // EMPTY: the store resolves the carved-out string deps into
                // refs at intern (the splitter has no store to resolve
                // against).
                task_depends_on: Vec::new(),
            },
            preferred_secondaries,
            preferred_version,
            resolved_path,
            task_depends_on,
        )
    }

    /// Reconstruct a whole owned [`TaskInfo`] from this frozen core (its 13
    /// immutable fields, cloned) + a [`TaskRouting`] tail (the 3 mutable
    /// fields) + the ALREADY-RESOLVED string deps. The inverse of
    /// [`Self::from_task_info`] and the SINGLE place the 16-field rebuild
    /// lives — both `TaskState::to_task_info` and the affine-gate resolver
    /// delegate here so no caller re-spells it. A TRANSIENT allocation: only
    /// for callers that genuinely need a whole owned `TaskInfo` (a wire
    /// `TaskAssignment`, a pool insert).
    ///
    /// `deps` is the `Vec<TaskDep>` the def store rebuilds from this def's
    /// `task_depends_on: Vec<TaskDepRef>` ([`super::ClusterState::resolve_dep_refs`]):
    /// the rebuild needs the store (a ref → its prereq's `(phase_id,
    /// task_id)`), which a `&FrozenTaskDef` does not hold, so the resolved
    /// list is passed IN. The store-owning `TaskState::to_task_info` does
    /// the resolution at the seam where it holds the store.
    pub(crate) fn to_task_info(
        &self,
        routing: &super::types::TaskRouting,
        deps: Vec<TaskDep>,
    ) -> TaskInfo<I>
    where
        I: Clone,
    {
        TaskInfo {
            path: self.path.clone(),
            size: self.size,
            identifier: self.identifier.clone(),
            phase_id: self.phase_id.clone(),
            type_id: self.type_id.clone(),
            kind: self.kind,
            setup_affinity: self.setup_affinity.clone(),
            upload_file: self.upload_file.clone(),
            required_files: self.required_files.clone(),
            affinity_id: self.affinity_id.clone(),
            payload: self.payload.clone(),
            task_id: self.task_id.clone(),
            task_depends_on: deps,
            preferred_secondaries: routing.preferred_secondaries.clone(),
            preferred_version: routing.preferred_version,
            resolved_path: routing.resolved_path.clone(),
        }
    }
}

/// Split a whole owned [`TaskInfo`] into a STANDALONE shared `def` (a fresh
/// `Arc`, NOT interned in any store) + its [`TaskRouting`] tail. The
/// un-interned sibling of [`super::ClusterState::intern_task_def`]: for
/// callers that build a `TaskState` WITHOUT a store to intern into (the
/// cluster_state unit tests construct states directly). Production
/// construction routes through `intern_task_def` so equal defs dedup; this
/// helper exists only where there is no store to dedup against — today that
/// is exclusively the unit tests, so it is `#[cfg(test)]`.
#[cfg(test)]
pub(crate) fn split_task_def<I>(task: TaskInfo<I>) -> (Arc<FrozenTaskDef<I>>, super::types::TaskRouting) {
    let (mut frozen, preferred_secondaries, preferred_version, resolved_path, deps) =
        FrozenTaskDef::from_task_info(task);
    // No store to resolve `(phase_id, task_id)` against (this is the
    // store-less unit-test splitter): carry the deps' originator-stamped
    // `def_id` when present, else the UNBOUND sentinel. The tests that need
    // resolvable deps route through a real `ClusterState` store instead.
    frozen.task_depends_on = deps
        .iter()
        .map(|dep| TaskDepRef {
            def_id: dep.def_id.map(TaskDefId).unwrap_or(TaskDefId::UNBOUND),
            inherit_outputs: dep.inherit_outputs,
        })
        .collect();
    (
        Arc::new(frozen),
        super::types::TaskRouting {
            preferred_secondaries,
            preferred_version,
            resolved_path,
        },
    )
}

/// The replicated frozen-def registry: a dense def vector indexed by
/// [`TaskDefId`], a content-hash → id map, and an `Arc<str>` intern pool
/// that collapses equal phase/type ids to one allocation across the
/// whole store.
///
/// REPLICATED state (like `tasks`): a full clone carries every map (the
/// `Arc` clones are cheap). The hand-rolled `Default` / `Clone` impls
/// (rather than derives) keep both free of an `I: Default` / `I: Clone`
/// bound — `Vec`/`HashMap` construction and `Arc::clone` need neither, so
/// the store stays usable for every `I` the generic `ClusterState<I>`
/// `Default` / bounded `Clone` impls require.
pub(crate) struct TaskDefStore<I> {
    /// Slot = `TaskDefId.0`. Each occupied entry is shared (`Arc`) so
    /// resolving a def hands out a cheap clone. `Option` slots make the
    /// vector SPARSE-tolerant: a primary-allocated wire id is placed at
    /// its EXACT slot ([`Self::intern_at`]) even when an earlier id has
    /// not been observed yet (out-of-order `TaskAdded` delivery), so a
    /// gap is a not-yet-seen def, NOT a mis-placement. Resolution stays
    /// the O(1) slot read it was when the vector was dense.
    defs: Vec<Option<Arc<FrozenTaskDef<I>>>>,
    /// Content hash ([`compute_task_hash`]) → the def's id. The dedup
    /// gate AND one half of the hash↔id BIJECTION: a re-intern of an
    /// already-known hash reuses its existing id and mints nothing.
    hash_to_id: HashMap<String, TaskDefId>,
    /// The next id this store's NODE-LOCAL allocator
    /// ([`Self::alloc_for_hash`]) would mint. Distinct from `defs.len()`
    /// (the prior dense-position allocator): a sparse [`Self::intern_at`]
    /// of a wire-carried id may leave gaps below `next_id`, and a promoted
    /// primary's [`Self::resume_alloc_floor`] re-anchors this PAST every
    /// id it has observed so it never re-mints a live id on failover (the
    /// epoch-/failover-safety the wire-agreed id requires — a node-local
    /// cold-start counter would alias).
    next_id: u32,
    /// `Arc<str>` intern pool: maps an id string to its canonical `Arc`,
    /// so equal phase/type ids across distinct defs share one allocation.
    /// Keyed and valued by the same `Arc<str>` (a get-or-insert returns
    /// the canonical clone).
    str_intern: HashMap<Arc<str>, Arc<str>>,
    /// `(phase_id, task_id)` IDENTITY → the def's id (L5). The reverse of
    /// `resolve(def_id) → (phase_id, task_id)`: the FALLBACK a dep
    /// resolution uses when the incoming [`TaskDep`] carries no
    /// originator-stamped `def_id` (a node-local / direct-apply dep — the
    /// L2 by-content path). Populated at [`Self::put_slot`] from the def's
    /// own `(phase_id, task_id)`, so a prereq's identity is resolvable the
    /// moment its def is interned. A new def for an already-present identity
    /// (a re-intern under the same hash NoOps before reaching `put_slot`, so
    /// this only ever observes the first placement per identity) keeps the
    /// first binding.
    identity_to_id: HashMap<(PhaseId, String), TaskDefId>,
    /// `task_id` (PHASE-LESS) → the def's id (L5). The phaseless dep-
    /// resolution fallback, mirroring `PendingPool::extend`'s
    /// `known_ids_phaseless`: a dep whose stored `phase_id` does NOT match
    /// the prereq's real phase (the common case for a bare-string
    /// cross-phase dep, which the consumer boundary resolves to the
    /// ENCLOSING phase, not the prereq's) still resolves by task_id alone —
    /// exactly the tolerance the pre-L5 string-identity path had. First
    /// binding wins; the phased `identity_to_id` is consulted FIRST so an
    /// exact match always dominates a phaseless one.
    task_id_to_id: HashMap<String, TaskDefId>,
    /// Content hash → the def's COMMITTED AFFINE-id, populated ONLY for
    /// [`TaskKind::SecondaryAffine`] defs whose `SecondaryCellRegistered`
    /// has been APPLIED (the affine analogue of `hash_to_id`): the dedup gate
    /// AND one half of the hash↔affine_id BIJECTION the wire-agreed
    /// [`Self::intern_cell_at`] enforces, so every replica converges on the
    /// SAME dense affine-id for an affine def's content. A non-affine def is
    /// never recorded here, keeping the affine-id space dense for the
    /// per-secondary bitvector index. The COMMITTED binding the AF-sched query
    /// [`Self::cell_id_for_hash`] reads — distinct from the primary's
    /// pre-broadcast `cell_id_reserved` below (the affine twin of how a def's
    /// `hash_to_id` reservation is distinct from its placed def slot, so the
    /// originator's own registration apply still does real work / is Applied).
    cell_hash_to_id: HashMap<String, SecondaryCellId>,
    /// PRIMARY-side pre-broadcast affine-id RESERVATIONS — `hash → affine_id`
    /// minted by [`Self::alloc_for_cell_hash`] at the broadcast stamp step
    /// BEFORE the matching `SecondaryCellRegistered` is applied. Keeps the
    /// stamp pass's id allocation idempotent on hash (a re-stamp reuses the
    /// reservation) WITHOUT pre-committing the binding, so the originator's own
    /// apply of its `SecondaryCellRegistered` is the FIRST commit (Applied,
    /// hence broadcast) rather than a NoOp the wire-filter would drop — the
    /// affine twin of `alloc_for_hash`-reserves / `intern_at`-fills.
    cell_id_reserved: HashMap<String, SecondaryCellId>,
    /// AFFINE-id → the content hash it is bound to — the reverse of
    /// `cell_hash_to_id`, the half of the bijection [`Self::intern_cell_at`]
    /// consults to detect an id rebind (a new hash claiming an already-bound
    /// affine slot). Also the seam AF-sched reads to map a bitvector cell
    /// index back to its affine def.
    cell_id_to_hash: HashMap<SecondaryCellId, String>,
    /// The next affine-id this store's allocator would mint — the affine
    /// twin of `next_id`, re-anchored PAST every observed affine-id by
    /// [`Self::resume_cell_alloc_floor`] on failover so a promoted primary
    /// never re-mints a live affine-id (the same epoch-/failover-safety the
    /// def-id allocator has; a node-local cold counter would alias).
    next_cell_id: u32,
}

/// A hash↔id BIJECTION violation observed by [`TaskDefStore::intern_at`]:
/// the wire-carried `(def_id, hash)` pair contradicts a binding the store
/// already holds. A converged content-addressed registry NEVER produces
/// one (equal content ⇒ equal hash ⇒ the same id on every node); it is the
/// loud signal of a genuine fault — two primaries minting different ids for
/// one hash, or an id re-used for a second hash (the failover-aliasing the
/// epoch-safe allocator exists to prevent). The apply rule logs it and
/// drops the mutation (NoOp), debug-asserting in a debug build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DefBijectionError {
    /// `hash` is already bound to `existing` but the wire carried a
    /// DIFFERENT `wire` id for it.
    HashRebound {
        hash: String,
        existing: TaskDefId,
        wire: TaskDefId,
    },
    /// The wire `id` slot is already occupied by a def for a DIFFERENT
    /// hash than the incoming one.
    IdRebound { id: TaskDefId },
}

impl<I> Default for TaskDefStore<I> {
    fn default() -> Self {
        Self {
            defs: Vec::new(),
            hash_to_id: HashMap::new(),
            next_id: 0,
            str_intern: HashMap::new(),
            identity_to_id: HashMap::new(),
            task_id_to_id: HashMap::new(),
            cell_hash_to_id: HashMap::new(),
            cell_id_reserved: HashMap::new(),
            cell_id_to_hash: HashMap::new(),
            next_cell_id: 0,
        }
    }
}

impl<I> Clone for TaskDefStore<I> {
    fn clone(&self) -> Self {
        Self {
            defs: self.defs.clone(),
            hash_to_id: self.hash_to_id.clone(),
            next_id: self.next_id,
            str_intern: self.str_intern.clone(),
            identity_to_id: self.identity_to_id.clone(),
            task_id_to_id: self.task_id_to_id.clone(),
            cell_hash_to_id: self.cell_hash_to_id.clone(),
            cell_id_reserved: self.cell_id_reserved.clone(),
            cell_id_to_hash: self.cell_id_to_hash.clone(),
            next_cell_id: self.next_cell_id,
        }
    }
}

impl<I> std::fmt::Debug for TaskDefStore<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskDefStore")
            .field("defs", &self.defs.len())
            .field("hash_to_id", &self.hash_to_id.len())
            .field("next_id", &self.next_id)
            .field("str_intern", &self.str_intern.len())
            .field("cell_hash_to_id", &self.cell_hash_to_id.len())
            .field("next_cell_id", &self.next_cell_id)
            .finish()
    }
}

impl<I> TaskDefStore<I> {
    /// Canonical `Arc<str>` for `s` from the intern pool: returns the
    /// stored clone if `s` is already interned, else inserts `s`'s `Arc`
    /// mapping to itself and returns it. So two equal ids end up sharing
    /// ONE allocation regardless of which def introduced it first.
    fn intern_str(&mut self, s: &str) -> Arc<str> {
        let probe: Arc<str> = Arc::from(s);
        self.str_intern
            .entry(Arc::clone(&probe))
            .or_insert(probe)
            .clone()
    }

    /// Fold a frozen def's recurring `Arc<str>` ids (phase/type only —
    /// `identifier: I` is opaque and may not be `Arc<str>`-backed) onto the
    /// canonical intern-pool allocations, in place. The single str-intern
    /// site both the node-local [`Self::intern`] and the wire-id
    /// [`Self::intern_at`] route a stored def through.
    fn canonicalize_strs(&mut self, frozen: &mut FrozenTaskDef<I>) {
        let phase = self.intern_str(frozen.phase_id.as_str());
        frozen.phase_id = PhaseId::new(phase);
        let ty = self.intern_str(frozen.type_id.as_str());
        frozen.type_id = TypeId::new(ty);
    }

    /// Place `frozen` into the `defs` slot at `id`, growing the sparse
    /// vector with empty slots as needed. The single slot-write the two
    /// fill paths share; the caller owns the bijection/dedup decision.
    ///
    /// Does NOT stamp the self-describing `def_id`: only the WIRE-AGREED
    /// [`Self::intern_at`] path stamps it (a primary-allocated, CRDT-agreed
    /// id is portable), while the node-local [`Self::intern`] fallback leaves
    /// the [`TaskDefId::UNBOUND`] sentinel — a node-local id is intra-node
    /// only and legitimately differs across replicas, so persisting it would
    /// make a restoring replica assert a binding that was never agreed
    /// (a spurious bijection conflict). See [`Self::intern_at`].
    fn put_slot(&mut self, id: TaskDefId, frozen: Arc<FrozenTaskDef<I>>) {
        let idx = id.0 as usize;
        // Register the `(phase_id, task_id)` → id identity reverse-index
        // (L5) so a later dep with no originator-stamped def_id resolves to
        // this prereq by identity. Keep the FIRST binding for an identity
        // (a re-intern under the same hash NoOps before reaching here, so a
        // second placement for one identity only arises across a genuine
        // content/identity collision — the existing binding is authoritative).
        self.identity_to_id
            .entry((frozen.phase_id.clone(), frozen.task_id.clone()))
            .or_insert(id);
        self.task_id_to_id
            .entry(frozen.task_id.clone())
            .or_insert(id);
        if idx >= self.defs.len() {
            self.defs.resize(idx + 1, None);
        }
        self.defs[idx] = Some(frozen);
        // Keep the allocator strictly above every observed id so a later
        // node-local mint never collides with a wire-placed id.
        self.next_id = self.next_id.max(id.0 + 1);
    }

    /// The def id for a `(phase_id, task_id)` IDENTITY, if a def with that
    /// identity has been interned. The L5 dep-resolution fallback for an
    /// incoming [`TaskDep`] that carries no originator-stamped def_id.
    ///
    /// Tries the EXACT `(phase_id, task_id)` first, then falls back to a
    /// PHASE-LESS `task_id` match — mirroring `PendingPool::extend`'s
    /// `known_ids_phaseless` tolerance, so a bare-string cross-phase dep
    /// (resolved to the ENCLOSING phase at the consumer boundary, which need
    /// not be the prereq's phase) still resolves by task_id, exactly as it
    /// did pre-L5 when the string identity was stored verbatim. The exact
    /// match dominates so a genuine same-task_id-in-two-phases dep with the
    /// right phase always wins.
    fn id_for_identity(&self, phase_id: &PhaseId, task_id: &str) -> Option<TaskDefId> {
        // Borrow-free lookup keyed by the owned tuple shape.
        self.identity_to_id
            .get(&(phase_id.clone(), task_id.to_string()))
            .copied()
            .or_else(|| self.task_id_to_id.get(task_id).copied())
    }

    /// NODE-LOCAL allocate-and-intern: the un-agreed L2 fallback used when
    /// no primary-allocated id rides the wire (a `def_id: None` `TaskAdded`,
    /// the in-process direct-apply / unit-test path). If the hash is already
    /// known, returns the existing id and mints NOTHING (the
    /// content-addressed dedup gate). Otherwise mints the next node-local
    /// id, canonicalizes the def's `Arc<str>` ids, and records it.
    ///
    /// The def's `task_depends_on` is taken AS-IS (already the resolved
    /// [`TaskDepRef`] list): the caller — `intern_task_def`'s apply path
    /// (resolving string deps via [`Self::dep_refs_from_deps`]) or
    /// `register_restored_def`'s restore path (the refs already decoded
    /// inline) — owns dep resolution, so the store's place-step stays a
    /// single concern.
    pub(crate) fn intern(&mut self, hash: String, frozen: FrozenTaskDef<I>) -> TaskDefId {
        self.intern_reporting_placement(hash, frozen).0
    }

    /// As [`Self::intern`], but also reports whether this call NEWLY PLACED
    /// the def (`true`) or hit the content-addressed dedup gate and minted
    /// nothing (`false`). The L5 two-step intern reads the flag so it only
    /// fills dep refs on a fresh placement — never re-writing (and thus
    /// never `Arc::make_mut`-forking) an already-resolved shared def.
    fn intern_reporting_placement(
        &mut self,
        hash: String,
        mut frozen: FrozenTaskDef<I>,
    ) -> (TaskDefId, bool) {
        if let Some(&existing) = self.hash_to_id.get(&hash) {
            return (existing, false);
        }
        self.canonicalize_strs(&mut frozen);
        let id = self.alloc();
        self.put_slot(id, Arc::new(frozen));
        self.hash_to_id.insert(hash, id);
        (id, true)
    }

    /// Mint the next node-local [`TaskDefId`] from the epoch-safe `next_id`
    /// allocator (NOT `defs.len()` — a sparse `intern_at` can leave gaps).
    fn alloc(&mut self) -> TaskDefId {
        let id = TaskDefId(self.next_id);
        self.next_id += 1;
        id
    }

    /// PRIMARY-side id allocation for `hash` at the broadcast STAMP step,
    /// idempotent on hash: returns the existing id if `hash` is already
    /// bound (a re-added hash reuses its def id — the bijection), else
    /// reserves the next allocator id for it WITHOUT yet placing a def
    /// (the def slot is filled by the matching [`Self::intern_at`] when the
    /// stamped `TaskAdded` is applied). The reservation records the
    /// hash→id binding so the originator's own apply observes it as the
    /// idempotent fill case.
    pub(crate) fn alloc_for_hash(&mut self, hash: &str) -> TaskDefId {
        if let Some(&existing) = self.hash_to_id.get(hash) {
            return existing;
        }
        let id = self.alloc();
        self.hash_to_id.insert(hash.to_string(), id);
        id
    }

    /// RECEIVE-side wire-id intern: place the wire-carried def at EXACTLY
    /// `id`, enforcing the hash↔id BIJECTION so every replica converges on
    /// the same id for a hash. Returns the (possibly already-bound) id on
    /// success, or a [`DefBijectionError`] on a contradiction:
    ///
    ///   * hash already bound to a DIFFERENT id than `id` → `HashRebound`;
    ///   * hash NEW but `id`'s slot already holds a def for another hash →
    ///     `IdRebound`.
    ///
    /// The idempotent cases are NOT errors: a re-add of a hash already
    /// bound to `id` reuses it (and fills the slot if a prior
    /// [`Self::alloc_for_hash`] reservation left it empty), exactly as the
    /// node-local [`Self::intern`] re-add mints nothing.
    ///
    /// SELF-DESCRIBING: this is the WIRE-AGREED intern path, so it STAMPS the
    /// established id onto the def's `def_id` before storing it — a
    /// primary-allocated, CRDT-agreed id IS portable, so persisting it inline
    /// lets a restoring replica re-anchor the def at the SAME id. (The
    /// node-local [`Self::intern`] fallback deliberately does NOT stamp: a
    /// node-local id is intra-node only.)
    ///
    /// The def's `task_depends_on` is taken AS-IS (already the resolved
    /// [`TaskDepRef`] list): the caller owns dep resolution (see
    /// [`Self::intern`]).
    pub(crate) fn intern_at(
        &mut self,
        id: TaskDefId,
        hash: String,
        mut frozen: FrozenTaskDef<I>,
    ) -> Result<TaskDefId, DefBijectionError> {
        if let Some(&existing) = self.hash_to_id.get(&hash) {
            if existing != id {
                return Err(DefBijectionError::HashRebound {
                    hash,
                    existing,
                    wire: id,
                });
            }
            // Idempotent re-add (or the originator's own apply after its
            // `alloc_for_hash` reservation): ensure the slot is filled, then
            // return the established id. A re-add against an already-placed
            // slot mints nothing.
            if self.resolve(existing).is_none() {
                self.canonicalize_strs(&mut frozen);
                frozen.def_id = existing;
                self.put_slot(existing, Arc::new(frozen));
            }
            return Ok(existing);
        }
        // Hash is NEW: the slot must be free (a new hash claiming an
        // occupied slot is the id-rebind fault — the slot is bound to a
        // hash other than this one, since this hash is not in `hash_to_id`).
        if self.resolve(id).is_some() {
            return Err(DefBijectionError::IdRebound { id });
        }
        self.canonicalize_strs(&mut frozen);
        frozen.def_id = id;
        self.put_slot(id, Arc::new(frozen));
        self.hash_to_id.insert(hash, id);
        Ok(id)
    }

    /// Resolve an id to its shared frozen def. `None` for an id this store
    /// never placed — either never minted, or a sparse gap below `next_id`
    /// for a wire id whose `TaskAdded` has not arrived yet.
    pub(crate) fn resolve(&self, id: TaskDefId) -> Option<&Arc<FrozenTaskDef<I>>> {
        self.defs.get(id.0 as usize).and_then(|slot| slot.as_ref())
    }

    /// Fill the dep refs of an ALREADY-PLACED def (L5) — the second step of
    /// the two-step intern the apply path uses: place the def FIRST (so its
    /// own `(phase_id, task_id)` identity is registered), THEN resolve its
    /// deps and write them here. This makes a SELF-referential dep resolve to
    /// the def's own id rather than the UNBOUND sentinel, and keeps the
    /// resolution consulting the def's own just-registered identity. A no-op
    /// if the slot is unexpectedly empty (the caller just placed it, so this
    /// is defensive). `Arc::make_mut` copy-on-writes; right after placement
    /// the store holds the sole strong ref, so it mutates in place.
    fn fill_dep_refs(&mut self, id: TaskDefId, refs: Vec<TaskDepRef>)
    where
        I: Clone,
    {
        if let Some(Some(arc)) = self.defs.get_mut(id.0 as usize) {
            Arc::make_mut(arc).task_depends_on = refs;
        }
    }

    /// Translate a string-identity [`TaskDep`] list into the compact
    /// [`TaskDepRef`] list a [`FrozenTaskDef`] stores (L5) — the
    /// intern-side conversion. For each edge the prereq's def id is taken
    /// from the originator-stamped `dep.def_id` when present (the
    /// production replicated path — forward-ref-safe, the originator
    /// resolved over the whole batch); else from the `(phase_id, task_id)`
    /// identity reverse-index (the node-local / direct-apply fallback). An
    /// edge that resolves to NEITHER is preserved with the
    /// [`TaskDefId::UNBOUND`] sentinel rather than dropped: the
    /// loud-unknown-dep failure is the SCHEDULER's concern
    /// (`PendingPool::extend` / spawn validation over the string deps) — the
    /// def-store layer is additive and never silently mutates the dep SET,
    /// so an unresolvable ref round-trips back to an unresolvable
    /// `(phase_id, task_id)` at read and still fails loud there. The
    /// `inherit_outputs` flag rides every ref (CL-A3 — not lossy).
    fn dep_refs_from_deps(&self, deps: &[TaskDep]) -> Vec<TaskDepRef> {
        deps.iter()
            .map(|dep| TaskDepRef {
                def_id: dep
                    .def_id
                    .map(TaskDefId)
                    .or_else(|| self.id_for_identity(&dep.phase_id, &dep.task_id))
                    .unwrap_or(TaskDefId::UNBOUND),
                inherit_outputs: dep.inherit_outputs,
            })
            .collect()
    }

    /// Rebuild the string-identity [`TaskDep`] list from a
    /// [`FrozenTaskDef`]'s compact [`TaskDepRef`] list (L5) — the read-side
    /// conversion every frozen-def dep CONSUMER (the dispatch `to_task_info`,
    /// `task_deps_for_identity`, the affine gate, the settled-spill capture)
    /// routes through. Each ref's `def_id` resolves to its prereq def, whose
    /// `(phase_id, task_id)` becomes the rebuilt edge; `inherit_outputs`
    /// rides verbatim. A ref that resolves to no def (an
    /// [`TaskDefId::UNBOUND`] sentinel, or a not-yet-observed wire id)
    /// rebuilds an edge whose `(phase_id, task_id)` is the EMPTY-phase
    /// migration sentinel — it carries no false identity, and the
    /// downstream loud-unknown-dep failure (the scheduler / the dispatch
    /// gate) surfaces it exactly as a missing string dep would. The rebuilt
    /// `TaskDep` carries `def_id: None` (the wire re-stamps it at the next
    /// origination if needed). Owned: callers need a whole list.
    /// Rebuild the string-identity [`TaskDep`] list from compact
    /// [`TaskDepRef`]s (L5). A `fallback` resolves a ref whose def has LEFT
    /// the in-memory store (a settled + evicted prereq, whose id no longer
    /// resolves here): the in-memory store is settled-blind, so the
    /// settled-vs-in-memory split stays a CALLER concern (the
    /// [`ClusterState`]-level wrapper passes the settled reverse index) without
    /// duplicating the per-ref iteration. A caller with no settled half passes
    /// `|_| None`.
    pub(crate) fn resolve_dep_refs_with_fallback(
        &self,
        refs: &[TaskDepRef],
        fallback: impl Fn(TaskDefId) -> Option<(PhaseId, String)>,
    ) -> Vec<TaskDep> {
        refs.iter()
            .map(|r| {
                // In-memory store first, then the caller's fallback (settled).
                let resolved = self
                    .resolve(r.def_id)
                    .map(|def| (def.phase_id.clone(), def.task_id.clone()))
                    .or_else(|| fallback(r.def_id));
                match resolved {
                    Some((phase_id, task_id)) => TaskDep {
                        task_id,
                        phase_id,
                        inherit_outputs: r.inherit_outputs,
                        def_id: None,
                    },
                    None => TaskDep {
                        // No resolvable prereq: rebuild the migration-sentinel
                        // shape (empty phase, empty id) so the edge carries no
                        // false identity and the loud-unknown-dep failure fires
                        // downstream, exactly as a missing string dep would.
                        task_id: String::new(),
                        phase_id: PhaseId::default(),
                        inherit_outputs: r.inherit_outputs,
                        def_id: None,
                    },
                }
            })
            .collect()
    }

    /// The id a content `hash` resolves to, if this store has bound it.
    #[allow(dead_code)]
    pub(crate) fn id_for_hash(&self, hash: &str) -> Option<TaskDefId> {
        self.hash_to_id.get(hash).copied()
    }

    /// The def id for a `(phase_id, task_id)` IDENTITY, if a def with that
    /// identity has been interned — the public read of the L5 reverse-index,
    /// used by the originator's dep-stamp pass to resolve a dep already in a
    /// PRIOR batch (the in-batch forward-refs come from the batch-local map
    /// the stamp pass builds).
    pub(crate) fn id_for_identity_pub(&self, phase_id: &PhaseId, task_id: &str) -> Option<u32> {
        self.id_for_identity(phase_id, task_id).map(|id| id.0)
    }

    /// The next id the node-local allocator would mint — the failover-resume
    /// floor a promoted primary re-anchors against so it never re-mints a
    /// live id (see [`Self::resume_alloc_floor`]).
    pub(crate) fn next_id_floor(&self) -> u32 {
        self.next_id
    }

    /// Re-anchor the node-local allocator so the next minted id is at least
    /// `floor` — the failover-safety seam a promoted primary fires so it
    /// resumes PAST every replicated def id rather than from a cold counter
    /// (the aliasing CL-A2 forbids). Monotone: never lowers `next_id`. The
    /// caller supplies `max(observed id) + 1`; here the in-memory store's
    /// own max already feeds `next_id` (every `put_slot` advances it). This
    /// method is settled-BLIND by design — the full settled-scan over spilled
    /// entries is composed by its `ClusterState` wrapper
    /// [`super::ClusterState::resume_def_alloc_floor`], which folds
    /// `next_id_floor()` together with the settled base's `max_def_id` before
    /// calling here (L6a).
    pub(crate) fn resume_alloc_floor(&mut self, floor: u32) {
        self.next_id = self.next_id.max(floor);
    }

    // ── AFFINE-id allocation (sibling id space to TaskDefId) ──
    //
    // Mints a DENSE u32 affine-id per SecondaryAffine def, the SAME way the
    // def-id machinery above mints `TaskDefId`: a primary-side
    // `alloc_for_cell_hash` reservation, a wire-agreed `intern_cell_at`
    // placement under hash↔id bijection, and a `resume_cell_alloc_floor`
    // failover re-anchor. Only the SecondaryAffine subset is recorded, so the
    // value stays dense for the per-secondary bitvector index.

    /// Mint the next affine-id from the epoch-safe `next_cell_id` allocator.
    fn alloc_cell_id(&mut self) -> SecondaryCellId {
        let id = SecondaryCellId(self.next_cell_id);
        self.next_cell_id += 1;
        id
    }

    /// PRIMARY-side affine-id RESERVATION for an affine def's content `hash`,
    /// idempotent on hash: returns the COMMITTED id if already bound, else the
    /// existing RESERVATION, else mints a fresh one into `cell_id_reserved`. The
    /// affine twin of [`Self::alloc_for_hash`] — the broadcast stamp step calls
    /// it for a SecondaryAffine `TaskAdded` so the wire `SecondaryCellRegistered`
    /// carries the agreed affine-id. It RESERVES but does NOT commit the
    /// binding (so the matching registration apply is the first commit / is
    /// Applied / is broadcast), exactly as `alloc_for_hash` reserves without
    /// placing the def slot.
    pub(crate) fn alloc_for_cell_hash(&mut self, hash: &str) -> SecondaryCellId {
        if let Some(&committed) = self.cell_hash_to_id.get(hash) {
            return committed;
        }
        if let Some(&reserved) = self.cell_id_reserved.get(hash) {
            return reserved;
        }
        let id = self.alloc_cell_id();
        self.cell_id_reserved.insert(hash.to_string(), id);
        id
    }

    /// RECEIVE-side wire-agreed affine intern: bind the affine def's `hash` to
    /// EXACTLY `id`, enforcing the hash↔affine_id BIJECTION so every replica
    /// converges on the same affine-id. The affine twin of [`Self::intern_at`]
    /// (def-id placement), but bijection-only — it records the hash↔id binding,
    /// NOT a def slot (the def itself is placed by the def-id `intern_at`; this
    /// path only owns the affine-id binding). Idempotent on a same-id re-add;
    /// a [`DefBijectionError`] on a contradiction (a converged registry never
    /// produces one).
    pub(crate) fn intern_cell_at(
        &mut self,
        id: SecondaryCellId,
        hash: &str,
    ) -> Result<SecondaryCellId, DefBijectionError> {
        if let Some(&existing) = self.cell_hash_to_id.get(hash) {
            if existing != id {
                return Err(DefBijectionError::HashRebound {
                    hash: hash.to_string(),
                    existing: TaskDefId(existing.0),
                    wire: TaskDefId(id.0),
                });
            }
            return Ok(existing);
        }
        // Hash is NEW: the affine slot must be free (a new hash claiming an
        // occupied affine-id is the id-rebind fault).
        if self.cell_id_to_hash.contains_key(&id) {
            return Err(DefBijectionError::IdRebound { id: TaskDefId(id.0) });
        }
        self.cell_hash_to_id.insert(hash.to_string(), id);
        self.cell_id_to_hash.insert(id, hash.to_string());
        // Drop any pre-broadcast reservation now that the binding is COMMITTED
        // (the originator's own commit retires its reservation).
        self.cell_id_reserved.remove(hash);
        // Keep the allocator strictly above every observed affine-id so a
        // later node-local mint never collides with a wire-placed id.
        self.next_cell_id = self.next_cell_id.max(id.0 + 1);
        Ok(id)
    }

    /// The affine-id a content `hash` resolves to, if bound (an affine def).
    pub(crate) fn cell_id_for_hash(&self, hash: &str) -> Option<SecondaryCellId> {
        self.cell_hash_to_id.get(hash).copied()
    }

    /// The content hash an affine-id is bound to — the AF-sched seam mapping a
    /// bitvector cell index back to its affine def.
    #[allow(dead_code)]
    pub(crate) fn cell_hash_for_id(&self, id: SecondaryCellId) -> Option<&str> {
        self.cell_id_to_hash.get(&id).map(String::as_str)
    }

    /// The kind of the def a bound cell-id belongs to, resolved from the def
    /// itself (`cell_id → hash → def → kind`) — the SINGLE source of truth, so
    /// the substrate never duplicates a per-kind cell-id set. `None` when the
    /// cell-id is unbound or its def is not (yet) interned in this store. The
    /// cell-id space is KIND-BLIND, but the dispatch leaves need to know a
    /// cell's kind (the eager-prep filler enumerates eager-prep cells; the
    /// affine scheduler keys off the dependent's affine deps), so the def's
    /// `kind` is the kind authority for its cell.
    pub(crate) fn kind_for_cell_id(&self, id: SecondaryCellId) -> Option<TaskKind> {
        let hash = self.cell_id_to_hash.get(&id)?;
        let def_id = self.hash_to_id.get(hash)?;
        self.resolve(*def_id).map(|def| def.kind)
    }

    /// Every bound cell-id whose def is a [`TaskKind::SecondaryEagerPrep`] —
    /// the enumeration the eager-prep idle-filler dispatch leaf consumes to
    /// derive its per-secondary candidates. Derived live from the def kinds
    /// (no duplicated per-kind set), so a promoted primary that re-anchored the
    /// cell bindings + defs via `register_restored_def` re-derives the exact
    /// same set with no queue to rebuild (the model-A failover property). The
    /// eager-prep cell count is small (one per prep def), so the scan is cheap.
    pub(crate) fn eager_prep_cell_ids(&self) -> Vec<SecondaryCellId> {
        self.cell_id_to_hash
            .keys()
            .copied()
            .filter(|&id| self.kind_for_cell_id(id) == Some(TaskKind::SecondaryEagerPrep))
            .collect()
    }

    /// The next affine-id the allocator would mint — the failover-resume floor
    /// a promoted primary re-anchors against (the affine twin of
    /// [`Self::next_id_floor`]).
    pub(crate) fn next_cell_id_floor(&self) -> u32 {
        self.next_cell_id
    }

    /// Re-anchor the affine-id allocator so the next minted id is at least
    /// `floor` — the failover-safety seam (the affine twin of
    /// [`Self::resume_alloc_floor`]). Monotone: never lowers `next_cell_id`.
    pub(crate) fn resume_cell_alloc_floor(&mut self, floor: u32) {
        self.next_cell_id = self.next_cell_id.max(floor);
    }
}

impl<I: dynrunner_core::Identifier> super::ClusterState<I> {
    /// FAILOVER def-id resume (L6a / CL-A2): re-anchor the def allocator PAST
    /// every def-id this replica has inherited — both halves of the ledger:
    ///
    ///   * the IN-MEMORY def store ([`TaskDefStore::next_id_floor`], which
    ///     every `intern_at`/`put_slot` already advanced past its slots); and
    ///   * the SETTLED records ([`super::settled::SettledStore::max_def_id`]):
    ///     a settled task's def left `definitions` (the snapshot ships defs by
    ///     value separately from the settled base, so a fresh store seeded by
    ///     `install_settled_base` + restore does NOT re-anchor a settled id),
    ///     yet a promoted primary must resume PAST it or it re-mints a settled
    ///     task's id for a DIFFERENT new task — the cross-epoch aliasing a raw
    ///     def-id dep ref (L5) would resolve to the WRONG def.
    ///
    /// The single seam the `PrimaryChanged` apply arm fires at the
    /// `primary_epoch` advance (the same seam a promotion crosses). Monotone
    /// (`resume_alloc_floor` never lowers) — a non-promoting adopter's call is
    /// a harmless no-op. Mirrors the `next_secondary_id` failover re-derive:
    /// scan the UNFILTERED inherited fact sources for `max + 1`.
    pub(crate) fn resume_def_alloc_floor(&mut self) {
        let settled_floor = self
            .settled
            .max_def_id()
            .map_or(0, |m| m.saturating_add(1));
        let floor = self.definitions.next_id_floor().max(settled_floor);
        self.definitions.resume_alloc_floor(floor);
    }

    /// Rebuild the string-identity [`TaskDep`] list from a frozen def's
    /// compact [`TaskDepRef`] list (L5) — the [`ClusterState`]-level seam the
    /// frozen-def dep CONSUMERS route through (the dispatch `to_task_info`,
    /// `task_deps_for_identity`, the affine gate, the settled-spill capture):
    /// they hold `&self` (the store), a `&FrozenTaskDef` does not, so the
    /// resolution lives here and delegates to
    /// [`TaskDefStore::resolve_dep_refs_with_fallback`].
    ///
    /// SETTLED-aware (the recompose linchpin): a ref whose prereq has
    /// COMPLETED + SETTLED no longer resolves in the in-memory `definitions`
    /// store (the settled def was evicted), so the bare store would rebuild
    /// the unresolved-sentinel edge and a promoted-primary hydrate of a
    /// still-Pending dependent would fail `UnknownTaskDep`. We pass the
    /// settled `def_id → identity` reverse index as the fallback, so the
    /// rebuilt edge carries the settled prereq's REAL `(phase_id, task_id)` —
    /// IDENTICAL to the live graph (where the dependent keeps the completed
    /// dep in `task_depends_on` and the pool's `completed_tasks` pre-resolves
    /// it as satisfied). The split stays a single concern: the in-memory
    /// store is settled-blind; this seam composes the two halves.
    pub(crate) fn resolve_dep_refs(&self, refs: &[TaskDepRef]) -> Vec<TaskDep> {
        self.definitions.resolve_dep_refs_with_fallback(refs, |id| {
            self.settled
                .identity_for_def_id(id)
                .map(|(phase, task)| (phase.clone(), task.to_string()))
        })
    }

    /// Reconstruct a whole owned [`TaskInfo`] from a [`TaskState`] (L5) —
    /// the store-resolving wrapper every `to_task_info` consumer that holds
    /// a `&ClusterState` routes through: it resolves the state's def
    /// `task_depends_on` refs to string deps via [`Self::resolve_dep_refs`]
    /// (a `TaskState` has no store) and delegates to
    /// [`TaskState::to_task_info`]. The SINGLE seam, so no consumer re-spells
    /// the resolve + rebuild.
    pub(crate) fn task_to_info(&self, state: &super::types::TaskState<I>) -> TaskInfo<I>
    where
        I: Clone,
    {
        let deps = self.resolve_dep_refs(&state.def().task_depends_on);
        state.to_task_info(deps)
    }

    /// Split a whole owned [`TaskInfo`] into the shared frozen `def` (interned
    /// under `hash` in `self.definitions`, deduplicated by content) + the
    /// per-entry mutable [`TaskRouting`] tail. The single construction-site
    /// helper a `TaskState` builder calls when it holds a whole `TaskInfo`
    /// and `&mut self` (the apply / merge / hydrate paths): it owns the
    /// `from_task_info` split + `intern` + `resolve` sequence so no caller
    /// re-spells it. This is the NODE-LOCAL-allocation half: the id is minted
    /// from the local monotone allocator, the L2 by-content-hash convergence
    /// (equal content yields equal hash yields the same `Arc` dedup). The
    /// wire-stamped sibling [`Self::intern_task_def_at`] interns at the
    /// primary-allocated wire id on the replicated path and delegates HERE as
    /// its un-stamped fallback (direct-apply tests, any pre-stamp local apply).
    pub(crate) fn intern_task_def(
        &mut self,
        hash: &str,
        task: TaskInfo<I>,
    ) -> (Arc<FrozenTaskDef<I>>, super::types::TaskRouting) {
        let (frozen, preferred_secondaries, preferred_version, resolved_path, deps) =
            FrozenTaskDef::from_task_info(task);
        // TWO-STEP intern (L5): place the def with EMPTY refs FIRST so its own
        // `(phase_id, task_id)` identity is registered, THEN resolve its
        // carved-out string deps into compact refs (originator-stamped def_id
        // first, else the identity reverse-index — which now includes this
        // def, so a self-referential dep resolves to the def's own id) and
        // fill them. A re-intern under a known hash hits the dedup gate
        // (placed=false) and leaves the existing (already-resolved) def
        // untouched.
        let (id, placed) = self
            .definitions
            .intern_reporting_placement(hash.to_string(), frozen);
        if placed {
            let refs = self.definitions.dep_refs_from_deps(&deps);
            self.definitions.fill_dep_refs(id, refs);
        }
        let def = self
            .definitions
            .resolve(id)
            .expect("freshly interned def resolves")
            .clone();
        (
            def,
            super::types::TaskRouting {
                preferred_secondaries,
                preferred_version,
                resolved_path,
            },
        )
    }

    /// The DEF-BEFORE-STATE construction helper the `TaskAdded` apply arm
    /// calls: insert the frozen def into the store BEFORE the referencing
    /// `TaskState` is set, resolving the in-memory `Arc<FrozenTaskDef>` the
    /// state carries. Honors the wire-carried, primary-allocated `def_id`:
    ///
    ///   * `Some(wire)` — the production replicated path: intern the def at
    ///     EXACTLY `wire` ([`TaskDefStore::intern_at`]) so this replica uses
    ///     the SAME id the originator allocated. A hash↔id BIJECTION
    ///     violation (a converged registry never produces one) is logged
    ///     LOUD and the construction is REFUSED (`None`), so the apply arm
    ///     NoOps the mutation rather than corrupting the registry.
    ///   * `None` — the un-allocated local-apply fallback (direct-apply
    ///     tests, any pre-stamp local apply): node-local allocation
    ///     ([`TaskDefStore::intern`]), the L2 by-content-hash convergence.
    ///
    /// Returns `None` ONLY on a bijection violation (the loud-but-safe
    /// drop); every well-formed call returns the `(def, routing)` the arm
    /// writes onto the new `TaskState`.
    pub(crate) fn intern_task_def_at(
        &mut self,
        def_id: Option<u32>,
        hash: &str,
        task: TaskInfo<I>,
    ) -> Option<(Arc<FrozenTaskDef<I>>, super::types::TaskRouting)> {
        let Some(wire) = def_id else {
            return Some(self.intern_task_def(hash, task));
        };
        if self.settled.is_def_id_settled(TaskDefId(wire)) {
            // SETTLED-id collision (CL-A2 allocator settled-safety): the wire
            // id already belongs to a SETTLED (evicted) task, invisible to
            // `intern_at`'s in-memory free-slot check — placing here would
            // SILENTLY occupy the settled slot and alias a def-id dep ref onto
            // the wrong def. The settled hash itself NoOps upstream (the
            // `TaskAdded` arm's `settled_contains` guard), so a NON-settled
            // hash reaching here with a settled wire id is a genuine
            // cross-epoch alias — fall back to node-local CONTENT interning
            // (the `None`-arm path), the same degrade `register_restored_def`
            // uses; the def resolves by hash under a fresh local id.
            tracing::error!(
                target: "dynrunner_cluster_state",
                hash,
                wire,
                "TaskAdded wire def-id collides with a SETTLED id — a new task's \
                 wire-carried id is already held by a settled (evicted) task \
                 (a failover cross-epoch transient). Interning node-local by \
                 content so it never aliases onto the settled prereq's slot."
            );
            return Some(self.intern_task_def(hash, task));
        }
        let (frozen, preferred_secondaries, preferred_version, resolved_path, deps) =
            FrozenTaskDef::from_task_info(task);
        // TWO-STEP intern at the wire id (L5, mirrors `intern_task_def`):
        // place the def with EMPTY refs FIRST so its `(phase_id, task_id)`
        // identity is registered, THEN resolve its deps (so a self-ref
        // resolves to the just-placed id) and fill. The bijection check lives
        // in `intern_at`; an idempotent re-add against an already-filled slot
        // leaves its refs untouched (the resolve+fill runs only on a fresh
        // placement, detected by the slot being empty before `intern_at`).
        let fresh = self.definitions.resolve(TaskDefId(wire)).is_none();
        let id = match self
            .definitions
            .intern_at(TaskDefId(wire), hash.to_string(), frozen)
        {
            Ok(id) => id,
            Err(err) => {
                tracing::error!(
                    target: "dynrunner_cluster_state",
                    ?err,
                    "TaskAdded def-id BIJECTION violation — the wire-carried \
                     (def_id, hash) contradicts an established binding (a \
                     converged content-addressed registry never produces one; \
                     two primaries minting different ids for one hash, or a \
                     failover-aliased id reuse). Dropping the TaskAdded."
                );
                debug_assert!(false, "TaskAdded def-id bijection violation: {err:?}");
                return None;
            }
        };
        if fresh {
            let refs = self.definitions.dep_refs_from_deps(&deps);
            self.definitions.fill_dep_refs(id, refs);
        }
        let def = self
            .definitions
            .resolve(id)
            .expect("def placed at wire id resolves")
            .clone();
        Some((
            def,
            super::types::TaskRouting {
                preferred_secondaries,
                preferred_version,
                resolved_path,
            },
        ))
    }

    /// REBUILD the def-store maps from a self-describing restored def: the
    /// snapshot/AE/merge restore seam (`restore_collecting_resumed`) calls
    /// this for every restored `TaskState` so the local store regains the
    /// id↔def + hash↔id bindings the snapshot dropped (it ships defs INLINE
    /// by value, not the store). The decode rebuilt the def CONTENT inside
    /// the state's `Arc<FrozenTaskDef>`; this re-interns it at the id the def
    /// CARRIES so `resolve(def_id)` works on the restoring replica
    /// (late-joiner / promoted-primary / AE), the prerequisite for L5's
    /// def_id-based dep refs.
    ///
    ///   * a def carrying a real id (a wire-agreed id stamped at
    ///     [`TaskDefStore::intern_at`]) is placed at EXACTLY that id —
    ///     bijection-enforced, so on the CONVERGED happy path a fresh replica
    ///     re-anchors the def at the SAME id (the L5 prerequisite).
    ///   * a legacy/un-agreed def carrying [`TaskDefId::UNBOUND`] (a
    ///     node-local intern, or a pre-self-describing snapshot) falls back to
    ///     node-local [`TaskDefStore::intern`] (the L2 by-content-hash
    ///     convergence) — the same fallback `intern_task_def_at`'s `None` arm
    ///     uses. A node-local id is intra-node only, so it is NOT asserted as
    ///     portable.
    ///
    /// COLLISION (the carried id contradicts a binding this replica already
    /// holds) is logged LOUD but is NOT a crash: unlike the LIVE wire — where
    /// the whole `TaskAdded` NoOps and is redelivered — a restore has ALREADY
    /// merged the authoritative `TaskState`, so the def must not be lost. It
    /// can legitimately arise as a TRANSIENT across a failover (two primary
    /// epochs each minting from their own allocator before the
    /// `resume_alloc_floor` reconciliation observes the other's ids), so the
    /// restore DEGRADES gracefully: re-anchor the def by CONTENT
    /// ([`TaskDefStore::intern`]) so it still resolves by hash (under this
    /// replica's local id), the existing binding untouched. The def content
    /// always round-trips via the inline state, so nothing is lost.
    ///
    /// Idempotent: a re-restore re-presents the same `(id, hash)` and the
    /// bijection's same-id re-add mints nothing.
    pub(crate) fn register_restored_def(&mut self, hash: &str, def: &Arc<FrozenTaskDef<I>>)
    where
        I: Clone,
    {
        let carried = def.def_id;
        if carried == TaskDefId::UNBOUND {
            // Un-agreed / legacy def: no self-describing portable id —
            // re-anchor by content hash, exactly like the un-allocated apply
            // fallback.
            self.definitions.intern(hash.to_string(), (**def).clone());
            self.reanchor_restored_affine_id(hash, def);
            return;
        }
        if self.settled.is_def_id_settled(carried) {
            // SETTLED-id collision (CL-A2 allocator settled-safety): the
            // carried id already belongs to a task that COMPLETED + SETTLED —
            // its def left the in-memory store, so `intern_at`'s in-memory
            // free-slot check would NOT see the collision and would place this
            // (DIFFERENT) restored task SILENTLY onto the settled slot, so a
            // def-id dep ref pointing at the settled prereq would then resolve
            // to the WRONG def (the alias the floor-resume guards against on
            // the node-local-mint path, here on the carried-id path
            // `intern_at` ignores). A settled prereq's own def never reaches
            // this loop (a settled entry is evicted from the fat snapshot
            // batch), so a fat task carrying a settled id is always a genuine
            // cross-epoch alias — degrade it by CONTENT, identical to the
            // in-memory IdRebound path below; the def still resolves by hash
            // under a fresh local id and is never lost.
            tracing::error!(
                target: "dynrunner_cluster_state",
                hash,
                carried = carried.0,
                "snapshot-restore def-id collides with a SETTLED id — a restored \
                 def carries an id a settled (evicted) task already holds (a \
                 failover cross-epoch transient). Re-anchoring the def by \
                 content so it never aliases onto the settled prereq's slot."
            );
            self.definitions.intern(hash.to_string(), (**def).clone());
            self.reanchor_restored_affine_id(hash, def);
            return;
        }
        if let Err(err) =
            self.definitions
                .intern_at(carried, hash.to_string(), (**def).clone())
        {
            tracing::error!(
                target: "dynrunner_cluster_state",
                ?err,
                hash,
                "snapshot-restore def-id collision — a restored def's \
                 self-describing (def_id, hash) contradicts an established \
                 binding (a converged registry never produces one; a failover \
                 cross-epoch transient can). Re-anchoring the def by content; \
                 the existing id binding is kept and the def content round-trips \
                 via the inline task state."
            );
            // Degrade to content-addressed: the def still resolves by hash on
            // this replica (under its local id), never lost.
            self.definitions.intern(hash.to_string(), (**def).clone());
        }
        self.reanchor_restored_affine_id(hash, def);
    }

    /// INVARIANT GUARD (`all tasks are global`): every task that crosses a
    /// persist / broadcast / restore seam MUST carry a GLOBAL, primary-allocated
    /// `def_id` — `TaskDefId::UNBOUND` is valid ONLY transiently on the
    /// originating primary BEFORE the stamp pass runs. This durable guard catches
    /// the WHOLE bug class at the seam: a future task-creation path that skips the
    /// originate def-id stamp (the recompose self-cycle's root cause) fails LOUD
    /// here instead of silently re-minting onto a live id on the next failover.
    ///
    /// Loud-but-safe, LOG-ONLY (the `register_restored_def` precedent): this
    /// persist/restore seam is DEFENCE-IN-DEPTH behind the hard `debug_assert!`
    /// the originate broadcast seam (`broadcast::assert_stamped_for_broadcast`)
    /// already carries — that one fires at development time on the REAL
    /// production origination path. Here we only `tracing::error!` per offending
    /// hash and do NOT panic: a snapshot is also taken in many in-process unit
    /// fixtures that intern via the node-local L2 fallback (a bare
    /// `apply(TaskAdded { def_id: None })`), a legitimate non-broadcast shape, so
    /// a panic here would conflate "an unstamped PRODUCTION origination path"
    /// (the bug class) with "a node-local test seed" (intended). The loud log
    /// keeps production observability without aborting a live primary's snapshot.
    /// `seam` names the crossing for the operator. Returns the count of UNBOUND
    /// defs found (0 on the healthy production path).
    pub(crate) fn assert_all_defs_global(
        &self,
        tasks: &std::collections::HashMap<String, super::types::TaskState<I>>,
        seam: &str,
    ) -> usize {
        let mut unbound = 0usize;
        for (hash, state) in tasks {
            if state.def().def_id == TaskDefId::UNBOUND {
                unbound += 1;
                tracing::error!(
                    target: "dynrunner_cluster_state",
                    hash = %hash,
                    seam,
                    "INVARIANT (all tasks are global): a task with an UNBOUND \
                     def_id is crossing the {seam} seam — every broadcast / \
                     persisted / restored task should carry a primary-allocated, \
                     wire-agreed def_id. A PRODUCTION origination path that hits \
                     this slipped past the def-id stamp chokepoint \
                     (broadcast::stamp_def_ids); a node-local unit seed is benign."
                );
            }
        }
        unbound
    }

    /// Re-anchor a restored `SecondaryAffine` def's affine-id↔hash binding from
    /// the value the [`FrozenTaskDef::affine_id`] carries inline (the affine
    /// twin of the `def_id` re-anchor above — both restore self-describing,
    /// snapshot-portable ids).
    ///
    /// The affine-id binding lives in a SEPARATE registry
    /// ([`TaskDefStore::cell_hash_to_id`]) populated ONLY by
    /// `intern_cell_at` (driven on the live path by
    /// `SecondaryCellRegistered`). Interning the def's CONTENT does NOT touch
    /// that registry, so without this re-anchor a snapshot-restored affine def
    /// resolves `cell_id_for_hash == None` on the restoring replica — the
    /// affine-dep placement (`affine_placement_for`) then finds the dependent's
    /// affine deps EMPTY, never places the per-secondary unit, and the import
    /// never dispatches (the run stalls with the import phase held open). The
    /// `SecondaryCellRegistered` mutation is a SEED-time fact (injected at cold
    /// seed); it is not re-broadcast to a primary that promotes from a snapshot,
    /// so the inline `affine_id` is the ONLY carrier across the relocation/
    /// failover handoff. `None` for every ordinary (Work/Setup) def — a no-op.
    ///
    /// Idempotent + bijection-safe: `intern_cell_at` NoOps a same-(id, hash)
    /// re-add and rejects (logged loud, dropped) a contradicting binding, the
    /// same loud-but-safe contract `apply_secondary_affine_registered` uses.
    fn reanchor_restored_affine_id(&mut self, hash: &str, def: &Arc<FrozenTaskDef<I>>) {
        let Some(affine_id) = def.affine_id else {
            return;
        };
        if let Err(err) = self.definitions.intern_cell_at(affine_id, hash) {
            tracing::error!(
                target: "dynrunner_cluster_state",
                ?err,
                hash,
                "snapshot-restore affine-id collision — a restored affine def's \
                 self-describing (affine_id, hash) contradicts an established \
                 binding. Dropping the re-anchor; the def still resolves by \
                 content hash."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_core::{RunnerIdentifier, TypeId};

    /// A minimal frozen def fixture, cp-ed in shape from the
    /// `cluster_state::tests::mk_task` `TaskInfo` builder (minus the 3
    /// mutable tail fields the frozen core excludes).
    fn mk_frozen(name: &str, phase: &str) -> FrozenTaskDef<RunnerIdentifier> {
        FrozenTaskDef {
            // UN-interned literal: intern stamps the real id on store.
            def_id: TaskDefId::UNBOUND,
            affine_id: None,
            path: PathBuf::from(format!("/tasks/{name}")),
            size: 0,
            identifier: RunnerIdentifier::from(name),
            phase_id: PhaseId::from(phase),
            type_id: TypeId::from("t0"),
            kind: TaskKind::default(),
            setup_affinity: None,
            upload_file: None,
            required_files: None,
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: name.into(),
            task_depends_on: Vec::new(),
        }
    }

    /// A full `TaskInfo` fixture (cp-ed from `cluster_state::tests::mk_task`)
    /// so `from_task_info` can be round-tripped against an original.
    fn mk_task(name: &str) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: PathBuf::from(format!("/tasks/{name}")),
            size: 7,
            identifier: RunnerIdentifier::from(name),
            phase_id: PhaseId::from("p0"),
            type_id: TypeId::from("t0"),
            affinity_id: Some(AffinityId::from("a0")),
            payload: serde_json::json!({ "k": name }),
            task_id: name.into(),
            task_depends_on: Vec::new(),
            preferred_secondaries: SoftPreferredSecondaries::default(),
            preferred_version: TaskVersion::default(),
            kind: TaskKind::default(),
            setup_affinity: None,
            upload_file: None,
            required_files: None,
            resolved_path: None,
        }
    }

    #[test]
    fn intern_idempotent_on_hash() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        let a = store.intern("h".into(), mk_frozen("x", "p0"));
        let b = store.intern("h".into(), mk_frozen("x", "p0"));
        assert_eq!(a, b);
        assert_eq!(store.defs.len(), 1);
    }

    #[test]
    fn new_hash_new_id() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        let a = store.intern("h1".into(), mk_frozen("x", "p0"));
        let b = store.intern("h2".into(), mk_frozen("y", "p0"));
        assert_ne!(a, b);
        assert_eq!(a, TaskDefId(0));
        assert_eq!(b, TaskDefId(1));
        assert_eq!(store.defs.len(), 2);
    }

    #[test]
    fn reintern_mints_nothing() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        store.intern("h1".into(), mk_frozen("x", "p0"));
        store.intern("h2".into(), mk_frozen("y", "p0"));
        let before = store.next_id_floor();
        let again = store.intern("h1".into(), mk_frozen("x", "p0"));
        assert_eq!(again, TaskDefId(0));
        assert_eq!(store.next_id_floor(), before);
    }

    #[test]
    fn str_intern_shares_arc() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        let id_a = store.intern("h1".into(), mk_frozen("x", "shared-phase"));
        let id_b = store.intern("h2".into(), mk_frozen("y", "shared-phase"));

        // Two distinct defs sharing a phase id ("shared-phase") and a type
        // id ("t0") ⇒ exactly TWO pool allocations (one per distinct
        // string), NOT four: the dedup the intern pool exists for.
        assert_eq!(store.str_intern.len(), 2);
        let pool_arc = store.str_intern.get("shared-phase").cloned().unwrap();

        // Load-bearing: both stored `PhaseId`s back onto the SAME `Arc<str>`
        // — the very pool `Arc`. `PhaseId::clone` is an `Arc::clone`
        // (transparent newtype), so rebuilding through `PhaseId::new(pool)`
        // and cloning the stored id must `ptr_eq` (same backing allocation).
        let a_phase = store.resolve(id_a).unwrap().phase_id.clone();
        let b_phase = store.resolve(id_b).unwrap().phase_id.clone();
        assert_eq!(a_phase, b_phase);
        let from_pool = PhaseId::new(Arc::clone(&pool_arc));
        assert_eq!(a_phase, from_pool);
        assert_eq!(b_phase, from_pool);
        // Strong-count rose past the pool's own one ref ⇒ the stored defs
        // hold clones of the pool `Arc`, not independent allocations.
        assert!(Arc::strong_count(&pool_arc) >= 3);
    }

    #[test]
    fn from_task_info_round_trips() {
        let original = mk_task("rt");
        let expected = original.clone();
        let (frozen, prefs, version, resolved, deps) = FrozenTaskDef::from_task_info(original);
        // L5: the splitter carves the string deps OUT and leaves the frozen
        // core's `task_depends_on` (now `Vec<TaskDepRef>`) EMPTY — the store
        // fills it at intern. `mk_task` carries no deps, so both the carved
        // list and the empty ref list round-trip the original's empty deps.
        assert!(frozen.task_depends_on.is_empty());
        let rebuilt = TaskInfo {
            path: frozen.path,
            size: frozen.size,
            identifier: frozen.identifier,
            phase_id: frozen.phase_id,
            type_id: frozen.type_id,
            kind: frozen.kind,
            setup_affinity: frozen.setup_affinity,
            upload_file: frozen.upload_file,
            required_files: frozen.required_files,
            affinity_id: frozen.affinity_id,
            payload: frozen.payload,
            task_id: frozen.task_id,
            task_depends_on: deps,
            preferred_secondaries: prefs,
            preferred_version: version,
            resolved_path: resolved,
        };
        assert_eq!(rebuilt.path, expected.path);
        assert_eq!(rebuilt.size, expected.size);
        assert_eq!(rebuilt.identifier, expected.identifier);
        assert_eq!(rebuilt.phase_id, expected.phase_id);
        assert_eq!(rebuilt.type_id, expected.type_id);
        assert_eq!(rebuilt.kind, expected.kind);
        assert_eq!(rebuilt.setup_affinity, expected.setup_affinity);
        assert_eq!(rebuilt.upload_file, expected.upload_file);
        assert_eq!(rebuilt.required_files, expected.required_files);
        assert_eq!(rebuilt.affinity_id, expected.affinity_id);
        assert_eq!(rebuilt.payload, expected.payload);
        assert_eq!(rebuilt.task_id, expected.task_id);
        assert_eq!(rebuilt.task_depends_on, expected.task_depends_on);
        assert_eq!(rebuilt.preferred_secondaries, expected.preferred_secondaries);
        assert_eq!(rebuilt.preferred_version, expected.preferred_version);
        assert_eq!(rebuilt.resolved_path, expected.resolved_path);
    }

    #[test]
    fn next_id_floor_is_len() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        assert_eq!(store.next_id_floor(), 0);
        store.intern("h1".into(), mk_frozen("x", "p0"));
        assert_eq!(store.next_id_floor(), 1);
        store.intern("h2".into(), mk_frozen("y", "p0"));
        assert_eq!(store.next_id_floor(), 2);
    }

    // ── L3a: primary-allocated, wire-agreed def ids ──

    /// `intern_at` places the wire-carried def at EXACTLY the requested id —
    /// the convergence primitive: a receiver uses the originator's id, never
    /// a node-local position. A SPARSE id (a gap below it) is tolerated.
    #[test]
    fn intern_at_places_at_wire_id() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        // Place id 5 first (a gap 0..=4): out-of-order wire delivery.
        let id = store
            .intern_at(TaskDefId(5), "h5".into(), mk_frozen("x", "p0"))
            .unwrap();
        assert_eq!(id, TaskDefId(5));
        assert!(store.resolve(TaskDefId(5)).is_some());
        assert!(store.resolve(TaskDefId(0)).is_none(), "gap is a not-yet-seen def");
        // The allocator resumed past the placed id so a later node-local mint
        // never collides with the wire-placed slot.
        assert_eq!(store.next_id_floor(), 6);
        let local = store.intern("h-local".into(), mk_frozen("y", "p0"));
        assert_eq!(local, TaskDefId(6));
    }

    /// `intern_at` is idempotent on a re-add of a hash already bound to the
    /// SAME id (at-least-once delivery / the originator's own apply after its
    /// `alloc_for_hash` reservation) — it mints nothing and reuses the id.
    #[test]
    fn intern_at_idempotent_on_same_hash_same_id() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        let a = store
            .intern_at(TaskDefId(3), "h".into(), mk_frozen("x", "p0"))
            .unwrap();
        let b = store
            .intern_at(TaskDefId(3), "h".into(), mk_frozen("x", "p0"))
            .unwrap();
        assert_eq!(a, TaskDefId(3));
        assert_eq!(b, TaskDefId(3));
        assert_eq!(store.next_id_floor(), 4);
    }

    /// `alloc_for_hash` reserves the binding WITHOUT placing a def; the
    /// matching `intern_at` then FILLS the reserved slot (the originator's
    /// two-step stamp→apply path). A second `alloc_for_hash` for the same
    /// hash reuses the reservation.
    #[test]
    fn alloc_for_hash_reserves_then_intern_at_fills() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        let reserved = store.alloc_for_hash("h");
        assert_eq!(reserved, TaskDefId(0));
        // Reserved but not yet placed: resolve is None until the def lands.
        assert!(store.resolve(reserved).is_none());
        // Idempotent reservation.
        assert_eq!(store.alloc_for_hash("h"), TaskDefId(0));
        // The originator's own apply fills the slot at the reserved id.
        let id = store
            .intern_at(reserved, "h".into(), mk_frozen("x", "p0"))
            .unwrap();
        assert_eq!(id, reserved);
        assert!(store.resolve(reserved).is_some());
    }

    /// BIJECTION: a hash already bound to one id, re-presented on the wire
    /// with a DIFFERENT id, is a `HashRebound` error (never produced by a
    /// converged content-addressed registry).
    #[test]
    fn intern_at_hash_rebound_errors() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        store
            .intern_at(TaskDefId(0), "h".into(), mk_frozen("x", "p0"))
            .unwrap();
        let err = store
            .intern_at(TaskDefId(1), "h".into(), mk_frozen("x", "p0"))
            .unwrap_err();
        assert_eq!(
            err,
            DefBijectionError::HashRebound {
                hash: "h".into(),
                existing: TaskDefId(0),
                wire: TaskDefId(1),
            }
        );
    }

    /// BIJECTION: a NEW hash claiming an id slot already bound to a DIFFERENT
    /// hash is an `IdRebound` error (the failover-aliasing the epoch-safe
    /// allocator exists to prevent).
    #[test]
    fn intern_at_id_rebound_errors() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        store
            .intern_at(TaskDefId(0), "h-a".into(), mk_frozen("a", "p0"))
            .unwrap();
        let err = store
            .intern_at(TaskDefId(0), "h-b".into(), mk_frozen("b", "p0"))
            .unwrap_err();
        assert_eq!(err, DefBijectionError::IdRebound { id: TaskDefId(0) });
    }

    /// `resume_alloc_floor` re-anchors the allocator forward (failover
    /// resume) and is MONOTONE — it never lowers `next_id`.
    #[test]
    fn resume_alloc_floor_is_monotone() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        store.intern("h0".into(), mk_frozen("x", "p0"));
        assert_eq!(store.next_id_floor(), 1);
        store.resume_alloc_floor(10);
        assert_eq!(store.next_id_floor(), 10);
        // A lower floor is a no-op (a promoted primary never regresses).
        store.resume_alloc_floor(3);
        assert_eq!(store.next_id_floor(), 10);
        // The next node-local mint respects the resumed floor (no live-id reuse).
        let id = store.intern("h-new".into(), mk_frozen("y", "p0"));
        assert_eq!(id, TaskDefId(10));
    }

    // ── L5: compact def-id dep refs ──

    use dynrunner_core::TaskDep;

    /// A string `TaskDep` resolves to a compact `TaskDepRef` at intern (via
    /// the store's identity index), and the read-side `resolve_dep_refs`
    /// rebuilds the prereq's `(phase_id, task_id)` — with the per-edge
    /// `inherit_outputs` PRESERVED across both directions (CL-A3: the ref is
    /// not lossy).
    #[test]
    fn dep_ref_round_trips_and_preserves_inherit_outputs() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        // Intern the prereq first so its identity is known.
        let prereq_id = store.intern("h-prereq".into(), mk_frozen("prereq", "phase-A"));
        // A dep on the prereq with inherit_outputs=true and no stamped def_id
        // (resolves via the identity index).
        let deps = vec![TaskDep {
            task_id: "prereq".into(),
            phase_id: PhaseId::from("phase-A"),
            inherit_outputs: true,
            def_id: None,
        }];
        let refs = store.dep_refs_from_deps(&deps);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].def_id, prereq_id, "resolved to the prereq's def id");
        assert!(refs[0].inherit_outputs, "per-edge flag carried onto the ref");

        let rebuilt = store.resolve_dep_refs_with_fallback(&refs, |_| None);
        assert_eq!(rebuilt.len(), 1);
        assert_eq!(rebuilt[0].task_id, "prereq");
        assert_eq!(rebuilt[0].phase_id, PhaseId::from("phase-A"));
        assert!(rebuilt[0].inherit_outputs, "inherit_outputs preserved on rebuild");
    }

    /// An ORIGINATOR-stamped dep `def_id` is used directly — no identity
    /// lookup needed, so it resolves even when the prereq's def is NOT yet in
    /// this store (the receive-side forward-ref-safety the wire stamp buys).
    #[test]
    fn dep_ref_uses_stamped_def_id_without_identity_lookup() {
        let store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        let deps = vec![TaskDep {
            task_id: "prereq".into(),
            phase_id: PhaseId::from("phase-A"),
            inherit_outputs: false,
            def_id: Some(42),
        }];
        let refs = store.dep_refs_from_deps(&deps);
        assert_eq!(refs[0].def_id, TaskDefId(42), "stamped def_id used verbatim");
    }

    /// The PHASE-LESS fallback: a dep whose stored phase does NOT match the
    /// prereq's real phase (a bare-string cross-phase dep resolved to the
    /// enclosing phase) still resolves by task_id alone — the pre-L5
    /// tolerance `PendingPool::extend`'s phaseless set carried.
    #[test]
    fn dep_ref_phaseless_fallback_resolves_cross_phase() {
        let mut store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        let prereq_id = store.intern("h-prereq".into(), mk_frozen("prereq", "build"));
        // The dep names the WRONG (enclosing) phase "compile" — phaseless
        // fallback still finds the prereq, and the rebuild yields its REAL
        // phase.
        let deps = vec![TaskDep {
            task_id: "prereq".into(),
            phase_id: PhaseId::from("compile"),
            inherit_outputs: false,
            def_id: None,
        }];
        let refs = store.dep_refs_from_deps(&deps);
        assert_eq!(refs[0].def_id, prereq_id);
        let rebuilt = store.resolve_dep_refs_with_fallback(&refs, |_| None);
        assert_eq!(
            rebuilt[0].phase_id,
            PhaseId::from("build"),
            "rebuild yields the prereq's REAL phase, not the dep's stored one"
        );
    }

    /// An UNRESOLVABLE dep (no stamped def_id, no known identity) maps to the
    /// UNBOUND sentinel ref, and the read-side rebuild yields the empty
    /// identity — carrying NO false `(phase_id, task_id)` so the downstream
    /// loud-unknown-dep failure fires exactly as a missing string dep would
    /// (the def-id layer never silently fabricates a real identity).
    #[test]
    fn unresolvable_dep_maps_to_unbound_then_empty_identity() {
        let store: TaskDefStore<RunnerIdentifier> = TaskDefStore::default();
        let deps = vec![TaskDep {
            task_id: "ghost".into(),
            phase_id: PhaseId::from("phase-A"),
            inherit_outputs: true,
            def_id: None,
        }];
        let refs = store.dep_refs_from_deps(&deps);
        assert_eq!(refs[0].def_id, TaskDefId::UNBOUND);
        assert!(refs[0].inherit_outputs, "flag still carried on the sentinel ref");
        let rebuilt = store.resolve_dep_refs_with_fallback(&refs, |_| None);
        assert!(rebuilt[0].task_id.is_empty(), "no false identity fabricated");
        assert!(rebuilt[0].phase_id.as_str().is_empty());
        assert!(rebuilt[0].inherit_outputs, "flag preserved through the sentinel");
    }
}
