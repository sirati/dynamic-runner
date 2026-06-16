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
//! L1 is ADDITIVE: the store + its `from_task_info` splitter are owned by
//! `ClusterState` and exercised by this module's tests, but no production
//! caller interns or resolves yet (the originate/apply wiring is a later
//! leaf). The constructor-, intern-, and resolve-surfaces are therefore
//! `#[allow(dead_code)]` until that leaf lands — the methods are real and
//! tested, just not yet called outside `#[cfg(test)]`.

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
    pub task_depends_on: Vec<TaskDep>,
}

impl<I> FrozenTaskDef<I> {
    /// Split a [`TaskInfo`] into its frozen core + the 3 mutable tail
    /// values the runtime owns. The destructure names EVERY `TaskInfo`
    /// field with NO `..` rest, so a future `TaskInfo` field is a
    /// COMPILE ERROR here until the developer classifies it
    /// frozen-vs-mutable.
    pub(crate) fn from_task_info(
        t: TaskInfo<I>,
    ) -> (
        FrozenTaskDef<I>,
        SoftPreferredSecondaries,
        TaskVersion,
        Option<PathBuf>,
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
            },
            preferred_secondaries,
            preferred_version,
            resolved_path,
        )
    }

    /// Reconstruct a whole owned [`TaskInfo`] from this frozen core (its 13
    /// immutable fields, cloned) + a [`TaskRouting`] tail (the 3 mutable
    /// fields). The inverse of [`Self::from_task_info`] and the SINGLE place
    /// the 16-field rebuild lives — both `TaskState::to_task_info` and the
    /// affine-gate resolver delegate here so no caller re-spells it. A
    /// TRANSIENT allocation: only for callers that genuinely need a whole
    /// owned `TaskInfo` (a wire `TaskAssignment`, a pool insert).
    pub(crate) fn to_task_info(&self, routing: &super::types::TaskRouting) -> TaskInfo<I>
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
            task_depends_on: self.task_depends_on.clone(),
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
    let (frozen, preferred_secondaries, preferred_version, resolved_path) =
        FrozenTaskDef::from_task_info(task);
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
        if idx >= self.defs.len() {
            self.defs.resize(idx + 1, None);
        }
        self.defs[idx] = Some(frozen);
        // Keep the allocator strictly above every observed id so a later
        // node-local mint never collides with a wire-placed id.
        self.next_id = self.next_id.max(id.0 + 1);
    }

    /// NODE-LOCAL allocate-and-intern: the un-agreed L2 fallback used when
    /// no primary-allocated id rides the wire (a `def_id: None` `TaskAdded`,
    /// the in-process direct-apply / unit-test path). If the hash is already
    /// known, returns the existing id and mints NOTHING (the
    /// content-addressed dedup gate). Otherwise mints the next node-local
    /// id, canonicalizes the def's `Arc<str>` ids, and records it.
    pub(crate) fn intern(&mut self, hash: String, mut frozen: FrozenTaskDef<I>) -> TaskDefId {
        if let Some(&existing) = self.hash_to_id.get(&hash) {
            return existing;
        }
        self.canonicalize_strs(&mut frozen);
        let id = self.alloc();
        self.put_slot(id, Arc::new(frozen));
        self.hash_to_id.insert(hash, id);
        id
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

    /// The id a content `hash` resolves to, if this store has bound it.
    #[allow(dead_code)]
    pub(crate) fn id_for_hash(&self, hash: &str) -> Option<TaskDefId> {
        self.hash_to_id.get(hash).copied()
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
    /// own max already feeds `next_id` (every `put_slot` advances it), so a
    /// `resume_alloc_floor(next_id_floor())` is the L3a resume — the full
    /// settled-scan over spilled entries is a later leaf.
    pub(crate) fn resume_alloc_floor(&mut self, floor: u32) {
        self.next_id = self.next_id.max(floor);
    }
}

impl<I: dynrunner_core::Identifier> super::ClusterState<I> {
    /// Split a whole owned [`TaskInfo`] into the shared frozen `def` (interned
    /// under `hash` in `self.definitions`, deduplicated by content) + the
    /// per-entry mutable [`TaskRouting`] tail. The single construction-site
    /// helper a `TaskState` builder calls when it holds a whole `TaskInfo`
    /// and `&mut self` (the apply / merge / hydrate paths): it owns the
    /// `from_task_info` split + `intern` + `resolve` sequence so no caller
    /// re-spells it. Local interning per node is fine — the in-memory `Arc`
    /// is what dedups; wire-agreed ids are a later leaf.
    pub(crate) fn intern_task_def(
        &mut self,
        hash: &str,
        task: TaskInfo<I>,
    ) -> (Arc<FrozenTaskDef<I>>, super::types::TaskRouting) {
        let (frozen, preferred_secondaries, preferred_version, resolved_path) =
            FrozenTaskDef::from_task_info(task);
        let id = self.definitions.intern(hash.to_string(), frozen);
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
        let (frozen, preferred_secondaries, preferred_version, resolved_path) =
            FrozenTaskDef::from_task_info(task);
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
        let (frozen, prefs, version, resolved) = FrozenTaskDef::from_task_info(original);
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
            task_depends_on: frozen.task_depends_on,
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
}
