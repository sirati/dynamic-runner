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
#![allow(dead_code)]

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

/// The FROZEN core of a [`TaskInfo`]: the 13 immutable fields that make
/// up a task's identity + dispatch recipe, EXCLUDING the 3 mutable tail
/// fields the runtime rewrites in place (`preferred_secondaries`,
/// `preferred_version`, `resolved_path`).
///
/// Generic over the identifier type `I` for the same reason `TaskInfo`
/// is. The serde bound mirrors `TaskInfo`'s so the def round-trips on a
/// future def-transfer wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
pub(crate) struct FrozenTaskDef<I> {
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
    /// Index = `TaskDefId.0`. Each entry is shared (`Arc`) so resolving a
    /// def hands out a cheap clone.
    defs: Vec<Arc<FrozenTaskDef<I>>>,
    /// Content hash ([`compute_task_hash`]) → the def's id. The dedup
    /// gate: a re-intern of an already-known hash mints nothing.
    hash_to_id: HashMap<String, TaskDefId>,
    /// `Arc<str>` intern pool: maps an id string to its canonical `Arc`,
    /// so equal phase/type ids across distinct defs share one allocation.
    /// Keyed and valued by the same `Arc<str>` (a get-or-insert returns
    /// the canonical clone).
    str_intern: HashMap<Arc<str>, Arc<str>>,
}

impl<I> Default for TaskDefStore<I> {
    fn default() -> Self {
        Self {
            defs: Vec::new(),
            hash_to_id: HashMap::new(),
            str_intern: HashMap::new(),
        }
    }
}

impl<I> Clone for TaskDefStore<I> {
    fn clone(&self) -> Self {
        Self {
            defs: self.defs.clone(),
            hash_to_id: self.hash_to_id.clone(),
            str_intern: self.str_intern.clone(),
        }
    }
}

impl<I> std::fmt::Debug for TaskDefStore<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskDefStore")
            .field("defs", &self.defs.len())
            .field("hash_to_id", &self.hash_to_id.len())
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

    /// Intern a frozen def under its content `hash`. If the hash is
    /// already known, returns the existing id and mints NOTHING (the
    /// content-addressed dedup gate — the store is append-only and a
    /// hash binds to exactly one def). Otherwise the def's `Arc<str>`-
    /// backed ids (phase/type) are folded through the intern pool so
    /// equal ids share one allocation, the def is pushed, and the new id
    /// is recorded and returned.
    pub(crate) fn intern(&mut self, hash: String, mut frozen: FrozenTaskDef<I>) -> TaskDefId {
        if let Some(&existing) = self.hash_to_id.get(&hash) {
            return existing;
        }
        // Collapse the recurring `Arc<str>` ids onto canonical pool
        // allocations before storing (phase/type only — `identifier: I`
        // is opaque and may not be `Arc<str>`-backed).
        let phase = self.intern_str(frozen.phase_id.as_str());
        frozen.phase_id = PhaseId::new(phase);
        let ty = self.intern_str(frozen.type_id.as_str());
        frozen.type_id = TypeId::new(ty);

        let id = TaskDefId(self.defs.len() as u32);
        self.defs.push(Arc::new(frozen));
        self.hash_to_id.insert(hash, id);
        id
    }

    /// Resolve an id to its shared frozen def. `None` for an id this
    /// store never minted (e.g. one from a replica that is ahead).
    pub(crate) fn resolve(&self, id: TaskDefId) -> Option<&Arc<FrozenTaskDef<I>>> {
        self.defs.get(id.0 as usize)
    }

    /// The id a content `hash` resolves to, if this store has interned it.
    pub(crate) fn id_for_hash(&self, hash: &str) -> Option<TaskDefId> {
        self.hash_to_id.get(hash).copied()
    }

    /// The next id this store would mint (`max id + 1`, i.e. the def
    /// count) — the resume helper that re-anchors id minting after a
    /// restore so a respawned originator never re-uses a live id.
    pub(crate) fn next_id_floor(&self) -> u32 {
        self.defs.len() as u32
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
}
