//! Read-only anti-entropy projection of `ClusterState`.
//!
//! Single concern: build a compact [`StateDigest`] fingerprint of the
//! whole replicated ledger so peers can detect divergence cheaply on a
//! periodic cadence. This is a PURE PROJECTION — it reads the same state
//! `snapshot()` reads, with the SAME exhaustive-destructure completeness
//! guard, and produces no mutation. It holds NO merge logic and does not
//! touch the CRDT apply/merge lattice; the actual reconciliation is the
//! existing snapshot RPC + `restore()`. The digest only summarises "what
//! does this replica hold" so the detector ([`StateDigest::is_behind`])
//! can answer "should I pull".
//!
//! Determinism + order-independence: every map/set field folds its
//! entries with XOR over a per-entry `u64` hash, so the result is
//! invariant under iteration order and re-computing it on a converged
//! replica yields the same value. The scalar fields (`primary_epoch`, the
//! run latches) are carried verbatim.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::StateDigest;

use super::ClusterState;
use super::TaskState;

/// Hash a single hashable value to a `u64` via the standard library's
/// default hasher. Stable within a process build; the digest is only ever
/// compared between peers running the SAME binary (the wire protocol is
/// version-locked per run), so cross-build hash-stability is not a
/// requirement — only determinism + order-independence within the run.
fn hash_one<H: Hash>(value: H) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Per-task-state rank used in the task fold so a replica whose entry
/// advanced to a stronger state (e.g. `Pending` → `Completed` for the
/// same key) produces a different fold even at an unchanged count. The
/// ranking mirrors the snapshot merge's `task_state_rank` ordering
/// (Pending < Blocked < InFlight < terminals) — divergence detection only
/// needs the ranks to DIFFER when the states differ, which this provides.
fn task_state_rank<I>(s: &TaskState<I>) -> u8 {
    match s {
        TaskState::Pending { .. } => 0,
        TaskState::Blocked { .. } => 1,
        TaskState::InFlight { .. } => 2,
        TaskState::Completed { .. } => 3,
        TaskState::Failed { .. } => 4,
        TaskState::Unfulfillable { .. } => 5,
        TaskState::InvalidTask { .. } => 6,
    }
}

impl<I: Identifier> ClusterState<I> {
    /// Build a compact anti-entropy [`StateDigest`] of the whole ledger.
    ///
    /// Sibling to [`Self::snapshot`] and bound by the SAME structural-
    /// completeness guard: the exhaustive destructure below (NO `..`)
    /// names every `ClusterState` field, so a future field is a COMPILE
    /// ERROR at this site until the developer classifies it
    /// summarise-vs-node-local. This is the only mechanism that catches a
    /// silently-omitted replicated field escaping the digest (so a peer
    /// could never become "behind" on a field the detector cannot see).
    ///
    /// Read-only: every binding is consumed by a count or an
    /// order-independent fold; nothing is mutated.
    pub fn digest(&self) -> StateDigest {
        // Exhaustive destructure (NO `..` rest pattern) — the structural
        // completeness guard, mirroring `snapshot()`. Every `ClusterState`
        // field is NAMED here. The node-local fields (dispatcher senders,
        // hooks, the epoch mirror) are bound to `_`-prefixed names and
        // deliberately excluded: they are not replicated, so they carry no
        // convergence signal — the SAME classification `snapshot()` makes.
        let ClusterState {
            // ── replicated (summarised) ──
            tasks,
            current_primary: _current_primary,
            primary_epoch,
            phase_deps,
            run_complete,
            run_aborted,
            // ── replicated but DELIBERATELY EXCLUDED from the digest ──
            // (membership/role sets — non-monotone-via-removal and not
            // snapshot-healable; see the classification comment below.)
            role_table: _role_table,
            peer_state: _peer_state,
            peer_holdings: _peer_holdings,
            task_outputs,
            secondary_capacities,
            // ── node-local: not replicated, carries no convergence signal ──
            // (see the field docs on `ClusterState`; same set `snapshot()`
            // classifies node-local).
            primary_epoch_mirror: _primary_epoch_mirror,
            role_change_hooks: _role_change_hooks,
            lifecycle_tx: _lifecycle_tx,
            matcher_trigger_tx: _matcher_trigger_tx,
            worker_mgmt_tx: _worker_mgmt_tx,
            task_completed_tx: _task_completed_tx,
        } = self;

        // `current_primary` is summarised via `primary_epoch` (the
        // epoch+identity move together on the `PrimaryChanged`/restore
        // path; a higher epoch is the monotone divergence signal). It
        // needs no separate digest field. `peer_holdings` is steady-state
        // best-effort metadata reconstructed from live announces and is
        // NOT carried in the digest: it is not a convergence-critical
        // ledger (a stale holdings map self-heals on the next per-peer
        // announce), and including it would add periodic churn without a
        // correctness payoff.
        //
        // `role_table` (the `observers` + `can_be_primary` id sets) and
        // `peer_state` (the alive/dead membership ledger) are EXCLUDED from
        // the digest for the SAME class of reason `peer_holdings` is, for
        // two independent causes:
        //   1. Non-monotone-via-removal + NOT snapshot-healable. The live
        //      apply path REMOVES ids (`PeerRemoved` drops from
        //      `observers`/`can_be_primary`; `SetCanBePrimary(false)`
        //      removes), but `restore()` is additive/sticky — it replaces a
        //      role set only when local is empty (else keeps local) and
        //      inserts alive entries only into VACANT slots (a local `Dead`
        //      is sticky, never resurrected). A pull can therefore never
        //      reconcile a stale extra id, so summarising these would loop a
        //      no-op pull every cadence tick.
        //   2. They converge over their OWN paths. Additions flow via the
        //      live `PeerJoined`/`PeerRemoved`/`SetCanBePrimary` broadcasts
        //      plus the post-mesh `rebroadcast_full_roster` re-emit; and the
        //      alive-set divergence is INTENTIONAL per the honest-liveness
        //      design (each node owns its own liveness view), so anti-
        //      entropy must NOT force-converge it — it must never resurrect
        //      a peer a node correctly buried as dead.
        // All three are bound above so the destructure stays exhaustive;
        // their EXCLUSION from the digest is the deliberate classification
        // this comment records. (See `StateDigest::is_behind` for the
        // mirror-image rationale on the detector side.)

        // Tasks: count + order-independent XOR-fold of a per-entry hash
        // that combines the task's wire-hash KEY with its state-RANK, so a
        // same-key entry that advanced to a stronger state changes the
        // fold even at an unchanged count.
        let mut tasks_hash = 0u64;
        for (key, state) in tasks {
            tasks_hash ^= hash_one((key, task_state_rank(state)));
        }

        // Per-secondary capacity: count + XOR-fold over the KEYS. Capacity
        // is set-once/static, so the key-set identity detects a missing
        // entry without folding the (equal-by-construction) record value.
        let mut secondary_capacities_hash = 0u64;
        for key in secondary_capacities.keys() {
            secondary_capacities_hash ^= hash_one(key);
        }

        // Keyed-output cache: count + XOR-fold over the KEYS (per-key
        // first-write-wins, so the key-set identity detects a missing
        // entry).
        let mut task_outputs_hash = 0u64;
        for key in task_outputs.keys() {
            task_outputs_hash ^= hash_one(key);
        }

        StateDigest {
            tasks_count: tasks.len() as u64,
            tasks_hash,
            secondary_capacities_count: secondary_capacities.len() as u64,
            secondary_capacities_hash,
            task_outputs_count: task_outputs.len() as u64,
            task_outputs_hash,
            phase_deps_count: phase_deps.len() as u64,
            primary_epoch: *primary_epoch,
            run_complete: *run_complete,
            run_aborted: run_aborted.is_some(),
        }
    }
}
