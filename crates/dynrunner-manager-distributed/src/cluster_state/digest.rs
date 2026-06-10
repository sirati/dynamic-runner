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
            current_primary,
            primary_epoch,
            phase_deps,
            run_complete,
            run_aborted,
            // The discovery-debt latch IS summarised (it is snapshot-healable
            // via the sticky `max`-join), carried VERBATIM (the full
            // `DiscoveryDebt` value, NOT a bool) into the digest so the
            // detector can compare lattice height in BOTH directions. A bool
            // is provably insufficient for a 3-state lattice: it would map
            // `Undeclared` and `Settled` to the same `false`, so a replica
            // that missed the `Declared` (Undeclared) could never detect it
            // is behind an `Owed` peer and would never pull — silent
            // non-convergence (the mode-2 stall). The scalar enum + `Ord`
            // compare in `is_behind` carries all three states.
            discovery_debt,
            // The role-CAPABILITY 2P-set IS summarised (C6): it is a
            // proper CRDT (merged monotonically in `restore`), so folding
            // it is detect-WITH-heal — a flagged divergence is one a
            // snapshot pull's `restore` of the 2P-set actually heals.
            capabilities,
            // ── replicated but DELIBERATELY EXCLUDED from the digest ──
            // `role_table` is a node-local PROJECTION of `capabilities ×
            // peer_state-alive` (rebuilt by `reproject_roles`), so it
            // carries no convergence signal of its own. `peer_state`
            // LIVENESS is intentionally divergent (honest-liveness; each
            // node owns its view) and not snapshot-healable for Dead ids,
            // so it stays excluded (see the classification comment below).
            role_table: _role_table,
            peer_state: _peer_state,
            peer_holdings: _peer_holdings,
            task_outputs,
            secondary_capacities,
            // Replicated grow-only-MAX maps (F4 + P3) — summarised: count +
            // VALUE-folding XOR (the count diverges before convergence, so
            // the fold must see the value, same shape as `task_outputs`).
            phase_event_tallies,
            retry_passes_used,
            unfulfillable_reinject_used,
            // Replicated grow-only SET (F7) — summarised: count + KEY+VALUE
            // XOR-fold (a missing event key is caught by the count; the
            // VALUE is folded too, mirroring the grow-only-MAX shape).
            respawn_events,
            // Replicated grow-only SET of ended phases (#343) — summarised:
            // count + KEY-only XOR-fold (the set carries no values; the
            // key-set identity detects a missing entry, same shape as
            // `secondary_capacities_hash`).
            phases_ended,
            // Replicated custom-message inbox + watermarks (F5) —
            // summarised: count + KEY+VALUE XOR-fold each (the inbox
            // value changes on `Unhandled → Handled` at an equal count,
            // and the watermark value advances at an equal origin
            // count, so both folds must see the value — the grow-max /
            // grow-set shape).
            custom_messages,
            custom_terminal_watermarks,
            // Replicated static phase-graph metadata, but EXCLUDED from the
            // digest: `phase_may_be_empty` is originated in the SAME seed
            // batch as `phase_deps` (both set-once at run start, paired in
            // every originator) and the snapshot restore heals it on the
            // same first-bootstrap-adopt path. So whenever the digest's
            // `phase_deps_hash` flags a graph divergence and the restore
            // pulls, `phase_may_be_empty` converges with it — it carries no
            // INDEPENDENT convergence signal. A `_`-bound exclusion, same
            // rationale as `role_table` (a derived/co-converging field).
            phase_may_be_empty: _phase_may_be_empty,
            // ── node-local: not replicated, carries no convergence signal ──
            // (see the field docs on `ClusterState`; same set `snapshot()`
            // classifies node-local).
            primary_epoch_mirror: _primary_epoch_mirror,
            role_change_hooks: _role_change_hooks,
            lifecycle_tx: _lifecycle_tx,
            matcher_trigger_tx: _matcher_trigger_tx,
            worker_mgmt_tx: _worker_mgmt_tx,
            task_completed_tx: _task_completed_tx,
            // node-local: the originator's per-hash version counter carries
            // no convergence signal (each replica mints its own).
            task_seq: _task_seq,
        } = self;

        // `peer_holdings` is steady-state best-effort metadata
        // reconstructed from live announces and is NOT carried in the
        // digest: it is not a convergence-critical ledger (a stale
        // holdings map self-heals on the next per-peer announce), and
        // including it would add periodic churn without a correctness
        // payoff.
        //
        // `role_table` (the `observers` + `can_be_primary` id sets) is a
        // node-local PROJECTION of `capabilities × peer_state-alive`
        // (rebuilt by `reproject_roles`), so it carries no convergence
        // signal of its OWN — the capability convergence is captured by
        // the `capabilities_hash` below, and the alive composition is
        // node-local liveness. `peer_state` (the alive/dead membership
        // ledger) is EXCLUDED because the alive-set divergence is
        // INTENTIONAL per the honest-liveness design (each node owns its
        // own liveness view), so anti-entropy must NOT force-converge it —
        // it must never resurrect a peer a node correctly buried as dead;
        // and Dead ids are not snapshotted, so a Dead-set divergence is not
        // snapshot-healable. All are bound above so the destructure stays
        // exhaustive; their EXCLUSION is the deliberate classification this
        // comment records. (See `StateDigest::is_behind` for the
        // mirror-image rationale on the detector side.)

        // Tasks: count + order-independent XOR-fold of a per-entry hash
        // that combines the task's wire-hash KEY with the SHARED
        // `hashable_join_key` projection of its state. The fold derives
        // from the SAME `task_join_key` the merge comparator uses, so a
        // divergence the merge would heal is one the digest can see (and
        // vice versa) — a same-key entry that advanced to a stronger state,
        // OR two divergent failure records at equal rank (the version +
        // payload content hash discriminate them, C4), changes the fold.
        let mut tasks_hash = 0u64;
        for (key, state) in tasks {
            tasks_hash ^= hash_one((key, super::merge::hashable_join_key(state)));
        }

        // Per-secondary capacity: count + XOR-fold over the KEYS. Capacity
        // is set-once/static, so the key-set identity detects a missing
        // entry without folding the (equal-by-construction) record value.
        let mut secondary_capacities_hash = 0u64;
        for key in secondary_capacities.keys() {
            secondary_capacities_hash ^= hash_one(key);
        }

        // Keyed-output cache: count + KEY+VALUE-content-hash fold (AE-5).
        // Was key-only; now also folds the output VALUE so a divergent
        // value at an equal key is detected (the apply/restore
        // first-write-wins makes the value equal-by-construction once
        // converged, but a genuine pre-convergence value split is now
        // visible). `TaskOutputs` is `Hash`-able by its content.
        let mut task_outputs_hash = 0u64;
        for (key, value) in task_outputs {
            task_outputs_hash ^= hash_one((key, value));
        }

        // Capabilities: count + XOR-fold over the 2P-set entries (C6),
        // derived from the SHARED `capability_fold` projection so a
        // divergence the merge would heal is one the digest sees. Folds
        // `(id, is_observer, can_be_primary, cap_version, is_departed)`
        // per entry — detect-WITH-heal (the 2P-set merges monotonically in
        // `restore`, so a flagged divergence a snapshot pull resolves).
        let mut capabilities_hash = 0u64;
        for (id, entry) in capabilities {
            capabilities_hash ^= super::merge::capability_fold(id, entry);
        }

        // Grow-only-MAX maps (F4 + P3): count + order-independent XOR-fold
        // of the `(key, value)` PAIRS (folds the VALUE because the count
        // diverges before convergence — same shape as `task_outputs_hash`,
        // so a same-key divergent-count entry is detected by `field_behind`
        // and pulled). The fold rule is spelled once in
        // `grow_max::fold_grow_max`.
        let phase_event_tallies_hash = super::grow_max::fold_grow_max(phase_event_tallies);
        let retry_passes_used_hash = super::grow_max::fold_grow_max(retry_passes_used);
        let unfulfillable_reinject_used_hash =
            super::grow_max::fold_grow_max(unfulfillable_reinject_used);

        // Grow-only SET (F7): count + order-independent XOR-fold of the
        // `(new_id, record)` PAIRS (same KEY+VALUE shape as the grow-only-MAX
        // fold). The fold rule is spelled once in `grow_max::fold_grow_set`.
        let respawn_events_hash = super::grow_max::fold_grow_set(respawn_events);

        // Grow-only SET of ended phases (#343): count + order-independent
        // XOR-fold over the phase ids (key-only — the set carries no
        // values, like `secondary_capacities_hash`).
        let mut phases_ended_hash = 0u64;
        for phase in phases_ended {
            phases_ended_hash ^= hash_one(phase);
        }

        // F5 custom-message inbox: count + KEY+VALUE fold (an
        // `Unhandled → Handled` transition at an equal count changes the
        // fold; the snapshot pull's sticky-latch merge heals the lagging
        // side). Watermarks: the grow-max KEY+VALUE fold.
        let custom_messages_hash = super::grow_max::fold_grow_set(custom_messages);
        let custom_terminal_watermarks_hash =
            super::grow_max::fold_grow_max(custom_terminal_watermarks);

        StateDigest {
            tasks_count: tasks.len() as u64,
            tasks_hash,
            secondary_capacities_count: secondary_capacities.len() as u64,
            secondary_capacities_hash,
            task_outputs_count: task_outputs.len() as u64,
            task_outputs_hash,
            phase_deps_count: phase_deps.len() as u64,
            // CRD-3/D-G: the canonical order-independent content hash of
            // the static phase-dependency graph, so a divergent-but-equal-
            // count graph is detected (the count-only line could not see
            // it). Shares the one `canonical_phase_deps_hash` helper with
            // the restore deterministic merge.
            phase_deps_hash: super::merge::canonical_phase_deps_hash(phase_deps),
            // CRD-2/D-P: the current-primary identity hash, so a same-epoch
            // DIFFERENT-identity split is detectable (a higher epoch is
            // caught by `primary_epoch` alone, but two replicas at the same
            // epoch with different `current_primary` carry the same epoch —
            // only this hash separates them; restore's lower-id-wins then
            // converges both in one round).
            current_primary_hash: hash_one(current_primary),
            capabilities_count: capabilities.len() as u64,
            capabilities_hash,
            primary_epoch: *primary_epoch,
            run_complete: *run_complete,
            run_aborted: run_aborted.is_some(),
            // Discovery-debt lattice height, carried VERBATIM (the full
            // three-state enum, not a bool) so the AE detector can compare
            // it in both directions: `is_behind` is "the peer is STRICTLY
            // higher in the lattice" (`self.discovery_debt <
            // other.discovery_debt`). See `StateDigest::is_behind`.
            discovery_debt: *discovery_debt,
            phase_event_tallies_count: phase_event_tallies.len() as u64,
            phase_event_tallies_hash,
            retry_passes_used_count: retry_passes_used.len() as u64,
            retry_passes_used_hash,
            unfulfillable_reinject_used_count: unfulfillable_reinject_used.len() as u64,
            unfulfillable_reinject_used_hash,
            respawn_events_count: respawn_events.len() as u64,
            respawn_events_hash,
            phases_ended_count: phases_ended.len() as u64,
            phases_ended_hash,
            custom_messages_count: custom_messages.len() as u64,
            custom_messages_hash,
            custom_terminal_watermarks_count: custom_terminal_watermarks.len() as u64,
            custom_terminal_watermarks_hash,
        }
    }
}
