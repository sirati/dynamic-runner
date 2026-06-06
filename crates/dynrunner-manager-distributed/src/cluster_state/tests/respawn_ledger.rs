//! Tests for the replicated respawn ledger (F7) — the grow-only SET
//! `respawn_events` on `ClusterState`.
//!
//! Pins, mirroring the grow-only-MAX / `secondary_capacities` patterns:
//!
//!   - The originator union-inserts an accepted event under its unique
//!     `new_id`; the accessor reads the whole set back.
//!   - The set round-trips through snapshot/restore so a promoted primary
//!     inherits the FULL ledger; the merge is union-by-key, so a stale
//!     peer's snapshot can never REMOVE an event (the property that makes
//!     the respawn admission budget + cooldown survive failover).
//!   - The digest folds the set (count + KEY+VALUE), so a replica that
//!     recorded an event a peer lacks is detected as ahead via `is_behind`,
//!     and the two go quiescent once a snapshot/restore converges them.
//!   - The budget (`should_respawn`) reading the inherited ledger after a
//!     promotion sees the SAME family / total counts + cooldown — the
//!     budget is NOT re-granted on failover.

use super::*;
use crate::cluster_state::RespawnEventRecord;
use crate::primary::respawn::{RespawnBudget, RespawnDecision};
use dynrunner_protocol_primary_secondary::RemovalCause;
use std::time::{Duration, SystemTime};

fn record(original_id: &str, at: SystemTime) -> RespawnEventRecord {
    RespawnEventRecord {
        original_id: original_id.to_string(),
        cause: RemovalCause::KeepaliveMiss,
        at,
    }
}

/// The originator union-inserts; the accessor reads the whole set back. A
/// re-insert of an already-present `new_id` is a no-op (grow-only SET —
/// never mutates a value, never duplicates).
#[test]
fn respawn_event_originate_and_read() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert!(s.respawn_events().is_empty());

    let t0 = SystemTime::UNIX_EPOCH;
    s.record_respawn_event("secondary-1".to_string(), record("secondary-0", t0));
    s.record_respawn_event("secondary-2".to_string(), record("secondary-1", t0));
    assert_eq!(s.respawn_events().len(), 2);
    assert_eq!(
        s.respawn_events().get("secondary-1").unwrap().original_id,
        "secondary-0"
    );

    // Re-insert of an existing key is a no-op: the value is not mutated
    // and the count does not grow (idempotent grow-only SET).
    s.record_respawn_event("secondary-1".to_string(), record("DIFFERENT", t0));
    assert_eq!(s.respawn_events().len(), 2);
    assert_eq!(
        s.respawn_events().get("secondary-1").unwrap().original_id,
        "secondary-0",
        "an idempotent re-insert must not mutate an existing record"
    );
}

/// The ledger round-trips through snapshot/restore so a promoted primary
/// inherits the FULL respawn ledger via union-merge.
#[test]
fn respawn_events_snapshot_round_trip() {
    let mut live = ClusterState::<RunnerIdentifier>::new();
    let t0 = SystemTime::UNIX_EPOCH;
    live.record_respawn_event("secondary-1".to_string(), record("secondary-0", t0));
    live.record_respawn_event("secondary-2".to_string(), record("secondary-1", t0));

    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.restore(live.snapshot());

    assert_eq!(promoted.respawn_events().len(), 2);
    assert_eq!(
        promoted.respawn_events().get("secondary-2").unwrap().original_id,
        "secondary-1"
    );
}

/// Union-by-key merge on restore: a stale peer's snapshot can never REMOVE
/// an event the local replica holds, and a peer's NEW event is unioned in.
/// This is the failover-correctness property — a promotion inherits the
/// ledger, a stale snapshot cannot re-grant the budget by shrinking it.
#[test]
fn respawn_events_restore_is_grow_only_union() {
    let t0 = SystemTime::UNIX_EPOCH;

    let mut local = ClusterState::<RunnerIdentifier>::new();
    local.record_respawn_event("secondary-1".to_string(), record("secondary-0", t0));
    local.record_respawn_event("secondary-2".to_string(), record("secondary-1", t0));

    // A STALE peer holds FEWER events — restore must NOT drop the local
    // events (union never removes).
    let stale = ClusterState::<RunnerIdentifier>::new();
    local.restore(stale.snapshot());
    assert_eq!(
        local.respawn_events().len(),
        2,
        "an empty stale snapshot must not shrink the local ledger"
    );

    // A peer holding a DIFFERENT event unions it in (the set grows).
    let mut ahead = ClusterState::<RunnerIdentifier>::new();
    ahead.record_respawn_event("secondary-3".to_string(), record("secondary-2", t0));
    local.restore(ahead.snapshot());
    assert_eq!(
        local.respawn_events().len(),
        3,
        "a peer's new event must be unioned into the local ledger"
    );
}

/// Digest convergence: a replica that recorded a respawn event a peer
/// lacks is detected as ahead; after the peer pulls + restores, the two
/// are quiescent (neither behind). Mirrors the grow-only-MAX /
/// `secondary_capacities` digest tests.
#[test]
fn respawn_events_digest_detect_then_quiesce() {
    let t0 = SystemTime::UNIX_EPOCH;
    let mut ahead = ClusterState::<RunnerIdentifier>::new();
    ahead.record_respawn_event("secondary-1".to_string(), record("secondary-0", t0));

    let behind = ClusterState::<RunnerIdentifier>::new();
    // The empty replica is behind the one holding an event.
    assert!(behind.digest().is_behind(&ahead.digest()));
    assert!(!ahead.digest().is_behind(&behind.digest()));

    // Pull + restore converges them — quiescent both ways.
    let mut healed = behind;
    healed.restore(ahead.snapshot());
    assert_eq!(healed.digest(), ahead.digest());
    assert!(!healed.digest().is_behind(&ahead.digest()));
    assert!(!ahead.digest().is_behind(&healed.digest()));
}

/// THE F7 failover property: a respawned-family budget is NOT re-granted
/// after promotion. Seed N respawn events into the CRDT (a contiguous
/// family chain + the total), snapshot → restore into a freshly-promoted
/// primary's cluster_state, and assert `should_respawn` against the
/// INHERITED ledger sees the SAME family count, total count, and cooldown
/// it would have on the pre-failover primary — the budget is not reset.
#[test]
fn respawn_budget_survives_promotion() {
    let budget = RespawnBudget {
        max_per_secondary: 3,
        max_total: 10,
        cooldown: Duration::from_secs(30),
    };
    // A deterministic timeline: three deaths in ONE family chain
    // (secondary-0 → 1 → 2 → 3), each respawn's new_id becoming the next
    // death's original_id. `now` is the cooldown reference.
    let base = SystemTime::UNIX_EPOCH;
    let now = base + Duration::from_secs(1000);
    let mut live = ClusterState::<RunnerIdentifier>::new();
    live.record_respawn_event(
        "secondary-1".to_string(),
        record("secondary-0", base + Duration::from_secs(10)),
    );
    live.record_respawn_event(
        "secondary-2".to_string(),
        record("secondary-1", base + Duration::from_secs(20)),
    );
    live.record_respawn_event(
        "secondary-3".to_string(),
        record("secondary-2", base + Duration::from_secs(30)),
    );

    // Pre-failover: the family of `secondary-3` already has 3 events
    // (== max_per_secondary), so a 4th respawn in the chain is rejected.
    assert_eq!(
        budget.should_respawn("secondary-3", live.respawn_events(), now),
        RespawnDecision::RejectFamilyBudget
    );

    // Promote: a fresh primary restores the snapshot (the F1 seeded path).
    let mut promoted = ClusterState::<RunnerIdentifier>::new();
    promoted.restore(live.snapshot());

    // The promoted primary's ledger is byte-identical, so the budget sees
    // the SAME family count — the per-secondary budget is NOT re-granted.
    assert_eq!(
        promoted.respawn_events().len(),
        3,
        "the promoted primary inherits the full ledger"
    );
    assert_eq!(
        budget.should_respawn("secondary-3", promoted.respawn_events(), now),
        RespawnDecision::RejectFamilyBudget,
        "the family budget must NOT be re-granted on failover"
    );

    // Cooldown ALSO survives: a fresh family (no prior events) is admitted,
    // but a request inside the cooldown window of the inherited family's
    // most-recent event (at base+30s) is rejected — proving the cooldown
    // timer did NOT restart on promotion.
    let within_cooldown = base + Duration::from_secs(45); // 15s after the latest event < 30s cooldown
    assert_eq!(
        budget.should_respawn("secondary-3", promoted.respawn_events(), within_cooldown),
        RespawnDecision::RejectFamilyBudget,
        "family budget still binds first; cooldown is the secondary guard"
    );
    // A DIFFERENT family that has not exhausted its per-secondary budget
    // but whose member appears in the inherited ledger's cooldown window:
    // use a fresh family with one inherited event to isolate cooldown.
    let mut cooldown_only = ClusterState::<RunnerIdentifier>::new();
    cooldown_only.record_respawn_event(
        "fam-b-1".to_string(),
        record("fam-b-0", base + Duration::from_secs(30)),
    );
    let mut promoted_b = ClusterState::<RunnerIdentifier>::new();
    promoted_b.restore(cooldown_only.snapshot());
    assert_eq!(
        budget.should_respawn("fam-b-0", promoted_b.respawn_events(), within_cooldown),
        RespawnDecision::RejectCooldown,
        "the inherited cooldown timer must NOT restart on promotion"
    );
    // Past the cooldown window, the same family is admitted again.
    let past_cooldown = base + Duration::from_secs(30) + Duration::from_secs(31);
    assert_eq!(
        budget.should_respawn("fam-b-0", promoted_b.respawn_events(), past_cooldown),
        RespawnDecision::Accept,
        "once the inherited cooldown elapses the family is admissible"
    );
}
