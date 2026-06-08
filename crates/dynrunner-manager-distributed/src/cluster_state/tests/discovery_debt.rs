//! `DiscoveryDebt` CRDT semantics (V6): the per-run sticky-monotone
//! "discovery owed" THREE-state lattice `Undeclared ⊑ Owed ⊑ Settled`.
//!
//! Pins:
//!   * the default is `Undeclared` (the lattice BOTTOM — every legacy /
//!     cold mode-1 run is `!= Owed`, so no existing path is affected);
//!   * the apply rule is sticky-monotone — `DiscoveryDebtDeclared` is
//!     Applied ONLY from `Undeclared` (→ `Owed`); `DiscoverySettled`
//!     ratchets to `Settled`; a `DiscoveryDebtDeclared` AFTER `Settled`
//!     (or `Owed`) is a NoOp (the monotonicity lives in the apply rule,
//!     not the wire, so a reordered/redelivered `Declared` can never drag
//!     a run back DOWN the lattice);
//!   * the snapshot/restore join is `max` — a higher snapshot ratchets a
//!     lower local up, and a lower snapshot never overwrites a higher
//!     local (an `Owed` snapshot does NOT overwrite a local `Settled`; a
//!     `Settled` snapshot ratchets a local `Owed`);
//!   * a pre-field snapshot (no `discovery_debt` key) decodes `Undeclared`.

use super::*;
use crate::cluster_state::DiscoveryDebt;

#[test]
fn fresh_state_is_undeclared_by_default() {
    // Every cold mode-1 / legacy run is `Undeclared` (the lattice BOTTOM)
    // from t0 — it never owes discovery (`!= Owed`), so no existing path is
    // affected.
    let s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(s.discovery_debt(), DiscoveryDebt::Undeclared);
}

#[test]
fn declared_sets_owed_settled_ratchets_and_declare_after_settle_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // Declare: Undeclared → Owed (Applied, the first declare from the
    // lattice BOTTOM).
    assert_eq!(
        s.apply(ClusterMutation::DiscoveryDebtDeclared),
        ApplyOutcome::Applied
    );
    assert_eq!(s.discovery_debt(), DiscoveryDebt::Owed);

    // Re-declare while already Owed is a NoOp (monotone — Declared only
    // fires from Undeclared).
    assert_eq!(
        s.apply(ClusterMutation::DiscoveryDebtDeclared),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.discovery_debt(), DiscoveryDebt::Owed);

    // Settle: Owed → Settled (Applied iff it changed).
    assert_eq!(
        s.apply(ClusterMutation::DiscoverySettled),
        ApplyOutcome::Applied
    );
    assert_eq!(s.discovery_debt(), DiscoveryDebt::Settled);

    // A duplicate / re-broadcast Settle is idempotent (NoOp).
    assert_eq!(
        s.apply(ClusterMutation::DiscoverySettled),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.discovery_debt(), DiscoveryDebt::Settled);

    // THE monotonicity pin: a `Declared` that arrives AFTER `Settled`
    // (reordered / redelivered wire) is a NoOp — it can NEVER drag the run
    // back DOWN the lattice. This is the case the distinct `Undeclared`
    // BOTTOM exists to disambiguate from the cold first-declare above.
    assert_eq!(
        s.apply(ClusterMutation::DiscoveryDebtDeclared),
        ApplyOutcome::NoOp
    );
    assert_eq!(
        s.discovery_debt(),
        DiscoveryDebt::Settled,
        "a Declared after Settled must NOT re-arm the debt"
    );
}

#[test]
fn settled_ratchet_survives_snapshot_restore() {
    // A run that owed discovery and then settled it must carry `Settled`
    // through a snapshot → restore (the promotion path) so the promoted
    // primary inherits "discovery done" and does NOT re-run discovery.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::DiscoveryDebtDeclared);
    s.apply(ClusterMutation::DiscoverySettled);
    assert_eq!(s.discovery_debt(), DiscoveryDebt::Settled);

    let mut relocated = ClusterState::<RunnerIdentifier>::new();
    relocated.restore(s.snapshot());
    assert_eq!(
        relocated.discovery_debt(),
        DiscoveryDebt::Settled,
        "Settled must survive snapshot/restore (no re-discovery on failover)"
    );
}

#[test]
fn owed_snapshot_ratchets_an_undeclared_local() {
    // The convergence the single-bool digest could NOT carry: a replica
    // that never saw the live `Declared` (still `Undeclared`) must learn
    // `Owed` from a snapshot pull (the `max`-join ratchets it up). Without
    // this a promoted-from-Undeclared primary reads `!= Owed` → skips
    // discovery → the mode-2 run stalls / false-completes.
    let mut undeclared_local = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(undeclared_local.discovery_debt(), DiscoveryDebt::Undeclared);

    let mut owed_peer = ClusterState::<RunnerIdentifier>::new();
    owed_peer.apply(ClusterMutation::DiscoveryDebtDeclared);

    undeclared_local.restore(owed_peer.snapshot());
    assert_eq!(
        undeclared_local.discovery_debt(),
        DiscoveryDebt::Owed,
        "an Owed snapshot ratchets an Undeclared local up to Owed"
    );
}

#[test]
fn owed_snapshot_does_not_overwrite_local_settled() {
    // The `max`-join: a stale peer still carrying `Owed` must NOT drag a
    // locally-`Settled` replica back DOWN to `Owed` (Settled is the TOP).
    let mut local = ClusterState::<RunnerIdentifier>::new();
    local.apply(ClusterMutation::DiscoveryDebtDeclared);
    local.apply(ClusterMutation::DiscoverySettled);
    assert_eq!(local.discovery_debt(), DiscoveryDebt::Settled);

    // A snapshot from a replica that is still `Owed` (declared, not yet
    // settled).
    let mut owed_peer = ClusterState::<RunnerIdentifier>::new();
    owed_peer.apply(ClusterMutation::DiscoveryDebtDeclared);
    assert_eq!(owed_peer.discovery_debt(), DiscoveryDebt::Owed);

    local.restore(owed_peer.snapshot());
    assert_eq!(
        local.discovery_debt(),
        DiscoveryDebt::Settled,
        "an Owed snapshot must NOT overwrite a local Settled"
    );
}

#[test]
fn settled_snapshot_ratchets_a_local_owed() {
    // The partition-heal direction: a peer that has `Settled` ratchets a
    // local `Owed → Settled` on restore (both replicas converge to
    // `Settled` regardless of pull direction).
    let mut local = ClusterState::<RunnerIdentifier>::new();
    local.apply(ClusterMutation::DiscoveryDebtDeclared);
    assert_eq!(local.discovery_debt(), DiscoveryDebt::Owed);

    let mut settled_peer = ClusterState::<RunnerIdentifier>::new();
    settled_peer.apply(ClusterMutation::DiscoveryDebtDeclared);
    settled_peer.apply(ClusterMutation::DiscoverySettled);

    local.restore(settled_peer.snapshot());
    assert_eq!(
        local.discovery_debt(),
        DiscoveryDebt::Settled,
        "a Settled peer ratchets a local Owed to Settled (convergence)"
    );
}

/// Backward-compat: a snapshot from a sender that PREDATES the
/// `discovery_debt` field (its JSON omits the key) must decode as
/// `Undeclared` (`#[serde(default)]`) — the never-declared BOTTOM — not a
/// missing-field error. Mirrors the legacy wire BYTES, and `Undeclared`
/// loses to any peer's higher state so a legacy snapshot never drags a
/// declared run down.
#[test]
fn legacy_snapshot_without_discovery_debt_decodes_undeclared() {
    let legacy = serde_json::json!({
        "tasks": {},
        "current_primary": "primary-x",
        "primary_epoch": 4,
        "phase_deps": {},
        "peer_holdings": {},
        "task_outputs": {},
        "secondary_capacities": {},
        "alive_members": [],
        "run_complete": false,
        "run_aborted": null
    });
    let decoded: crate::cluster_state::ClusterStateSnapshot<RunnerIdentifier> =
        serde_json::from_str(&legacy.to_string()).unwrap();
    assert_eq!(
        decoded.discovery_debt,
        DiscoveryDebt::Undeclared,
        "a pre-field snapshot must decode discovery_debt as Undeclared (BOTTOM)"
    );
}
