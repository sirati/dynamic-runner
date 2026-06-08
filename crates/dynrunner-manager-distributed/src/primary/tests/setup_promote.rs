//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

/// Pre-seeded bootstrap exit semantics: the counter-based exit at the top
/// of `operational_loop` fires immediately when
/// `completed + failed >= total_tasks && active_workers == 0`. Pins the
/// cold path where `seed_cluster_state` ran locally and `total_tasks` was
/// non-zero at startup.
#[tokio::test(flavor = "current_thread")]
async fn pre_seeded_counter_exit_unchanged() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let (_sec_id, _to_sec_rx, _incoming_tx) = secondary_ends.into_iter().next().unwrap();

            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_millis(50),
                // Pre-seeded bootstrap: `seed_cluster_state` ran locally, so
                // `total_tasks` is set by `run()` from `binaries.len()`
                // and the counter-based exit must fire on the very first
                // iteration once completions cover the total.
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Pre-seeded mid-run state: 2 tasks total, both already in the
            // completed set (mirrors what would normally arrive via
            // TaskComplete handlers). No active workers. The counter
            // check on the first iteration is `2+0 >= 2 && 0 == 0` —
            // must trip immediately.
            let phase = dynrunner_core::PhaseId::from("default");
            let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
                [phase.clone()],
                std::collections::HashMap::new(),
            )
            .expect("default-phase pool");
            primary.pending = Some(pool);
            primary.total_tasks = 2;
            primary.completed_tasks.insert("h-legacy-1".into());
            primary.completed_tasks.insert("h-legacy-2".into());

            // Bounded wait. The counter-check exit should fire on
            // iteration 1 of the loop — well under 1s. A 5s ceiling is
            // overkill but stays consistent with the other operational-
            // loop tests.
            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.operational_loop(),
            )
            .await;

            match exit {
                Ok(Ok(())) => {
                    // Exit path pinning: the pre-seeded counter-based exit
                    // fired, not the `cluster_state.run_complete()` branch.
                    assert!(
                        !primary.cluster_state_for_test().run_complete(),
                        "pre-seeded bootstrap exit must be via the counter check, \
                     not via the cluster_state.run_complete() branch"
                    );
                }
                Ok(Err(e)) => {
                    panic!("operational_loop returned Err in pre-seeded bootstrap scenario: {e}")
                }
                Err(_) => panic!(
                    "pre-seeded bootstrap operational_loop did not exit within 5s \
                 despite the counter check `2+0 >= 2 && active_workers == 0` \
                 being satisfied on the first iteration — regression on the \
                 historical exit semantics"
                ),
            }
        })
        .await;
}

// ────────────────────────────────────────────────────────────────────────
// Bootstrap-relocation: RelocationPolicy + select_relocation_target +
// relocate_primary_to (mesh-always pillar 2 — the primary relocates off the
// submitter onto a compute peer).
// ────────────────────────────────────────────────────────────────────────

/// One advertised-memory `ResourceAmount` vec (the live welcome shape).
fn relocate_mem(bytes: u64) -> Vec<dynrunner_core::ResourceAmount> {
    vec![dynrunner_core::ResourceAmount {
        kind: dynrunner_core::ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Seed a cluster member into the primary's `cluster_state`: an Alive
/// `PeerJoined { is_observer, can_be_primary }` plus a `SecondaryCapacity {
/// worker_count }`. A peer is eligible for relocation iff it is alive AND has
/// worker_count > 0 AND `can_be_primary` AND is NOT an observer — exactly what
/// `select_relocation_target` filters on (observers carry worker_count == 0
/// structurally, so they are excluded by both the worker filter and the
/// explicit observers filter).
fn seed_member<S, E>(
    primary: &mut crate::primary::PrimaryCoordinator<S, E, TestId>,
    id: &str,
    worker_count: u32,
    is_observer: bool,
    can_be_primary: bool,
) where
    S: dynrunner_scheduler_api::Scheduler<TestId>,
    E: dynrunner_scheduler_api::ResourceEstimator<TestId>,
{
    let cs = primary.cluster_state_mut_for_test();
    cs.apply(ClusterMutation::PeerJoined {
        peer_id: id.into(),
        is_observer,
        can_be_primary,
        cap_version: Default::default(),
    });
    cs.apply(ClusterMutation::SecondaryCapacity {
        secondary: id.into(),
        worker_count,
        resources: relocate_mem(8 * 1024 * 1024 * 1024),
    });
}

/// Selection picks the LOWEST-id member of `alive ∩ can_be_primary −
/// observers`. Seed: sec-0 (observer → excluded), sec-1 (eligible), sec-2
/// (eligible, higher id), sec-3 (can_be_primary=false → excluded). The min
/// eligible is `sec-1` (NOT sec-0, which is an observer despite being
/// lowest-id).
#[tokio::test(flavor = "current_thread")]
async fn select_relocation_target_picks_lowest_eligible_compute_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // sec-0: lowest id but an OBSERVER → excluded.
            seed_member(&mut primary, "sec-0", 0, true, false);
            // sec-1: eligible (alive, workers, can_be_primary, not observer).
            seed_member(&mut primary, "sec-1", 2, false, true);
            // sec-2: eligible but a higher id than sec-1.
            seed_member(&mut primary, "sec-2", 2, false, true);
            // sec-3: workers but can_be_primary=false → excluded.
            seed_member(&mut primary, "sec-3", 2, false, false);

            assert_eq!(
                primary.select_relocation_target().as_deref(),
                Some("sec-1"),
                "must pick the LOWEST-id eligible compute peer — sec-0 is an \
                 observer (excluded), sec-3 lacks can_be_primary (excluded), so \
                 sec-1 (< sec-2) wins"
            );
        })
        .await;
}

/// No eligible compute peer → `None` (the caller maps this to a hard
/// `NoRelocationTarget` error under `RelocateToComputePeer`). Seed only an
/// observer and a non-can_be_primary worker — neither is promotable.
#[tokio::test(flavor = "current_thread")]
async fn select_relocation_target_none_when_no_eligible_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            seed_member(&mut primary, "obs-0", 0, true, false);
            seed_member(&mut primary, "sec-0", 2, false, false);
            assert_eq!(
                primary.select_relocation_target(),
                None,
                "an observer + a can_be_primary=false worker leave NO promotable \
                 compute peer; selection must be None so the bootstrap errors \
                 rather than silently staying setup-primary"
            );
        })
        .await;
}

/// Self is excluded from the candidate set even when it advertises a
/// worker-secondary capability under its own id with `can_be_primary`. Seed
/// the primary's own id as an eligible-looking member and one OTHER eligible
/// peer; selection must skip self and pick the other peer.
#[tokio::test(flavor = "current_thread")]
async fn select_relocation_target_excludes_self() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let own_id = config.node_id.clone();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The submitter's own id advertised (defensively) as a
            // worker-secondary that would otherwise be eligible.
            seed_member(&mut primary, &own_id, 2, false, true);
            seed_member(&mut primary, "sec-9", 2, false, true);
            assert_eq!(
                primary.select_relocation_target().as_deref(),
                Some("sec-9"),
                "selection must exclude this primary's OWN id even when it \
                 advertises an eligible-looking worker-secondary capability — \
                 the submitter must never relocate the role to itself"
            );
        })
        .await;
}

/// `relocate_primary_to` originates `PrimaryChanged { new=chosen,
/// reason=Transferred, epoch=primary_epoch()+1 }`, advances the LOCAL
/// `current_primary` to the chosen peer, and does NOT set `primary_id` to
/// self (this host is stepping DOWN, not asserting authority). The broadcast
/// frame reaches the connected secondary verbatim.
#[tokio::test(flavor = "current_thread")]
async fn relocate_primary_to_originates_transferred_to_chosen_not_self() {
    use dynrunner_protocol_primary_secondary::PrimaryChangeReason;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let (_sec_id, mut to_sec_rx, _incoming_tx) =
                secondary_ends.into_iter().next().unwrap();
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The chosen peer is the lowest-id eligible compute peer.
            seed_member(&mut primary, "sec-0", 2, false, true);
            let chosen = primary
                .select_relocation_target()
                .expect("an eligible compute peer was seeded");
            assert_eq!(chosen, "sec-0");

            let epoch_before = primary.cluster_state_for_test().primary_epoch();
            primary.relocate_primary_to(chosen.clone()).await;
            // Let the mesh-pump drain the egress queue onto the wire so the
            // broadcast frame lands on the secondary's inbound channel.
            settle_pump().await;

            // (1) LOCAL apply named the CHOSEN peer the primary (not self).
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some("sec-0"),
                "the local apply of the Transferred PrimaryChanged must advance \
                 current_primary to the chosen peer"
            );
            // (2) `primary_id` is NOT set to self — this host is stepping down.
            assert_ne!(
                primary.primary_id.as_deref(),
                Some(primary.config.node_id.as_str()),
                "relocate must NOT set primary_id=self; the setup is handing the \
                 role away, not asserting it (that is activate_local_primary's job)"
            );
            assert_eq!(
                primary.primary_id, None,
                "relocate leaves primary_id unset — only activate_local_primary \
                 (the stay-local arm) sets it to self"
            );

            // (3) The broadcast frame is the Transferred PrimaryChanged at
            // epoch+1, naming the chosen peer.
            let mut saw_transfer = false;
            while let Ok(msg) = to_sec_rx.try_recv() {
                if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
                    for m in mutations {
                        if let ClusterMutation::PrimaryChanged { new, epoch, reason } = m {
                            assert_eq!(new, "sec-0", "PrimaryChanged must name the chosen peer");
                            assert_eq!(
                                epoch,
                                epoch_before + 1,
                                "relocate epoch must be primary_epoch()+1 (strictly supersede)"
                            );
                            assert!(
                                matches!(reason, PrimaryChangeReason::Transferred),
                                "the relocate reason must be Transferred, not Election"
                            );
                            saw_transfer = true;
                        }
                    }
                }
            }
            assert!(
                saw_transfer,
                "relocate_primary_to must broadcast a PrimaryChanged{{Transferred}} \
                 frame to the connected fleet"
            );
        })
        .await;
}

/// Race tolerance (F6): a concurrent failover election at the SAME `epoch+1`
/// can win the equal-epoch lex tiebreak against this primary's
/// `relocate_primary_to { chosen }`, so the converged `current_primary` is the
/// lex-LOWER winner, NOT `chosen` — `relocate_primary_to` must NOT assert its
/// target won. Both originations independently picked `primary_epoch()+1` from
/// the SAME starting epoch (the concurrency), so they collide at one epoch and
/// the lex tiebreak decides. Drive it order-independently: relocate toward the
/// lex-HIGHER `sec-9` (it originates + applies at epoch E = primary_epoch()+1),
/// then apply the concurrent election naming the lex-LOWER `sec-0` at that SAME
/// epoch E. The CRDT register-adopt rule (`primary_register_adopt`,
/// equal-epoch → lex-lower wins) converges on `sec-0` regardless of which apply
/// lands first.
#[tokio::test(flavor = "current_thread")]
async fn relocate_primary_to_tolerates_concurrent_lex_lower_winner() {
    use dynrunner_protocol_primary_secondary::PrimaryChangeReason;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            seed_member(&mut primary, "sec-0", 2, false, true);
            seed_member(&mut primary, "sec-9", 2, false, true);

            // The relocate originates at E = primary_epoch()+1 and names the
            // lex-HIGHER sec-9. Capture E AFTER the relocate so it reflects the
            // epoch the relocate used.
            let epoch_before = primary.cluster_state_for_test().primary_epoch();
            primary.relocate_primary_to("sec-9".into()).await;
            let collide_epoch = epoch_before + 1;
            assert_eq!(
                primary.cluster_state_for_test().primary_epoch(),
                collide_epoch,
                "the relocate must originate at primary_epoch()+1"
            );
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some("sec-9"),
                "before the concurrent election lands, the relocate named sec-9"
            );

            // The concurrent failover election names the lex-LOWER sec-0 at the
            // SAME epoch E. The equal-epoch lex tiebreak (sec-0 < sec-9) wins,
            // so sec-0 overwrites sec-9 — convergent, and relocate_primary_to
            // adds no logic forcing sec-9 to stay.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: collide_epoch,
                    reason: PrimaryChangeReason::Election,
                });
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some("sec-0"),
                "the equal-epoch lex tiebreak converges on the lex-lower winner \
                 sec-0; relocate must tolerate a DIFFERENT successor than its \
                 target (the Transferred reason is advisory, not asserted)"
            );
        })
        .await;
}

/// `RelocationPolicy::StayLocal` runs the stay-local bootstrap tail
/// (`activate_local_primary` → `run_operational_and_finalize`): a stay-local
/// primary, seeded pre-complete with zero tasks, drives
/// `bootstrap_tail_dispatch` to a clean `Ok(())` and asserts itself the local
/// primary (`primary_id == self`, `current_primary == self`). It must NEVER
/// take the relocate arm even with an eligible compute peer present.
#[tokio::test(flavor = "current_thread")]
async fn relocation_policy_stay_local_activates_local_primary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = test_primary_config();
            let own_id = config.node_id.clone();
            let (mut primary, _mesh) = build_test_primary_with_policy(
                config,
                transport,
                crate::primary::RelocationPolicy::StayLocal,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // An eligible compute peer IS present — a StayLocal policy must
            // still NOT relocate to it.
            seed_member(&mut primary, "sec-0", 2, false, true);

            // Drive only the bootstrap tail directly (no full run): total=0 so
            // the operational loop's counter exit fires immediately.
            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.bootstrap_tail_dispatch(0),
            )
            .await;
            assert!(
                matches!(exit, Ok(Ok(()))),
                "StayLocal bootstrap_tail_dispatch must run the in-place tail to \
                 a clean Ok(()); got {exit:?}"
            );
            assert_eq!(
                primary.primary_id.as_deref(),
                Some(own_id.as_str()),
                "StayLocal must activate THIS node as the local primary \
                 (primary_id == self)"
            );
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some(own_id.as_str()),
                "StayLocal's activate_local_primary must name self the primary"
            );
        })
        .await;
}

/// `RelocationPolicy::RelocateToComputePeer` with an EMPTY candidate set is a
/// hard `RunError::NoRelocationTarget` (pillar 2: the submitter must never
/// stay primary). Seed no eligible compute peer and drive the bootstrap tail.
#[tokio::test(flavor = "current_thread")]
async fn relocation_policy_relocate_empty_candidate_set_is_no_relocation_target() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary_with_policy(
                test_primary_config(),
                transport,
                crate::primary::RelocationPolicy::RelocateToComputePeer,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Only an observer + a non-promotable worker → no eligible peer.
            seed_member(&mut primary, "obs-0", 0, true, false);
            seed_member(&mut primary, "sec-0", 2, false, false);

            let exit = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                primary.bootstrap_tail_dispatch(0),
            )
            .await
            .expect("bootstrap_tail_dispatch must return promptly on the empty-candidate path");
            assert!(
                matches!(exit, Err(crate::primary::RunError::NoRelocationTarget)),
                "RelocateToComputePeer with no eligible compute peer must surface \
                 RunError::NoRelocationTarget, never silently stay local; got {exit:?}"
            );
        })
        .await;
}
