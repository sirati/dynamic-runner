//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

/// Helper: minimal `PrimaryConfig` for a 1-secondary in-process test.
fn preferred_secondaries_test_config() -> PrimaryConfig {
    PrimaryConfig {
        num_secondaries: 0,
        connect_timeout: Duration::from_secs(5),
        peer_timeout: Duration::from_secs(5),
        ..test_primary_config()
    }
}

/// Helper: register `secondary_id` in `self.secondaries` at the
/// `Operational` typestate so `broadcast_cold_seed`'s validation sees
/// it as a member of the known-set. The connection's wire-flow fields
/// are inert (no `transport.broadcast` actually crosses them here under
/// the `setup_test(0)` harness's empty outgoing map; the in-process
/// `ClusterMutation` broadcast loops only over registered outgoing
/// senders, of which there are none).
fn register_operational_secondary(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    secondary_id: &str,
) {
    use crate::state::{SecondaryConnection, SecondaryConnectionState};
    let conn = SecondaryConnection::new(secondary_id.into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0, None)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        secondary_id.into(),
        SecondaryConnectionState::Operational(conn),
    );
}

/// `broadcast_cold_seed` walks `self.all_binaries`, finds each task's
/// `preferred_secondaries` list, and emits exactly one
/// `unknown_preferred_secondary` warn per offending id (multiple
/// tasks referencing the same offending id collapse to one warn via
/// the validator's dedup set). Known ids never trigger a warn.
#[tokio::test(flavor = "current_thread")]
async fn cold_seed_warns_on_unknown_preferred_secondary_id() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let (mut primary, _mesh) = build_test_primary(
                preferred_secondaries_test_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Known fleet: secondary-known is operational. secondary-unknown
            // is NOT registered — every task referencing it must fall into
            // the warn path.
            register_operational_secondary(&mut primary, "secondary-known");

            // Two tasks both reference the same unknown id (dedup invariant);
            // a third references a mix of known + unknown; a fourth has only
            // the known id (silent path).
            let mut t1 = make_binary("a", 10);
            t1.preferred_secondaries =
                dynrunner_core::SoftPreferredSecondaries::new(vec!["secondary-unknown".into()]);
            let mut t2 = make_binary("b", 20);
            t2.preferred_secondaries =
                dynrunner_core::SoftPreferredSecondaries::new(vec!["secondary-unknown".into()]);
            let mut t3 = make_binary("c", 30);
            t3.preferred_secondaries = dynrunner_core::SoftPreferredSecondaries::new(vec![
                "secondary-known".into(),
                "secondary-other-unknown".into(),
            ]);
            let mut t4 = make_binary("d", 40);
            t4.preferred_secondaries =
                dynrunner_core::SoftPreferredSecondaries::new(vec!["secondary-known".into()]);
            primary.all_binaries = vec![t1, t2, t3, t4];

            primary.broadcast_cold_seed().await;

            let warned = primary.preferred_secondaries_validator.warned_snapshot();
            // Both unknown ids appear once; the known id is silent.
            assert!(
                warned.contains("secondary-unknown"),
                "validator must record secondary-unknown as warned; got {warned:?}"
            );
            assert!(
                warned.contains("secondary-other-unknown"),
                "validator must record secondary-other-unknown as warned; got {warned:?}"
            );
            assert!(
                !warned.contains("secondary-known"),
                "known id must not be recorded as warned; got {warned:?}"
            );
            assert_eq!(
                warned.len(),
                2,
                "exactly two distinct unknown ids → exactly two warned entries; got {warned:?}"
            );

            // Second `broadcast_cold_seed` call is idempotent on the warn
            // dedup set — no new entries land, the same two stay recorded.
            // (The staged seed is empty here, so only the validator path
            // re-runs; it re-evaluates but emits nothing new because the
            // dedup set already holds both ids.)
            primary.broadcast_cold_seed().await;
            let warned_again = primary.preferred_secondaries_validator.warned_snapshot();
            assert_eq!(
                warned, warned_again,
                "second broadcast_cold_seed must not change the warned set \
             (dedup invariant); first={warned:?} second={warned_again:?}"
            );
        })
        .await;
}

/// `handle_cluster_mutation` revalidation: a batch containing
/// `PeerJoined { peer_id: "secondary-late" }` must forget that id
/// from the validator's dedup set AND re-run validation against the
/// post-apply cluster_state task view + the updated known set. After
/// joining `secondary-late`, a task that named only `secondary-late`
/// as its preference is no longer offending — the dedup set drops
/// the entry and the re-validation emits no fresh warn for it.
#[tokio::test(flavor = "current_thread")]
async fn peer_joined_revalidates_preferred_secondaries() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let (mut primary, _mesh) = build_test_primary(
                preferred_secondaries_test_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Start with NO secondaries registered. Seed a task that
            // names `secondary-late` (currently unknown). Validation
            // after seed must warn for it.
            let mut task = make_binary("a", 10);
            task.preferred_secondaries =
                dynrunner_core::SoftPreferredSecondaries::new(vec!["secondary-late".into()]);
            primary.all_binaries = vec![task];
            primary.broadcast_cold_seed().await;
            let warned = primary.preferred_secondaries_validator.warned_snapshot();
            assert!(
                warned.contains("secondary-late"),
                "pre-join validation must warn for the unknown id; got {warned:?}"
            );

            // Now the late peer joins. Register it in `self.secondaries`
            // so the known-set is correct at re-validation time (the
            // post-apply `PeerJoined` re-validation reads from
            // `self.secondaries.keys()`). Then drive
            // `handle_cluster_mutation` with the PeerJoined batch.
            register_operational_secondary(&mut primary, "secondary-late");
            let join = dynrunner_protocol_primary_secondary::DistributedMessage::ClusterMutation {
                target: None,
                sender_id: "setup".into(),
                timestamp: crate::primary::wire::timestamp_now(),
                mutations: vec![
                    dynrunner_protocol_primary_secondary::ClusterMutation::PeerJoined {
                        peer_id: "secondary-late".into(),
                        is_observer: false,
                        can_be_primary: false,
                        cap_version: Default::default(),
                        member_gen: 0,
                    },
                ],
            };
            primary.handle_cluster_mutation(join).await;

            // Re-validation pathway: forget(id) + validate. The id was
            // previously warned but is now in `self.secondaries`, so the
            // re-walk finds it in the known set and does not re-insert
            // into `warned`. Net effect: dedup set no longer contains
            // `secondary-late`.
            let warned_after = primary.preferred_secondaries_validator.warned_snapshot();
            assert!(
                !warned_after.contains("secondary-late"),
                "post-join re-validation must drop the now-known id from the \
             warned set; got {warned_after:?}"
            );
        })
        .await;
}
