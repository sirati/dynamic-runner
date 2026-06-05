//! A co-located secondary observes its own primary's `RunComplete` over
//! the loopback channel AFTER a `SetupPending` yield/re-entry.
//!
//! On a promoted / multi-role host the co-located `PrimaryCoordinator` is
//! not a mesh peer of itself, so its `RunComplete` (and every other
//! `Destination::All` broadcast) reaches the co-located secondary's
//! `process_tasks` loop ONLY through the in-process loopback channel — the
//! QUIC mesh never closes, so the `recv_peer()==None` break cannot fire.
//! The loopback inbound receiver is therefore the SOLE terminal-signal path
//! on such a node.
//!
//! In pre-staged mode the secondary yields `RunOutcome::SetupPending` so the
//! wrapper can run discovery, then re-enters `process_tasks`. The loopback
//! receiver must survive that yield/re-entry: it lives on `OperationalState`
//! (resumable per-run state) and is restored there before the
//! `SetupPending` return, NOT dropped as a fire-once latch. If it were lost
//! on re-entry the loopback arm would park on `pending()` forever and the
//! secondary would never observe `RunComplete` — idling in epoll with its
//! container/process tree leaked.
//!
//! This drives the production path end-to-end: real setup handshake →
//! `Configuring → Operational` → first `process_tasks` seeds the loopback
//! receiver from the coordinator slot into `OperationalState` and yields
//! `SetupPending` → discovery ingest → re-entry re-attaches it from
//! `OperationalState` → `RunComplete` delivered ON THE LOOPBACK →
//! the loop breaks and reaches terminal `Done`. The mesh transport is kept
//! open throughout (the fake primary holds its sender and never closes the
//! channel), so the ONLY way the loop can terminate is the loopback
//! `RunComplete && no_active_tasks` arm — exactly the path the prior
//! `drop(submitter)`→mesh-close e2e could not reach.

#![cfg(test)]

use std::collections::HashMap;

use dynrunner_core::PhaseId;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler::ResourceStealingScheduler;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

use super::super::test_helpers::{
    FakeWorkerFactory, FixedEstimator, TestId, channel_mesh_to_primary, seed_member,
    set_current_primary,
};
use super::super::{RunOutcome, SecondaryConfig, SecondaryCoordinator};
use super::processing::make_binary;

/// A fake primary that drives the secondary through setup in PRE-STAGED
/// mode (so it yields `SetupPending`), then holds the channel open
/// indefinitely. It deliberately never broadcasts `RunComplete` over the
/// mesh — in this scenario the run-over signal arrives on the loopback
/// instead, so the mesh stays alive and the test isolates the loopback exit
/// arm. Returns only when its inbound channel closes (the secondary tore
/// down).
async fn pre_staged_fake_primary(
    secondary_id: String,
    mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    // This scenario models the PROMOTED, co-located node: it is already the
    // recognized `current_primary` (seeded on its mirror below) BEFORE setup
    // runs, so its `SecondaryWelcome` / `CertExchange` resolve to
    // `Destination::Primary == self` → the in-process loopback (CH2), not the
    // wire. The fake primary therefore never sees those frames; it drives the
    // load-bearing setup trio (PeerInfo / InitialAssignment / TransferComplete)
    // straight away and the secondary's `wait_for_setup` consumes them as
    // before.
    to_secondary
        .send(DistributedMessage::PeerInfo {
            sender_id: "primary".into(),
            timestamp: 0.0,
            peers: vec![],
        })
        .unwrap();

    // Pre-staged InitialAssignment: an EMPTY ledger with `pre_staged_mode:
    // true` is the discovery-yield carrier — the secondary will yield
    // `SetupPending` so the wrapper can run `discover_items`.
    to_secondary
        .send(DistributedMessage::InitialAssignment {
            pre_staged_mode: true,
            uses_file_based_items: true,
            sender_id: "primary".into(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            zip_files: vec![],
            workers_ready: vec![],
            staged_files: vec![],
        })
        .unwrap();

    to_secondary
        .send(DistributedMessage::TransferComplete {
            sender_id: "primary".into(),
            timestamp: 0.0,
            total_files: 0,
            total_bytes: 0,
        })
        .unwrap();

    // Drain whatever the secondary emits (task requests, keepalives) and
    // keep the channel alive. We never close `to_secondary` here, so the
    // secondary's mesh `recv_peer` stays pending — the loopback arm is the
    // only reachable exit.
    while from_secondary.recv().await.is_some() {}
}

/// PRE-FIX: hangs (the loopback receiver is `None` on re-entry → the
/// loopback arm parks on `pending()` → the loop never sees `RunComplete`).
/// POST-FIX: the second `run_until_setup_or_done` returns
/// `RunOutcome::Terminal` and the lifecycle records `Done`.
#[tokio::test(flavor = "current_thread")]
async fn colocated_loopback_run_complete_breaks_loop_after_setup_pending_reentry() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = SecondaryConfig {
                secondary_id: "sec-0".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    1024 * 1024 * 1024,
                )]),
                hostname: "test-host".into(),
                // Long keepalive so no liveness/election tick perturbs the
                // bounded run.
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
                retry_max_passes: 1,
                oom_retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                can_be_primary: true,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
            };

            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(pre_staged_fake_primary(
                secondary_id,
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            // Channel-backed mesh with the fake primary folded in as the
            // `"primary"` member; `recv_peer` stays pending while the
            // primary holds its sender, so the mesh never closes.
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut secondary = SecondaryCoordinator::new(
                config,
                unified,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            secondary.set_bootstrap_primary_id("primary".to_string());

            // Co-located loopback inbound (channel CH1): the only path the
            // co-located primary's `RunComplete` can reach this loop. The
            // test owns the sender and plays the primary's broadcast leg.
            let (loopback_tx, loopback_rx) = tokio_mpsc::unbounded_channel();
            secondary.register_colocated_loopback_inbound(loopback_rx);

            // Pre-staged setup-discovery now fires only on the SINGLE
            // designated discoverer once it has BECOME the authority — the
            // promoted, co-located node this scenario models. Seed that node's
            // replicated mirror straight onto `cluster_state` (the CRDT, NOT
            // the FSM-side `apply_primary_changed`, so no real co-located
            // primary build is triggered — this test injects the run-over
            // signal on CH1 directly):
            //   - `seed_member` makes `sec-0` the sole alive, `can_be_primary`,
            //     non-observer worker-secondary → the lowest-id designate;
            //   - `set_current_primary` makes `current_primary == self` → the
            //     authority axis.
            // CH2 (the co-located-primary inbound sender) is wired to a sink so
            // the secondary's `SecondaryWelcome` / `CertExchange`, which now
            // resolve `Destination::Primary` to self → loopback, have a
            // receiver instead of being dropped (the fake primary deliberately
            // does not wait for them — see `pre_staged_fake_primary`).
            seed_member(&mut secondary, "sec-0", true, false);
            set_current_primary(&mut secondary, "sec-0");
            let (colocated_tx, _colocated_rx) = tokio_mpsc::unbounded_channel();
            secondary.register_colocated_primary_inbound(colocated_tx);

            let mut factory = FakeWorkerFactory;

            // First entry: real setup → `Configuring → Operational`; the
            // `process_tasks` take-site seeds the loopback receiver from the
            // coordinator slot into `OperationalState` → the empty pre-staged
            // ledger makes `process_tasks` yield `SetupPending`.
            let first = secondary
                .run_until_setup_or_done(&mut factory)
                .await
                .expect("first run_until_setup_or_done must not error");
            assert!(
                matches!(first, RunOutcome::SetupPending),
                "pre-staged empty ledger must yield SetupPending, got {first:?}",
            );

            // Ingest discovery with ONE real item. This seeds the ledger
            // (clearing `setup_discovery_pending` on the count axis) and
            // latches the fire-once guard, WITHOUT broadcasting/applying a
            // local `RunComplete` — so on re-entry the loop's
            // `cluster_state.run_complete()` is still false and the ONLY way
            // it can terminate is a `RunComplete` arriving over the loopback.
            // (`active_tasks` stays empty: ingest only seeds the CRDT; no
            // worker assignment is dispatched, so `no_active_tasks` holds.)
            let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            deps.insert(PhaseId::from("default"), vec![]);
            secondary
                .ingest_setup_discovery(vec![make_binary("item-0", 1)], deps)
                .await
                .expect("ingest must succeed");

            // The co-located primary's clean-completion broadcast, delivered
            // on the loopback exactly as the egress edge's `Destination::All`
            // leg would (see `primary/coordinator.rs::send_to`'s Broadcast
            // arm). Queued before re-entry; the loopback arm drains it on the
            // second pass.
            //
            // The send result is NOT asserted: if the loopback receiver was
            // dropped across the SetupPending yield (the very defect under
            // test), this send returns `Err(SendError)` because there is no
            // receiver left to deliver to. Either way the load-bearing signal
            // is the re-entry below — it can only reach `Terminal` if the
            // receiver survived and drains this frame.
            let _ = loopback_tx.send(DistributedMessage::ClusterMutation {
                sender_id: "primary".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::RunComplete],
            });

            // Re-entry: with the loopback receiver re-attached from
            // `OperationalState`, the loopback arm drains `RunComplete`, the
            // `run_complete() && no_active_tasks` break fires, and the loop
            // reaches terminal `Done`. PRE-FIX this hangs (loopback `None` on
            // re-entry), so the bounded timeout below is the hang detector.
            let second = tokio::time::timeout(
                Duration::from_secs(5),
                secondary.run_until_setup_or_done(&mut factory),
            )
            .await
            .expect(
                "re-entry timed out: the secondary never observed RunComplete on the loopback \
                 — the loopback receiver was lost across the SetupPending re-entry",
            )
            .expect("second run_until_setup_or_done must not error");

            assert!(
                matches!(second, RunOutcome::Terminal),
                "re-entry must reach Terminal once RunComplete lands on the loopback, got {second:?}",
            );
            assert!(
                matches!(
                    secondary.terminal(),
                    Some(super::super::SecondaryTerminal::Done)
                ),
                "the loopback RunComplete must record the Done terminal",
            );

            // Drop the secondary so the fake primary's inbound closes and
            // its task returns.
            drop(secondary);
            let _ = primary_handle.await;
        })
        .await;
}
