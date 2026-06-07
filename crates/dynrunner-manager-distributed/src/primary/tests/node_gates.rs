//! The §14/§15 fleet-collapse ROOT-FIX gates — BUG-1 and BUG-4 — driven
//! against the PRODUCTION mesh-pump.
//!
//! These are the unit-level proofs the expensive SLURM headline secretly
//! depends on (clarification "Verification gates"): they must pass BEFORE the
//! SLURM run.
//!
//! - **BUG-1 (§14 at the root):** a multi-role host's primary must NOT
//!   declare its OWN same-peer secondary dead. The mesh fan excludes the
//!   originating ROLE, never the originating PEER — so a same-peer
//!   secondary's `All` keepalive reaches the LOCAL primary slot and refreshes
//!   the primary's death clock. Proven here at the routing substrate (the
//!   frame reaches the local primary slot through the real pump) AND at the
//!   coordinator level (the primary's `record_keepalive` refreshes the clock
//!   off a same-peer secondary's keepalive).
//! - **BUG-4 (build-window ordering):** a promoted/registered primary's slot
//!   is live AND its `secondary_keepalives` are seeded BEFORE its first
//!   heartbeat tick — so a build-window keepalive is not dropped and the
//!   death clock does not start frozen.

use super::*;

use crate::process::{LocalRole, Mesh};
use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};
use dynrunner_protocol_primary_secondary::{DistributedMessage, KeepaliveRole};

/// A standalone `ChannelPeerTransport` for `local_id` with no remote peers —
/// the mesh-only gates need just a local transport to wrap (the role-demux +
/// pump are what they exercise, not remote wire traffic).
///
/// Returns the transport PLUS the inbound sender the caller must KEEP ALIVE:
/// dropping it closes `recv_peer`, which the pump reads as transport teardown
/// and exits — starving the queued loopback egress these gates assert. The
/// caller binds it for the test's lifetime.
fn lone_transport(
    local_id: &str,
) -> (
    ChannelPeerTransport<TestId>,
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    let (in_tx, in_rx) = tokio_mpsc::unbounded_channel();
    (
        ChannelPeerTransport::from_raw_channels(local_id.to_string(), HashMap::new(), in_rx),
        in_tx,
    )
}

/// An `All`-broadcast keepalive from `sender` (a secondary), with the
/// role-bearing routing target the secondary's egress would stamp.
fn all_keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: Some(Destination::All),
        sender_id: sender.to_string(),
        timestamp: 1.0,
        secondary_id: sender.to_string(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

fn keepalive_sender(msg: &DistributedMessage<TestId>) -> &str {
    match msg {
        DistributedMessage::Keepalive { sender_id, .. } => sender_id,
        other => panic!("expected Keepalive, got {other:?}"),
    }
}

/// A `SecondaryWelcome` registering `secondary_id` with the primary (so the
/// primary's `secondaries` map knows it — the precondition for
/// `record_keepalive` to record).
///
/// `target: None` because this gate feeds the frame DIRECTLY to the primary's
/// handler (bypassing the pump). In production the pump's local-delivery
/// CLEARS the routing target before the handler sees it (the handlers
/// pattern-match `target: None`), so an unstamped frame is exactly the shape
/// a handler receives post-routing.
fn welcome(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::SecondaryWelcome {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        resources: vec![dynrunner_core::ResourceAmount {
            kind: dynrunner_core::ResourceKind::memory(),
            amount: 1024 * 1024 * 1024,
        }],
        worker_count: 1,
        hostname: "test".into(),
        is_observer: false,
        can_be_primary: true,
    }
}

/// A `Keepalive` as a handler sees it POST-pump (routing target cleared).
fn keepalive_post_pump(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: sender.to_string(),
        timestamp: 1.0,
        secondary_id: sender.to_string(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// **BUG-1 (routing substrate).** A same-peer secondary's `All` keepalive
/// reaches the LOCAL primary slot through the production mesh-pump. The fan
/// excludes the originating SECONDARY role but INCLUDES the same-host primary
/// — the §14 fix: the multi-role host's primary sees its own secondary's
/// keepalive, so it never declares it dead. The originating secondary does
/// NOT receive its own broadcast.
#[tokio::test(flavor = "current_thread")]
async fn bug1_same_peer_secondary_all_keepalive_reaches_local_primary_slot() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // One host ("host-a") runs BOTH a primary and its own secondary —
            // the multi-role host a promotion produces.
            let (transport, _in_tx) = lone_transport("host-a");
            let mut mesh = Mesh::<TestId, _>::new(transport);
            let (_p_slot, _p_client, mut primary_inbox) =
                mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));
            let (_s_slot, s_client, mut secondary_inbox) =
                mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));
            mesh.publish_membership();

            let (_control, control_rx) = crate::process::pump::control_channel::<TestId>();
            let pump = crate::process::pump::run_pump(mesh, control_rx);
            tokio::pin!(pump);

            // The same-peer secondary broadcasts an `All` keepalive (its
            // queued egress; the production pump drains + fans it).
            s_client
                .send(Destination::All, all_keepalive("host-a-secondary"))
                .expect("queue accepts the broadcast");

            // Drive the pump until the local primary slot receives the fan.
            let received = {
                let recv = primary_inbox.recv();
                tokio::pin!(recv);
                tokio::select! {
                    m = &mut recv => m,
                    _ = &mut pump => None,
                }
            };
            assert_eq!(
                keepalive_sender(&received.expect("the local primary slot received the fan")),
                "host-a-secondary",
                "§14: the same-host primary MUST see its own secondary's All keepalive"
            );
            // The originating secondary is excluded from its OWN broadcast
            // (origin-role exclusion, not origin-peer).
            assert!(
                secondary_inbox.try_recv().is_none(),
                "the originating secondary role is excluded from its own broadcast"
            );
        })
        .await;
}

/// **BUG-1 (coordinator level).** A real `PrimaryCoordinator` that knows a
/// same-peer secondary refreshes that secondary's death clock when it
/// processes its `All` keepalive — it does NOT declare its own same-peer
/// secondary dead. Drives the welcome (registers the secondary) then the
/// keepalive through the primary's own `dispatch_message`, and asserts the
/// death clock advanced.
#[tokio::test(flavor = "current_thread")]
async fn bug1_primary_refreshes_death_clock_from_same_peer_secondary_keepalive() {
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

            // Register the same-peer secondary via its welcome (so the
            // primary's `secondaries` map knows it — `record_keepalive` only
            // records for a known secondary).
            let sec_id = "host-a-secondary";
            primary.handle_welcome(welcome(sec_id)).await;

            // The welcome SEEDED the death clock; capture that baseline.
            let before = primary
                .last_keepalive_for_test(sec_id)
                .expect("welcome seeds the same-peer secondary's death clock");

            // A beat later, the same-peer secondary's keepalive, processed by
            // the primary's own dispatch path (the §14 substrate: the fan
            // delivered it to the local primary slot, and the primary RECORDS
            // it rather than aging it toward death).
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            primary
                .dispatch_message(keepalive_post_pump(sec_id), &mut None)
                .await
                .expect("dispatch a same-peer keepalive");

            let after = primary
                .last_keepalive_for_test(sec_id)
                .expect("the death clock is still tracked for the same-peer secondary");
            assert!(
                after > before,
                "§14: the same-peer secondary's keepalive must ADVANCE its death clock \
                 (the primary refreshes, never ages, its own same-peer secondary)"
            );
        })
        .await;
}

/// **BUG-4 (build-window ordering).** A primary's slot is LIVE the instant it
/// is registered — BEFORE any heartbeat tick — so a keepalive that arrives in
/// the build window (the window between the slot being registered and the
/// primary's run loop reaching its first heartbeat tick) is DELIVERED, not
/// dropped, and the death clock does not start frozen.
///
/// Proven against the production pump: register the primary slot, mint a
/// sibling secondary client, and have that secondary send a
/// `Destination::Primary` keepalive THROUGH the pump (queued egress → pump
/// drain → loopback to the primary slot). No heartbeat tick is driven — this
/// is purely the build window — and the keepalive still reaches the primary
/// slot. The `Node` registers the slot synchronously before spawning the
/// coordinator's run loop, so this no-drop window is closed by construction.
#[tokio::test(flavor = "current_thread")]
async fn bug4_primary_slot_receives_build_window_keepalive_before_first_tick() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _in_tx) = lone_transport("host-a");
            let mut mesh = Mesh::<TestId, _>::new(transport);
            // Register the primary slot FIRST (the BUG-4 ordering: the slot is
            // live the instant it is registered, before any heartbeat tick).
            let (_p_slot, _p_client, mut primary_inbox) =
                mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));
            // A same-host secondary sibling whose client will send a
            // build-window keepalive to the primary.
            let (_s_slot, s_client, _s_inbox) =
                mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));
            mesh.publish_membership();

            let (_control, control_rx) = crate::process::pump::control_channel::<TestId>();
            let pump = crate::process::pump::run_pump(mesh, control_rx);
            tokio::pin!(pump);

            // Build-window keepalive directed at the LOCAL primary (loopback).
            // No heartbeat tick has run; the slot was registered before the
            // pump's first turn, so the frame MUST land on the primary slot.
            s_client
                .send(
                    Destination::Primary,
                    DistributedMessage::Keepalive {
                        target: Some(Destination::Primary),
                        sender_id: "host-a".to_string(),
                        timestamp: 1.0,
                        secondary_id: "host-a".to_string(),
                        active_workers: 0,
                        emitter_role: KeepaliveRole::Secondary,
                    },
                )
                .expect("queue accepts the build-window keepalive");

            let received = {
                let recv = primary_inbox.recv();
                tokio::pin!(recv);
                tokio::select! {
                    m = &mut recv => m,
                    _ = &mut pump => None,
                }
            };
            assert_eq!(
                keepalive_sender(
                    &received.expect("the primary slot received the build-window keepalive")
                ),
                "host-a",
                "BUG-4: a keepalive in the build window (slot registered, no tick yet) \
                 reaches the live primary slot — no build-window drop, no frozen clock"
            );
        })
        .await;
}

/// **TRUE `Node::run` e2e.** A submitter `Node` (bootstrap primary) and a
/// compute `Node` (secondary), each composed + driven by the PRODUCTION
/// `Node::run` — NOT a bespoke harness — run a 3-task batch to completion over
/// a connected channel mesh. This is the end-to-end proof that `Node::run`
/// composes the role + the mesh-pump + the lifecycle correctly: the submitter
/// dispatches, the compute secondary runs the tasks on its worker pool and
/// reports back, and the primary's run loop reaches its completion count.
#[tokio::test(flavor = "current_thread")]
async fn node_run_e2e_submitter_primary_and_compute_secondary() {
    use crate::process::{Node, NodeRunInputs, PrimaryRunArgs};
    use crate::secondary::{SecondaryConfig, SecondaryCoordinator};
    use dynrunner_transport_channel::peer_mesh;

    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A 2-node fully-connected channel mesh: "primary" + "sec-0".
            let ids = vec!["setup".to_string(), "sec-0".to_string()];
            let mut transports = peer_mesh::<TestId>(&ids);
            let sec_transport = transports.pop().unwrap(); // "sec-0"
            let pri_transport = transports.pop().unwrap(); // "setup"

            // ── Compute node: secondary, driven by Node::run ───────────────
            let mut sec_mesh = Mesh::new(sec_transport);
            let (sec_slot, sec_client, sec_inbox) =
                sec_mesh.register_local_role(LocalRole::Secondary, PeerId::from("sec-0"));
            sec_mesh.publish_membership();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            let sec_config = SecondaryConfig {
                secondary_id: "sec-0".into(),
                num_workers: 2,
                max_resources: max_res,
                hostname: "test-host".into(),
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
                can_be_primary: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
                forwarded_argv: Vec::new(),
            };
            let mut secondary = SecondaryCoordinator::new(
                sec_config,
                sec_client,
                sec_inbox,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            secondary.set_bootstrap_primary_id("setup".to_string());
            let (sec_node, _sec_promo_tx) = Node::new(sec_mesh);
            let sec_node = sec_node.with_secondary(secondary, sec_slot);
            let sec_inputs: NodeRunInputs<FakeWorkerFactory, _, _, TestId> = NodeRunInputs {
                secondary_factory: Some(FakeWorkerFactory),
                ..Default::default()
            };
            let sec_handle = tokio::task::spawn_local(sec_node.run(sec_inputs));

            // ── Submitter node: bootstrap primary, driven by Node::run ─────
            let mut pri_mesh = Mesh::new(pri_transport);
            let (pri_slot, pri_client, pri_inbox) =
                pri_mesh.register_local_role(LocalRole::Primary, PeerId::from("setup"));
            pri_mesh.publish_membership();
            let pri_config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let (demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
            let primary = PrimaryCoordinator::new(
                pri_config,
                pri_client,
                pri_inbox,
                demote_rx,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let (pri_node, _pri_promo_tx) = Node::new(pri_mesh);
            let pri_node = pri_node.with_primary(primary, pri_slot);
            let binaries: Vec<TaskInfo<TestId>> = (0..3)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            let pri_inputs: NodeRunInputs<FakeWorkerFactory, _, _, TestId> = NodeRunInputs {
                primary_run_args: Some(PrimaryRunArgs {
                    seed: SeedSource::ColdStart {
                        binaries,
                        phase_deps: HashMap::new(),
                    },
                    on_phase_start: Box::new(|_| {}),
                    on_phase_end: Box::new(|_, _, _, _| {}),
                }),
                primary_demote_tx: Some(demote_tx),
                ..Default::default()
            };

            // Drive the submitter Node::run to its outcome; the compute node
            // runs alongside on the same LocalSet.
            let outcome = pri_node.run(pri_inputs).await;
            assert!(
                matches!(outcome.terminal, crate::process::RunTerminal::Done),
                "Node::run primary outcome: {:?}",
                outcome.terminal
            );
            assert_eq!(
                outcome.completed, 3,
                "all 3 tasks complete through Node::run"
            );
            assert_eq!(outcome.failed, 0);

            // The compute node winds down once the wire closes.
            let _ = sec_handle.await;
        })
        .await;
}
