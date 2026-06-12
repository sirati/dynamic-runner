//! Production replay (asm-tokenizer run_20260611_005927): the pre-primary
//! wait must survive a bootstrap wire that is ABSENT or LOSSY at boot.
//!
//! In that run the chosen primary wedged and the other three secondaries
//! spent the FULL `unconfigured_deadline` (10 min) producing only
//! bootstrap + WSS dial-churn lines: the blocking bring-up dial parked
//! each node BEFORE the coordinator existed, so the setup-wait's digest
//! beacon, its 60s stall WARN, and its digest/snapshot heal arms were all
//! structurally unreachable — the node was deaf (no acceptor) and mute
//! (no beacon) for the whole window, then exited cold.
//!
//! The structural fix moves the ENTIRE pre-primary wait into the
//! coordinator's `wait_for_setup`: the bootstrap dial becomes a
//! background transport concern (the wire folds in whenever it lands)
//! and the welcome/cert handshake becomes a RETRYING `wait_for_setup`
//! arm — a no-route send is absorbed and retried on a capped backoff
//! (give-up stays owned by the unconfigured-deadline), and the retry
//! stays armed until the WHOLE setup trio has landed (a re-welcome is
//! the trio-retransmit request the primary's duplicate-welcome re-serve
//! answers — run_20260612_105712).
//!
//! Two pins:
//!
//! - `no_route_boot_keeps_waiting_and_beacons`: a secondary whose
//!   bootstrap wire is DOWN at `run` entry must not abort on the
//!   handshake's no-route — it stays in the setup-wait, emitting its
//!   anti-entropy digest beacon on the jittered cadence so any peer
//!   that reaches it (or that it reaches once the background dial
//!   lands) can identify and heal it.
//!   REVERT-CHECK: pre-fix the run future returns
//!   `Err("no route to setup: …")` immediately — the once-or-die
//!   handshake aborts the node the instant the wire is not up.
//!
//! - `lost_welcome_retried_until_primary_responds`: the wire is up but
//!   the first `SecondaryWelcome` is LOST (the production dying-wire
//!   shape — queued egress dies with the wire). The handshake must be
//!   re-sent on the retry cadence; the primary's welcome handling is
//!   idempotent, so the retry enrolls the node.
//!   REVERT-CHECK: pre-fix the welcome is sent exactly once and the
//!   node wedges in the trio-wait until the deadline.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, channel_mesh_to_primary, make_secondary_channel,
    make_secondary_recording_with_membership, start_secondary_pump,
};
use super::super::*;
use dynrunner_protocol_primary_secondary::MessageType;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

fn resilience_config(secondary_id: &str) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        // Short keepalive ⇒ short handshake-retry backoff floor, so the
        // paused-clock tests observe retries within seconds of virtual
        // time.
        keepalive_interval: Duration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 3,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        primary_silence_backstop: Duration::from_secs(120),
        // LONG so a pre-fix run wedges/aborts visibly (the test's own
        // bounded select fails first) instead of exiting through the
        // deadline.
        unconfigured_deadline: Duration::from_secs(600),
        can_be_primary: true,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    }
}

/// THE run_20260611_005927 boot shape, one layer up: the bootstrap wire
/// is not up when the coordinator enters its run loop (production: the
/// background dial is still churning against a dead tunnel). The
/// handshake's no-route must be absorbed — the node keeps waiting under
/// the unconfigured-deadline and its anti-entropy digest beacon flows on
/// the jittered cadence (the identifying first frame + the divergence
/// advertisement every heal path needs).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn no_route_boot_keeps_waiting_and_beacons() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let config = resilience_config("sec-noroute");
            let (mut harness, log, membership) =
                make_secondary_recording_with_membership(config, 0);
            // The bootstrap wire is DOWN at boot: remove the folded
            // primary ("setup") from the transport membership AND
            // republish the view BEFORE the run starts (the harness mint
            // published a setup-present snapshot), so the egress gate
            // reads `has_route("setup") == false` from the very first
            // welcome send — the production not-yet-dialed shape.
            membership.borrow_mut().retain(|id| id.as_str() != "setup");
            harness.publish_membership();
            let (mut secondary, _guard) = start_secondary_pump(harness);
            secondary.set_bootstrap_primary_id("setup".to_string());

            // Let the setup-wait run for several beacon periods of
            // virtual time, then assert the digest cadence flowed.
            let driver = async {
                tokio::time::sleep(Duration::from_secs(150)).await;
                let digests = log
                    .borrow()
                    .iter()
                    .filter(|m| matches!(m.msg_type(), MessageType::StateDigest))
                    .count();
                assert!(
                    digests >= 2,
                    "a no-route-boot secondary must keep beaconing its \
                     anti-entropy digest from the setup-wait (saw {digests} \
                     digest broadcasts in 150s — the run_20260611_005927 \
                     beacon-dark shape)"
                );
            };

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    panic!(
                        "the setup-wait must keep waiting when the bootstrap \
                         wire is down at boot (the background dial owns wire \
                         recovery; the unconfigured-deadline owns give-up) — \
                         got {res:?}"
                    );
                }
                () = driver => { /* beacon observed while still waiting */ }
            }
        })
        .await;
}

/// The dying-wire welcome loss: the wire is up, but the primary never
/// sees the FIRST `SecondaryWelcome` (production: the frame was queued
/// onto a wire that died before delivery; the redial later restored the
/// pipe but nothing re-sent the welcome). The handshake retry must
/// re-send it; observing the SECOND welcome on the wire is the
/// discriminator.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn lost_welcome_retried_until_primary_responds() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            // Held open so the bootstrap wire stays up; this fake primary
            // simply never ACTS on the first welcome (the loss is
            // modelled at the receiver).
            let _hold_primary_inbound = pri_to_sec_tx;

            let config = resilience_config("sec-lostwelcome");
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard) = start_secondary_pump(harness);

            let driver = async {
                let mut welcomes = 0u32;
                // PERSISTENT wedge deadline (the fires-under-load law —
                // this very test's first draft armed a per-iteration
                // sleep and the secondary's own ~20s digest beacon reset
                // it forever): the panic window is fixed at arming and
                // only a second welcome exits before it.
                let wedge_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
                loop {
                    let frame = tokio::select! {
                        f = sec_to_pri_rx.recv() => f.expect("wire open"),
                        _ = tokio::time::sleep_until(wedge_deadline) => panic!(
                            "LOST-WELCOME WEDGE (the run_20260611_005927 \
                             enrolment gap): the secondary never re-sent its \
                             SecondaryWelcome — a welcome lost on a dying \
                             wire permanently un-enrolls the node even after \
                             the redial restores the pipe (saw {welcomes} \
                             welcome(s))"
                        ),
                    };
                    if matches!(frame.msg_type(), MessageType::SecondaryWelcome) {
                        welcomes += 1;
                        if welcomes >= 2 {
                            break; // the retry reached the wire
                        }
                    }
                }
            };

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    panic!("setup wait must keep waiting pre-announce, got {res:?}");
                }
                () = driver => { /* second welcome observed on the wire */ }
            }
        })
        .await;
}

/// THE asm-dataset LMU 5-of-15 welcome-loss replay: the secondary's first
/// welcome NEVER reaches the primary, and BEFORE its first retry can fire
/// it receives a BROADCAST from the primary (the `PeerJoined`
/// ClusterMutation the primary originates when it accepts some OTHER
/// secondary's welcome — production 11:06:15, "primary announced (first
/// setup frame)"). Receiving a broadcast proves NOTHING about this
/// node's welcome having landed, so the retry must stay armed and the
/// welcome must be RE-SENT.
///
/// REVERT-CHECK (the production bug): pre-fix the retry arm was gated on
/// the lifecycle still being `AwaitingPrimary`, and the broadcast fires
/// the `AwaitingPrimary → Configuring` announce — the arm disarmed off a
/// broadcast, the welcome was never re-sent, the primary never learned of
/// this node, and 600s later it proceeded with quorum
/// (missing_no_welcome=[0,1,2,4,10]) into a fleet whose setup deadlines
/// had already killed it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn broadcast_announce_does_not_disarm_welcome_retry() {
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation as CM;
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = resilience_config("sec-bcast-disarm");
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard) = start_secondary_pump(harness);

            let driver = async {
                // The first welcome reaches the wire (and is "lost" — this
                // fake primary never acts on it).
                loop {
                    let frame = sec_to_pri_rx.recv().await.expect("wire open");
                    if matches!(frame.msg_type(), MessageType::SecondaryWelcome) {
                        break;
                    }
                }
                // The primary's broadcast announce: a `PeerJoined` for a
                // SIBLING secondary (originated by the sibling's accepted
                // welcome) fanned to All. It reaches this secondary at
                // virtual t≈0 — strictly BEFORE its first retry (the
                // keepalive-derived 100ms backoff floor) can fire, the
                // exact production interleaving.
                pri_to_sec_tx
                    .send(DistributedMessage::ClusterMutation {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        mutations: vec![CM::PeerJoined {
                            peer_id: "sec-other".into(),
                            is_observer: false,
                            can_be_primary: true,
                            cap_version: Default::default(),
                            member_gen: 0,
                        }],
                    })
                    .expect("inbound open");

                // The discriminator: a SECOND welcome must still reach the
                // wire (the retry survived the broadcast). The wedge
                // deadline is PERSISTENT (fires-under-load law).
                let wedge_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
                loop {
                    let frame = tokio::select! {
                        f = sec_to_pri_rx.recv() => f.expect("wire open"),
                        _ = tokio::time::sleep_until(wedge_deadline) => panic!(
                            "BROADCAST-DISARM (the asm-dataset LMU 5-of-15 \
                             welcome loss): receiving the primary's broadcast \
                             announce disarmed the welcome retry — the \
                             welcome was never re-sent and the primary can \
                             never learn of this node"
                        ),
                    };
                    if matches!(frame.msg_type(), MessageType::SecondaryWelcome) {
                        break; // the retry survived the broadcast
                    }
                }
            };

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    panic!("setup wait must keep waiting for the trio, got {res:?}");
                }
                () = driver => { /* post-broadcast welcome observed */ }
            }
        })
        .await;
}

/// The retry-persistence pin (the run_20260612_105712 wedge): a DIRECTED
/// setup frame landing (`InitialAssignment`) proves the welcome was
/// received, but it must NOT disarm the handshake retry while the trio
/// is still incomplete — the re-welcome is the trio-retransmit request
/// the primary's duplicate-welcome re-serve answers. Pre-fix the retry
/// disarmed on `got_assignment`/`got_transfer`, so a member whose roster
/// broadcast was lost to the leg-registration race sat at
/// `got_peer_info=false` forever with NO recovery channel (zero
/// keepalives → silence-judged dead at ~124s). Retries end with the
/// trio: the gate releasing exits the wait, and with it the retry arm.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn partial_trio_keeps_welcome_retry_armed() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = resilience_config("sec-partial-trio");
            let secondary_id = config.secondary_id.clone();
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard) = start_secondary_pump(harness);

            let driver = async {
                // Welcome observed → answer with the DIRECTED halves of
                // the trio (empty InitialAssignment + TransferComplete —
                // the production replacement received exactly these two;
                // the roster broadcast was lost).
                loop {
                    let frame = sec_to_pri_rx.recv().await.expect("wire open");
                    if matches!(frame.msg_type(), MessageType::SecondaryWelcome) {
                        break;
                    }
                }
                pri_to_sec_tx
                    .send(DistributedMessage::InitialAssignment {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        secondary_id: secondary_id.clone(),
                        zip_files: Vec::new(),
                        workers_ready: Vec::new(),
                        staged_files: Vec::new(),
                        pre_staged_mode: false,
                        uses_file_based_items: true,
                    })
                    .expect("inbound open");
                pri_to_sec_tx
                    .send(DistributedMessage::TransferComplete {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        total_files: 0,
                        total_bytes: 0,
                    })
                    .expect("inbound open");

                // The discriminator: with `got_peer_info` still false, a
                // FURTHER welcome must reach the wire (the retry stayed
                // armed past the directed halves). The wedge deadline is
                // PERSISTENT (fires-under-load law).
                let wedge_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
                loop {
                    let frame = tokio::select! {
                        f = sec_to_pri_rx.recv() => f.expect("wire open"),
                        _ = tokio::time::sleep_until(wedge_deadline) => panic!(
                            "PARTIAL-TRIO DISARM (run_20260612_105712): the \
                             directed trio halves disarmed the welcome retry \
                             with got_peer_info still false — the lost roster \
                             has no retransmit channel and the member wedges \
                             until it is silence-judged dead"
                        ),
                    };
                    if matches!(frame.msg_type(), MessageType::SecondaryWelcome) {
                        break; // the retry survived the partial trio
                    }
                }
            };

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    panic!("setup wait must keep waiting for the trio, got {res:?}");
                }
                () = driver => { /* post-partial-trio welcome observed */ }
            }
        })
        .await;
}
