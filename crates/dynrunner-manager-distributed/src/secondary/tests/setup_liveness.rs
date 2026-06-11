//! Production replay (asm-dataset LMU bring-up, 15 secondaries): the
//! pre-`Operational` deadline must measure PRIMARY SILENCE, never
//! slow-fleet assembly.
//!
//! In that run every welcomed, announce-received secondary sat in the
//! trio-wait while the primary spent its FULL 600s `connect_timeout`
//! straggler window waiting for 5 lost welcomes. The secondaries' setup
//! deadlines (600s, armed EARLIER) all fired first — 15/15 exited
//! "setup deadline elapsed despite peers reachable — primary
//! unresponsive" at 11:15:50 — and the primary's 11:16:10 quorum-proceed
//! relocated into a fleet dead for 20 seconds.
//!
//! The structural fix is the re-armable [`super::super::setup_deadline`]:
//! every frame whose sender is the primary (the assembling primary's
//! setup-liveness digest beacon, its `PeerJoined` broadcasts, its
//! directed setup frames) EXTENDS the deadline, so it elapses only after
//! a full `unconfigured_deadline` of true primary silence.
//!
//! Two pins:
//!
//! - `setup_deadline_rearms_on_primary_liveness`: a primary that keeps
//!   broadcasting its digest (alive, assembling) holds the secondary in
//!   the trio-wait far past the configured horizon.
//!   REVERT-CHECK: pre-fix the deadline was a FIXED
//!   `tokio::time::timeout` — the secondary exits at the horizon despite
//!   the live primary, the production fleet death.
//!
//! - the same test's tail: once the primary goes SILENT, the deadline
//!   fires one full horizon later — the dead-primary detection the knob
//!   exists for is preserved, not weakened.
//!
//! - `peer_digest_does_not_extend_setup_deadline`: a NON-primary peer's
//!   digest beacon is NOT primary liveness — a primary-less secondary in
//!   a chatty mesh still exits at its horizon ("setup deadline elapsed
//!   despite peers reachable" stays honest).

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, TestId, channel_mesh_to_primary, make_secondary_channel,
    start_secondary_pump,
};
use super::super::*;
use crate::cluster_state::ClusterState;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

fn liveness_config(secondary_id: &str) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
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
        // SHORT horizon so the paused-clock test observes both the
        // re-arm survival (far past 10s under a live primary) and the
        // true-silence expiry (10s after the last primary frame).
        unconfigured_deadline: Duration::from_secs(10),
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

/// A live, assembling primary (digest beacon flowing) holds the
/// secondary in the trio-wait far past the configured horizon; true
/// primary silence then fires the deadline one full horizon later.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn setup_deadline_rearms_on_primary_liveness() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = liveness_config("sec-liveness");
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard) = start_secondary_pump(harness);

            // The assembling primary's setup-liveness beacon: a digest
            // broadcast from the BOOTSTRAP PRIMARY ("setup") every 5s for
            // 60s of virtual time — 6× the 10s horizon. The digest equals
            // the secondary's own empty replica, so the reconcile arm is
            // a NoOp (no pull churn); only the SENDER identity carries
            // the liveness.
            let feeder = async {
                for _ in 0..12 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    pri_to_sec_tx
                        .send(DistributedMessage::StateDigest {
                            target: None,
                            sender_id: "setup".into(),
                            timestamp: 0.0,
                            digest: ClusterState::<TestId>::new().digest(),
                        })
                        .expect("inbound open");
                }
                // Beacon stops here (t=60): the primary goes silent.
            };

            let mut factory = FakeWorkerFactory;
            let run = secondary.run_until_setup_or_done(&mut factory);
            tokio::pin!(run);

            // Phase 1 — live primary: the run future must NOT finish
            // while the beacon flows (pre-fix it exits Err at t=10s, the
            // production fleet death).
            tokio::select! {
                res = &mut run => panic!(
                    "FLEET-DEATH GEOMETRY (asm-dataset LMU): the setup \
                     deadline fired despite a LIVE primary broadcasting \
                     its setup-liveness beacon — got {res:?}"
                ),
                () = feeder => { /* 60s survived under a live primary */ }
            }

            // Phase 2 — true silence: the deadline must fire ONE horizon
            // after the last primary frame (the dead-primary detection
            // the knob exists for). Generous 30s budget.
            match tokio::time::timeout(Duration::from_secs(30), &mut run).await {
                Ok(Err(e)) => assert!(
                    e.contains("setup deadline") && e.contains("elapsed"),
                    "the silence expiry surfaces the deadline error: {e}"
                ),
                other => panic!(
                    "a silent primary must still expire the setup deadline \
                     one horizon after its last frame; got {other:?}"
                ),
            }
        })
        .await;
}

/// A NON-primary peer's digest beacon must NOT extend the deadline: a
/// primary-less secondary in a chatty mesh exits at its horizon — the
/// "despite peers reachable" exit stays a faithful dead-PRIMARY signal.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn peer_digest_does_not_extend_setup_deadline() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = liveness_config("sec-peer-chatter");
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard) = start_secondary_pump(harness);

            // A SIBLING secondary's digest beacon every 2s, forever. The
            // sender is NOT the primary, so none of these may re-arm the
            // deadline.
            let chatter = async {
                loop {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if pri_to_sec_tx
                        .send(DistributedMessage::StateDigest {
                            target: None,
                            sender_id: "sec-sibling".into(),
                            timestamp: 0.0,
                            digest: ClusterState::<TestId>::new().digest(),
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            };

            let mut factory = FakeWorkerFactory;
            let run = secondary.run_until_setup_or_done(&mut factory);
            tokio::pin!(run);

            // The deadline must fire within ~one horizon (10s) + slack,
            // sibling chatter notwithstanding.
            tokio::select! {
                res = &mut run => match res {
                    Err(e) => assert!(
                        e.contains("setup deadline") && e.contains("elapsed"),
                        "the horizon expiry surfaces the deadline error: {e}"
                    ),
                    other => panic!("expected the deadline Err, got {other:?}"),
                },
                () = chatter => unreachable!("chatter never ends"),
                _ = tokio::time::sleep(Duration::from_secs(40)) => panic!(
                    "sibling-peer chatter kept a primary-less secondary alive \
                     past its setup deadline — peer digests must not read as \
                     primary liveness"
                ),
            }
        })
        .await;
}
