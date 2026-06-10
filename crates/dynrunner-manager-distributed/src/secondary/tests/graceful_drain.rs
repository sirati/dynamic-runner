//! Secondary-side graceful-abort drain exit.
//!
//! Pins (b) of the graceful-abort contract: with the replicated
//! `graceful_abort_requested` latch set and NO local active work, the
//! secondary tears down MID-RUN — before any `RunComplete` — announcing a
//! DELIBERATE self-departure (`PeerRemoved { SelfDeparture }`, the
//! existing graceful-leave path) and exiting cleanly
//! (`SecondaryTerminal::Done`). The departure must never look like a
//! death: the secondary emits NO election traffic (`PromotionVote` /
//! `PromotionConfirm`) and NO `SecondaryFatalError` on the way out, so
//! nothing on the primary side reads the exit as a failover trigger (the
//! respawn suppression for the resulting lifecycle event is pinned on the
//! primary side in `primary::respawn::tests`).

use super::super::test_helpers::{
    FakeWorkerFactory, TestId, channel_mesh_to_primary, make_secondary_channel,
    start_secondary_pump,
};
use super::super::{RunOutcome, SecondaryTerminal};
use super::processing::setup_terminal_config;
use dynrunner_protocol_primary_secondary::{DistributedMessage, MessageType, RemovalCause};
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

/// A fake primary that completes the full setup trio, broadcasts the
/// `GracefulAbortRequested` latch (NO `RunComplete` — the drain exit must
/// fire mid-run), keeps the loop awake with periodic keepalives, and
/// COLLECTS every frame the secondary sends until the secondary drops its
/// end — the test asserts the departure shape on the returned frames.
async fn fake_primary_graceful(
    secondary_id: String,
    mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) -> Vec<DistributedMessage<TestId>> {
    let mut collected = Vec::new();
    // Welcome + cert exchange.
    let (mut got_welcome, mut got_cert) = (false, false);
    while !got_welcome || !got_cert {
        match from_secondary.recv().await {
            Some(msg) => {
                match msg.msg_type() {
                    MessageType::SecondaryWelcome => got_welcome = true,
                    MessageType::CertExchange => got_cert = true,
                    _ => {}
                }
                collected.push(msg);
            }
            None => return collected,
        }
    }
    // The setup trio releases `wait_for_setup` into the operational loop.
    to_secondary
        .send(DistributedMessage::PeerInfo {
            target: None,
            sender_id: "setup".into(),
            timestamp: 0.0,
            peers: vec![],
        })
        .unwrap();
    to_secondary
        .send(DistributedMessage::InitialAssignment {
            target: None,
            pre_staged_mode: false,
            uses_file_based_items: true,
            sender_id: "setup".into(),
            timestamp: 0.0,
            secondary_id,
            zip_files: vec![],
            workers_ready: vec![],
            staged_files: vec![],
        })
        .unwrap();
    to_secondary
        .send(DistributedMessage::TransferComplete {
            target: None,
            sender_id: "setup".into(),
            timestamp: 0.0,
            total_files: 0,
            total_bytes: 0,
        })
        .unwrap();
    // The graceful-abort latch (the freeze) — NOT a run terminal.
    to_secondary
        .send(DistributedMessage::ClusterMutation {
            target: None,
            sender_id: "setup".into(),
            timestamp: 0.0,
            mutations: vec![
                dynrunner_protocol_primary_secondary::ClusterMutation::GracefulAbortRequested,
            ],
        })
        .unwrap();
    // Collect the secondary's outbound until the SelfDeparture
    // announcement lands (the secondary breaks immediately after sending
    // it, so everything it could ever emit precedes it), nudging the
    // operational loop awake with periodic keepalives so the
    // bottom-of-loop drain check re-evaluates even if the latch frame was
    // consumed during the setup wait. NOT until-channel-close: the test
    // harness's pump guard DETACHES (never aborts) the pump task, so the
    // secondary's outbound sender outlives the coordinator and the close
    // would never be observed.
    let is_departure = |msg: &DistributedMessage<TestId>| {
        matches!(
            msg,
            DistributedMessage::ClusterMutation { mutations, .. }
                if mutations.iter().any(|m| matches!(
                    m,
                    dynrunner_protocol_primary_secondary::ClusterMutation::PeerRemoved {
                        cause: RemovalCause::SelfDeparture(_),
                        ..
                    }
                ))
        )
    };
    loop {
        tokio::select! {
            maybe = from_secondary.recv() => {
                match maybe {
                    Some(msg) => {
                        let done = is_departure(&msg);
                        collected.push(msg);
                        if done {
                            // Sweep anything already queued behind the
                            // departure (there should be nothing — the
                            // break follows the announce immediately).
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            while let Ok(extra) = from_secondary.try_recv() {
                                collected.push(extra);
                            }
                            return collected;
                        }
                    }
                    None => return collected,
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(25)) => {
                let _ = to_secondary.send(DistributedMessage::Keepalive {
                    target: None,
                    sender_id: "setup".into(),
                    timestamp: 0.0,
                    secondary_id: "setup".into(),
                    active_workers: 0,
                    emitter_role: dynrunner_protocol_primary_secondary::KeepaliveRole::Primary,
                });
            }
        }
    }
}

/// The drained secondary exits MID-RUN under the latch: clean
/// `SecondaryTerminal::Done`, a deliberate `PeerRemoved { SelfDeparture }`
/// announcement on the wire, and ZERO election / fatal-error traffic.
#[tokio::test(flavor = "current_thread")]
async fn drained_secondary_departs_deliberately_without_election() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = setup_terminal_config();
            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(fake_primary_graceful(
                secondary_id.clone(),
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut secondary = make_secondary_channel(config, unified);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, guard) = start_secondary_pump(secondary);

            let mut factory = FakeWorkerFactory;
            let outcome = secondary
                .run_until_setup_or_done(&mut factory)
                .await
                .expect("the drain exit returns Ok(RunOutcome::Terminal)");
            assert!(
                matches!(outcome, RunOutcome::Terminal),
                "expected RunOutcome::Terminal, got {outcome:?}"
            );
            // Clean exit — the drain is DELIBERATE, never an abort/failure.
            match secondary.terminal() {
                Some(SecondaryTerminal::Done) => {}
                other => panic!("expected SecondaryTerminal::Done, got {other:?}"),
            }
            assert!(
                secondary.cluster_state().graceful_abort_requested(),
                "the latch was applied before the exit"
            );
            assert!(
                !secondary.cluster_state().run_complete(),
                "the exit fired MID-RUN — no RunComplete was ever broadcast"
            );

            // Release the secondary's channel ends so the collector sees
            // the close and returns the frames.
            drop(secondary);
            drop(guard);
            let frames = primary_handle.await.expect("collector task");

            // The deliberate departure announcement is on the wire.
            let self_departures = frames
                .iter()
                .filter(|f| {
                    matches!(
                        f,
                        DistributedMessage::ClusterMutation { mutations, .. }
                            if mutations.iter().any(|m| matches!(
                                m,
                                dynrunner_protocol_primary_secondary::ClusterMutation::PeerRemoved {
                                    id,
                                    cause: RemovalCause::SelfDeparture(reason),
                                    ..
                                } if id == &secondary_id
                                    && reason.as_str().contains("graceful abort")
                            ))
                    )
                })
                .count();
            assert_eq!(
                self_departures, 1,
                "exactly one deliberate SelfDeparture announcement: {frames:?}"
            );

            // No failover machinery fired on the way out: zero election
            // traffic, zero fatal-error reports.
            for f in &frames {
                assert!(
                    !matches!(
                        f.msg_type(),
                        MessageType::PromotionVote
                            | MessageType::PromotionConfirm
                            | MessageType::TimeoutDetected
                            | MessageType::SecondaryFatalError
                    ),
                    "a draining secondary must never emit election / fatal \
                     traffic; saw {:?}",
                    f.msg_type()
                );
            }
        })
        .await;
}
