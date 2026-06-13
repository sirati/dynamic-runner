//! Production replay: the bootstrap relocation `PrimaryChanged` broadcast
//! is LOST on the wire while every secondary is still mid-setup
//! (asm-tokenizer run_20260610_185621 — the leaderless 8+ min wedge).
//!
//! The submitter's relocate (`relocate_primary_to`) is a ONE-SHOT
//! broadcast: when it races a transport outage (the production trace shows
//! the secondaries' bootstrap wires flapping — `bootstrap_redial` kept
//! re-folding the link minutes later) the announcement is gone and NOTHING
//! retransmits it. The relocated submitter (now the standalone observer)
//! keeps broadcasting its anti-entropy `StateDigest` every ~20s — the
//! digest carries `primary_epoch` + `current_primary`, so a pull-and-
//! restore would heal the missed announcement — but a secondary wedged in
//! `wait_for_setup` used to drop `StateDigest` / snapshot-reply frames
//! in the silent `other =>` arm, so the heal was structurally unreachable
//! pre-`Operational`: nobody ever promoted and the fleet idled forever.
//!
//! These tests replay that exact sequence over the channel mesh + the
//! production pump:
//!
//! - `lost_relocation_announcement_heals_via_setup_anti_entropy`: the
//!   CHOSEN secondary (the relocation target) misses the announcement,
//!   then receives the observer's digest. It must pull the snapshot,
//!   restore the primary fact naming ITSELF, and fire the
//!   `PromotionSignal` — from `Configuring`, without ever completing the
//!   setup trio (the setup peer never sends one).
//! - `lost_relocation_announcement_non_chosen_follows_new_primary`: a
//!   NON-chosen secondary heals the same way and converges its mirror on
//!   the new primary (the `bootstrap_redial` re-fold check: the
//!   placeholder must not stay the resolved primary), WITHOUT firing any
//!   promotion — and a later run-terminal digest round still tears it
//!   down cleanly through the loop-head terminal check.
//!
//! REVERT-CHECK: pre-fix both tests time out — the digest frames land in
//! `wait_for_setup`'s drop arm, no `RequestSnapshotStream` is ever sent,
//! no promotion fires, and the secondary sits in the trio-wait exactly
//! like the production fleet.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, TestId, channel_mesh_to_primary, make_secondary_channel,
    start_secondary_pump,
};
use super::super::*;
use crate::cluster_state::ClusterState;
use dynrunner_protocol_primary_secondary::{ClusterMutation, MessageType, PrimaryChangeReason};
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

/// The relocated-submitter observer, modelled at the wire: it completed
/// the welcome/cert handshake and the `PeerInfo` announce AS THE PRIMARY,
/// then relocated — the `PrimaryChanged { Transferred }` broadcast is
/// deliberately NEVER delivered (the lost wire frame). From then on it
/// behaves exactly like the standalone observer the submitter swaps into:
/// it broadcasts its (ahead) anti-entropy `StateDigest` and answers
/// `RequestSnapshotStream` pulls with its converged snapshot stream.
///
/// `digest_rounds` is the scripted sequence of donor states: each entry is
/// digested + broadcast, then ONE snapshot pull is answered from it before
/// the next round is sent. The link is then held open (draining the
/// secondary's outbound) like the production tunnel.
async fn fake_relocated_observer(
    digest_rounds: Vec<ClusterState<TestId>>,
    mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    // Welcome + cert exchange — the pre-relocation primary's handshake.
    let (mut got_welcome, mut got_cert) = (false, false);
    while !got_welcome || !got_cert {
        match from_secondary.recv().await {
            Some(msg) => match msg.msg_type() {
                MessageType::SecondaryWelcome => got_welcome = true,
                MessageType::CertExchange => got_cert = true,
                _ => {}
            },
            None => return,
        }
    }
    // The primary's announce (first setup frame): PeerInfo. The secondary
    // enters `Configuring` and spawns workers off it — the production fleet
    // state at relocation time. The trio is NEVER completed (the setup peer
    // relocates without sending an assignment).
    to_secondary
        .send(DistributedMessage::PeerInfo {
            target: None,
            sender_id: "setup".into(),
            timestamp: 0.0,
            peers: Vec::new(),
        })
        .unwrap();

    // ── The relocation broadcast is LOST here (deliberately not sent). ──

    for donor in digest_rounds {
        // The observer's anti-entropy digest broadcast (ahead: it applied
        // the relocation locally before the broadcast was lost).
        to_secondary
            .send(DistributedMessage::StateDigest {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                digest: donor.digest(),
                sender_is_observer: false,
            })
            .unwrap();
        // The disciplined pull (#491): the behind secondary first
        // broadcasts a `PullProbe`; this donor (ahead) answers with an
        // `ahead` `PullProbeReply`, which selects it as the pull target; the
        // secondary then sends the `RequestSnapshotStream` we answer from
        // this round's donor state. Answer the probe AND the resulting pull.
        loop {
            match from_secondary.recv().await {
                Some(DistributedMessage::PullProbe { sender_id, .. }) => {
                    // Reply ahead with an idle inbox so the secondary selects
                    // us as its pull target.
                    to_secondary
                        .send(DistributedMessage::PullProbeReply {
                            target: None,
                            sender_id: "setup".into(),
                            timestamp: 0.0,
                            requester: sender_id,
                            inbox_size: 0,
                            ahead: true,
                        })
                        .unwrap();
                }
                Some(DistributedMessage::RequestSnapshotStream { stream_id, .. }) => {
                    for reply in
                        crate::snapshot_stream::stream_frames_for_test(&donor, "setup", &stream_id)
                    {
                        to_secondary.send(reply).unwrap();
                    }
                    break;
                }
                Some(_) => {}
                None => return,
            }
        }
    }
    // Hold the link open, draining the secondary's outbound (the tunnel
    // stays up; the run-over teardown is CRDT-cued, not link-cued).
    while from_secondary.recv().await.is_some() {}
}

/// The donor (observer-side) state holding the relocation fact the
/// broadcast was supposed to deliver: `PrimaryChanged { new: chosen,
/// epoch: 1, Transferred }`.
fn donor_with_primary(chosen: &str) -> ClusterState<TestId> {
    let mut donor = ClusterState::<TestId>::new();
    donor.apply(ClusterMutation::PrimaryChanged {
        new: chosen.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Transferred,
    });
    donor
}

fn race_config(secondary_id: &str) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
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
        // LONG so a pre-fix run wedges visibly (the test's own bounded
        // select fails first) instead of exiting through the deadline.
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

/// THE production wedge, chosen-secondary side: the relocation
/// announcement is lost while `sec-0` (the relocation target) is in
/// `Configuring`; the observer's next digest round must heal it and the
/// `PromotionSignal` must fire — from `Configuring`, with the setup trio
/// still incomplete.
#[tokio::test(flavor = "current_thread")]
async fn lost_relocation_announcement_heals_via_setup_anti_entropy() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = race_config("sec-0");
            let observer_handle = tokio::task::spawn_local(fake_relocated_observer(
                vec![donor_with_primary("sec-0")],
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            // Take the C4 promotion receiver out BEFORE the pump split (the
            // pump helper drops the harness shell, receiver included).
            let (_dummy_tx, dummy_rx) = tokio_mpsc::unbounded_channel();
            let mut promotion_rx = std::mem::replace(&mut harness.promotion_rx, dummy_rx);
            let (mut secondary, _guard) = start_secondary_pump(harness);

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    panic!(
                        "setup wait must keep waiting for the promoted primary's trio, \
                         got {res:?}"
                    );
                }
                sig = promotion_rx.recv() => {
                    let sig = sig.expect("promotion channel open");
                    assert_eq!(
                        sig.epoch, 1,
                        "promotion fires at the relocation epoch the snapshot healed"
                    );
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    panic!(
                        "LEADERLESS WEDGE (the production shape): the observer's \
                         StateDigest/snapshot-stream heal never fired the \
                         PromotionSignal — setup-wait dropped the anti-entropy frames"
                    );
                }
            }

            assert_eq!(
                secondary.cluster_state().current_primary(),
                Some("sec-0"),
                "the healed mirror names this secondary the primary"
            );

            observer_handle.abort();
        })
        .await;
}

/// The same lost announcement on a NON-chosen secondary: the heal must
/// converge its mirror on the new primary (`sec-0`), fire NO promotion,
/// and a later digest round carrying the run-terminal must still tear it
/// down through the existing loop-head terminal check (proving the healed
/// secondary keeps participating in convergence, not just the role fact).
#[tokio::test(flavor = "current_thread")]
async fn lost_relocation_announcement_non_chosen_follows_new_primary() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            // Round 1: the relocation fact naming sec-0 (NOT this node).
            // Round 2: the same plus the run-complete latch — the clean
            // teardown cue for a secondary the promoted primary never
            // configured.
            let round1 = donor_with_primary("sec-0");
            let mut round2 = donor_with_primary("sec-0");
            round2.apply(ClusterMutation::RunComplete);

            let config = race_config("sec-1");
            let observer_handle = tokio::task::spawn_local(fake_relocated_observer(
                vec![round1, round2],
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (_dummy_tx, dummy_rx) = tokio_mpsc::unbounded_channel();
            let mut promotion_rx = std::mem::replace(&mut harness.promotion_rx, dummy_rx);
            let (mut secondary, _guard) = start_secondary_pump(harness);

            let mut factory = FakeWorkerFactory;
            let outcome = tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    res.expect("run_until_setup_or_done returns Ok(RunOutcome::Terminal)")
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    panic!(
                        "LEADERLESS WEDGE (the production shape): the non-chosen \
                         secondary never converged on the relocated primary / the \
                         run terminal — setup-wait dropped the anti-entropy frames"
                    );
                }
            };
            assert!(
                matches!(outcome, RunOutcome::Terminal),
                "expected RunOutcome::Terminal, got {outcome:?}"
            );
            assert!(
                matches!(secondary.terminal(), Some(SecondaryTerminal::Done)),
                "run-complete teardown, got {:?}",
                secondary.terminal()
            );
            assert_eq!(
                secondary.cluster_state().current_primary(),
                Some("sec-0"),
                "the healed mirror follows the relocated primary — the bootstrap \
                 placeholder must not remain the resolved primary"
            );
            assert!(
                promotion_rx.try_recv().is_err(),
                "a non-chosen secondary must never fire a promotion"
            );

            observer_handle.abort();
        })
        .await;
}

/// The EMIT half of the heal (production replay 2212c136, part 1): a
/// trio-waiting secondary must BROADCAST its own anti-entropy
/// `StateDigest` on the jittered cadence — from `Configuring`, with the
/// setup trio incomplete. This is what makes the heal REACHABLE in the
/// demoted-submitter topology: the submitter's accept loop registers a
/// (re-dialed) connection only on its FIRST inbound frame, and the
/// transport-level bootstrap-redial re-fold sends nothing — so a MUTE
/// trio-wait leaves the wire permanently unregistered and the observer's
/// digest broadcasts fan over an empty writer table forever (the 4/4
/// leaderless wedge: observer ticking, secondaries receiving nothing).
///
/// REVERT-CHECK: pre-fix this times out — the trio-wait never sends a
/// single frame after its welcome/cert handshake.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn setup_wait_emits_anti_entropy_digest_on_cadence() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = race_config("sec-0");
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard) = start_secondary_pump(harness);

            // The submitter side, inline: complete the welcome/cert
            // handshake, send the announce (PeerInfo → `Configuring`), then
            // WAIT for the secondary's own digest broadcast.
            let driver = async {
                let (mut got_welcome, mut got_cert) = (false, false);
                while !got_welcome || !got_cert {
                    match sec_to_pri_rx.recv().await.expect("wire open") {
                        m if matches!(m.msg_type(), MessageType::SecondaryWelcome) => {
                            got_welcome = true;
                        }
                        m if matches!(m.msg_type(), MessageType::CertExchange) => got_cert = true,
                        _ => {}
                    }
                }
                pri_to_sec_tx
                    .send(DistributedMessage::PeerInfo {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        peers: Vec::new(),
                    })
                    .unwrap();
                // The pivot: a digest from the trio-waiting secondary within
                // a couple of anti-entropy periods (the per-node jittered
                // period is ≤ 25s; paused time auto-advances).
                loop {
                    let frame = tokio::select! {
                        f = sec_to_pri_rx.recv() => f.expect("wire open"),
                        _ = tokio::time::sleep(Duration::from_secs(60)) => panic!(
                            "MUTE TRIO-WAIT (the production shape): the secondary \
                             never broadcast its anti-entropy StateDigest from \
                             Configuring — a re-dialed bootstrap wire could never \
                             re-register and the relocation heal stays unreachable"
                        ),
                    };
                    if let DistributedMessage::StateDigest { sender_id, .. } = frame {
                        assert_eq!(sender_id, "sec-0");
                        break;
                    }
                }
            };

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    panic!("setup wait must keep waiting for the trio, got {res:?}");
                }
                () = driver => { /* digest observed on the wire */ }
            }
        })
        .await;
}

/// PRE-ANNOUNCE variant (production verdict for run_20260611_005927's
/// digest gap): secondaries 1-3 sat from +1s to the unconfigured
/// deadline WITHOUT ever receiving a single primary frame — still in
/// `AwaitingPrimary`, the state BEFORE the existing test's `PeerInfo`
/// announce. The anti-entropy emit cadence is armed at `wait_for_setup`
/// entry, i.e. BEFORE the first frame, so the digest beacon must flow
/// from `AwaitingPrimary` too: it is the identifying first frame that
/// registers a (re-dialed) wire AND the divergence beacon that lets a
/// parked fleet pull a primary fact none of them ever held locally.
///
/// REVERT-CHECK: with the cadence armed only after the first primary
/// frame this times out — the pre-announce fleet is mute for the whole
/// unconfigured window (the "zero digest lines" production signature).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn setup_wait_emits_anti_entropy_digest_pre_announce() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            // Held open so the bootstrap wire stays up (the production
            // shape: tunnel alive, primary mute) — never sends a frame.
            let _hold_primary_inbound = pri_to_sec_tx;

            let config = race_config("sec-0");
            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (mut secondary, _guard) = start_secondary_pump(harness);

            // The mute-primary side: drain the welcome/cert handshake,
            // send NOTHING back, and wait for the secondary's own digest
            // broadcast from `AwaitingPrimary`.
            let driver = async {
                let (mut got_welcome, mut got_cert) = (false, false);
                while !got_welcome || !got_cert {
                    match sec_to_pri_rx.recv().await.expect("wire open") {
                        m if matches!(m.msg_type(), MessageType::SecondaryWelcome) => {
                            got_welcome = true;
                        }
                        m if matches!(m.msg_type(), MessageType::CertExchange) => got_cert = true,
                        _ => {}
                    }
                }
                // NO PeerInfo — the announce never arrives. The digest
                // must still show up within a couple of jittered periods.
                loop {
                    let frame = tokio::select! {
                        f = sec_to_pri_rx.recv() => f.expect("wire open"),
                        _ = tokio::time::sleep(Duration::from_secs(60)) => panic!(
                            "MUTE PRE-ANNOUNCE WAIT (the run_20260611_005927 \
                             digest-gap shape): the secondary never broadcast \
                             its anti-entropy StateDigest from AwaitingPrimary \
                             — a fleet parked before the first primary frame \
                             stays beacon-dark for the whole unconfigured \
                             window"
                        ),
                    };
                    if let DistributedMessage::StateDigest { sender_id, .. } = frame {
                        assert_eq!(sender_id, "sec-0");
                        break;
                    }
                }
            };

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    panic!("setup wait must keep waiting pre-announce, got {res:?}");
                }
                () = driver => { /* digest observed on the wire */ }
            }
        })
        .await;
}

/// The relocated-submitter observer behind the production REGISTRATION
/// GATE (production replay 2212c136, part 2): the secondary's bootstrap
/// wire flapped at the relocate instant, the `PrimaryChanged { Transferred }`
/// broadcast is LOST, and the re-dialed wire is UNREGISTERED on the
/// submitter's accept loop — a connection registers only on its FIRST
/// inbound frame (`network/accept.rs`), and the bootstrap-redial re-fold
/// sends nothing. So NOTHING this observer broadcasts reaches the
/// secondary until the secondary itself speaks on the fresh wire; from
/// then on the wire is registered and the digest rounds flow.
async fn registration_gated_observer(
    digest_rounds: Vec<ClusterState<TestId>>,
    mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    // Pre-flap handshake — identical to `fake_relocated_observer`.
    let (mut got_welcome, mut got_cert) = (false, false);
    while !got_welcome || !got_cert {
        match from_secondary.recv().await {
            Some(msg) => match msg.msg_type() {
                MessageType::SecondaryWelcome => got_welcome = true,
                MessageType::CertExchange => got_cert = true,
                _ => {}
            },
            None => return,
        }
    }
    to_secondary
        .send(DistributedMessage::PeerInfo {
            target: None,
            sender_id: "setup".into(),
            timestamp: 0.0,
            peers: Vec::new(),
        })
        .unwrap();

    // ── FLAP: the wire drops at the relocate instant; the relocation
    // `PrimaryChanged` broadcast is LOST. The secondary re-dials, but the
    // fresh wire is UNREGISTERED: every digest this observer broadcasts
    // fans over a writer table that has no entry for the secondary. The
    // gate models the accept-loop contract — deliver NOTHING until the
    // secondary's first post-flap frame arrives. ──
    if from_secondary.recv().await.is_none() {
        return;
    }

    // Registered. The observer's digest cadence now reaches the secondary;
    // same scripted rounds as `fake_relocated_observer`.
    for donor in digest_rounds {
        to_secondary
            .send(DistributedMessage::StateDigest {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                digest: donor.digest(),
                sender_is_observer: false,
            })
            .unwrap();
        loop {
            match from_secondary.recv().await {
                Some(DistributedMessage::PullProbe { sender_id, .. }) => {
                    to_secondary
                        .send(DistributedMessage::PullProbeReply {
                            target: None,
                            sender_id: "setup".into(),
                            timestamp: 0.0,
                            requester: sender_id,
                            inbox_size: 0,
                            ahead: true,
                        })
                        .unwrap();
                }
                Some(DistributedMessage::RequestSnapshotStream { stream_id, .. }) => {
                    for reply in
                        crate::snapshot_stream::stream_frames_for_test(&donor, "setup", &stream_id)
                    {
                        to_secondary.send(reply).unwrap();
                    }
                    break;
                }
                Some(_) => {}
                None => return,
            }
        }
    }
    while from_secondary.recv().await.is_some() {}
}

/// THE full production wedge (2212c136, deterministic 4/4), replayed:
/// lost relocation announcement + flapped bootstrap wire + the
/// first-frame registration gate. The heal can only engage end-to-end if
/// the trio-waiting secondary SPEAKS (its setup-phase anti-entropy digest
/// is the registering first frame); the observer's digest round then
/// proves it behind, the snapshot pull restores the primary fact naming
/// ITSELF, and the `PromotionSignal` fires.
///
/// REVERT-CHECK: pre-fix the secondary stays mute, the gate never opens,
/// no digest is ever delivered, no promotion fires — the leaderless wedge,
/// surfaced here as the bounded-select panic.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn relocation_heal_engages_through_first_frame_registration_gate() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let config = race_config("sec-0");
            let observer_handle = tokio::task::spawn_local(registration_gated_observer(
                vec![donor_with_primary("sec-0")],
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            let unified =
                channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut harness = make_secondary_channel(config, unified);
            harness.set_bootstrap_primary_id("setup".to_string());
            let (_dummy_tx, dummy_rx) = tokio_mpsc::unbounded_channel();
            let mut promotion_rx = std::mem::replace(&mut harness.promotion_rx, dummy_rx);
            let (mut secondary, _guard) = start_secondary_pump(harness);

            let mut factory = FakeWorkerFactory;
            tokio::select! {
                res = secondary.run_until_setup_or_done(&mut factory) => {
                    panic!(
                        "setup wait must keep waiting for the promoted primary's trio, \
                         got {res:?}"
                    );
                }
                sig = promotion_rx.recv() => {
                    let sig = sig.expect("promotion channel open");
                    assert_eq!(
                        sig.epoch, 1,
                        "promotion fires at the relocation epoch the snapshot healed"
                    );
                }
                // Generous bound: the secondary's first digest lands within
                // one jittered period (≤25s) and the heal round-trip is
                // immediate; 300s of paused time stays below the 600s
                // unconfigured deadline.
                _ = tokio::time::sleep(Duration::from_secs(300)) => {
                    panic!(
                        "LEADERLESS WEDGE (the production shape): the secondary \
                         never spoke on the re-dialed wire, the registration gate \
                         never opened, and the observer's digest heal never fired \
                         the PromotionSignal"
                    );
                }
            }

            assert_eq!(
                secondary.cluster_state().current_primary(),
                Some("sec-0"),
                "the healed mirror names this secondary the primary"
            );

            observer_handle.abort();
        })
        .await;
}

/// The seam itself, pinned at the unit level: a snapshot heal that newly
/// names THIS node primary fires the SAME `PromotionSignal` the live
/// `PrimaryChanged` apply fires (`on_primary_identity_advanced` is one
/// writer for both paths), and a repeated identical restore is a NoOp
/// (no duplicate signal). A peer-named heal advances the mirror without
/// any promotion.
#[tokio::test(flavor = "current_thread")]
async fn snapshot_restore_runs_the_primary_identity_seam() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use super::super::test_helpers::{
                RecordingPeer, SecondaryHarness, election_config, make_secondary_recording,
            };

            // Self-named heal → promotion fires once, idempotent on
            // re-restore. The heal arrives as the package stream a
            // production responder would emit.
            let (mut sec, _log) = make_secondary_recording(election_config("worker-a"), 1);
            let donor = donor_with_primary("worker-a");
            let frames = crate::snapshot_stream::stream_frames_for_test(&donor, "setup", "wa/0");
            let restore_all = |sec: &mut SecondaryHarness<RecordingPeer<TestId>>,
                               frames: &[DistributedMessage<TestId>]|
             -> bool {
                let mut advanced = false;
                for f in frames {
                    if let DistributedMessage::SnapshotStreamPackage {
                        sender_id,
                        stream_id,
                        cursor,
                        payload,
                        done,
                        ..
                    } = f
                    {
                        advanced |= sec.restore_snapshot_stream_frame(
                            sender_id,
                            stream_id,
                            cursor.as_deref(),
                            payload,
                            *done,
                        );
                    }
                }
                advanced
            };
            assert!(
                restore_all(&mut sec, &frames),
                "the heal advances the primary identity"
            );
            let sig = sec
                .promotion_rx
                .try_recv()
                .expect("self-named snapshot heal fires the PromotionSignal");
            assert_eq!(sig.epoch, 1);
            assert!(
                !restore_all(&mut sec, &frames),
                "an identical re-restore is a NoOp"
            );
            assert!(
                sec.promotion_rx.try_recv().is_err(),
                "no duplicate promotion on the NoOp re-restore"
            );

            // Peer-named heal → mirror follows, no promotion.
            let (mut sec_b, _log_b) = make_secondary_recording(election_config("worker-b"), 1);
            let donor_b = donor_with_primary("worker-a");
            let frames_b =
                crate::snapshot_stream::stream_frames_for_test(&donor_b, "setup", "wb/0");
            assert!(restore_all(&mut sec_b, &frames_b));
            assert_eq!(sec_b.cluster_state().current_primary(), Some("worker-a"));
            assert!(
                sec_b.promotion_rx.try_recv().is_err(),
                "a peer-named heal must not fire a promotion"
            );
        })
        .await;
}
