//! `persistent_dial_failure_trigger_*` — the per-leg forward-recovery
//! trigger seam (#419).
//!
//! The bug: a late-joiner observer reaches each peer through a per-peer
//! `ssh -L` forward (`127.0.0.1:<local_port>`). When ONE forward's ssh
//! child dies, the 5s reconnect ticker re-dials the now-dead local
//! endpoint FOREVER — the run-level lost-visibility trigger never fires
//! while the OTHER legs keep the observer Visible, so the dead leg's
//! forward is never rebuilt.
//!
//! The fix surfaces a transport-level signal: when this node OWNS the
//! dial to a peer and keeps failing past the dial-failure summary
//! boundary, the peer id is published on the
//! [`PeerNetwork::notify_persistent_dial_failures`] sink. A subscriber
//! (the local-forward registry) maps the id to its forward and rebuilds.
//!
//! These tests pin the transport contract ONLY (it stays ssh-agnostic):
//! the id is emitted on the boundary for a dial-owner peer, NOT for a
//! connected peer, NOT for a peer whose dial side this node does not own
//! (lower-id-dials), and the emission rides the same throttle as the
//! operator summary (once at the threshold, then once per recurrence —
//! never per tick).

use std::sync::{Arc, Mutex};

use super::super::PeerNetwork;
use super::super::dial::{DialAttempt, dial_peer};
use super::super::reconnect::{DIAL_SUMMARY_RECURRENCE, DIAL_SUMMARY_THRESHOLD};
use super::TestId;
use super::log_capture::{CaptureLayer, CapturedEvent};
use dynrunner_protocol_primary_secondary::PeerConnectionInfo;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// A peer this node ("peer-a") OWNS the dial to (lower-id-dials: "peer-a"
/// sorts below "peer-z"), with an unreachable forward-style endpoint,
/// registered in `peer_dial_info` but never connected — the exact
/// "tracked, perpetually failing to dial through a dead forward" shape.
fn dialed_unreachable_peer() -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: "peer-z".into(),
        cert: String::new(),
        ipv4: Some("127.0.0.1".into()),
        port: 59123,
        ipv6: None,
        is_observer: false,
        liveness_port: None,
    }
}

/// A peer whose dial side this node does NOT own (lower-id-dials:
/// "peer-0" sorts below "peer-a" — `'0' < 'a'` — so "peer-0" dials US).
/// Its leg is not a forward this node rebuilds; the trigger must never
/// fire for it.
fn non_dialed_peer() -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: "peer-0".into(),
        cert: String::new(),
        ipv4: Some("127.0.0.1".into()),
        port: 59124,
        ipv6: None,
        is_observer: false,
        liveness_port: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn trigger_fires_at_threshold_for_dial_owned_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a", None).await.unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            peer_a.notify_persistent_dial_failures(tx);

            let info = dialed_unreachable_peer();
            let peer_id = info.secondary_id.clone();
            peer_a.peer_dial_info.insert(peer_id.clone(), info);

            // Below the threshold: NO trigger (a transient blip that
            // heals in a tick or two must not rebuild a forward).
            for _ in 0..(DIAL_SUMMARY_THRESHOLD - 1) {
                peer_a.process_reconnect_tick();
            }
            assert!(
                rx.try_recv().is_err(),
                "trigger must stay silent before the summary threshold"
            );

            // The threshold tick: exactly one id emitted, for this peer.
            peer_a.process_reconnect_tick();
            assert_eq!(
                rx.try_recv().ok(),
                Some(peer_id.clone()),
                "the threshold tick must publish the undialable peer id"
            );
            assert!(
                rx.try_recv().is_err(),
                "exactly one emission at the threshold, not a burst"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn trigger_throttles_to_recurrence_not_every_tick() {
    // Past the threshold the trigger must recur only once per
    // RECURRENCE window — the same throttle as the operator summary —
    // so a permanently-dead forward nudges the registry a handful of
    // times across a long outage, not every 5s tick.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a", None).await.unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            peer_a.notify_persistent_dial_failures(tx);

            let info = dialed_unreachable_peer();
            peer_a
                .peer_dial_info
                .insert(info.secondary_id.clone(), info);

            let total_ticks = DIAL_SUMMARY_THRESHOLD + 2 * DIAL_SUMMARY_RECURRENCE;
            for _ in 0..total_ticks {
                peer_a.process_reconnect_tick();
            }
            let mut emitted = 0usize;
            while rx.try_recv().is_ok() {
                emitted += 1;
            }
            // Exactly the boundary set {THRESHOLD, +RECURRENCE,
            // +2·RECURRENCE} ⇒ 3 emissions across `total_ticks` ticks.
            assert_eq!(
                emitted, 3,
                "trigger must fire only at threshold + recurrence boundaries"
            );
            assert!(
                (emitted as u32) < total_ticks,
                "throttle must suppress the vast majority of ticks"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn trigger_silent_for_connected_peer() {
    // A peer present in `connections` reads as reconnected every tick,
    // never accrues a failed-dial count, and so never triggers a
    // rebuild — even past where the threshold would otherwise fire.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a", None).await.unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            peer_a.notify_persistent_dial_failures(tx);

            let info = dialed_unreachable_peer();
            let peer_id = info.secondary_id.clone();
            peer_a.peer_dial_info.insert(peer_id.clone(), info);
            let (conn_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            peer_a.connections.insert(peer_id.clone(), conn_tx);

            for _ in 0..(DIAL_SUMMARY_THRESHOLD + 2) {
                peer_a.process_reconnect_tick();
            }
            assert!(
                rx.try_recv().is_err(),
                "a connected peer must never trigger a forward rebuild"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn trigger_silent_for_non_dial_owned_peer() {
    // The trigger must fire ONLY for a peer this node owns the dial to:
    // a lower-id peer dials US (its leg is not a forward this node
    // rebuilds), so its dial-failure summary is the "peer leg missing"
    // narration, never a rebuild trigger.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a", None).await.unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            peer_a.notify_persistent_dial_failures(tx);

            let info = non_dialed_peer();
            peer_a
                .peer_dial_info
                .insert(info.secondary_id.clone(), info);

            for _ in 0..(DIAL_SUMMARY_THRESHOLD + 2) {
                peer_a.process_reconnect_tick();
            }
            assert!(
                rx.try_recv().is_err(),
                "a peer whose dial side this node does NOT own must never \
                 trigger a forward rebuild (its leg is not a forward here)"
            );
        })
        .await;
}

// ---------------------------------------------------------------------
// Redial-spam rate-limit (#419, also-in-scope): the per-attempt dial
// failure lines (`no valid cert … trying WSS`, `WSS race … dial gave
// up`) flooded the FULL log at 2 lines / 5s / dead-leg forever. The fix
// keeps the FIRST-contact (`Initial`) dial loud — WARN/ERROR, one-shot —
// but drops the REDIAL path's per-attempt failures to DEBUG, because the
// throttled `peer unreachable` summary in `process_reconnect_tick` (once
// per episode + once per recurrence window) already owns the redial
// path's operator-level visibility. These tests pin the LEVEL split.
// ---------------------------------------------------------------------

/// A cert-less peer (forces `dial_peer` straight to WSS — the production
/// forwarded-seed shape) whose advertised address is a CLOSED localhost
/// port (connection refused immediately, so the dial fails fast without
/// burning the 10s per-attempt timeout).
fn cert_less_peer_at_closed_port() -> PeerConnectionInfo {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener); // free the port so the dial is refused, not accepted
    PeerConnectionInfo {
        secondary_id: "peer-z".into(),
        cert: String::new(),
        ipv4: Some("127.0.0.1".into()),
        port,
        ipv6: None,
        is_observer: false,
        liveness_port: None,
    }
}

/// Capture the dial-failure lines this crate emits for a single
/// `dial_peer` call against a refused endpoint, returning their levels.
async fn dial_failure_levels(attempt: DialAttempt) -> Vec<(tracing::Level, String)> {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let info = cert_less_peer_at_closed_port();
    let conn = dial_peer(&info.secondary_id, &info, attempt).await;
    assert!(conn.is_none(), "the refused dial must fail");

    records
        .lock()
        .unwrap()
        .iter()
        .filter(|e| {
            e.target.starts_with("dynrunner_transport_quic")
                && (e.message.contains("trying WSS") || e.message.contains("dial gave up"))
        })
        .map(|e| (e.level, e.message.clone()))
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn redial_dial_failures_are_debug_initial_failures_stay_loud() {
    // Initial dial: first-contact failures are operator-significant and
    // one-shot — they MUST stay loud (WARN for the no-cert step, ERROR
    // for the terminal give-up).
    let initial = dial_failure_levels(DialAttempt::Initial).await;
    assert!(
        initial
            .iter()
            .any(|(lvl, msg)| *lvl == tracing::Level::WARN && msg.contains("trying WSS")),
        "initial no-cert step must stay at WARN; got {initial:?}"
    );
    assert!(
        initial
            .iter()
            .any(|(lvl, msg)| *lvl == tracing::Level::ERROR && msg.contains("dial gave up")),
        "initial terminal give-up must stay at ERROR; got {initial:?}"
    );

    // Redial: the SAME failures on every 5s tick — they must drop to
    // DEBUG so they no longer flood the full log (the throttled summary
    // owns the redial narration). NO WARN or ERROR may appear.
    let redial = dial_failure_levels(DialAttempt::Redial { attempt: 7 }).await;
    assert!(
        !redial.is_empty(),
        "the redial must still emit its failure lines (at DEBUG), not go silent"
    );
    assert!(
        redial
            .iter()
            .all(|(lvl, _)| *lvl == tracing::Level::DEBUG),
        "every redial-path dial-failure line must be DEBUG (rate-limit); got {redial:?}"
    );
}
