//! Tests for the observer-side resource-holdings announcer.
//!
//! Covers two surfaces:
//! - The task body's retry-with-backoff loop + epoch-mirror reads
//!   (driven through a `CapturingSender` fake whose failure schedule
//!   is table-driven).
//! - The production [`PeerMeshAnnouncerSender`] wire shape — pins the
//!   `DistributedMessage::ClusterMutation` envelope field-for-field
//!   so a rename on either side of the boundary breaks compilation /
//!   assertion, not silently the wire.

#![cfg(test)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use super::sender::{AnnouncerOutboxItem, PeerMeshAnnouncerSender};
use super::task::run_observer_announcer;
use super::types::{AnnounceTrigger, AnnouncerSender, PeerResourceHoldingsUpdatedPayload};

/// Fake sender backing the announcer's three contract tests.
///
/// Captures every successful delivery's body so assertions can
/// inspect the wire shape end-to-end. A `failure_schedule` vector
/// is consumed front-to-back: each entry decides whether the
/// matching `send_holdings` call fails (`Some(error)`) or
/// succeeds (`None`). The default is `Ok` once the schedule is
/// exhausted — this lets tests opt into retry-on-failure
/// scenarios without rebuilding the fixture per case.
#[derive(Default)]
struct CapturingSender {
    captured: Arc<Mutex<Vec<PeerResourceHoldingsUpdatedPayload>>>,
    failure_schedule: Arc<Mutex<Vec<Option<String>>>>,
}

impl CapturingSender {
    fn new() -> Self {
        Self::default()
    }
    fn with_failure_schedule(failures: Vec<Option<String>>) -> Self {
        Self {
            captured: Arc::new(Mutex::new(Vec::new())),
            failure_schedule: Arc::new(Mutex::new(failures)),
        }
    }
    fn captured_handle(&self) -> Arc<Mutex<Vec<PeerResourceHoldingsUpdatedPayload>>> {
        Arc::clone(&self.captured)
    }
}

impl AnnouncerSender for CapturingSender {
    async fn send_holdings(
        &mut self,
        body: &PeerResourceHoldingsUpdatedPayload,
    ) -> Result<(), String> {
        let next = {
            let mut sched = self.failure_schedule.lock().unwrap();
            if sched.is_empty() {
                None
            } else {
                sched.remove(0)
            }
        };
        match next {
            Some(err) => Err(err),
            None => {
                self.captured.lock().unwrap().push(body.clone());
                Ok(())
            }
        }
    }
}

/// On a simulated `PrimaryChanged` (modelled by a trigger pushed
/// onto the announcer's channel), the announcer emits exactly one
/// `PeerResourceHoldingsUpdated` carrying the observer's static
/// holdings.
#[tokio::test]
async fn observer_announcer_broadcasts_on_primary_change() {
    let (tx, rx) = mpsc::channel(8);
    let sender = CapturingSender::new();
    let captured = sender.captured_handle();
    let epoch = Arc::new(AtomicU64::new(3));
    let holdings: HashSet<String> = ["/nix/store/aaa".into(), "/nix/store/bbb".into()]
        .into_iter()
        .collect();

    let handle = tokio::spawn(run_observer_announcer(
        rx,
        holdings,
        "observer-x".into(),
        sender,
        Arc::clone(&epoch),
    ));

    tx.send(AnnounceTrigger).await.unwrap();
    // Drop tx → rx closes → announcer exits after draining the
    // pending trigger. Waiting on the join handle is the
    // deterministic synchronization rather than a `sleep`.
    drop(tx);
    handle.await.unwrap();

    let observed = captured.lock().unwrap().clone();
    assert_eq!(observed.len(), 1, "exactly one broadcast per trigger");
    assert_eq!(observed[0].peer_id, "observer-x");
    // Sorted on the wire — see `build_payload` rationale.
    assert_eq!(
        observed[0].holdings,
        vec!["/nix/store/aaa".to_string(), "/nix/store/bbb".to_string()],
    );
    assert_eq!(observed[0].epoch, 3);
}

/// The broadcast carries the CURRENT `primary_epoch`, not a value
/// captured at trigger time. The test models the failover sequence:
/// epoch=5 before the trigger, then the apply path bumps the mirror
/// to 6, the announcer wakes and reads the post-`PrimaryChanged`
/// value off the shared atomic.
#[tokio::test]
async fn observer_announcer_includes_current_primary_epoch() {
    let (tx, rx) = mpsc::channel(8);
    // Sender blocks on first call until released — gives us a
    // deterministic window to mutate the epoch mirror after the
    // trigger fires but before the send observes the value.
    let sender = CapturingSender::new();
    let captured = sender.captured_handle();
    let epoch = Arc::new(AtomicU64::new(5));

    let handle = tokio::spawn(run_observer_announcer(
        rx,
        HashSet::from(["/nix/store/foo".to_string()]),
        "observer-y".into(),
        sender,
        Arc::clone(&epoch),
    ));

    // Bump the mirror to 6 BEFORE pushing the trigger — same end
    // state as "apply path wrote 6 and fired the hook which pushed
    // the trigger". Memory-order: `Release` here pairs with the
    // task's `Acquire` load.
    epoch.store(6, Ordering::Release);
    tx.send(AnnounceTrigger).await.unwrap();
    drop(tx);
    handle.await.unwrap();

    let observed = captured.lock().unwrap().clone();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed[0].epoch, 6,
        "announcer must read the post-PrimaryChanged epoch off the mirror"
    );
}

/// First delivery attempt fails (transient send error); the
/// announcer's retry loop drives the second attempt to success
/// after a single backoff sleep. The captured body has the
/// expected shape and the eventual broadcast count is exactly one.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn observer_announcer_retries_on_send_failure() {
    let (tx, rx) = mpsc::channel(8);
    let sender =
        CapturingSender::with_failure_schedule(vec![Some("transient transport error".into())]);
    let captured = sender.captured_handle();
    let epoch = Arc::new(AtomicU64::new(7));

    let handle = tokio::spawn(run_observer_announcer(
        rx,
        HashSet::from(["/nix/store/zzz".to_string()]),
        "observer-z".into(),
        sender,
        Arc::clone(&epoch),
    ));

    tx.send(AnnounceTrigger).await.unwrap();
    // Advance virtual time past the initial 100 ms backoff so the
    // retry fires deterministically. `start_paused` keeps real time
    // out of the test — without it the test would either flake on
    // CI under load or burn 100 ms per run.
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(tx);
    handle.await.unwrap();

    let observed = captured.lock().unwrap().clone();
    assert_eq!(
        observed.len(),
        1,
        "first failure then second success → exactly one persisted body"
    );
    assert_eq!(observed[0].peer_id, "observer-z");
    assert_eq!(observed[0].epoch, 7);
    assert_eq!(observed[0].holdings, vec!["/nix/store/zzz".to_string()]);
}

// ── Production AnnouncerSender wire-shape tests ──

/// `PeerMeshAnnouncerSender::send_holdings` rewraps the typed
/// body into the canonical
/// `DistributedMessage::ClusterMutation {
/// mutations: vec![
/// ClusterMutation::PeerResourceHoldingsUpdated { … } ] }`
/// envelope and posts it onto the outbox. The reply oneshot
/// resolves with the drain-side outcome; `send_holdings` returns
/// that outcome unchanged.
///
/// Pins the wire shape that downstream `cluster_state.apply` is
/// expected to consume — without this assertion a field-rename
/// on either side of the boundary would silently break the
/// holdings broadcast without firing any compile error.
#[tokio::test]
async fn production_announcer_sender_wraps_body_in_cluster_mutation() {
    use dynrunner_core::RunnerIdentifier;
    use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};

    let (outbox_tx, mut outbox_rx) =
        tokio::sync::mpsc::channel::<AnnouncerOutboxItem<RunnerIdentifier>>(8);
    let mut sender =
        PeerMeshAnnouncerSender::<RunnerIdentifier>::new("observer-prod".into(), outbox_tx);

    let body = PeerResourceHoldingsUpdatedPayload {
        peer_id: "observer-prod".into(),
        holdings: vec!["/nix/store/aaa".into(), "/nix/store/bbb".into()],
        epoch: 9,
    };

    // Drive `send_holdings` and the drain side concurrently so
    // the oneshot resolves before the await completes. Without
    // pairing the two halves, the send-side future would block
    // forever on `reply_rx.await`.
    let send_fut = sender.send_holdings(&body);
    let drain_fut = async {
        let item = outbox_rx.recv().await.expect("outbox carries one item");
        // Reply Ok so `send_holdings` resolves to Ok(()).
        item.reply
            .send(Ok(()))
            .expect("send-side still awaiting reply");
        item.msg
    };
    let (send_result, captured_msg) = tokio::join!(send_fut, drain_fut);
    send_result.expect("send_holdings resolves to Ok when drain replies Ok");

    // Assert the wire shape end-to-end. The envelope MUST be the
    // top-level `ClusterMutation` variant; its `mutations` vec
    // MUST carry exactly one `PeerResourceHoldingsUpdated` whose
    // fields mirror the body field-for-field.
    match captured_msg {
        DistributedMessage::ClusterMutation {
            sender_id,
            timestamp,
            mutations,
            target: _,
        } => {
            assert_eq!(sender_id, "observer-prod");
            assert!(
                timestamp.is_finite() && timestamp > 0.0,
                "timestamp must be a finite positive f64 (real wall-clock)"
            );
            assert_eq!(mutations.len(), 1, "exactly one mutation per announce");
            match &mutations[0] {
                ClusterMutation::PeerResourceHoldingsUpdated {
                    peer_id,
                    holdings,
                    epoch,
                } => {
                    assert_eq!(peer_id, "observer-prod");
                    assert_eq!(
                        holdings,
                        &vec!["/nix/store/aaa".to_string(), "/nix/store/bbb".to_string()]
                    );
                    assert_eq!(*epoch, 9);
                }
                other => panic!("expected PeerResourceHoldingsUpdated, got {other:?}"),
            }
        }
        other => panic!("expected DistributedMessage::ClusterMutation, got {other:?}"),
    }
}

/// The drain-side reply propagates back: a transport `Err`
/// surfaces from `send_holdings`, which the announcer's
/// retry-with-backoff loop interprets exactly the way it does
/// for any fake-sender error (see
/// `observer_announcer_retries_on_send_failure`).
///
/// Without this, a transport-side delivery failure could be
/// masked into `Ok` by an off-by-one in the reply wiring, and
/// the retry loop would silently never retry.
#[tokio::test]
async fn production_announcer_sender_propagates_drain_error() {
    use dynrunner_core::RunnerIdentifier;

    let (outbox_tx, mut outbox_rx) =
        tokio::sync::mpsc::channel::<AnnouncerOutboxItem<RunnerIdentifier>>(8);
    let mut sender =
        PeerMeshAnnouncerSender::<RunnerIdentifier>::new("observer-prop".into(), outbox_tx);

    let body = PeerResourceHoldingsUpdatedPayload {
        peer_id: "observer-prop".into(),
        holdings: vec!["/nix/store/qqq".into()],
        epoch: 4,
    };

    let send_fut = sender.send_holdings(&body);
    let drain_fut = async {
        let item = outbox_rx.recv().await.expect("outbox carries one item");
        // Reply Err — simulates `peer_transport.send` failing.
        item.reply
            .send(Err("simulated transport failure".into()))
            .expect("send-side still awaiting reply");
    };
    let (send_result, ()) = tokio::join!(send_fut, drain_fut);
    let err = send_result.expect_err("drain-side Err must surface from send_holdings");
    assert!(
        err.contains("simulated transport failure"),
        "send_holdings must propagate the drain-side error verbatim; got {err}"
    );
}
