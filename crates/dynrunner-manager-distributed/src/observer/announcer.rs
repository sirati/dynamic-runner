//! Observer-side resource-holdings announcer task.
//!
//! # Concern
//!
//! Single concern: react to `AnnounceTrigger`s on a channel by sending
//! exactly one `PeerResourceHoldingsUpdated` broadcast carrying the
//! observer's static `holdings` and the cluster's **current**
//! `primary_epoch`. The task body owns the retry-with-backoff loop
//! that wraps every individual delivery attempt, so the registering
//! site (a synchronous `RoleChangeHook` closure) never blocks.
//!
//! # Module boundary
//!
//! The boundary the task crosses is:
//!
//! - **In**: a `mpsc::Receiver<AnnounceTrigger>` populated by the role-
//!   change hook installed at observer construction time AND by a
//!   one-shot fire after snapshot restore. Senders live at the
//!   registration site; this task only ever reads.
//! - **In**: an `Arc<AtomicU64>` mirror of `ClusterState::primary_epoch`
//!   kept current synchronously by the apply path (see
//!   [`crate::cluster_state::ClusterState::primary_epoch_mirror`]).
//!   The announcer reads the mirror at send time so the epoch on the
//!   wire reflects the post-`PrimaryChanged` value even if multiple
//!   role changes coalesced into one trigger.
//! - **Out**: an [`AnnouncerSender`] trait-bound handle. Production
//!   wiring routes the call through the observer's `PeerTransport`
//!   via `Address::Role(Role::Primary)` (after a cross-task hop —
//!   the transport is owned by the run-loop task, so the impl
//!   forwards onto a coordinator-side outbox and awaits the
//!   delivery outcome). Tests pass a fake whose failure schedule is
//!   table-driven.
//!
//! The announcer never touches `PeerTransport` directly. That
//! decoupling lets the same loop body cover both the production
//! cross-task case and the in-test direct-handle case without
//! special-cased branches.
//!
//! # Epoch-supersede semantics
//!
//! Triggers are coalesced by the channel: a burst of `PrimaryChanged`
//! mutations during a flap (epoch 5 → 6 → 7 within a few ms) may
//! produce N pushes but only one effective announce if the announcer
//! is still backing off its previous send. That is correct: the
//! announcement carries `epoch = mirror.load()`, which observes the
//! **latest** value. Receivers apply the higher-epoch update and
//! discard the lower (the E1 apply rule keys on epoch monotonicity).
//! Back-pressure isn't load-bearing — `try_send` failures at the
//! registration site drop the trigger and the next fire (or the
//! still-pending coalesced fire) covers the missing one.
//!
//! # Retry loop
//!
//! Backoff starts at [`INITIAL_BACKOFF`] and doubles up to
//! [`MAX_BACKOFF`]. The retry condition is the trait's `Err` return.
//! On `Ok` the loop returns to draining `rx`; on `Err` it sleeps and
//! re-issues the send with the **freshly re-read** epoch — a stale
//! send-in-flight whose retry lands after another `PrimaryChanged`
//! must carry the newer epoch, not the value captured at the
//! triggering instant.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

/// Initial retry backoff. Picked to match the keepalive cadence's
/// lower bound — anything faster would burn CPU on a transport that
/// is clearly partitioned, anything slower delays the first retry
/// past the user-observable threshold for a flap that recovers
/// quickly.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Backoff ceiling. 5 s is the same upper bound the secondary's
/// election / failover timers use for "slow path is still considered
/// active"; bounding here keeps the announcer from hibernating past
/// the next role-change tick.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Unit signal pushed onto the announcer's channel by the role-change
/// hook (and by the initial post-restore fire). Carries no payload —
/// the announcer reads the current epoch from the shared mirror at
/// send time, so the trigger is a pure "go ahead" notification and
/// stale captures are impossible by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnnounceTrigger;

/// Typed body that crosses the announcer → transport boundary. Mirrors
/// the planned E1 wire variant
/// (`ClusterMutation::PeerResourceHoldingsUpdated { peer_id, holdings,
/// epoch }`) field-for-field so the production [`AnnouncerSender`]
/// impl can rewrap it into the real `ClusterMutation` once E1 lands
/// with a mechanical field-by-field copy.
///
/// TODO(E1-merge): swap construction of this body at the production
/// `AnnouncerSender` impl site to wrap the body into
/// `DistributedMessage::ClusterMutation { mutations: vec![
/// ClusterMutation::PeerResourceHoldingsUpdated { peer_id, holdings,
/// epoch } ] }`. The body type itself stays — it's the announcer's
/// stable internal payload representation that decouples the
/// observer's lifecycle from the wire variant's spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerResourceHoldingsUpdatedPayload {
    pub peer_id: String,
    pub holdings: Vec<String>,
    pub epoch: u64,
}

/// Transport-side delivery boundary the announcer talks through.
///
/// Single concern: convert a typed announcement body into a single
/// best-effort attempt at delivering it to whichever peer currently
/// holds the primary role. Returning `Err(msg)` triggers the
/// announcer's retry-with-backoff loop; the error is logged at the
/// announcer level but not otherwise inspected.
///
/// The trait is `&mut self` rather than `&self` because the
/// production impl owns an outbox channel handle that may need
/// mutable access on send; the channel handle itself can be cloned
/// before construction to deal with multi-call concurrency.
pub trait AnnouncerSender {
    fn send_holdings(
        &mut self,
        body: &PeerResourceHoldingsUpdatedPayload,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

/// Drain `rx` and, for every trigger observed, emit one
/// `PeerResourceHoldingsUpdated` announcement (carrying the current
/// `primary_epoch` read off the shared mirror) until delivery
/// succeeds. Exits when `rx` closes (the observer's `SecondaryCoordinator`
/// dropped the last sender on shutdown).
///
/// The task is `pub` and lives in this module rather than the
/// secondary's `mod.rs` because its concern is independent of the
/// secondary's run loop — it owns its own channel state and its own
/// retry policy, and the secondary just `spawn_local`s it (alongside
/// the peer-lifecycle dispatcher) at observer construction.
///
/// # `holdings` is `HashSet` on the input and `Vec` on the wire
///
/// The CRDT apply rule (planned in E1) is set-keyed: a peer's holdings
/// are a set. The wire format is a `Vec` for stable JSON encoding.
/// Sorting before send keeps the wire deterministic for log diffing,
/// which is operationally load-bearing during the dual-cluster
/// failover scenario (operators correlate broadcasts across nodes by
/// JSON-equal payload).
pub async fn run_observer_announcer<S: AnnouncerSender>(
    mut rx: mpsc::Receiver<AnnounceTrigger>,
    holdings: HashSet<String>,
    peer_id: String,
    mut sender: S,
    primary_epoch: Arc<AtomicU64>,
) {
    while let Some(_trigger) = rx.recv().await {
        let mut backoff = INITIAL_BACKOFF;
        loop {
            // Re-read the epoch on every retry: a `PrimaryChanged`
            // that lands while we're backing off must carry through
            // to the next attempt, not get stamped with the value
            // captured at the trigger we're currently servicing.
            let epoch = primary_epoch.load(Ordering::Acquire);
            let body = build_payload(&peer_id, &holdings, epoch);
            match sender.send_holdings(&body).await {
                Ok(()) => break,
                Err(e) => {
                    tracing::warn!(
                        target: "dynrunner_observer_announcer",
                        peer_id = %peer_id,
                        epoch,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "observer-announcer send failed; retrying after backoff",
                    );
                    tokio::time::sleep(backoff).await;
                    // Exponential with cap. Saturating_mul prevents the
                    // duration overflow if the cap is ever raised to a
                    // value where 2× wraps.
                    backoff = (backoff.saturating_mul(2)).min(MAX_BACKOFF);
                }
            }
        }
    }
}

/// Build the announcement body. Extracted as a free function so the
/// "sort holdings for stable wire" invariant has one writer and the
/// tests can pin it against the same builder the runtime uses.
fn build_payload(
    peer_id: &str,
    holdings: &HashSet<String>,
    epoch: u64,
) -> PeerResourceHoldingsUpdatedPayload {
    let mut as_vec: Vec<String> = holdings.iter().cloned().collect();
    // Stable order on the wire — see `run_observer_announcer` doc.
    as_vec.sort();
    PeerResourceHoldingsUpdatedPayload {
        peer_id: peer_id.to_string(),
        holdings: as_vec,
        epoch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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
}
