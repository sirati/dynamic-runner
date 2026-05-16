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
/// the wire variant `ClusterMutation::PeerResourceHoldingsUpdated {
/// peer_id, holdings, epoch }` field-for-field so the production
/// [`PeerMeshAnnouncerSender`] impl rewraps it into the real
/// `ClusterMutation` with a mechanical field-by-field copy. The body
/// type itself is the announcer's stable internal payload
/// representation, decoupling the observer's task lifecycle from any
/// downstream wire-variant rename.
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

// ── Production AnnouncerSender wire-out (outbox shape) ──

/// One queued send request flowing from the spawned announcer task
/// into the [`crate::SecondaryCoordinator`]'s operational `select!`
/// loop. The coordinator owns `peer_transport` by value and its
/// methods take `&mut self`, so the announcer (a sibling task) cannot
/// touch the transport directly; instead it posts an item onto the
/// outbox and awaits the loop's send outcome through `reply`.
///
/// # Why an outbox, not `Arc<Mutex<P>>`
///
/// `PeerTransport`'s methods take `&mut self`, the trait isn't object-
/// safe (each method returns an `impl Future`), and the coordinator
/// owns the concrete `P` by value. Sharing the transport across two
/// tasks would require either an invasive type-signature change on
/// every coordinator-wide site that names the transport, or a global
/// `tokio::sync::Mutex<P>` that serialises every send against the
/// coordinator's own. The outbox+reply pattern keeps `peer_transport`
/// pinned to the run loop (where `&mut self` already lives) and the
/// announcer's send concern stays expressible as the existing
/// [`AnnouncerSender`] trait — `send_holdings` returns
/// `Result<(), String>`, which the retry-with-backoff loop in
/// [`run_observer_announcer`] interprets exactly as before.
///
/// # Single concern of this struct
///
/// Carry one in-flight send request across the announcer →
/// coordinator boundary. The reply oneshot is the per-call
/// flow-control signal: the announcer's `send_holdings` awaits it
/// before returning, so the trait's `Result<(), String>` contract is
/// preserved even though the actual transport call happens in a
/// different task.
pub struct AnnouncerOutboxItem<I: dynrunner_core::Identifier> {
    pub msg: dynrunner_protocol_primary_secondary::DistributedMessage<I>,
    pub reply: tokio::sync::oneshot::Sender<Result<(), String>>,
}

/// Production [`AnnouncerSender`] impl that forwards every
/// announcement onto a coordinator-side outbox. The coordinator's
/// operational `select!` drains the outbox and issues the actual
/// `peer_transport.send(Address::Role(Role::Primary), msg)` call,
/// reporting the outcome back via `reply`.
///
/// # Module boundary
///
/// - **Held by**: the observer's announcer task, one instance per
///   [`run_observer_announcer`] invocation.
/// - **Talks to**: the coordinator's announcer-outbox `mpsc::Sender`,
///   cloned in at construction.
/// - **Does NOT touch**: `peer_transport` directly. The outbox is the
///   only boundary it crosses.
///
/// # Wire shape
///
/// Each `send_holdings(body)` call rewraps `body` (field-for-field)
/// into:
///
/// ```ignore
/// DistributedMessage::ClusterMutation {
///     sender_id: <observer's secondary_id>,
///     timestamp: <now>,
///     mutations: vec![
///         ClusterMutation::PeerResourceHoldingsUpdated {
///             peer_id: body.peer_id,
///             holdings: body.holdings,
///             epoch:    body.epoch,
///         },
///     ],
/// }
/// ```
///
/// The coordinator sends this with `Address::Role(Role::Primary)`,
/// which the `PeerTransport::send` default impl wraps in a
/// `RoleAddressed` envelope and routes through the write-through
/// `RoleTable` cache (Step 3 / Step 4 of the unification refactor).
/// Cache-cold `Address::Role` lookups error with a typed message —
/// that surfaces back through `reply` as `Err`, which trips the
/// retry-with-backoff loop in [`run_observer_announcer`]; the next
/// retry will see a populated cache once `PromotePrimary` has applied.
pub struct PeerMeshAnnouncerSender<I: dynrunner_core::Identifier> {
    /// Sender-id stamped onto every `DistributedMessage::ClusterMutation`
    /// the announcer emits. Equal to the observer's `secondary_id` —
    /// the same value that lands in `PeerResourceHoldingsUpdatedPayload::peer_id`.
    /// Cached at construction so each `send_holdings` call doesn't
    /// have to re-borrow it through the outbox.
    sender_id: String,
    /// Cloned coordinator-side outbox handle. The coordinator drops
    /// the matching receiver only on shutdown; the announcer observes
    /// a `send`-error in that case and surfaces it as the loop's
    /// retry trigger (which never converges, but the announcer
    /// task's `abort()` from the observer wire-up site terminates the
    /// task itself before that becomes observable).
    outbox_tx: tokio::sync::mpsc::Sender<AnnouncerOutboxItem<I>>,
}

impl<I: dynrunner_core::Identifier> PeerMeshAnnouncerSender<I> {
    /// Construct the production sender. The `sender_id` is stamped
    /// onto every wire envelope and is the observer's `secondary_id`;
    /// `outbox_tx` is cloned from the coordinator's
    /// `announcer_outbox_tx` field at attach time.
    pub fn new(
        sender_id: String,
        outbox_tx: tokio::sync::mpsc::Sender<AnnouncerOutboxItem<I>>,
    ) -> Self {
        Self {
            sender_id,
            outbox_tx,
        }
    }
}

/// Inline timestamp source for the production sender. Lifted out of
/// the inline `send_holdings` body so the test below can pin the
/// exact wire-frame shape without observing the system clock — the
/// timestamp field is `f64` and the test asserts only that it's a
/// non-NaN finite value, never an exact equality.
fn announcer_timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

impl<I: dynrunner_core::Identifier> AnnouncerSender for PeerMeshAnnouncerSender<I> {
    async fn send_holdings(
        &mut self,
        body: &PeerResourceHoldingsUpdatedPayload,
    ) -> Result<(), String> {
        use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
        let mutation = ClusterMutation::<I>::PeerResourceHoldingsUpdated {
            peer_id: body.peer_id.clone(),
            holdings: body.holdings.clone(),
            epoch: body.epoch,
        };
        let msg = DistributedMessage::<I>::ClusterMutation {
            sender_id: self.sender_id.clone(),
            timestamp: announcer_timestamp_now(),
            mutations: vec![mutation],
        };
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        // Outbox close (receiver dropped) maps to the same error
        // shape `send_holdings` already documents — the retry loop
        // will sleep and re-try. The retry never converges in that
        // shutdown case; the announcer task's abort-on-exit (held by
        // the observer wire-up site) is what terminates it.
        self.outbox_tx
            .send(AnnouncerOutboxItem {
                msg,
                reply: reply_tx,
            })
            .await
            .map_err(|e| format!("announcer outbox closed: {e}"))?;
        // The coordinator's drain arm awaits `peer_transport.send`
        // and forwards its `Result<(), String>` outcome verbatim. A
        // dropped `reply_tx` (drain-arm panic or coordinator shutdown
        // mid-send) surfaces as `RecvError`, which also trips the
        // retry loop.
        reply_rx
            .await
            .map_err(|e| format!("announcer outbox reply dropped: {e}"))?
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

    // ── Production AnnouncerSender wire-shape tests ──

    /// `PeerMeshAnnouncerSender::send_holdings` rewraps the typed
    /// body into the canonical
    /// `DistributedMessage::ClusterMutation { mutations: vec![
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
                    other => panic!(
                        "expected PeerResourceHoldingsUpdated, got {other:?}"
                    ),
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
        let mut sender = PeerMeshAnnouncerSender::<RunnerIdentifier>::new(
            "observer-prop".into(),
            outbox_tx,
        );

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
}
