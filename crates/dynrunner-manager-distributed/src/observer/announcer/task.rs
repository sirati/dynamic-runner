//! Announcer task body — the retry-with-backoff loop that drains
//! `AnnounceTrigger`s and drives one delivery attempt per trigger to
//! success.
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

use super::types::{AnnounceTrigger, AnnouncerSender, PeerResourceHoldingsUpdatedPayload};

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
pub(super) fn build_payload(
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
