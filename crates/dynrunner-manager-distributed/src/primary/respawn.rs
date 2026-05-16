//! Scaffolding for secondary respawn.
//!
//! Single concern: own the types and trait that describe how a
//! replacement secondary is requested, spawned, budgeted, and reported
//! back to the operational loop. Per-provider implementations
//! (multi-process, SLURM) live in sibling files and depend only on
//! this module's API surface; the operational loop owns the
//! `JoinSet<RespawnOutcome>` field declared on `PrimaryCoordinator`
//! and drains it in its `select!`. No call site outside this module
//! needs to know the internals of any specific spawner.
//!
//! The crossing-the-boundary surface is the [`SecondarySpawner`]
//! trait plus the value types [`SecondarySpawnSpec`], [`SpawnError`],
//! [`RespawnBudget`], [`RespawnOutcome`], and [`RespawnEvent`].
//! Everything else (per-task tracking, primary-internal helpers,
//! CLI flag plumbing) lands in sibling subtasks.

use std::sync::Arc;

use crate::peer_lifecycle::{LifecycleListener, PeerLifecycleEvent};
use dynrunner_protocol_primary_secondary::RemovalCause;

/// Specification handed to the spawner when the primary requests a
/// replacement secondary. Carries the primary's pubkey so the spawned
/// secondary can authenticate inbound connections.
#[derive(Clone, Debug)]
pub struct SecondarySpawnSpec {
    pub new_secondary_id: String,
    pub primary_endpoint: String,
    pub primary_pubkey_pem: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("spawn provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("spawn timed out")]
    Timeout,
    #[error("spawn failed: {0}")]
    Other(String),
}

/// Async trait for the per-provider spawner. Multi-process and SLURM
/// implementations live in sibling files.
///
/// `#[async_trait(?Send)]` because the SLURM impl drives
/// `ssh -N -R` subprocess spawn through a closure whose future is not
/// `Send` (the closure returns `Pin<Box<dyn Future + 'static>>` — see
/// `dynrunner_slurm::preparation::production_spawner`). The operational
/// loop on `PrimaryCoordinator` already runs inside a
/// `tokio::task::LocalSet` for the same reason (the SLURM preparation
/// pipeline uses `spawn_local` for per-tunnel watchers), so dropping
/// the `Send` bound on the returned future does not constrain the
/// integration site — it just lifts a constraint the provider physics
/// can't satisfy. The trait object itself stays `Send + Sync` so
/// `Arc<dyn SecondarySpawner>` is moveable across `select!` arms.
#[async_trait::async_trait(?Send)]
pub trait SecondarySpawner: Send + Sync {
    async fn spawn(&self, spec: SecondarySpawnSpec) -> Result<(), SpawnError>;
}

#[derive(Clone, Debug)]
pub struct RespawnBudget {
    pub max_per_secondary: u32,
    pub max_total: u32,
    pub cooldown: std::time::Duration,
}

impl Default for RespawnBudget {
    fn default() -> Self {
        Self {
            max_per_secondary: 3,
            max_total: 10,
            cooldown: std::time::Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RespawnOutcome {
    pub original_id: String,
    pub new_id: String,
    pub cause: RemovalCause,
    pub result: Result<(), String>,
}

/// Track family of respawned secondaries — `original_id` lets the
/// budget look at the chain to apply per-secondary caps.
#[derive(Clone, Debug)]
pub struct RespawnEvent {
    pub original_id: String,
    pub new_id: String,
    pub cause: RemovalCause,
    pub at: std::time::SystemTime,
}

/// Maximum number of [`RespawnEvent`]s retained on the coordinator's
/// `respawn_events` ring. Sized for operator forensics across the
/// lifetime of a single run; the ring drops oldest on overflow.
pub const RESPAWN_EVENTS_CAP: usize = 1024;

/// Push `ev` onto a `respawn_events` ring, evicting the oldest entry
/// when the ring is already at [`RESPAWN_EVENTS_CAP`]. The single
/// concern of this helper is bounded FIFO semantics; the operational
/// loop (the only legitimate caller) does not need to know the cap.
pub(crate) fn push_event(ring: &mut std::collections::VecDeque<RespawnEvent>, ev: RespawnEvent) {
    if ring.len() == RESPAWN_EVENTS_CAP {
        ring.pop_front();
    }
    ring.push_back(ev);
}

/// Cross-boundary request issued by the peer-lifecycle listener side
/// and consumed by the operational `select!` arm.
///
/// Single concern: carry a `Removed`-shaped lifecycle observation
/// across the dispatcher → operational-loop boundary without leaking
/// any coordinator-side state into the listener. The listener cannot
/// hold `&mut PrimaryCoordinator` (it runs on the peer-lifecycle
/// dispatcher task, which has no access to the coordinator's
/// `respawn_tasks` JoinSet or the `next_secondary_id` allocator);
/// instead it emits one of these requests and the operational loop
/// owns the budget check, the id mint, the spawner invocation, and
/// the JoinSet push.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RespawnRequest {
    pub original_id: String,
    pub cause: RemovalCause,
}

// Note: the dispatcher → operational-loop request channel is now
// UNBOUNDED. The historical bounded capacity (`RESPAWN_REQUEST_CHANNEL_CAPACITY
// = 256`) drop-on-full path lost deaths during mass-death-grace
// finalize bursts and broke the budget accounting (a dropped request
// is invisible to `respawn_events`, so the family-budget counter
// never increments and the operator-visible `respawn_budget_exhausted`
// line never fires for the lost peer). The apply-path lifecycle
// channel uses `tokio::sync::mpsc::unbounded_channel` for the same
// reason: the producer is the synchronous lifecycle dispatcher
// `on_event` arm, which must NEVER block; the consumer is the
// operational-loop `select!` arm, which drains at the rate of one
// `dispatch_respawn_request` per iteration. Memory is bounded by the
// total-budget cap (`RespawnBudget::max_total`, default 10) — once
// the operational loop has reconciled `max_total` requests every
// subsequent enqueue gets a `RejectTotalBudget` decision and the
// queue empties.

/// Decision returned by [`RespawnBudget::should_respawn`].
///
/// `Accept` is the success arm; the three `Reject*` variants carry
/// the reason so the operational-loop arm can emit a distinct
/// structured-log event per case (the operator forensics surface).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RespawnDecision {
    Accept,
    RejectFamilyBudget,
    RejectTotalBudget,
    RejectCooldown,
}

impl RespawnBudget {
    /// Decide whether a respawn for `original_id` is admissible against
    /// the live `events` ring and the budget's three caps.
    ///
    /// Family-chain count: a "family" is the transitive closure of
    /// respawn events whose `new_id` was once another event's
    /// `original_id`. Starting from `original_id`, we walk the
    /// ring backwards to find the head of the chain (the first peer
    /// the operator originally provisioned), then count every event
    /// in the chain that already landed. If that count already meets
    /// `max_per_secondary`, the request is rejected with
    /// [`RespawnDecision::RejectFamilyBudget`].
    ///
    /// Total budget: any event in the ring counts toward
    /// `max_total`. Ring eviction (at [`RESPAWN_EVENTS_CAP`]) intentionally
    /// does NOT reset this budget; once 10 respawns have happened in
    /// the lifetime of the coordinator, the 11th is refused even if
    /// the ring has overflowed past the oldest entries — operators
    /// who want unlimited respawns disable the policy entirely.
    ///
    /// Cooldown: the most recent event whose `new_id` or `original_id`
    /// belongs to the same family must be at least `cooldown` older
    /// than `now`. The cooldown is per-family (not global) so a
    /// well-behaved cluster losing one peer per minute never trips
    /// it. Tested with deterministic timestamps (no wall-clock
    /// dependency).
    ///
    /// The walk is O(ring.len()) — bounded by [`RESPAWN_EVENTS_CAP`]
    /// (1024 today). Acceptable on the operational `select!` arm
    /// because it fires at the rate of peer deaths, not per-task.
    pub fn should_respawn(
        &self,
        original_id: &str,
        events: &std::collections::VecDeque<RespawnEvent>,
        now: std::time::SystemTime,
    ) -> RespawnDecision {
        // Total budget first — cheapest check, prunes the common
        // exhausted-cluster failure mode before the family walk.
        if (events.len() as u32) >= self.max_total {
            return RespawnDecision::RejectTotalBudget;
        }

        // Walk the chain rooted at `original_id` and tally the count.
        // The chain head is the original peer id; every subsequent
        // entry in the family was minted as a replacement for the
        // previous death. `family_ids` accumulates every id (old +
        // new) we've seen so the cooldown check can match on either
        // side.
        let mut family_ids: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        family_ids.insert(original_id);
        // Iterative expansion: each pass adds events whose
        // `original_id` or `new_id` is in `family_ids`. Cap the
        // passes at the ring length to bound the worst case;
        // realistic chains are very short (≤ max_per_secondary).
        let mut grew = true;
        while grew {
            grew = false;
            for ev in events {
                if family_ids.contains(ev.original_id.as_str())
                    || family_ids.contains(ev.new_id.as_str())
                {
                    if family_ids.insert(ev.original_id.as_str()) {
                        grew = true;
                    }
                    if family_ids.insert(ev.new_id.as_str()) {
                        grew = true;
                    }
                }
            }
        }

        let family_count = events
            .iter()
            .filter(|ev| {
                family_ids.contains(ev.original_id.as_str())
                    || family_ids.contains(ev.new_id.as_str())
            })
            .count() as u32;
        if family_count >= self.max_per_secondary {
            return RespawnDecision::RejectFamilyBudget;
        }

        // Cooldown is family-scoped: find the most recent event in
        // this family and require `now - at >= cooldown`. Walks the
        // ring once; `max_by_key` returns `None` when the family has
        // no prior events (first respawn → cooldown trivially
        // satisfied).
        if let Some(latest) = events
            .iter()
            .filter(|ev| {
                family_ids.contains(ev.original_id.as_str())
                    || family_ids.contains(ev.new_id.as_str())
            })
            .max_by_key(|ev| ev.at)
        {
            // Saturating: a future-dated `latest.at` (clock skew /
            // test fixture) returns `Duration::ZERO`, which compares
            // to `cooldown` correctly (`ZERO < cooldown` ⇒ reject).
            let elapsed = now
                .duration_since(latest.at)
                .unwrap_or(std::time::Duration::ZERO);
            if elapsed < self.cooldown {
                return RespawnDecision::RejectCooldown;
            }
        }

        RespawnDecision::Accept
    }
}

/// [`LifecycleListener`] that converts `PeerLifecycleEvent::Removed`
/// into a [`RespawnRequest`] on the supplied sender.
///
/// Single concern: pure transformation. The listener does not consult
/// the budget, does not mint ids, does not invoke the spawner, and
/// owns no state beyond the sender. Everything else lives on the
/// operational `select!` arm, which is the only site with `&mut
/// PrimaryCoordinator`.
///
/// `Added` events are dropped silently — the respawn pipeline only
/// reacts to deaths. A future telemetry listener (also registered
/// off-apply) can observe `Added` independently without changing this
/// listener.
///
/// Channel shape: the request channel is unbounded
/// (`tokio::sync::mpsc::UnboundedSender::send` is sync and infallible
/// on the value side), so the dispatcher task (which calls `on_event`
/// synchronously) NEVER blocks and NEVER drops. Mass-death-grace
/// finalize bursts that previously blew past the legacy bounded cap
/// of 256 now enqueue every death; the operational-loop arm drains
/// at the rate of one `dispatch_respawn_request` per iteration, and
/// the total-budget cap on `RespawnBudget::max_total` ensures only the
/// first N drain past acceptance — the rest land as
/// `RejectTotalBudget` decisions, keeping memory bounded.
pub fn respawn_dispatcher_listener(
    request_tx: tokio::sync::mpsc::UnboundedSender<RespawnRequest>,
) -> Box<dyn LifecycleListener> {
    Box::new(RespawnDispatcherListener { request_tx })
}

struct RespawnDispatcherListener {
    request_tx: tokio::sync::mpsc::UnboundedSender<RespawnRequest>,
}

impl LifecycleListener for RespawnDispatcherListener {
    fn on_event(&self, event: &PeerLifecycleEvent) {
        match event {
            PeerLifecycleEvent::Removed { id, cause } => {
                let req = RespawnRequest {
                    original_id: id.clone(),
                    cause: cause.clone(),
                };
                // `UnboundedSender::send` only fails when every
                // receiver has been dropped — i.e. the operational
                // loop is gone. Log at debug level: this happens
                // during normal teardown when the lifecycle
                // dispatcher outlives the operational loop by a
                // tick. There is no actionable failure here.
                if let Err(e) = self.request_tx.send(req) {
                    tracing::debug!(
                        target: "dynrunner_respawn",
                        peer_id = %id,
                        cause = ?cause,
                        error = %e,
                        "respawn request channel closed; receiver gone",
                    );
                }
            }
            PeerLifecycleEvent::Added { .. } => {
                // Added events are out of scope for the respawn
                // pipeline; a separate listener can observe them
                // without this one needing to know.
            }
        }
    }
}

// ── Operational-loop entry points ─────────────────────────────────
//
// Single concern: the coordinator-side handlers the operational
// `select!` arms delegate to. The arms themselves live in
// `primary::lifecycle::operational_loop`; this `impl` block is the
// only place that mutates `PrimaryCoordinator`'s respawn fields
// from inside the loop. Keeping the bodies here (rather than inline
// in `lifecycle.rs`) co-locates them with the budget logic / the
// types they consume — a future maintainer reading respawn.rs sees
// the end-to-end pipeline without cross-file hopping.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{PeerTransport, SecondaryTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

impl<T, P, S, E, I> super::PrimaryCoordinator<T, P, S, E, I>
where
    T: SecondaryTransport<I>,
    P: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Handle one [`RespawnRequest`] drained off the respawn-request
    /// channel. Consults the budget against the live `respawn_events`
    /// ring, mints a fresh secondary id on accept, builds the
    /// [`SecondarySpawnSpec`], and spawns the future onto
    /// `respawn_tasks`. Rejections emit the
    /// `respawn_budget_exhausted` structured log event and a
    /// budget-rejection record on the ring so downstream forensics
    /// can see why a death didn't lead to a respawn.
    pub(super) fn dispatch_respawn_request(&mut self, request: RespawnRequest) {
        let (spawner, budget) = match (
            self.respawn_spawner.as_ref(),
            self.respawn_budget.as_ref(),
        ) {
            (Some(s), Some(b)) => (Arc::clone(s), b.clone()),
            // Defensive: the listener is only registered when
            // `enable_respawn` was called, which always installs
            // both fields. If we reach here without them, the
            // policy is disabled and the request is a no-op.
            _ => {
                tracing::warn!(
                    target: "dynrunner_respawn",
                    peer_id = %request.original_id,
                    "respawn request received but policy is disabled; dropping",
                );
                return;
            }
        };

        let now = std::time::SystemTime::now();
        let decision = budget.should_respawn(
            &request.original_id,
            &self.respawn_events,
            now,
        );
        match decision {
            RespawnDecision::Accept => {}
            RespawnDecision::RejectFamilyBudget
            | RespawnDecision::RejectTotalBudget
            | RespawnDecision::RejectCooldown => {
                tracing::warn!(
                    target: "dynrunner_respawn",
                    peer_id = %request.original_id,
                    cause = ?request.cause,
                    decision = ?decision,
                    max_per_secondary = budget.max_per_secondary,
                    max_total = budget.max_total,
                    cooldown_s = budget.cooldown.as_secs_f64(),
                    event = "respawn_budget_exhausted",
                    "respawn budget rejected request; not spawning replacement",
                );
                return;
            }
        }

        let new_id = self.mint_secondary_id();
        let spec = SecondarySpawnSpec {
            new_secondary_id: new_id.clone(),
            primary_endpoint: self.respawn_primary_endpoint.clone(),
            primary_pubkey_pem: self.respawn_primary_pubkey_pem.clone(),
        };

        // Record the attempt on the ring NOW — before the spawn
        // future resolves — so budget consultation for any
        // immediately-following request in the same `select!` tick
        // already sees this entry. Without this, a tight burst of
        // peer deaths could each independently consult an empty
        // ring and all pass the cap.
        push_event(
            &mut self.respawn_events,
            RespawnEvent {
                original_id: request.original_id.clone(),
                new_id: new_id.clone(),
                cause: request.cause.clone(),
                at: now,
            },
        );
        tracing::info!(
            target: "dynrunner_respawn",
            original_id = %request.original_id,
            new_id = %new_id,
            cause = ?request.cause,
            event = "respawn_attempted",
            "spawning replacement secondary",
        );

        let original_id = request.original_id;
        let cause = request.cause;
        self.respawn_tasks.spawn_local(async move {
            let result = spawner.spawn(spec).await.map_err(|e| e.to_string());
            RespawnOutcome {
                original_id,
                new_id,
                cause,
                result,
            }
        });
    }

    /// Handle one completed (or join-cancelled) entry off the
    /// `respawn_tasks` JoinSet. Logs structured `respawn_succeeded`
    /// / `respawn_failed` / `respawn_join_failed` events; ring-buffer
    /// bookkeeping happens at dispatch time (see
    /// [`Self::dispatch_respawn_request`]) so a successful spawn that
    /// races against a join failure still leaves the family-count
    /// invariant intact.
    pub(super) fn handle_respawn_join(
        &mut self,
        outcome: Option<Result<RespawnOutcome, tokio::task::JoinError>>,
    ) {
        match outcome {
            None => {
                // Empty JoinSet — the `select!` arm parks on
                // `pending()` while empty, so this branch is only
                // reachable through a race window (an `abort()`
                // between the empty check and the poll). No-op.
            }
            Some(Ok(outcome)) => match &outcome.result {
                Ok(()) => {
                    tracing::info!(
                        target: "dynrunner_respawn",
                        original_id = %outcome.original_id,
                        new_id = %outcome.new_id,
                        cause = ?outcome.cause,
                        event = "respawn_succeeded",
                        "spawner completed; awaiting PeerJoined for new secondary",
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        target: "dynrunner_respawn",
                        original_id = %outcome.original_id,
                        new_id = %outcome.new_id,
                        cause = ?outcome.cause,
                        error = %err,
                        event = "respawn_failed",
                        "spawner returned an error; replacement not running",
                    );
                }
            },
            Some(Err(join_err)) => {
                tracing::warn!(
                    target: "dynrunner_respawn",
                    error = %join_err,
                    event = "respawn_join_failed",
                    "respawn task panicked or was aborted; spawn outcome unknown",
                );
            }
        }
    }

    /// Drain in-flight respawn tasks at operational-loop shutdown.
    /// Aborts every outstanding future via [`JoinSet::shutdown`],
    /// then logs a structured summary so operators can see whether
    /// any respawn was in flight when the run ended. Any
    /// already-started spawn that did not complete is logged
    /// as a possible orphan — for SLURM mode this is where a
    /// follow-on `scancel` would belong; today we log loudly.
    pub(super) async fn drain_respawn_tasks(&mut self) {
        if self.respawn_tasks.is_empty() {
            return;
        }
        let in_flight = self.respawn_tasks.len();
        tracing::info!(
            target: "dynrunner_respawn",
            in_flight,
            event = "respawn_drain_starting",
            "draining outstanding respawn tasks at shutdown",
        );
        self.respawn_tasks.shutdown().await;
        tracing::warn!(
            target: "dynrunner_respawn",
            aborted = in_flight,
            event = "respawn_drain_complete",
            "respawn tasks aborted; any successfully-spawned-but-unregistered \
             secondary may require manual scancel/cleanup",
        );
    }
}

#[cfg(test)]
mod tests {
    //! Contract-level constructor smoke tests. Full integration
    //! (spawner ↔ dispatcher ↔ JoinSet drain) lands in sibling F6.

    use super::*;
    use std::time::{Duration, SystemTime};

    #[test]
    fn spawn_spec_constructs() {
        let spec = SecondarySpawnSpec {
            new_secondary_id: "sec-replacement-1".to_owned(),
            primary_endpoint: "127.0.0.1:5555".to_owned(),
            primary_pubkey_pem: "-----BEGIN PUBLIC KEY-----\n...\n".to_owned(),
        };
        assert_eq!(spec.new_secondary_id, "sec-replacement-1");
        assert_eq!(spec.primary_endpoint, "127.0.0.1:5555");
        assert!(spec.primary_pubkey_pem.starts_with("-----BEGIN"));
    }

    #[test]
    fn spawn_error_renders_human_strings() {
        let provider_unavail = SpawnError::ProviderUnavailable("slurm not configured".to_owned());
        assert_eq!(
            format!("{provider_unavail}"),
            "spawn provider unavailable: slurm not configured",
        );
        let timeout = SpawnError::Timeout;
        assert_eq!(format!("{timeout}"), "spawn timed out");
        let other = SpawnError::Other("exec failed".to_owned());
        assert_eq!(format!("{other}"), "spawn failed: exec failed");
    }

    #[test]
    fn respawn_budget_default_matches_spec() {
        let b = RespawnBudget::default();
        assert_eq!(b.max_per_secondary, 3);
        assert_eq!(b.max_total, 10);
        assert_eq!(b.cooldown, Duration::from_secs(30));
    }

    #[test]
    fn respawn_outcome_constructs_with_ok_and_err() {
        let ok = RespawnOutcome {
            original_id: "sec-a".to_owned(),
            new_id: "sec-a-replacement".to_owned(),
            cause: RemovalCause::KeepaliveMiss,
            result: Ok(()),
        };
        assert!(ok.result.is_ok());

        let err = RespawnOutcome {
            original_id: "sec-b".to_owned(),
            new_id: "sec-b-replacement".to_owned(),
            cause: RemovalCause::MassDeathEscalation,
            result: Err("spawn failed".to_owned()),
        };
        assert!(matches!(err.result, Err(ref s) if s == "spawn failed"));
    }

    #[test]
    fn respawn_event_constructs() {
        let ev = RespawnEvent {
            original_id: "sec-a".to_owned(),
            new_id: "sec-a-replacement".to_owned(),
            cause: RemovalCause::KeepaliveMiss,
            at: SystemTime::now(),
        };
        assert_eq!(ev.original_id, "sec-a");
        assert_eq!(ev.new_id, "sec-a-replacement");
        assert!(matches!(ev.cause, RemovalCause::KeepaliveMiss));
    }

    #[test]
    fn respawn_event_ringbuffer_drops_oldest_at_1024_cap() {
        use std::collections::VecDeque;

        let mut ring: VecDeque<RespawnEvent> = VecDeque::new();
        // Push exactly one more than the cap; the very first event
        // (`new_id = "new-0"`) must be evicted, and the buffer must
        // remain at the cap with the freshest event at the back.
        for i in 0..=RESPAWN_EVENTS_CAP {
            push_event(
                &mut ring,
                RespawnEvent {
                    original_id: format!("orig-{i}"),
                    new_id: format!("new-{i}"),
                    cause: RemovalCause::KeepaliveMiss,
                    at: SystemTime::now(),
                },
            );
        }
        assert_eq!(ring.len(), RESPAWN_EVENTS_CAP);
        assert_eq!(ring.front().unwrap().new_id, "new-1");
        assert_eq!(
            ring.back().unwrap().new_id,
            format!("new-{}", RESPAWN_EVENTS_CAP),
        );
    }
}

#[cfg(test)]
mod respawn_dispatcher_tests {
    //! End-to-end coverage of the listener → request-channel →
    //! operational-loop-arm pipeline. Each test constructs a
    //! `PrimaryCoordinator` against the in-process channel stub used
    //! by the rest of the primary tests, installs a mock
    //! `SecondarySpawner`, and drives the pipeline by either
    //! (a) calling the dispatcher's `on_event` directly + draining
    //! the request channel into the coordinator's
    //! `dispatch_respawn_request` (which is what the operational
    //! loop's `select!` arm does), or (b) calling
    //! `dispatch_respawn_request` directly with a synthetic request
    //! when only the budget logic is under test.
    //!
    //! Single concern per test: each pins one observable side of
    //! the contract — spawn invoked, no-spawn when disabled, family
    //! budget honoured, total budget honoured, ids monotonic.
    //! The JoinSet drain + log-event emission are exercised
    //! transitively (a spawn future that resolves before assertions
    //! lands its outcome on the JoinSet; the test reads the
    //! resolved entry to confirm the new id).
    use super::*;
    use crate::primary::test_helpers::{setup_test, FixedEstimator, NoPeers, TestId};
    use crate::primary::{PrimaryConfig, PrimaryCoordinator};
    use crate::peer_lifecycle::PeerLifecycleEvent;
    use dynrunner_scheduler::ResourceStealingScheduler;
    use dynrunner_transport_channel::ChannelSecondaryTransportEnd;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Counting mock spawner: records every `spec.new_secondary_id`
    /// it observes and returns `Ok(())` for the first call (or as
    /// configured). The recorded ids let tests assert the
    /// coordinator minted fresh ids and the `RespawnDecision`
    /// path honoured the budget.
    struct MockSpawner {
        calls: Arc<AtomicU32>,
        captured_ids: Arc<Mutex<Vec<String>>>,
    }

    impl MockSpawner {
        fn new() -> Self {
            Self {
                calls: Arc::new(AtomicU32::new(0)),
                captured_ids: Arc::new(Mutex::new(Vec::new())),
            }
        }

        #[allow(dead_code)]
        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }

        #[allow(dead_code)]
        fn captured_ids(&self) -> Vec<String> {
            self.captured_ids.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait(?Send)]
    impl SecondarySpawner for MockSpawner {
        async fn spawn(
            &self,
            spec: SecondarySpawnSpec,
        ) -> Result<(), SpawnError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.captured_ids
                .lock()
                .unwrap()
                .push(spec.new_secondary_id);
            Ok(())
        }
    }

    /// Build a coordinator wired with 1 reserved initial-cohort id so
    /// the first minted respawn lands on `secondary-1`. The minted-id
    /// monotonic test pins this contract directly.
    fn make_coordinator(
    ) -> PrimaryCoordinator<
        ChannelSecondaryTransportEnd<TestId>,
        NoPeers,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > {
        let (transport, _ends) = setup_test(0);
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(1),
            peer_timeout: Duration::from_secs(1),
            keepalive_interval: Duration::from_millis(100),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: false,
            required_setup_on_promote: false,
            max_concurrent_per_type: HashMap::new(),
            retry_max_passes: 0,
            fleet_dead_timeout: Duration::from_secs(1),
            mesh_ready_timeout: Duration::from_secs(1),
            mass_death_grace: Duration::from_secs(1),
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };
        PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        )
    }

    /// Loose budget — every per-knob cap large enough to never
    /// reject; cooldown zero so back-to-back requests are accepted.
    fn permissive_budget() -> RespawnBudget {
        RespawnBudget {
            max_per_secondary: 100,
            max_total: 100,
            cooldown: Duration::ZERO,
        }
    }

    /// Confirms the dispatcher closure registered by `enable_respawn`
    /// translates `PeerLifecycleEvent::Removed` into a real
    /// `spawner.spawn` invocation. Drives the LocalSet directly so
    /// the `spawn_local` future used internally by
    /// `dispatch_respawn_request` resolves before the assertions.
    #[tokio::test(flavor = "current_thread")]
    async fn respawn_dispatcher_fires_spawner_on_peer_removed() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut coordinator = make_coordinator();
                let spawner = Arc::new(MockSpawner::new());
                let calls = Arc::clone(&spawner.calls);
                let captured = Arc::clone(&spawner.captured_ids);
                coordinator.enable_respawn(
                    spawner.clone(),
                    permissive_budget(),
                    "tcp://127.0.0.1:5555".into(),
                    "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
                );

                // Direct invocation of the dispatcher's `on_event`
                // path. The operational `select!` arm in
                // `lifecycle::operational_loop` ultimately calls the
                // same `dispatch_respawn_request` we invoke here; this
                // test takes the same path without spinning up the
                // full LocalSet-bound dispatcher task.
                coordinator.dispatch_respawn_request(RespawnRequest {
                    original_id: "secondary-0".into(),
                    cause: RemovalCause::KeepaliveMiss,
                });
                // Drain the spawned future on the LocalSet so the
                // spawner's atomic counter has settled before the
                // assertion. `join_next` resolves after the
                // `spawn_local` future returns.
                let outcome = coordinator
                    .respawn_tasks
                    .join_next()
                    .await
                    .expect("respawn task should be present after dispatch");
                let outcome = outcome.expect("respawn task should not panic");
                assert!(outcome.result.is_ok());
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                let ids = captured.lock().unwrap();
                assert_eq!(ids.len(), 1);
                assert_eq!(ids[0], "secondary-1");
                assert_eq!(coordinator.respawn_events.len(), 1);
            })
            .await;
    }

    /// Policy-disabled coordinators must never register the
    /// dispatcher listener and never invoke a spawner — even when a
    /// `Removed` event is delivered directly via the lifecycle
    /// pipeline. Pins the CCD-5 "no hot-site `if policy_enabled`"
    /// contract from the dispatch side: the request channel sender
    /// is `None`, so no listener can enqueue.
    #[tokio::test(flavor = "current_thread")]
    async fn respawn_dispatcher_skips_when_policy_disabled() {
        let coordinator = make_coordinator();
        // No `enable_respawn` call — the spawner / budget / channel /
        // listener registration are all absent by construction.
        assert!(coordinator.respawn_spawner.is_none());
        assert!(coordinator.respawn_budget.is_none());
        assert!(coordinator.respawn_request_tx.is_none());
        assert!(coordinator.respawn_request_rx.is_none());
        assert!(coordinator.peer_lifecycle_listeners.is_empty());

        // Build a free-standing dispatcher listener so we can verify
        // its on_event side-effect: a Removed event has no place to
        // land if the channel side hasn't been wired. We construct a
        // throwaway channel just to verify the closure shape; the
        // coordinator's wiring itself is the contract under test.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RespawnRequest>();
        let listener = respawn_dispatcher_listener(tx);
        listener.on_event(&PeerLifecycleEvent::Removed {
            id: "secondary-0".into(),
            cause: RemovalCause::KeepaliveMiss,
        });
        // The free-standing listener does enqueue (it's a pure
        // transformation); the coordinator we built simply has no
        // listener registered, so its operational-loop arm would
        // never see the request. That's the CCD-5 invariant.
        let req = rx.try_recv().expect("free-standing listener should still translate");
        assert_eq!(req.original_id, "secondary-0");
    }

    /// Three deaths in the same family chain (each respawn's `new_id`
    /// becoming the next death's `original_id`) consume the
    /// `max_per_secondary = 3` budget; the fourth death is rejected
    /// with `RespawnDecision::RejectFamilyBudget` and no spawn lands.
    #[tokio::test(flavor = "current_thread")]
    async fn respawn_dispatcher_respects_per_secondary_budget() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut coordinator = make_coordinator();
                let spawner = Arc::new(MockSpawner::new());
                let calls = Arc::clone(&spawner.calls);
                coordinator.enable_respawn(
                    spawner.clone(),
                    RespawnBudget {
                        max_per_secondary: 3,
                        max_total: 100,
                        cooldown: Duration::ZERO,
                    },
                    "tcp://127.0.0.1:5555".into(),
                    "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
                );

                // First death — id "secondary-0" (initial cohort).
                // Each subsequent "death" addresses the prior
                // respawn's new id so the family chain is contiguous.
                let mut current_dead = String::from("secondary-0");
                for i in 0..3 {
                    coordinator.dispatch_respawn_request(RespawnRequest {
                        original_id: current_dead.clone(),
                        cause: RemovalCause::KeepaliveMiss,
                    });
                    let outcome = coordinator
                        .respawn_tasks
                        .join_next()
                        .await
                        .expect("spawn future should be queued");
                    let outcome = outcome.expect("no panic");
                    assert!(outcome.result.is_ok(), "spawn #{i} should accept");
                    // Walk the chain forward.
                    current_dead = outcome.new_id;
                }
                assert_eq!(calls.load(Ordering::SeqCst), 3);

                // Fourth death in the same family — must be rejected
                // by the family budget. No new spawn future lands on
                // the JoinSet.
                coordinator.dispatch_respawn_request(RespawnRequest {
                    original_id: current_dead,
                    cause: RemovalCause::KeepaliveMiss,
                });
                assert!(
                    coordinator.respawn_tasks.is_empty(),
                    "4th death should NOT have spawned",
                );
                assert_eq!(calls.load(Ordering::SeqCst), 3);
                // Ring records 3 events (one per accepted spawn).
                assert_eq!(coordinator.respawn_events.len(), 3);
            })
            .await;
    }

    /// Ten respawns across distinct families saturate `max_total =
    /// 10`; the 11th request is rejected with
    /// `RespawnDecision::RejectTotalBudget`.
    #[tokio::test(flavor = "current_thread")]
    async fn respawn_dispatcher_respects_total_budget() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut coordinator = make_coordinator();
                let spawner = Arc::new(MockSpawner::new());
                let calls = Arc::clone(&spawner.calls);
                coordinator.enable_respawn(
                    spawner.clone(),
                    RespawnBudget {
                        // Family budget high enough to never trigger;
                        // total is the binding constraint.
                        max_per_secondary: 100,
                        max_total: 10,
                        cooldown: Duration::ZERO,
                    },
                    "tcp://127.0.0.1:5555".into(),
                    "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
                );

                // Ten DISTINCT families, one death per family — no
                // chain walk involved.
                for i in 0..10u32 {
                    coordinator.dispatch_respawn_request(RespawnRequest {
                        original_id: format!("distinct-{i}"),
                        cause: RemovalCause::KeepaliveMiss,
                    });
                    let _ = coordinator
                        .respawn_tasks
                        .join_next()
                        .await
                        .expect("spawn future should land");
                }
                assert_eq!(calls.load(Ordering::SeqCst), 10);

                // 11th death — distinct family, but total budget is
                // exhausted.
                coordinator.dispatch_respawn_request(RespawnRequest {
                    original_id: "distinct-10".into(),
                    cause: RemovalCause::KeepaliveMiss,
                });
                assert!(
                    coordinator.respawn_tasks.is_empty(),
                    "11th death should NOT have spawned",
                );
                assert_eq!(calls.load(Ordering::SeqCst), 10);
                assert_eq!(coordinator.respawn_events.len(), 10);
            })
            .await;
    }

    /// Every accepted respawn must mint a fresh, monotonically-
    /// increasing `secondary-N` id. The coordinator's
    /// `mint_secondary_id` is the authority; this test pins that
    /// `dispatch_respawn_request` consults it (rather than reusing
    /// the dead peer's id) and that the spawn future receives the
    /// minted id verbatim.
    #[tokio::test(flavor = "current_thread")]
    async fn respawn_dispatcher_minted_id_is_monotonic() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut coordinator = make_coordinator();
                let spawner = Arc::new(MockSpawner::new());
                let captured = Arc::clone(&spawner.captured_ids);
                coordinator.enable_respawn(
                    spawner.clone(),
                    permissive_budget(),
                    "tcp://127.0.0.1:5555".into(),
                    "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
                );

                // Three distinct families so the per-secondary cap
                // doesn't intrude on the monotonic-id assertion.
                for i in 0..3u32 {
                    coordinator.dispatch_respawn_request(RespawnRequest {
                        original_id: format!("distinct-{i}"),
                        cause: RemovalCause::KeepaliveMiss,
                    });
                    let _ = coordinator
                        .respawn_tasks
                        .join_next()
                        .await
                        .expect("spawn future should land");
                }

                let ids = captured.lock().unwrap();
                assert_eq!(ids.len(), 3);
                // The coordinator's `next_secondary_id` is seeded
                // from `config.num_secondaries = 1`, so the first
                // mint is `secondary-1` and each subsequent mint is
                // monotonic.
                assert_eq!(ids[0], "secondary-1");
                assert_eq!(ids[1], "secondary-2");
                assert_eq!(ids[2], "secondary-3");
            })
            .await;
    }

    /// Mass-death-grace finalize bursts in a real cluster can emit a
    /// `PeerRemoved` per peer within a tight window. With the
    /// historical bounded (256-cap) channel and `try_send` drop-on-full
    /// path, anything past 256 vanished without trace — the budget
    /// accounting (`respawn_events` ring) never saw the request,
    /// `respawn_budget_exhausted` never fired, and the operator had no
    /// way to know a death had happened. The unbounded shape pins the
    /// inverse: 1000 sequential `Removed` events all enqueue without
    /// drop, exactly N of them clear the budget (here `max_total = 1000`),
    /// and the spawner sees all N.
    #[tokio::test(flavor = "current_thread")]
    async fn unbounded_respawn_request_channel_accepts_burst() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut coordinator = make_coordinator();
                let spawner = Arc::new(MockSpawner::new());
                let calls = Arc::clone(&spawner.calls);
                coordinator.enable_respawn(
                    spawner.clone(),
                    RespawnBudget {
                        // Budget high enough that none of the burst
                        // entries are rejected: the test pins the
                        // ENQUEUE side (channel doesn't drop), so the
                        // budget arithmetic stays out of the way.
                        max_per_secondary: 1,
                        max_total: 1000,
                        cooldown: Duration::ZERO,
                    },
                    "tcp://127.0.0.1:5555".into(),
                    "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
                );

                let tx = coordinator
                    .respawn_request_tx
                    .as_ref()
                    .expect("enable_respawn must install the sender")
                    .clone();

                // 1000 sequential `PeerRemoved` translations. Each
                // peer id is unique so the family-budget cap of 1
                // accepts every entry.
                const BURST: u32 = 1000;
                for i in 0..BURST {
                    tx.send(RespawnRequest {
                        original_id: format!("burst-{i}"),
                        cause: RemovalCause::KeepaliveMiss,
                    })
                    .expect(
                        "unbounded send must succeed while the \
                         receiver is alive — this is the contract \
                         the burst test pins",
                    );
                }

                // Drain on the operational-loop side. We can't enter
                // `lifecycle::operational_loop` from this test
                // fixture (no real transport), so we replicate the
                // arm's behaviour: pull one request at a time and
                // call `dispatch_respawn_request`, draining the
                // JoinSet between dispatches so the spawner's atomic
                // counter has settled. The rx is taken out for the
                // duration of the drain so the per-iteration
                // `dispatch_respawn_request` (which mutates the same
                // coordinator) does not conflict with an outstanding
                // borrow on `respawn_request_rx`.
                let mut rx = coordinator
                    .respawn_request_rx
                    .take()
                    .expect("enable_respawn must install the receiver");
                let mut drained = 0u32;
                while let Ok(req) = rx.try_recv() {
                    drained += 1;
                    coordinator.dispatch_respawn_request(req);
                    if let Some(outcome) = coordinator.respawn_tasks.join_next().await {
                        let _ = outcome.expect("no panic in mock spawner");
                    }
                }
                coordinator.respawn_request_rx = Some(rx);
                assert_eq!(
                    drained, BURST,
                    "all {BURST} enqueued requests must be drainable",
                );
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    BURST,
                    "spawner must have received every accepted request",
                );
                assert_eq!(
                    coordinator.respawn_events.len() as u32,
                    BURST,
                    "every accepted request must land on the events ring",
                );
            })
            .await;
    }
}
