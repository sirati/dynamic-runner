//! Primary-side custom-message concern (F5): the `CustomMessage` ingest
//! arm and the handler-DISPATCH decision over the replicated inbox.
//!
//! Single concern: WHO invokes the consumer's `custom_message_handler`,
//! in WHAT order, and HOW each invocation's outcome lands in the
//! replicated inbox. The replicated facts it reads/originates
//! (`custom_messages` + the `CustomMessagePosted` /
//! `CustomMessageHandled` / `CustomMessageFailed` mutations) are owned
//! by `cluster_state/apply_custom.rs`; the at-least-once transport leg
//! is owned by the secondary's retention chokepoint; the ack echo is
//! the generic `ack_delivery_report` in `connect.rs`. This module never
//! touches any of those internals — it composes their APIs.
//!
//! # The dispatch decision (per the F5 design)
//!
//! * DROPPABLE (`important = false`): dispatch the handler directly on
//!   ingest, at-most-once — a raise is WARNed, the message is gone
//!   (lost on failover by design; never CRDT-resident), and the
//!   handler's queued commands are discarded (the same all-or-nothing
//!   handler contract as the important class).
//! * IMPORTANT: the ingest originates `CustomMessagePosted` (idempotent
//!   under transport replays — the `(origin, seq)` vacant-insert NoOps
//!   a duplicate), then runs the decision over EVERY `Unhandled` entry
//!   in `(origin, seq)` order. Each entry resolves TERMINALLY in the
//!   same pass:
//!     - clean handler return → the ATOMIC effect+terminal batch (see
//!       below): the handler's queued `PrimaryHandle` commands drain
//!       through the capturing variant of the one
//!       `drain_callback_queued_commands` chokepoint and their cluster
//!       mutations ride ONE wire frame together with
//!       `CustomMessageHandled` (effects first, terminal last);
//!     - raise → a USER ERROR, terminal `Failed`: the queued commands
//!       are discarded UNEXECUTED (all-or-nothing — no partial effect
//!       can ever land anywhere), `CustomMessageFailed` is originated
//!       ALONE, and a structured ERROR carries origin/seq/topic + the
//!       exception. No retry, no backoff, no poison cap — ever.
//! * PER-ORIGIN ORDER: the sorted `(origin, seq)` walk + the
//!   synchronous terminal resolution of every entry preserve the
//!   consumer's per-origin send order by construction (nothing is ever
//!   deferred, so nothing can be overtaken).
//! * NO BOUND: the unhandled set is never capped. The keep-up monitor
//!   ([`CustomBacklogMonitor`]) WARNs — rate-limited — when the backlog
//!   grows across consecutive heartbeat ticks or its oldest entry ages
//!   past [`CUSTOM_BACKLOG_OLDEST_WARN`].
//!
//! # Atomicity (both causality directions)
//!
//! It is impossible for a message's EFFECT to be in the CRDT without
//! its terminal state, or the terminal state without the effect:
//!
//! * One wire frame: the capturing drain diverts every mutation the
//!   handler's commands originate into one batch, the terminal fact is
//!   appended through the same chokepoint, and the batch flushes via
//!   `broadcast_applied_mutations` as ONE
//!   `DistributedMessage::ClusterMutation` frame. The replica-side
//!   batch apply (`secondary::dispatch::helpers::apply_cluster_mutations`
//!   and the primary-receive twin) is a synchronous, await-free loop
//!   over the frame's mutations — both-or-neither on every replica.
//!   (Frame size is a non-issue: even a 200-descriptor spawn batch +
//!   terminal is well under the 96 MiB wire limit, #366.)
//! * A primary that dies BEFORE the frame lands leaves `Unhandled` +
//!   no effect in every replica; the promoted primary's replay
//!   re-handles and the re-produced effect+terminal batch is absorbed
//!   by the idempotent spawn dedup (fail-SAFE).
//! * A raising handler's effect is discarded UNEXECUTED — never in the
//!   local pool, the local CRDT, or any snapshot — so `Failed` is
//!   always terminal-without-effect BY CONSTRUCTION, and the discarded
//!   commands' repliers receive an explicit rejection.
//!
//! The local primary's OWN apply of the captured batch happens
//! per-command during the drain (identical local semantics to the
//! plain drain — the capture only diverts the WIRE leg); the transient
//! local effect+`Unhandled` window this opens is the same benign
//! pre-terminal state a snapshot may legitimately carry (a death there
//! replays, and the dedup absorbs).
//!
//! # Dispatch triggers
//!
//! 1. The ingest arm (`handle_custom_message`) — the live path.
//! 2. The promotion replay: `run_pipeline`'s operational arm calls
//!    [`PrimaryCoordinator::dispatch_unhandled_custom_messages`] after
//!    hydrate, so a primary that died between landing and handling has
//!    its `Unhandled` residue re-dispatched on the promoted primary.
//!    The replay walks ONLY `Unhandled` entries — `Failed` (like
//!    `Handled`) is terminal and never re-dispatched.
//! 3. The heartbeat tick — the periodic backstop dispatch (idempotent,
//!    cheap when the inbox holds no `Unhandled` entry) and the keep-up
//!    monitor's observation point ([`PrimaryCoordinator::observe_custom_backlog`]).
//!
//! Every trigger passes the live `command_rx` so the handler's
//! in-runtime `PrimaryHandle` commands (the streamed-spawn site's
//! `spawn_tasks`) drain inline through the SAME
//! `drain_callback_queued_commands` chokepoint `on_phase_end` uses.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use super::PrimaryCoordinator;
use super::command_channel::PrimaryCommand;

/// Oldest-entry age past which the keep-up monitor WARNs even without
/// tick-over-tick growth: an `Unhandled` entry a minute old means the
/// dispatch decision has not resolved it across many heartbeat ticks.
pub(crate) const CUSTOM_BACKLOG_OLDEST_WARN: Duration = Duration::from_secs(60);

/// Minimum spacing between two keep-up WARNs (the rate limit): the
/// diagnostic is a trend signal, not a per-tick alarm.
pub(crate) const CUSTOM_BACKLOG_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Node-local keep-up monitor for the replicated custom-message inbox
/// (F5): tracks when each `Unhandled` key was FIRST OBSERVED on this
/// node (posted-at instants are node-local by design — wall-clock age
/// does not replicate) and decides — purely, testably — when the
/// "handler is not keeping up" WARN fires.
///
/// WARN policy: fire when the backlog GREW across consecutive
/// observations (this tick's count > last tick's) or the oldest live
/// entry is older than [`CUSTOM_BACKLOG_OLDEST_WARN`]; rate-limited to
/// one WARN per [`CUSTOM_BACKLOG_WARN_INTERVAL`]. There is NO bound on
/// the backlog itself — observability only, never backpressure.
#[derive(Debug, Default)]
pub(crate) struct CustomBacklogMonitor {
    /// First-observation instant per live `Unhandled` key. Entries are
    /// forgotten the observation after their key leaves the backlog.
    first_seen: HashMap<(String, u64), Instant>,
    /// The previous observation's backlog count (the growth detector).
    prev_count: usize,
    /// When the last WARN fired (the rate limit).
    last_warn: Option<Instant>,
}

/// One fired keep-up observation — the WARN's payload.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CustomBacklogReport {
    /// Live `Unhandled` entries at observation time.
    pub(crate) count: usize,
    /// Age of the oldest live entry (since first observed HERE).
    pub(crate) oldest_age: Duration,
}

impl CustomBacklogMonitor {
    /// Feed one observation (the current `Unhandled` key set at `now`);
    /// returns `Some(report)` exactly when the WARN should fire.
    pub(crate) fn observe(
        &mut self,
        live: &[(String, u64)],
        now: Instant,
    ) -> Option<CustomBacklogReport> {
        let live_set: std::collections::HashSet<&(String, u64)> = live.iter().collect();
        self.first_seen.retain(|k, _| live_set.contains(k));
        for k in live {
            self.first_seen.entry(k.clone()).or_insert(now);
        }
        let count = live.len();
        let grew = count > self.prev_count;
        self.prev_count = count;
        if count == 0 {
            return None;
        }
        let oldest_age = self
            .first_seen
            .values()
            .map(|t| now.duration_since(*t))
            .max()
            .unwrap_or_default();
        if !(grew || oldest_age > CUSTOM_BACKLOG_OLDEST_WARN) {
            return None;
        }
        if self
            .last_warn
            .is_some_and(|t| now.duration_since(t) < CUSTOM_BACKLOG_WARN_INTERVAL)
        {
            return None;
        }
        self.last_warn = Some(now);
        Some(CustomBacklogReport { count, oldest_age })
    }
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Ingest arm for a [`DistributedMessage::CustomMessage`] landing.
    ///
    /// The generic ack echo already ran in `dispatch_message` (every
    /// `delivery_seq`-stamped landing is acked BEFORE the handlers,
    /// including dedup-dropped duplicates — the #352 law), so this arm
    /// owns only the class routing:
    ///   * droppable → direct handler dispatch, done;
    ///   * important → originate `CustomMessagePosted` (a transport
    ///     replay NoOps on the `(origin, seq)` key), then run the
    ///     handler-dispatch decision over the whole `Unhandled` inbox.
    pub(super) async fn handle_custom_message(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let DistributedMessage::CustomMessage {
            origin_secondary_id,
            msg_seq,
            topic,
            data,
            important,
            ..
        } = msg
        else {
            return;
        };
        if !important {
            // DROPPABLE: at-most-once direct dispatch, no CRDT, no
            // retention, no retry. Same all-or-nothing handler
            // contract as the important class: a clean return drains
            // the handler's queued commands (plain per-command
            // broadcast — there is no terminal fact to be atomic
            // with); a raise loses the message by contract AND
            // discards the queued commands unexecuted.
            match self.invoke_custom_handler(&origin_secondary_id, &topic, &data, false) {
                Ok(()) => {
                    self.drain_callback_queued_commands(command_rx).await;
                }
                Err(reason) => {
                    let discarded = self.discard_callback_queued_commands(
                        command_rx,
                        "custom_message_handler raised; its queued commands are \
                         discarded (all-or-nothing handler semantics)",
                    );
                    tracing::warn!(
                        origin = %origin_secondary_id,
                        topic = %topic,
                        discarded_commands = discarded,
                        error = %reason,
                        "custom_message_handler raised for a droppable custom \
                         message; the message is lost (at-most-once contract) \
                         and its partial effect was discarded"
                    );
                }
            }
            return;
        }
        tracing::debug!(
            origin = %origin_secondary_id,
            msg_seq,
            topic = %topic,
            bytes = data.len(),
            "important custom message landed; posting to the replicated inbox"
        );
        // Originate the replicated `Unhandled` fact FIRST — the landing
        // must be CRDT-resident before any handler runs, so a death
        // between landing and handling leaves the entry `Unhandled` in
        // every replica (the promotion replay's input). A duplicate
        // landing NoOps here (vacant-insert) and was already re-acked.
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::CustomMessagePosted {
            origin: origin_secondary_id,
            seq: msg_seq,
            topic,
            data,
        }])
        .await;
        self.dispatch_unhandled_custom_messages(command_rx).await;
    }

    /// THE handler-dispatch decision (F5): walk every `Unhandled` inbox
    /// entry in `(origin, seq)` order and resolve each TERMINALLY —
    /// clean handler return → the atomic effect+`CustomMessageHandled`
    /// one-frame batch; raise → discard the partial effect unexecuted,
    /// originate `CustomMessageFailed` alone, structured ERROR (a raise
    /// is a USER ERROR: terminal, never retried). Within one origin the
    /// sorted walk + synchronous resolution preserve the consumer's
    /// per-origin send order. Idempotent + cheap when the inbox has no
    /// `Unhandled` entries (the steady-state hot path).
    ///
    /// Called from all three dispatch triggers (ingest, promotion
    /// replay, heartbeat backstop) — see the module doc.
    pub(crate) async fn dispatch_unhandled_custom_messages(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let unhandled = self.cluster_state.unhandled_custom_messages();
        if unhandled.is_empty() {
            return;
        }
        // BYSTANDER pre-drain: dispatch (with normal per-command
        // broadcast semantics) anything already queued on the channel
        // BEFORE the first handler runs, so the capture/discard windows
        // below hold exactly the handler's own commands — a discard
        // must never eat a command some other site queued earlier.
        self.drain_callback_queued_commands(command_rx).await;
        for (origin, seq, topic, data) in unhandled {
            match self.invoke_custom_handler(&origin, &topic, &data, true) {
                Ok(()) => {
                    // ATOMIC effect+terminal batch: drain the handler's
                    // queued commands CAPTURING their cluster mutations,
                    // append `CustomMessageHandled` through the same
                    // chokepoint (terminal LAST by construction), and
                    // flush everything as ONE wire frame — every
                    // replica applies the effect and the terminal
                    // together or not at all.
                    let batch = self
                        .drain_callback_queued_commands_capturing(
                            command_rx,
                            ClusterMutation::CustomMessageHandled {
                                origin: origin.clone(),
                                seq,
                            },
                        )
                        .await;
                    self.broadcast_applied_mutations(batch).await;
                }
                Err(reason) => {
                    // RAISE → terminal `Failed` (a USER ERROR — no
                    // retry, ever). All-or-nothing: the handler's
                    // queued commands are its partial effect and are
                    // discarded UNEXECUTED (nothing lands in the pool,
                    // the local CRDT, or any snapshot; blocked
                    // repliers get an explicit rejection).
                    let discarded = self.discard_callback_queued_commands(
                        command_rx,
                        "custom_message_handler raised; its queued commands are \
                         discarded (all-or-nothing handler semantics)",
                    );
                    tracing::error!(
                        origin = %origin,
                        msg_seq = seq,
                        topic = %topic,
                        discarded_commands = discarded,
                        error = %reason,
                        "custom_message_handler raised; the message is \
                         terminally Failed (payload dropped, partial effect \
                         discarded, never retried — a handler raise is a \
                         user error)"
                    );
                    // `CustomMessageFailed` is originated ALONE — the
                    // terminal-without-effect direction of the
                    // atomicity contract is satisfied by construction
                    // (there IS no effect).
                    self.apply_and_broadcast_cluster_mutations(vec![
                        ClusterMutation::CustomMessageFailed {
                            origin: origin.clone(),
                            seq,
                        },
                    ])
                    .await;
                }
            }
        }
    }

    /// Heartbeat-tick observation point for the keep-up monitor: feed
    /// the current `Unhandled` key set to [`CustomBacklogMonitor`] and
    /// emit the rate-limited WARN when it trips. Pure observability —
    /// the backlog is never bounded or shed.
    pub(crate) fn observe_custom_backlog(&mut self) {
        let keys = self.cluster_state.unhandled_custom_message_keys();
        if let Some(report) = self.custom_backlog_monitor.observe(&keys, Instant::now()) {
            tracing::warn!(
                unhandled = report.count,
                oldest_secs = report.oldest_age.as_secs_f64(),
                "primary custom-message handler is not keeping up: {} \
                 unhandled (oldest {}s)",
                report.count,
                report.oldest_age.as_secs(),
            );
        }
    }

    /// Invoke the consumer hook for ONE message. `None` hook = the
    /// consumer exposes no `custom_message_handler`: the message is
    /// consumed unhandled with a WARN (returning `Ok` so an important
    /// message latches `Handled` — a hook-less consumer must not grow
    /// the replicated inbox unboundedly, and every peer runs the same
    /// `TaskDefinition`, so no failover asymmetry exists).
    fn invoke_custom_handler(
        &mut self,
        origin: &str,
        topic: &str,
        data: &[u8],
        important: bool,
    ) -> Result<(), String> {
        match self.on_custom_message.as_mut() {
            Some(cb) => cb(origin, topic, data, important),
            None => {
                tracing::warn!(
                    origin = %origin,
                    topic = %topic,
                    important,
                    "custom message arrived but the TaskDefinition exposes no \
                     custom_message_handler; consuming it unhandled"
                );
                Ok(())
            }
        }
    }
}
