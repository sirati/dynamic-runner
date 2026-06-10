//! Per-task RECONCILIATION PROBE (#308) — the primary-side accounting
//! backstop that asks an in-flight task's HOLDER secondary "do you still
//! hold task X?" once the task has been in flight too long without a
//! terminal, and fails + requeues the task only when the holder itself
//! answers "not held".
//!
//! # What this covers (and what it deliberately does not)
//!
//! Terminal-loss has three distinct failure classes, each with its own
//! owner:
//!
//!   1. The terminal was SENT but its DELIVERY was lost (blackholed leg)
//!      — owned by the #352 `delivery_seq`/`TerminalAck` replay buffer.
//!   2. The holder is SILENT/DEAD — owned by the keepalive silence
//!      machinery (`silence_hard_multiple` backstop +
//!      `only_silent_held_work_remains` starvation oracle), which
//!      declares the holder dead and requeues everything it held.
//!   3. The holder is LIVE and answering, but its bookkeeping no longer
//!      knows the task (a sweep bug, stranded first-bind bookkeeping, a
//!      restarted process that re-joined empty) — so it will NEVER send a
//!      terminal and is not silent either. **This module owns exactly
//!      this complement**: the probe adjudicates live-but-incoherent
//!      holders. A probe that gets NO response inside its bounded window
//!      takes NO action — the silent-holder concern belongs to class 2
//!      and is not duplicated here; the probe simply re-arms and the
//!      task's fate rides the existing silence machinery.
//!
//! # Why a per-task PROBE and not a global progress watchdog
//!
//! The naive resurrection of the #324 stuck-worker watchdog (300s of no
//! global terminal progress → mass-fail EVERY held task) was VETOED:
//! beyond-300s spans of genuinely-quiet work (nix builds) are routine, so a global
//! no-progress deadline false-fires on healthy long tasks. The probe
//! never false-fires by construction: expiry emits a QUESTION, not a
//! verdict — a holder that still holds the task answers `held = true`
//! and the deadline simply re-arms (a 30-minute build is re-probed once
//! per timeout window and survives every time). Only the holder's own
//! `held = false` — ground truth from the one node that would have to
//! produce the terminal — fails the task, and even then through the
//! BACKPRESSURE-shaped requeue path (the task never ran to completion
//! anywhere; its retry budget is not consumed).
//!
//! # Why the deadlines are stored `Instant`s polled on the loop's cadence
//!
//! Ported from the parked `StuckWorkerWatchdog` timing unit
//! (`watchdog-persistent-deadline` @ 1e914505): the original #324 arm
//! was a bare `tokio::time::sleep(300s)` INSIDE the operational
//! `select!`, and `select!` REBUILDS every branch future on each
//! iteration — every heartbeat tick / mesh frame restarted the sleep
//! from zero, so the deadline could never elapse on a live cluster and
//! the watchdog was structurally dead. This type owns every deadline on
//! ITS OWN clock (stored [`Instant`]s), re-armed only by events that
//! legitimately reset the condition — never by select-loop quiescence.
//! The operational loop polls it once per iteration at the top of the
//! loop (the same ≤keepalive-interval cadence the sibling fleet-dead
//! check rides), so deadlines elapse against wall time regardless of how
//! busy the other arms are. The poll arithmetic is pure (`now` is
//! injected), so the regression the old code lacked — *fires under
//! constant poll activity* — is unit-tested below.
//!
//! # No `single_worker_mode` gate
//!
//! The vetoed mass-fail body had to be gated off during the OOM retry
//! bucket (single-worker passes legitimately exceed the timeout and a
//! blanket fail would poison the bucket). The probe needs no such gate:
//! a long-running OOM-bucket task is probed, its holder answers
//! `held = true`, and the deadline re-arms — zero side effects on the
//! bucket's dispatch shape. Gating it off would only blind the backstop
//! during the bucket for no benefit.
//!
//! # No failover coupling
//!
//! The probe is accounting reconciliation ONLY. Neither the probe send,
//! nor a response (in either polarity), nor a response-window expiry
//! touches any liveness input (`secondary_keepalives`, the silence
//! schedule, the election machinery). The one liveness side effect a
//! `TaskHoldResponse` has is the same one EVERY inbound frame has — the
//! `dispatch_message` preamble's `record_keepalive` — which is the
//! generic ingest concern, not this module's.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_core::{ErrorType, Identifier};
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, PeerId};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;

/// Wire `error_message` marker for a probe-verdict task loss. The
/// holder secondary positively denied holding the task, so the task is
/// LOST (its terminal will never come) but it did not FAIL anywhere —
/// `handle_task_failed` classifies this marker as backpressure-shaped
/// (requeue into the pool, no retry-budget consumption), exactly like
/// the "never actually ran" markers the secondaries emit.
pub(crate) const RECONCILIATION_LOST_WIRE_MESSAGE: &str =
    "reconciliation probe: holder denies holding task";

/// How often the prober does a full poll (view sync + deadline sweep),
/// regardless of how often the operational loop calls it. The loop
/// polls once per iteration — which on a busy mesh is far hotter than
/// the probe's 10-minute deadlines warrant — so
/// [`ReconciliationProber::poll_due`] early-outs (BEFORE the caller
/// builds the in-flight view) until this cadence elapses. 1s against a
/// 600s default timeout costs ≤1s of deadline precision, which is
/// noise.
const POLL_CADENCE: Duration = Duration::from_secs(1);

/// A probe the caller must send: ask `holder` whether it still holds
/// `task_hash`. Pure data — the prober decides WHEN, the coordinator
/// owns the wire send.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProbeRequest {
    pub(crate) task_hash: String,
    pub(crate) holder: String,
}

/// The prober's adjudication of one `TaskHoldResponse`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProbeVerdict {
    /// The holder confirmed it still holds the task (a long build) —
    /// the deadline re-armed; nothing to do.
    Rearmed,
    /// The holder positively denied holding the task: it is LOST and
    /// the caller must fail + requeue it through the existing terminal
    /// machinery.
    Lost,
    /// The response did not match an outstanding probe (no such tracked
    /// task, no probe outstanding, or the responder is not the holder
    /// that was probed — e.g. a stale answer from a previous holder
    /// after the task was requeued and re-dispatched). Dropped.
    Ignored,
}

/// Per-task tracking state. One entry per in-flight ledger entry,
/// synced from the ledger on every full poll.
struct TrackedTask {
    /// The holder secondary the deadline (and any outstanding probe) is
    /// measured against. A holder CHANGE observed at sync time (requeue
    /// → re-dispatch elsewhere between polls) resets the entry: fresh
    /// deadline, outstanding probe dropped — a stale response from the
    /// old holder must never adjudicate the new assignment (and
    /// `on_response` additionally keys on this field).
    holder: String,
    /// When the next probe fires for this task. Armed at first sight
    /// (`now + timeout`), re-armed on holder confirmation, response-
    /// window expiry, and holder change.
    deadline: Instant,
    /// `Some(response_deadline)` while a probe is outstanding. No
    /// second probe is emitted while outstanding; expiry clears it and
    /// re-arms `deadline` (the no-response case takes NO action — the
    /// silent-holder concern is owned by the keepalive machinery).
    outstanding: Option<Instant>,
}

/// Persistent per-task reconciliation deadlines. Owns ALL probe timing
/// (deadlines, response windows, the poll cadence) across operational-
/// loop iterations so deadlines elapse on wall time, not on `select!`
/// quiescence. The coordinator supplies only live observations: `now`,
/// the in-flight view (hash → holder), and inbound responses.
pub(crate) struct ReconciliationProber {
    /// How long a task may be in flight with no terminal before its
    /// holder is probed. Fixed at construction
    /// (`PrimaryConfig::task_reconciliation_timeout`).
    timeout: Duration,
    /// How long an emitted probe waits for its response before the
    /// prober gives up on it (clearing `outstanding` and re-arming the
    /// task's deadline). Fixed at construction; derived from the
    /// keepalive config (`keepalive_interval × keepalive_miss_threshold`
    /// — the cluster's established "should have heard back by now"
    /// quantum).
    response_window: Duration,
    /// Next instant a full poll is due ([`POLL_CADENCE`] throttle).
    /// `None` until the first poll (the first poll is always due).
    next_poll: Option<Instant>,
    tracked: HashMap<String, TrackedTask>,
}

impl ReconciliationProber {
    pub(crate) fn new(timeout: Duration, response_window: Duration) -> Self {
        Self {
            timeout,
            response_window,
            next_poll: None,
            tracked: HashMap::new(),
        }
    }

    /// Cheap pre-check the caller runs BEFORE building the in-flight
    /// view: is a full poll due? Keeps the per-iteration cost of a busy
    /// operational loop at one `Instant` compare.
    pub(crate) fn poll_due(&self, now: Instant) -> bool {
        match self.next_poll {
            None => true,
            Some(at) => now >= at,
        }
    }

    /// One full poll: sync the tracked set against the live in-flight
    /// view, expire outstanding response windows, and return the probes
    /// that fire this tick. `now` is injected so the decision is pure
    /// and the tests can drive a virtual clock.
    ///
    /// View sync is what re-arms on progress without any terminal-path
    /// hook: a terminal for task X removes X from the in-flight ledger,
    /// so X simply leaves the view and its tracking entry (and any
    /// outstanding probe) is dropped here. A task whose HOLDER changed
    /// (requeued + re-dispatched between polls) is reset to a fresh
    /// deadline against the new holder.
    pub(crate) fn poll(&mut self, now: Instant, view: &[(&str, &str)]) -> Vec<ProbeRequest> {
        if !self.poll_due(now) {
            return Vec::new();
        }
        self.next_poll = Some(now + POLL_CADENCE);

        // Drop tracking for tasks no longer in flight (terminal landed,
        // dead-secondary requeue, etc. — every removal from the ledger
        // means "no longer awaiting a terminal from that holder").
        self.tracked
            .retain(|hash, _| view.iter().any(|(h, _)| h == hash));

        let mut fired = Vec::new();
        for &(hash, holder) in view {
            match self.tracked.get_mut(hash) {
                None => {
                    // First sight: arm a full window from now. A task is
                    // never probed before one whole `timeout` of
                    // continuous in-flight time against one holder.
                    self.tracked.insert(
                        hash.to_string(),
                        TrackedTask {
                            holder: holder.to_string(),
                            deadline: now + self.timeout,
                            outstanding: None,
                        },
                    );
                }
                Some(entry) => {
                    if entry.holder != holder {
                        // Re-dispatched to a different holder since the
                        // last poll: fresh deadline, and any probe
                        // outstanding against the OLD holder is void.
                        entry.holder = holder.to_string();
                        entry.deadline = now + self.timeout;
                        entry.outstanding = None;
                        continue;
                    }
                    match entry.outstanding {
                        Some(response_deadline) => {
                            if now >= response_deadline {
                                // No response inside the bounded window.
                                // NO ACTION by this mechanism — a holder
                                // that cannot answer is the silent-
                                // secondary machinery's concern. Re-arm
                                // so a holder that comes back incoherent
                                // is probed again a full window later.
                                entry.outstanding = None;
                                entry.deadline = now + self.timeout;
                            }
                        }
                        None => {
                            if now >= entry.deadline {
                                entry.outstanding = Some(now + self.response_window);
                                fired.push(ProbeRequest {
                                    task_hash: hash.to_string(),
                                    holder: holder.to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }
        fired
    }

    /// Adjudicate one `TaskHoldResponse`. Only a response that matches
    /// an OUTSTANDING probe from the SAME holder counts; everything
    /// else is [`ProbeVerdict::Ignored`] (stale answers from a previous
    /// holder, duplicates after the verdict already landed, responses
    /// to a prior primary's probes after failover).
    pub(crate) fn on_response(
        &mut self,
        task_hash: &str,
        responder: &str,
        held: bool,
        now: Instant,
    ) -> ProbeVerdict {
        let Some(entry) = self.tracked.get_mut(task_hash) else {
            return ProbeVerdict::Ignored;
        };
        if entry.outstanding.is_none() || entry.holder != responder {
            return ProbeVerdict::Ignored;
        }
        if held {
            // Long build: confirmed alive-and-held. Re-arm a full
            // window; the task survives any number of these.
            entry.outstanding = None;
            entry.deadline = now + self.timeout;
            ProbeVerdict::Rearmed
        } else {
            // The holder denies holding it: LOST. Drop tracking — the
            // caller's fail+requeue removes it from the ledger, and a
            // re-dispatch re-enters tracking fresh via view sync.
            self.tracked.remove(task_hash);
            ProbeVerdict::Lost
        }
    }
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Top-of-loop reconciliation-probe tick. Polls the prober against
    /// the live in-flight ledger and sends one `TaskHoldQuery` per
    /// fired deadline to the task's holder secondary. Send failures are
    /// best-effort: the response window expires with silence and the
    /// task's deadline re-arms, so an undeliverable probe is naturally
    /// retried a full window later (an UNREACHABLE holder is the
    /// silence machinery's concern, not this one's).
    pub(crate) async fn reconciliation_probe_tick(&mut self) {
        let now = Instant::now();
        // Cheap early-out BEFORE building the view — the operational
        // loop calls this every iteration.
        if !self.recon_prober.poll_due(now) {
            return;
        }
        let probes = {
            let view: Vec<(&str, &str)> = self
                .in_flight
                .iter()
                .map(|(hash, entry)| (hash.as_str(), entry.secondary_id.as_str()))
                .collect();
            self.recon_prober.poll(now, &view)
        };
        for probe in probes {
            tracing::info!(
                task_hash = %probe.task_hash,
                holder = %probe.holder,
                timeout_s = self.config.task_reconciliation_timeout.as_secs_f64(),
                "task in flight past the reconciliation deadline with no \
                 terminal; probing its holder (held => re-arm, not held => \
                 fail + requeue, no response => left to the silence machinery)"
            );
            let query = DistributedMessage::TaskHoldQuery {
                target: None,
                sender_id: self.config.node_id.clone(),
                timestamp: super::wire::timestamp_now(),
                task_hash: probe.task_hash.clone(),
            };
            if let Err(e) = self
                .send_to(
                    Destination::Secondary(PeerId::from(probe.holder.as_str())),
                    query,
                )
                .await
            {
                tracing::debug!(
                    task_hash = %probe.task_hash,
                    holder = %probe.holder,
                    error = %e,
                    "TaskHoldQuery send failed (best-effort; the response \
                     window expires and the probe re-arms)"
                );
            }
        }
    }

    /// Inbound `TaskHoldResponse` handler — the probe VERDICT site.
    ///
    /// `held = true` re-arms inside the prober (nothing further here).
    /// `held = false` means the holder positively denies holding the
    /// task: it is LOST (its terminal will never come). The loss is
    /// routed through `handle_task_failed` as a backpressure-shaped
    /// `TaskFailed` (the [`RECONCILIATION_LOST_WIRE_MESSAGE`] marker),
    /// so ONE existing path owns the slot-free + pool-requeue +
    /// `TasksAdded` re-dispatch wakeup and the task's retry budget is
    /// untouched (it never ran to completion anywhere). The ledger is
    /// re-consulted at verdict time so a terminal that raced the
    /// response in (or a re-dispatch to a different holder) makes the
    /// verdict a safe no-op.
    pub(super) async fn handle_task_hold_response(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let DistributedMessage::TaskHoldResponse {
            target: None,
            sender_id,
            task_hash,
            held,
            ..
        } = msg
        else {
            return;
        };
        let verdict = self
            .recon_prober
            .on_response(&task_hash, &sender_id, held, Instant::now());
        match verdict {
            ProbeVerdict::Rearmed => {
                tracing::info!(
                    task_hash = %task_hash,
                    holder = %sender_id,
                    "reconciliation probe verdict: HELD — the holder confirms \
                     the task is still in its bookkeeping (a long task); \
                     deadline re-armed"
                );
            }
            ProbeVerdict::Ignored => {
                tracing::debug!(
                    task_hash = %task_hash,
                    responder = %sender_id,
                    held,
                    "reconciliation probe response without a matching \
                     outstanding probe (stale/duplicate); ignored"
                );
            }
            ProbeVerdict::Lost => {
                // Re-consult the ledger at verdict time: only fail the
                // task if it is STILL attributed to the denying holder.
                // A terminal that raced in (entry gone) or a re-dispatch
                // (different holder) makes this a no-op.
                let Some(entry) = self.in_flight.get(&task_hash) else {
                    tracing::debug!(
                        task_hash = %task_hash,
                        responder = %sender_id,
                        "probe verdict LOST raced a terminal (task no longer \
                         in flight); no-op"
                    );
                    return;
                };
                if entry.secondary_id != sender_id {
                    tracing::debug!(
                        task_hash = %task_hash,
                        responder = %sender_id,
                        holder = %entry.secondary_id,
                        "probe verdict LOST from a non-holder (task was \
                         re-dispatched); no-op"
                    );
                    return;
                }
                let worker_id = entry.local_worker_id.unwrap_or(0);
                let secondary_id = entry.secondary_id.clone();
                tracing::error!(
                    task_hash = %task_hash,
                    holder = %secondary_id,
                    worker_id,
                    "reconciliation probe verdict: NOT HELD — the holder \
                     denies holding the task; its terminal will never come \
                     (lost terminal / sweep bug / stranded bookkeeping). \
                     Failing + requeueing via the backpressure-shaped path"
                );
                let synthetic = DistributedMessage::TaskFailed {
                    target: None,
                    // The verdict is the primary's own adjudication; the
                    // frame's `secondary_id` still names the holder so
                    // the ledger free + backoff hit the right secondary.
                    sender_id: self.config.node_id.clone(),
                    timestamp: super::wire::timestamp_now(),
                    secondary_id,
                    worker_id,
                    task_hash,
                    error_type: ErrorType::Recoverable,
                    error_message: RECONCILIATION_LOST_WIRE_MESSAGE.into(),
                    delivery_seq: None,
                };
                self.handle_task_failed(synthetic, command_rx).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(600);
    const RESPONSE_WINDOW: Duration = Duration::from_secs(15);

    fn prober() -> ReconciliationProber {
        ReconciliationProber::new(TIMEOUT, RESPONSE_WINDOW)
    }

    /// THE regression the old `tokio::time::sleep` arm lacked: constant
    /// poll activity (here, polls every 5s — the heartbeat cadence that
    /// kept rebuilding the dead sleep) must NOT prevent the deadline
    /// firing. With one task continuously in flight on one holder, the
    /// per-task deadline elapses on its own clock and exactly one probe
    /// fires once the window has passed — no matter how many times the
    /// prober was polled in between.
    #[test]
    fn fires_under_constant_poll_activity_with_no_terminal() {
        let start = Instant::now();
        let mut p = prober();
        let view = [("hash-a", "sec-0")];

        let mut t = start;
        let step = Duration::from_secs(5);
        let mut fired = Vec::new();
        while t.duration_since(start) <= TIMEOUT + step {
            fired.extend(p.poll(t, &view));
            if t.duration_since(start) < TIMEOUT {
                assert!(
                    fired.is_empty(),
                    "must not fire before the per-task deadline (elapsed {:?})",
                    t.duration_since(start)
                );
            }
            t += step;
        }
        assert_eq!(
            fired,
            vec![ProbeRequest {
                task_hash: "hash-a".into(),
                holder: "sec-0".into(),
            }],
            "exactly one probe fires once the deadline elapses, despite \
             constant 5s poll activity in between"
        );
    }

    /// A task that completes normally (leaves the in-flight view before
    /// its deadline) is NEVER probed — even when time then passes far
    /// beyond the original deadline.
    #[test]
    fn never_fires_for_tasks_that_complete_normally() {
        let start = Instant::now();
        let mut p = prober();

        assert!(p.poll(start, &[("hash-a", "sec-0")]).is_empty());
        // Terminal lands well before the deadline: the task leaves the
        // view.
        let t1 = start + Duration::from_secs(60);
        assert!(p.poll(t1, &[]).is_empty());
        // Far beyond the original deadline: still nothing (the entry
        // was dropped at sync, not left to rot).
        let t2 = start + 3 * TIMEOUT;
        assert!(
            p.poll(t2, &[]).is_empty(),
            "a completed task must never be probed"
        );
    }

    /// A held-confirmation re-arms a full window, repeatedly: a build
    /// running 3× the deadline survives with zero false fires — each
    /// probe is answered `held = true` and the next probe only fires a
    /// full window later.
    #[test]
    fn held_response_rearms_repeatedly_long_build_survives() {
        let start = Instant::now();
        let mut p = prober();
        let view = [("hash-a", "sec-0")];
        assert!(p.poll(start, &view).is_empty());

        let mut probe_at = start + TIMEOUT;
        for round in 0..3 {
            // Just before this round's deadline: no fire.
            let near = probe_at - Duration::from_secs(1);
            assert!(
                p.poll(near, &view).is_empty(),
                "round {round}: must not fire before the re-armed deadline"
            );
            // At the deadline: exactly one probe.
            let fired = p.poll(probe_at, &view);
            assert_eq!(fired.len(), 1, "round {round}: one probe at the deadline");
            // While outstanding, no duplicate probe.
            assert!(
                p.poll(probe_at + Duration::from_secs(2), &view).is_empty(),
                "round {round}: no duplicate probe while one is outstanding"
            );
            // The holder confirms: re-arm.
            let answered = probe_at + Duration::from_secs(5);
            assert_eq!(
                p.on_response("hash-a", "sec-0", true, answered),
                ProbeVerdict::Rearmed,
                "round {round}: held => re-arm, never a verdict"
            );
            probe_at = answered + TIMEOUT;
        }
    }

    /// `held = false` from the probed holder is the LOST verdict, and
    /// tracking is dropped (the caller's fail+requeue removes the
    /// ledger entry; a later re-dispatch re-enters fresh).
    #[test]
    fn not_held_response_is_lost_verdict() {
        let start = Instant::now();
        let mut p = prober();
        let view = [("hash-a", "sec-0")];
        p.poll(start, &view);
        let fired = p.poll(start + TIMEOUT, &view);
        assert_eq!(fired.len(), 1);
        assert_eq!(
            p.on_response(
                "hash-a",
                "sec-0",
                false,
                start + TIMEOUT + Duration::from_secs(1)
            ),
            ProbeVerdict::Lost
        );
        // The verdict already landed; a duplicate response is ignored.
        assert_eq!(
            p.on_response(
                "hash-a",
                "sec-0",
                false,
                start + TIMEOUT + Duration::from_secs(2)
            ),
            ProbeVerdict::Ignored
        );
    }

    /// NO response inside the bounded window → NO action by this
    /// mechanism: the outstanding probe expires, the deadline re-arms,
    /// and no verdict is ever produced (the silent holder is the
    /// keepalive machinery's concern). A full window later the holder
    /// is probed again.
    #[test]
    fn no_response_takes_no_action_and_rearms() {
        let start = Instant::now();
        let mut p = prober();
        let view = [("hash-a", "sec-0")];
        p.poll(start, &view);
        let t_fire = start + TIMEOUT;
        assert_eq!(p.poll(t_fire, &view).len(), 1);

        // The response window expires with silence: no probe, no
        // verdict — and the deadline re-arms from the expiry.
        let t_expire = t_fire + RESPONSE_WINDOW;
        assert!(p.poll(t_expire, &view).is_empty());
        // A response arriving AFTER the window closed is ignored (no
        // outstanding probe any more).
        assert_eq!(
            p.on_response("hash-a", "sec-0", false, t_expire + Duration::from_secs(1)),
            ProbeVerdict::Ignored,
            "late response after the window must not produce a verdict"
        );
        // Just before the re-armed deadline: nothing.
        assert!(
            p.poll(t_expire + TIMEOUT - Duration::from_secs(1), &view)
                .is_empty()
        );
        // A full window after the expiry: probed again.
        assert_eq!(p.poll(t_expire + TIMEOUT, &view).len(), 1);
    }

    /// A holder CHANGE between polls (requeue → re-dispatch elsewhere)
    /// resets the entry: fresh deadline against the new holder, and a
    /// stale response from the OLD holder is ignored.
    #[test]
    fn holder_change_resets_and_voids_stale_responses() {
        let start = Instant::now();
        let mut p = prober();
        p.poll(start, &[("hash-a", "sec-0")]);
        let t_fire = start + TIMEOUT;
        assert_eq!(p.poll(t_fire, &[("hash-a", "sec-0")]).len(), 1);

        // Re-dispatched to sec-1 before sec-0 answered.
        let t_moved = t_fire + Duration::from_secs(2);
        assert!(p.poll(t_moved, &[("hash-a", "sec-1")]).is_empty());
        // sec-0's late answer must not adjudicate sec-1's assignment.
        assert_eq!(
            p.on_response("hash-a", "sec-0", false, t_moved + Duration::from_secs(1)),
            ProbeVerdict::Ignored,
            "a previous holder's stale denial must never fail the \
             re-dispatched assignment"
        );
        // The new holder's window measures from the move.
        assert!(
            p.poll(
                t_moved + TIMEOUT - Duration::from_secs(1),
                &[("hash-a", "sec-1")]
            )
            .is_empty()
        );
        let fired = p.poll(t_moved + TIMEOUT, &[("hash-a", "sec-1")]);
        assert_eq!(
            fired,
            vec![ProbeRequest {
                task_hash: "hash-a".into(),
                holder: "sec-1".into(),
            }]
        );
    }

    /// A response for a task that was never tracked / probed is ignored
    /// (e.g. an answer to a PRIOR primary's probe arriving after
    /// failover, or a misdirected frame).
    #[test]
    fn unsolicited_response_is_ignored() {
        let start = Instant::now();
        let mut p = prober();
        assert_eq!(
            p.on_response("hash-x", "sec-0", false, start),
            ProbeVerdict::Ignored
        );
        // Tracked but no probe outstanding: also ignored.
        p.poll(start, &[("hash-a", "sec-0")]);
        assert_eq!(
            p.on_response("hash-a", "sec-0", false, start + Duration::from_secs(5)),
            ProbeVerdict::Ignored
        );
    }

    /// The poll-cadence throttle: calls inside [`POLL_CADENCE`] of the
    /// last full poll are cheap no-ops (`poll_due` false), and the
    /// throttle never delays a deadline by more than the cadence.
    #[test]
    fn poll_cadence_throttles_hot_loops() {
        let start = Instant::now();
        let mut p = prober();
        assert!(p.poll_due(start));
        p.poll(start, &[("hash-a", "sec-0")]);
        // Immediately after a full poll: not due (a hot loop iterating
        // thousands of times per second pays one Instant compare).
        assert!(!p.poll_due(start + Duration::from_millis(10)));
        assert!(p.poll_due(start + POLL_CADENCE));
        // Deadline at TIMEOUT, polled slightly late by the cadence: the
        // probe still fires on the first due poll past it.
        let fired = p.poll(start + TIMEOUT + POLL_CADENCE, &[("hash-a", "sec-0")]);
        assert_eq!(fired.len(), 1);
    }
}
