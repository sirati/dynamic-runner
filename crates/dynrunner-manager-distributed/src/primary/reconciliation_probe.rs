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
//!
//! # One INFO line per sweep, not per task
//!
//! On a big cluster one sweep can launch hundreds of probes whose
//! verdicts land moments later — per-task INFO lines flooded the
//! operator log (~200 lines in one observed production second). The
//! prober therefore aggregates logging per SWEEP COHORT: the probes
//! launched by one poll tick form a cohort (per-task launch detail at
//! DEBUG), verdicts are tallied against it, and ONE INFO summary line
//! is emitted when the cohort fully resolves OR at the next due poll
//! tick, whichever comes first. Stragglers are counted as "no answer
//! (left to the silence machinery)" at emission; a verdict landing
//! after the flush is logged as a DEBUG late correction
//! (`late_after_sweep_summary`). The state-changing outcomes stay
//! attributable at INFO: the not-held (failed + requeued) and
//! no-answer task hashes ride inline in the aggregate line itself,
//! capped with a "+K more" tail; full per-task detail rides DEBUG.
//! Cohort accounting is LOG BOOKKEEPING ONLY — probe timing, verdict
//! adjudication, and the response-window semantics are untouched by
//! it.

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

/// One sweep cohort's resolved tallies — the payload of the single
/// per-sweep INFO summary line (see the module doc's "One INFO line
/// per sweep" section).
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SweepSummary {
    /// How many probes the sweep launched (tasks past the deadline).
    pub(crate) total: usize,
    /// How many holders confirmed (deadline re-armed).
    pub(crate) held: usize,
    /// Task hashes whose holder denied holding them (failed +
    /// requeued) — the state-changing outcome, attributable at INFO.
    pub(crate) lost: Vec<String>,
    /// Task hashes with no verdict by emission time — left to the
    /// silence machinery; a verdict that still arrives is reported as
    /// a DEBUG late correction.
    pub(crate) no_answer: Vec<String>,
}

/// What one due poll tick produced: the probes to send, plus the
/// PREVIOUS sweep's summary when this tick flushed it (the cohort had
/// not fully resolved by the time the next tick arrived).
#[derive(Debug)]
pub(crate) struct SweepTick {
    pub(crate) probes: Vec<ProbeRequest>,
    pub(crate) flushed: Option<SweepSummary>,
}

/// A task that has stayed CONTINUOUSLY in flight on ONE holder past the
/// stall-warn threshold — the holder still answers `held = true` (NOT
/// silent/dead), so the run will NOT auto-fail, but it is wedged on this
/// task with no other operator-visible signal. PURE OBSERVABILITY data
/// for the caller to WARN on; carrying it changes NO task fate (the task
/// stays tracked and surviving exactly as before).
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StallSignal {
    pub(crate) task_hash: String,
    pub(crate) holder: String,
    /// Total continuous in-flight time on the current holder at the
    /// crossing.
    pub(crate) in_flight: Duration,
}

/// One adjudicated `TaskHoldResponse`, plus its sweep-cohort log
/// bookkeeping (aggregation only — never probe semantics).
#[derive(Debug)]
pub(crate) struct VerdictOutcome {
    pub(crate) verdict: ProbeVerdict,
    /// `true` when the verdict's cohort summary was already emitted
    /// (the task was counted as no-answer there); the caller logs the
    /// verdict as a late DEBUG correction.
    pub(crate) late: bool,
    /// `Some` when this verdict resolved the LAST outstanding member
    /// of the live cohort — the caller emits the summary now instead
    /// of waiting for the next tick's flush.
    pub(crate) completed: Option<SweepSummary>,
    /// `Some` when this held-confirmation re-arm just crossed the
    /// stall-warn threshold on the current holder (first crossing of
    /// this continuous span) — the caller emits the operator WARN.
    /// PURE OBSERVABILITY: the task is NOT failed or requeued; it stays
    /// tracked and surviving. `None` on every other outcome (below
    /// threshold, already warned this span, not a held re-arm).
    pub(crate) stalled: Option<StallSignal>,
}

impl VerdictOutcome {
    fn ignored() -> Self {
        Self {
            verdict: ProbeVerdict::Ignored,
            late: false,
            completed: None,
            stalled: None,
        }
    }
}

/// The live (not yet emitted) sweep cohort: tallies accumulate as
/// verdicts land; `pending` empties toward completion.
struct SweepCohort {
    total: usize,
    held: usize,
    lost: Vec<String>,
    pending: Vec<String>,
}

impl SweepCohort {
    /// Close the cohort: anything still pending is "no answer" as of
    /// emission time.
    fn into_summary(self) -> SweepSummary {
        SweepSummary {
            total: self.total,
            held: self.held,
            lost: self.lost,
            no_answer: self.pending,
        }
    }
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
    /// When this task FIRST entered flight on its CURRENT holder. Stamped
    /// at first sight and re-stamped only on a holder CHANGE (a
    /// re-dispatch to a new holder is fresh progress, so the stall clock
    /// restarts). Same-holder re-arms — held-confirmations and
    /// no-response expiries — deliberately do NOT touch it: the whole
    /// point of the stall diagnostic is to accumulate total continuous
    /// in-flight time on ONE holder. PURELY informational — never
    /// consulted by probe timing or verdict adjudication.
    first_seen: Instant,
    /// `true` once the stall diagnostic has already been surfaced for the
    /// CURRENT holder's continuous in-flight span (rate-limit: one WARN
    /// per crossing). Cleared on a holder change (a fresh span may stall
    /// again). LOG BOOKKEEPING ONLY — never consulted by probe timing or
    /// verdict adjudication.
    stall_warned: bool,
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
    /// How long a task may stay CONTINUOUSLY in flight on ONE holder
    /// before a held-confirmation re-arm surfaces the operator stall
    /// WARN (`PrimaryConfig::task_inflight_stall_warn_after`). PURE
    /// OBSERVABILITY: crossing this NEVER changes a task's fate — it only
    /// flags a [`StallSignal`] in the verdict for the caller to log. A
    /// large multiple of `timeout` so it fires only well past any single
    /// re-probe window.
    stall_warn_after: Duration,
    /// Next instant a full poll is due ([`POLL_CADENCE`] throttle).
    /// `None` until the first poll (the first poll is always due).
    next_poll: Option<Instant>,
    tracked: HashMap<String, TrackedTask>,
    /// The most recent sweep's cohort, while it is still collecting
    /// verdicts toward its one summary line. LOG BOOKKEEPING ONLY —
    /// never consulted by the probe-timing or verdict logic.
    cohort: Option<SweepCohort>,
}

impl ReconciliationProber {
    pub(crate) fn new(
        timeout: Duration,
        response_window: Duration,
        stall_warn_after: Duration,
    ) -> Self {
        Self {
            timeout,
            response_window,
            stall_warn_after,
            next_poll: None,
            tracked: HashMap::new(),
            cohort: None,
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
    pub(crate) fn poll(&mut self, now: Instant, view: &[(&str, &str)]) -> SweepTick {
        if !self.poll_due(now) {
            return SweepTick {
                probes: Vec::new(),
                flushed: None,
            };
        }
        self.next_poll = Some(now + POLL_CADENCE);

        // Flush the previous sweep's cohort, if it did not fully
        // resolve before this tick: its summary is emitted now, with
        // the still-pending members counted as no-answer. A verdict
        // that lands after this flush is reported late at DEBUG by the
        // caller. (A task that left the view in between — terminal
        // raced in — also lands in no-answer: it never answered the
        // probe before emission; its real outcome is attributed by the
        // terminal path's own logs.)
        let flushed = self.cohort.take().map(SweepCohort::into_summary);

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
                            // Total-in-flight stall clock starts now and
                            // accumulates across same-holder re-arms.
                            first_seen: now,
                            stall_warned: false,
                        },
                    );
                }
                Some(entry) => {
                    if entry.holder != holder {
                        // Re-dispatched to a different holder since the
                        // last poll: fresh deadline, and any probe
                        // outstanding against the OLD holder is void. A
                        // new holder is fresh progress, so the stall
                        // clock restarts and a prior stall WARN is cleared
                        // (the fresh span may stall again on its own).
                        entry.holder = holder.to_string();
                        entry.deadline = now + self.timeout;
                        entry.outstanding = None;
                        entry.first_seen = now;
                        entry.stall_warned = false;
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
        // The probes launched by THIS tick form the new sweep cohort
        // (none exists here: completion emits eagerly via
        // `on_response`, and an unresolved one was flushed above).
        if !fired.is_empty() {
            self.cohort = Some(SweepCohort {
                total: fired.len(),
                held: 0,
                lost: Vec::new(),
                pending: fired.iter().map(|p| p.task_hash.clone()).collect(),
            });
        }
        SweepTick {
            probes: fired,
            flushed,
        }
    }

    /// Adjudicate one `TaskHoldResponse`. Only a response that matches
    /// an OUTSTANDING probe from the SAME holder counts; everything
    /// else is [`ProbeVerdict::Ignored`] (stale answers from a previous
    /// holder, duplicates after the verdict already landed, responses
    /// to a prior primary's probes after failover). A counted verdict
    /// is additionally tallied against the live sweep cohort (log
    /// aggregation only): resolving its last pending member returns
    /// the completed [`SweepSummary`] for immediate emission, and a
    /// verdict for an already-flushed cohort is flagged `late`.
    pub(crate) fn on_response(
        &mut self,
        task_hash: &str,
        responder: &str,
        held: bool,
        now: Instant,
    ) -> VerdictOutcome {
        let Some(entry) = self.tracked.get_mut(task_hash) else {
            return VerdictOutcome::ignored();
        };
        if entry.outstanding.is_none() || entry.holder != responder {
            return VerdictOutcome::ignored();
        }
        let mut stalled = None;
        let verdict = if held {
            // Long build: confirmed alive-and-held. Re-arm a full
            // window; the task survives any number of these.
            entry.outstanding = None;
            entry.deadline = now + self.timeout;
            // Stall diagnostic (PURE OBSERVABILITY — never a verdict).
            // The holder is alive and STILL HOLDS the task, yet it has
            // stayed continuously in flight on this one holder past the
            // threshold. The probe re-arms exactly as before — the task
            // is NOT failed or requeued — but on the FIRST crossing of
            // this continuous span we hand the caller a one-shot signal
            // to WARN the operator (a wedged holder answers `held = true`
            // forever, so this is the only signal). `stall_warned`
            // rate-limits to one per span; a holder change clears it.
            let in_flight = now.saturating_duration_since(entry.first_seen);
            if !entry.stall_warned && in_flight >= self.stall_warn_after {
                entry.stall_warned = true;
                stalled = Some(StallSignal {
                    task_hash: task_hash.to_string(),
                    holder: entry.holder.clone(),
                    in_flight,
                });
            }
            ProbeVerdict::Rearmed
        } else {
            // The holder denies holding it: LOST. Drop tracking — the
            // caller's fail+requeue removes it from the ledger, and a
            // re-dispatch re-enters tracking fresh via view sync.
            self.tracked.remove(task_hash);
            ProbeVerdict::Lost
        };

        // Sweep-cohort tally (log bookkeeping only). A verdict whose
        // cohort was already flushed (counted there as no-answer) is
        // `late`; the caller logs the correction at DEBUG.
        let mut late = true;
        let mut completed = None;
        if let Some(cohort) = self.cohort.as_mut()
            && let Some(idx) = cohort.pending.iter().position(|h| h == task_hash)
        {
            cohort.pending.swap_remove(idx);
            late = false;
            match verdict {
                ProbeVerdict::Rearmed => cohort.held += 1,
                ProbeVerdict::Lost => cohort.lost.push(task_hash.to_string()),
                ProbeVerdict::Ignored => unreachable!("ignored returns early above"),
            }
            if cohort.pending.is_empty() {
                completed = self.cohort.take().map(SweepCohort::into_summary);
            }
        }
        VerdictOutcome {
            verdict,
            late,
            completed,
            stalled,
        }
    }
}

/// This module's tracing target — the log-shape tests scope their
/// capture to it.
#[cfg(test)]
pub(crate) const LOG_TARGET: &str = module_path!();

/// How many task hashes the per-sweep summary line carries inline per
/// category before collapsing into a "+K more" tail — keeps the line
/// bounded on big clusters while a state-changing verdict stays
/// attributable at INFO (full detail is at DEBUG).
const SUMMARY_HASH_CAP: usize = 8;

/// Render up to [`SUMMARY_HASH_CAP`] hashes (`"-"` when empty,
/// `"+K more"` past the cap) for the per-sweep summary line.
fn capped_hashes(hashes: &[String]) -> String {
    if hashes.is_empty() {
        return "-".into();
    }
    let mut rendered = hashes
        .iter()
        .take(SUMMARY_HASH_CAP)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    if hashes.len() > SUMMARY_HASH_CAP {
        rendered.push_str(&format!(" (+{} more)", hashes.len() - SUMMARY_HASH_CAP));
    }
    rendered
}

/// Emit ONE sweep's aggregate — the only INFO line this module ever
/// logs (per-task launch + verdict detail is DEBUG). Called from both
/// emission sites: the tick that flushed an unresolved cohort, and the
/// verdict that resolved a cohort's last pending member.
fn log_sweep_summary(summary: &SweepSummary) {
    tracing::info!(
        not_held_tasks = %capped_hashes(&summary.lost),
        no_answer_tasks = %capped_hashes(&summary.no_answer),
        "{} tasks past the reconciliation deadline; {} holders confirmed \
         (re-armed), {} not held (failed + requeued), {} no answer (left \
         to the silence machinery)",
        summary.total,
        summary.held,
        summary.lost.len(),
        summary.no_answer.len(),
    );
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Top-of-loop reconciliation-probe tick. Polls the prober against
    /// the live in-flight ledger and sends one `TaskHoldQuery` per
    /// fired deadline to the task's holder secondary. Send failures are
    /// best-effort: the response window expires with silence and the
    /// task's deadline re-arms, so an undeliverable probe is naturally
    /// retried a full window later (an UNREACHABLE holder is the
    /// silence machinery's concern, not this one's). The tick is also
    /// one of the two per-sweep summary emission points: a previous
    /// sweep that did not fully resolve is flushed here as the one
    /// INFO aggregate line (see the module doc).
    pub(crate) async fn reconciliation_probe_tick(&mut self) {
        let now = Instant::now();
        // Cheap early-out BEFORE building the view — the operational
        // loop calls this every iteration.
        if !self.recon_prober.poll_due(now) {
            return;
        }
        let tick = {
            let view: Vec<(&str, &str)> = self
                .in_flight
                .iter()
                .map(|(hash, entry)| (hash.as_str(), entry.secondary_id.as_str()))
                .collect();
            self.recon_prober.poll(now, &view)
        };
        // The previous sweep did not fully resolve before this tick:
        // emit its one INFO summary now (stragglers as no-answer).
        if let Some(summary) = tick.flushed {
            log_sweep_summary(&summary);
        }
        for probe in tick.probes {
            tracing::debug!(
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
    ///
    /// Per-task verdict detail logs at DEBUG; the verdict that resolves
    /// the sweep cohort's last pending member emits the one INFO
    /// aggregate line here (the other emission point is the next
    /// tick's flush — see the module doc).
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
        let outcome = self
            .recon_prober
            .on_response(&task_hash, &sender_id, held, Instant::now());
        // This verdict resolved the sweep cohort's last pending member:
        // emit the one INFO summary now (regardless of how the verdict
        // body below plays out — the tally already counted it).
        if let Some(summary) = outcome.completed {
            log_sweep_summary(&summary);
        }
        // Stall diagnostic (#308 follow-up) — PURE OBSERVABILITY. The
        // holder just confirmed `held = true`, so the verdict below is a
        // plain re-arm and the task survives indefinitely exactly as
        // before; this WARN changes NOTHING about its fate. It only tells
        // the operator the run has been wedged on one task on one live
        // holder for an unusually long total time (a holder stuck in
        // uninterruptible I/O keeps answering `held = true`, so the probe
        // re-arms forever with no other visible signal).
        if let Some(stall) = outcome.stalled {
            let in_flight_secs = stall.in_flight.as_secs_f64();
            tracing::warn!(
                task_hash = %stall.task_hash,
                holder = %stall.holder,
                in_flight_secs,
                "in-flight task has been held without a terminal for \
                 {in_flight_secs}s — the holder still answers held=true \
                 (NOT silent/dead), so the run will NOT auto-fail; if no \
                 task legitimately runs this long the worker body may be \
                 wedged (stuck I/O / dead external dependency / unreachable \
                 substituter). Diagnostic only."
            );
        }
        match outcome.verdict {
            ProbeVerdict::Rearmed => {
                tracing::debug!(
                    task_hash = %task_hash,
                    holder = %sender_id,
                    late_after_sweep_summary = outcome.late,
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
                tracing::debug!(
                    task_hash = %task_hash,
                    holder = %secondary_id,
                    worker_id,
                    late_after_sweep_summary = outcome.late,
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
                    // Stamped at the send_to_primary chokepoint (ordering gate).
                    msgs_posted_through: None,
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
    /// 6× TIMEOUT — mirrors the production default's "large multiple of
    /// the re-probe window so it only fires well past any single window"
    /// shape (see `DEFAULT_TASK_INFLIGHT_STALL_WARN_AFTER`).
    const STALL_WARN_AFTER: Duration = Duration::from_secs(3600);

    fn prober() -> ReconciliationProber {
        ReconciliationProber::new(TIMEOUT, RESPONSE_WINDOW, STALL_WARN_AFTER)
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
            fired.extend(p.poll(t, &view).probes);
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

        assert!(p.poll(start, &[("hash-a", "sec-0")]).probes.is_empty());
        // Terminal lands well before the deadline: the task leaves the
        // view.
        let t1 = start + Duration::from_secs(60);
        assert!(p.poll(t1, &[]).probes.is_empty());
        // Far beyond the original deadline: still nothing (the entry
        // was dropped at sync, not left to rot).
        let t2 = start + 3 * TIMEOUT;
        assert!(
            p.poll(t2, &[]).probes.is_empty(),
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
        assert!(p.poll(start, &view).probes.is_empty());

        let mut probe_at = start + TIMEOUT;
        for round in 0..3 {
            // Just before this round's deadline: no fire.
            let near = probe_at - Duration::from_secs(1);
            assert!(
                p.poll(near, &view).probes.is_empty(),
                "round {round}: must not fire before the re-armed deadline"
            );
            // At the deadline: exactly one probe.
            let fired = p.poll(probe_at, &view).probes;
            assert_eq!(fired.len(), 1, "round {round}: one probe at the deadline");
            // While outstanding, no duplicate probe.
            assert!(
                p.poll(probe_at + Duration::from_secs(2), &view)
                    .probes
                    .is_empty(),
                "round {round}: no duplicate probe while one is outstanding"
            );
            // The holder confirms: re-arm.
            let answered = probe_at + Duration::from_secs(5);
            assert_eq!(
                p.on_response("hash-a", "sec-0", true, answered).verdict,
                ProbeVerdict::Rearmed,
                "round {round}: held => re-arm, never a verdict"
            );
            probe_at = answered + TIMEOUT;
        }
    }

    /// Drive one probe→held=true round on `view` and return the
    /// re-arm verdict's outcome. `probe_at` is the (already-elapsed)
    /// deadline; the held confirmation lands 5s later. Returns the
    /// `VerdictOutcome` (carrying any `stalled` signal) and the instant
    /// of the next deadline.
    fn held_round(
        p: &mut ReconciliationProber,
        view: &[(&str, &str)],
        holder: &str,
        probe_at: Instant,
    ) -> (VerdictOutcome, Instant) {
        let fired = p.poll(probe_at, view).probes;
        assert_eq!(fired.len(), 1, "one probe at the deadline");
        let answered = probe_at + Duration::from_secs(5);
        let outcome = p.on_response("hash-a", holder, true, answered);
        assert_eq!(
            outcome.verdict,
            ProbeVerdict::Rearmed,
            "held => re-arm, never a verdict"
        );
        (outcome, answered + TIMEOUT)
    }

    /// THE stall diagnostic: a holder that stays continuously in flight
    /// on ONE assignment, answering `held = true` across MANY re-probe
    /// windows, must (a) NOT report a stall before `stall_warn_after`,
    /// (b) report it EXACTLY ONCE at the first crossing, (c) NOT report
    /// it again on the next held re-arm (rate-limited per span), and
    /// (d) keep the task TRACKED + SURVIVING throughout — the diagnostic
    /// never fails or removes the task. A holder CHANGE restarts the
    /// stall clock so a fresh wedged span can warn again.
    #[test]
    fn long_held_task_warns_once_then_survives() {
        let start = Instant::now();
        let mut p = prober();
        let view = [("hash-a", "sec-0")];
        assert!(p.poll(start, &view).probes.is_empty());

        // Drive held re-arms while the total in-flight span is still
        // BELOW the threshold: no stall signal yet.
        let mut probe_at = start + TIMEOUT;
        let crossed_at;
        loop {
            let in_flight_at_answer =
                (probe_at + Duration::from_secs(5)).duration_since(start);
            let (outcome, next) = held_round(&mut p, &view, "sec-0", probe_at);
            if in_flight_at_answer < STALL_WARN_AFTER {
                assert!(
                    outcome.stalled.is_none(),
                    "must NOT report a stall before stall_warn_after \
                     (in_flight {in_flight_at_answer:?})"
                );
                probe_at = next;
            } else {
                // First held re-arm at/after the threshold: stall fires.
                let stall = outcome
                    .stalled
                    .expect("first crossing of stall_warn_after must report a stall");
                assert_eq!(stall.task_hash, "hash-a");
                assert_eq!(stall.holder, "sec-0");
                assert!(
                    stall.in_flight >= STALL_WARN_AFTER,
                    "reported in_flight must be past the threshold"
                );
                crossed_at = next;
                break;
            }
        }
        let probe_at = crossed_at;

        // The NEXT held re-arm (still the same holder, span only grown)
        // must NOT fire a second WARN — rate-limited to one per span.
        let (outcome, probe_at) = held_round(&mut p, &view, "sec-0", probe_at);
        assert!(
            outcome.stalled.is_none(),
            "a second held re-arm on the same span must not warn again"
        );

        // The task is STILL tracked and surviving — the diagnostic
        // removed/failed nothing. It is still probed on schedule.
        assert_eq!(
            p.poll(probe_at, &view).probes.len(),
            1,
            "the stalled task is still tracked + probed (not removed/failed)"
        );
        // And it still answers held => still re-arms (survives).
        assert_eq!(
            p.on_response("hash-a", "sec-0", true, probe_at + Duration::from_secs(5))
                .verdict,
            ProbeVerdict::Rearmed,
            "the stalled task survives indefinitely — no fate change"
        );

        // A holder CHANGE restarts the stall clock: a fresh span on the
        // new holder warns again only after a fresh stall_warn_after.
        let moved_at = probe_at + Duration::from_secs(10);
        assert!(p.poll(moved_at, &[("hash-a", "sec-1")]).probes.is_empty());
        // One held re-arm just past the new holder's first deadline is
        // far below stall_warn_after measured from the move: no stall.
        let (outcome, _) = held_round(&mut p, &[("hash-a", "sec-1")], "sec-1", moved_at + TIMEOUT);
        assert!(
            outcome.stalled.is_none(),
            "holder change reset the stall clock — fresh span must not \
             instantly re-warn"
        );
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
        let fired = p.poll(start + TIMEOUT, &view).probes;
        assert_eq!(fired.len(), 1);
        assert_eq!(
            p.on_response(
                "hash-a",
                "sec-0",
                false,
                start + TIMEOUT + Duration::from_secs(1)
            )
            .verdict,
            ProbeVerdict::Lost
        );
        // The verdict already landed; a duplicate response is ignored.
        assert_eq!(
            p.on_response(
                "hash-a",
                "sec-0",
                false,
                start + TIMEOUT + Duration::from_secs(2)
            )
            .verdict,
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
        assert_eq!(p.poll(t_fire, &view).probes.len(), 1);

        // The response window expires with silence: no probe, no
        // verdict — and the deadline re-arms from the expiry.
        let t_expire = t_fire + RESPONSE_WINDOW;
        assert!(p.poll(t_expire, &view).probes.is_empty());
        // A response arriving AFTER the window closed is ignored (no
        // outstanding probe any more).
        assert_eq!(
            p.on_response("hash-a", "sec-0", false, t_expire + Duration::from_secs(1))
                .verdict,
            ProbeVerdict::Ignored,
            "late response after the window must not produce a verdict"
        );
        // Just before the re-armed deadline: nothing.
        assert!(
            p.poll(t_expire + TIMEOUT - Duration::from_secs(1), &view)
                .probes
                .is_empty()
        );
        // A full window after the expiry: probed again.
        assert_eq!(p.poll(t_expire + TIMEOUT, &view).probes.len(), 1);
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
        assert_eq!(p.poll(t_fire, &[("hash-a", "sec-0")]).probes.len(), 1);

        // Re-dispatched to sec-1 before sec-0 answered.
        let t_moved = t_fire + Duration::from_secs(2);
        assert!(p.poll(t_moved, &[("hash-a", "sec-1")]).probes.is_empty());
        // sec-0's late answer must not adjudicate sec-1's assignment.
        assert_eq!(
            p.on_response("hash-a", "sec-0", false, t_moved + Duration::from_secs(1))
                .verdict,
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
            .probes
            .is_empty()
        );
        let fired = p.poll(t_moved + TIMEOUT, &[("hash-a", "sec-1")]).probes;
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
            p.on_response("hash-x", "sec-0", false, start).verdict,
            ProbeVerdict::Ignored
        );
        // Tracked but no probe outstanding: also ignored.
        p.poll(start, &[("hash-a", "sec-0")]);
        assert_eq!(
            p.on_response("hash-a", "sec-0", false, start + Duration::from_secs(5))
                .verdict,
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
        let fired = p
            .poll(start + TIMEOUT + POLL_CADENCE, &[("hash-a", "sec-0")])
            .probes;
        assert_eq!(fired.len(), 1);
    }

    /// Sweep-cohort aggregation, all-held: the LAST verdict of the
    /// cohort returns the completed summary (emitted immediately, not
    /// at the next tick); earlier verdicts return nothing.
    #[test]
    fn all_held_cohort_completes_on_last_verdict() {
        let start = Instant::now();
        let mut p = prober();
        let view = [("hash-a", "sec-0"), ("hash-b", "sec-1")];
        let armed = p.poll(start, &view);
        assert!(armed.probes.is_empty() && armed.flushed.is_none());

        let tick = p.poll(start + TIMEOUT, &view);
        assert_eq!(tick.probes.len(), 2, "both tasks past the deadline");
        assert!(tick.flushed.is_none(), "no previous cohort to flush");

        let t = start + TIMEOUT + Duration::from_secs(1);
        let first = p.on_response("hash-a", "sec-0", true, t);
        assert_eq!(first.verdict, ProbeVerdict::Rearmed);
        assert!(!first.late, "counted into the live cohort");
        assert!(
            first.completed.is_none(),
            "cohort still has a pending member"
        );

        let second = p.on_response("hash-b", "sec-1", true, t);
        assert_eq!(second.verdict, ProbeVerdict::Rearmed);
        assert!(!second.late);
        assert_eq!(
            second.completed.expect("last verdict completes the cohort"),
            SweepSummary {
                total: 2,
                held: 2,
                lost: vec![],
                no_answer: vec![],
            }
        );
    }

    /// Mixed sweep with a straggler: the cohort does not complete, so
    /// the NEXT due tick flushes it — held and lost tallied from their
    /// verdicts, the straggler counted as no-answer at emission.
    #[test]
    fn unresolved_cohort_flushes_at_next_tick_with_straggler_as_no_answer() {
        let start = Instant::now();
        let mut p = prober();
        let view = [
            ("hash-a", "sec-0"),
            ("hash-b", "sec-0"),
            ("hash-c", "sec-0"),
        ];
        p.poll(start, &view);
        let tick = p.poll(start + TIMEOUT, &view);
        assert_eq!(tick.probes.len(), 3);

        let t = start + TIMEOUT + Duration::from_millis(100);
        assert!(p.on_response("hash-a", "sec-0", true, t).completed.is_none());
        let lost = p.on_response("hash-b", "sec-0", false, t);
        assert_eq!(lost.verdict, ProbeVerdict::Lost);
        assert!(lost.completed.is_none(), "hash-c is still pending");

        // Next due tick (hash-b left the view via the fail+requeue):
        // the unresolved cohort is flushed.
        let t_next = start + TIMEOUT + POLL_CADENCE;
        let next = p.poll(t_next, &[("hash-a", "sec-0"), ("hash-c", "sec-0")]);
        assert!(next.probes.is_empty(), "nothing re-fires inside the window");
        assert_eq!(
            next.flushed.expect("the unresolved cohort flushes here"),
            SweepSummary {
                total: 3,
                held: 1,
                lost: vec!["hash-b".into()],
                no_answer: vec!["hash-c".into()],
            }
        );
    }

    /// A verdict arriving AFTER its cohort was flushed (it was counted
    /// there as no-answer) is flagged `late` and completes nothing —
    /// the caller logs a DEBUG correction, never a second summary.
    #[test]
    fn verdict_after_flush_is_flagged_late() {
        let start = Instant::now();
        let mut p = prober();
        let view = [("hash-a", "sec-0")];
        p.poll(start, &view);
        assert_eq!(p.poll(start + TIMEOUT, &view).probes.len(), 1);

        // Flushed unresolved at the next tick.
        let t_next = start + TIMEOUT + POLL_CADENCE;
        let flushed = p.poll(t_next, &view).flushed.expect("flush");
        assert_eq!(flushed.no_answer, vec!["hash-a".to_string()]);

        // The probe is still outstanding (window > cadence): the
        // verdict still re-arms — semantics untouched — but the log
        // bookkeeping marks it late.
        let outcome = p.on_response("hash-a", "sec-0", true, t_next + Duration::from_secs(1));
        assert_eq!(outcome.verdict, ProbeVerdict::Rearmed);
        assert!(outcome.late, "cohort already emitted: late correction");
        assert!(outcome.completed.is_none());
    }

    /// Ticks that launch nothing flush nothing (no empty summaries on
    /// quiet clusters), and a tick that launches a NEW sweep while the
    /// previous one is unresolved flushes the old cohort in the same
    /// tick that opens the new one.
    #[test]
    fn quiet_ticks_flush_nothing_and_new_sweep_flushes_previous() {
        let start = Instant::now();
        let mut p = prober();
        // hash-a first seen now; hash-b one cadence later — their
        // deadlines (and thus their sweeps) are offset by one tick.
        p.poll(start, &[("hash-a", "sec-0")]);
        let quiet = p.poll(start + POLL_CADENCE, &[("hash-a", "sec-0"), ("hash-b", "sec-0")]);
        assert!(quiet.probes.is_empty() && quiet.flushed.is_none());

        let first = p.poll(
            start + TIMEOUT,
            &[("hash-a", "sec-0"), ("hash-b", "sec-0")],
        );
        assert_eq!(first.probes.len(), 1, "only hash-a is past its deadline");
        assert!(first.flushed.is_none());

        // hash-b's deadline elapses one cadence later: the new sweep's
        // tick flushes hash-a's unresolved cohort.
        let second = p.poll(
            start + TIMEOUT + POLL_CADENCE,
            &[("hash-a", "sec-0"), ("hash-b", "sec-0")],
        );
        assert_eq!(
            second.probes,
            vec![ProbeRequest {
                task_hash: "hash-b".into(),
                holder: "sec-0".into(),
            }]
        );
        assert_eq!(
            second.flushed.expect("previous sweep flushed"),
            SweepSummary {
                total: 1,
                held: 0,
                lost: vec![],
                no_answer: vec!["hash-a".into()],
            }
        );
    }

    /// The summary line's hash list is bounded: up to the cap inline,
    /// then a "+K more" tail; empty renders as "-".
    #[test]
    fn capped_hashes_renders_bounded_lists() {
        assert_eq!(capped_hashes(&[]), "-");
        let two: Vec<String> = vec!["a".into(), "b".into()];
        assert_eq!(capped_hashes(&two), "a, b");
        let many: Vec<String> = (0..11).map(|i| format!("h{i}")).collect();
        let rendered = capped_hashes(&many);
        assert!(rendered.starts_with("h0, h1"));
        assert!(rendered.contains("h7"));
        assert!(!rendered.contains("h8,"), "past the cap is collapsed");
        assert!(rendered.ends_with("(+3 more)"), "{rendered}");
    }
}
