//! Observer lost-visibility reporter + the wake-stream loss policy.
//!
//! # Single concern
//!
//! ONE concern: track whether the zero-authority observer currently has
//! visibility into the run (any peer reachable, the named primary not
//! silent), emit FULL-LOG reports when that visibility is LOST, recurs
//! while still lost, or is REGAINED, AND tell the coordinator when a
//! reconnect ATTEMPT is due (the same ~60s cadence as the recurrence
//! report — one clock, one concern). It NEVER decides an exit — a
//! lost-visibility observer keeps observing, reporting + retrying, until
//! it OBSERVES the primary's run-terminal via the CRDT.
//!
//! # The wake-stream policy (`--important-stdio-only`)
//!
//! This module ALSO owns the operator wake-stream policy for connection
//! loss — the [`dynrunner_core::IMPORTANT_TARGET`] view is deliberately
//! quieter than the full log:
//!
//!   1. A loss reaches the wake stream ONLY once there has been no
//!      successful reconnect-and-sync for [`WAKE_LOSS_THRESHOLD`]
//!      (5 minutes) — the coordinator's `Visible` verdict already MEANS
//!      synced (it keys on inbound frames from the recognised primary, so
//!      a transport-level reconnect that never syncs stays `Lost` and
//!      resets nothing). A shorter blip produces NOTHING on the wake
//!      stream, ever — the full-log diagnostics above stay immediate and
//!      untouched. While the condition persists, the warning REPEATS at
//!      the [`WAKE_LOSS_RECURRENCE`] cadence (10 minutes): a 25-minute
//!      outage wakes the operator at ~+5, ~+15 and ~+25 — never the ~60s
//!      full-log recurrence.
//!   2. A reconnection is NEVER its own wake event. If (and only if) the
//!      loss WAS logged on the wake stream, regaining visibility parks a
//!      reconnection NOTE in the shared [`WakeNoteSlot`]; the note rides
//!      together with the NEXT wake-stream log that is emitted anyway
//!      (a phase start/completion, the periodic stats log, an error
//!      aggregation, …) — every wake emitter flushes the slot right
//!      after it emits.
//!   3. If ≥1 periodic-stats grid occurrence elapsed while the connection
//!      was down (a logged loss), the regain hands the coordinator an
//!      [`EndedOutage`] signal to forward to the periodic reporter, which
//!      runs ONE late stats log immediately (naturally carrying the note
//!      per rule 2). The grid itself never shifts — the late-run + the
//!      skip-one-occurrence bookkeeping live with the grid's owner,
//!      [`super::reporting::reporter`].
//!
//! The 5-minute threshold timer obeys the persistent-deadline law: the
//! deadline is derived from the STORED loss instant
//! ([`LostVisibilityReporter::wake_deadline`]) — the coordinator's select
//! arm rebuilds `sleep_until(stored)` each iteration, so constant sibling
//! activity can never push it back.
//!
//! This module owns the report/retry CADENCE (the lost-since clock + the
//! "is an attempt due" decision); it does NOT own the reconnect MECHANISM
//! — the coordinator acts on the returned [`RetryDirective`] by triggering
//! the [`super::reconnect::TunnelReconnector`] port (which the provider
//! layer rebuilds the `-R` tunnel behind). The split keeps the cadence
//! state in one place and the ssh-rebuild concern out of this state
//! machine entirely.
//!
//! # Why this exists (the BUG-B sever)
//!
//! Before this, the observer's run loop treated `peer_count() == 0` for
//! `fleet_dead_timeout` (§5) and a named-primary silence past
//! `peer_timeout` (§6) as a STRAND that returned `Err(ClusterCollapsed)`
//! — which the node-run outcome mapped to a RUN-FAILING terminal. But the
//! observer carries ZERO authority over the job: its OWN loss of transport
//! view says "I (observer) lost visibility", NOT "the cluster died". The
//! compute mesh (primary + secondaries, directly meshed) keeps running
//! autonomously. So visibility loss can never be the run's verdict. This
//! reporter REPLACES the §5/§6 strand-exit: it reports lost + retries, the
//! observer never self-strands.
//!
//! # Why the observer must DRIVE the reconnect (the `-R` rebuild)
//!
//! The relocated submitter→observer inherits the submitter's
//! [`dynrunner_transport_tunnel::TunneledPeerTransport`]: the compute peers
//! DIAL the submitter over per-secondary `ssh -R` reverse tunnels, the
//! submitter never dials out, and that transport has NO QUIC reconnect
//! ticker (the ticker lives only on the secondary/late-joiner
//! `PeerNetwork`). So when a `-R` tunnel drops (an external ssh blip /
//! `ServerAliveCountMax` exhaustion — there is no auto-reconnect on the ssh
//! side), NOTHING re-establishes the link on its own: the compute peer's
//! redial can't punch through a dead `-R`, and the observer's transport
//! cannot dial. The observer MUST actively trigger a `-R` rebuild. This
//! reporter therefore tells the coordinator when an attempt is due (the
//! returned [`RetryDirective`]); the coordinator fires the
//! [`super::reconnect::TunnelReconnector`] port.
//!
//! # Boundary
//!
//! The coordinator owns the liveness inputs (its `MembershipView`
//! peer-count + its `primary_last_seen` clock against the CRDT-named
//! primary) and the run-loop tick that drives the recurrence cadence. Each
//! top-of-loop it hands this reporter the CURRENT visibility verdict; the
//! reporter owns only the report-state machine (lost-since clock, last
//! report instant, the operator-facing emits) and returns whether a
//! reconnect attempt is due this iteration. It does NOT name a transport,
//! drive a dial, or know about ssh — the coordinator acts on the returned
//! directive by triggering the reconnect port.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dynrunner_core::IMPORTANT_TARGET;
use tokio::time::Instant;

/// How long to wait between repeated "still disconnected, still retrying"
/// reports while visibility stays lost. The owner directive: "if all
/// connection is lost they do not shut down, they report that connection
/// is lost and try reconnecting after a minute."
const REPORT_RECURRENCE: Duration = Duration::from_secs(60);

/// How long a connection must be CONTINUOUSLY down before the loss is
/// reported on the operator wake stream (`--important-stdio-only`). A blip
/// shorter than this produces nothing on the wake stream — neither a loss
/// nor a reconnection — ever. The full-log diagnostics are NOT gated on
/// this threshold (they stay immediate).
pub(crate) const WAKE_LOSS_THRESHOLD: Duration = Duration::from_secs(300);

/// How often the wake-stream loss warning REPEATS while the connection
/// stays down after the first ([`WAKE_LOSS_THRESHOLD`]-gated) warning.
/// One mechanism owns the whole cadence: [`LostVisibilityReporter::wake_deadline`]
/// derives the FIRST deadline from the loss instant and every subsequent
/// one from the last wake emit — there is no separate recurrence state.
pub(crate) const WAKE_LOSS_RECURRENCE: Duration = Duration::from_secs(600);

/// The shared "reconnection note" slot — the piggyback seam between the
/// wake-stream loss policy (the single WRITER: [`LostVisibilityReporter`]
/// parks a note here when a LOGGED loss ends) and every wake-stream
/// emitter in the observer process (the narrator's caller, the periodic
/// stats reporter, the idle alert, the failure-aggregation policies, the
/// coordinator's own important emits), each of which calls
/// [`Self::flush_after_host`] right after it emitted a wake-stream event.
///
/// A reconnection is NEVER its own wake event: the note rides together
/// with the next log that is emitted anyway — whichever emitter gets
/// there first takes the note (the slot is take-once), the rest see an
/// empty slot and no-op. Cloning shares the one slot; `Default` yields an
/// empty slot (a policy constructed without wiring flushes nothing).
#[derive(Clone, Debug, Default)]
pub struct WakeNoteSlot {
    note: Arc<Mutex<Option<String>>>,
}

impl WakeNoteSlot {
    /// Park a note to ride the next wake-stream emission. A note from an
    /// earlier outage that never found a host is OVERWRITTEN — the wake
    /// stream carries the latest reconnection fact, not a backlog.
    pub fn set(&self, text: String) {
        let mut guard = self.note.lock().unwrap_or_else(|p| p.into_inner());
        *guard = Some(text);
    }

    /// Emit the pending note (if any) on the wake stream, immediately
    /// after the host event the caller just emitted — then clear it.
    /// Idempotent: a second flush (or a flush with no pending note) is a
    /// no-op, so attaching this at every emitter never duplicates the
    /// note.
    pub fn flush_after_host(&self) {
        let taken = {
            let mut guard = self.note.lock().unwrap_or_else(|p| p.into_inner());
            guard.take()
        };
        if let Some(text) = taken {
            tracing::info!(target: IMPORTANT_TARGET, "{text}");
        }
    }

    /// Whether a note is currently parked (test/diagnostic accessor).
    #[cfg(test)]
    pub(crate) fn is_pending(&self) -> bool {
        self.note
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }
}

/// A LOGGED outage just ended — the regain signal the coordinator
/// forwards to the periodic stats reporter, which decides (it owns the
/// grid) whether a grid occurrence elapsed inside the down window and a
/// late stats run is due. Carries the loss instant as a `std::time`
/// instant because the reporter's [`super::reporting::Clock`] seam speaks
/// std instants (under a paused `tokio::time` both derive from the same
/// virtual clock).
#[derive(Debug, Clone, Copy)]
pub struct EndedOutage {
    /// When the (logged) loss episode began.
    pub down_since: std::time::Instant,
}

/// Whether the observer holds POSITIVE, CRDT-derived evidence that the
/// compute mesh is still alive at the moment it lost transport visibility.
///
/// # Why this gates the reassurance banner (the misdirection fix)
///
/// The observer's loss of its OWN transport view (zero reachable peers /
/// a silent named primary) says NOTHING about whether the compute mesh
/// survived: in the proven submitter-ssh-blip failure, systemd `--user`
/// SIGTERM'd the rootless-podman containers on all 14 non-primary nodes,
/// so the mesh genuinely died WHILE the observer had lost visibility. A
/// banner keyed only on the observer's own `peer_count()==0` cannot tell
/// "blip, mesh fine" from "mesh dead" apart, so it must NOT assert
/// autonomy it cannot verify. This field carries the ONE signal that CAN
/// distinguish them: the count of live worker-secondaries the observer's
/// last converged CRDT snapshot still holds. The coordinator derives it
/// (it owns the `ClusterState`); the reporter only chooses banner text
/// from it — the reporter never touches the CRDT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeshLiveness {
    /// The observer's last CRDT snapshot still shows ≥1 live
    /// worker-secondary member (`alive_count` > 0). POSITIVE evidence the
    /// compute mesh is running — the reassurance ("the mesh runs
    /// autonomously over its direct links") is now backed by a signal the
    /// observer can actually verify.
    KnownAlive { alive_count: usize },
    /// The observer has NO positive evidence the mesh is alive: its last
    /// snapshot shows zero live worker-secondaries (every member it knew
    /// of dropped out of the membership ledger), or it never held a
    /// roster at all. The observer must emit a NEUTRAL "compute-mesh state
    /// UNKNOWN" line — it CANNOT assert autonomy here, because this is
    /// exactly the shape the all-nodes-SIGTERM'd death also presents.
    Unknown,
}

/// The current visibility the coordinator observes each loop iteration.
///
/// `Visible` means the observer can see the run (a peer is reachable and,
/// if a primary is named, it is not silent past the threshold). `Lost`
/// carries a human reason for the operator log (which signal dropped) AND
/// the CRDT-derived [`MeshLiveness`] evidence the coordinator holds, which
/// gates whether the reporter may assert the mesh runs autonomously or must
/// stay neutral (see [`MeshLiveness`]). `Degraded` is the data-plane-vetoed
/// middle state: the named primary's KEEPALIVE class is not reaching this
/// observer while authenticated frames from the roster's legs ARE arriving
/// and applying — an addressing / primary-leg gap, NOT a connection loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Visibility {
    /// The observer currently sees the run.
    Visible,
    /// The observer currently cannot see the run; `reason` names which
    /// signal dropped (fleet empty / named-primary silent) for the log,
    /// and `mesh_liveness` carries the CRDT-derived evidence (if any) that
    /// the compute mesh survived the visibility loss.
    Lost {
        reason: String,
        mesh_liveness: MeshLiveness,
    },
    /// The observer's DATA PLANE is live (frames arriving + applying —
    /// the same ingest that feeds its periodic stats) while the named
    /// primary's keepalive / re-point class is not reaching it past the
    /// silence threshold. The production face (owner logs 2026-06-11): an
    /// observer WARNed "connection down for 300s with no successful
    /// reconnect+sync" in the SAME minute its 10-minute stats printed the
    /// cluster's fresh task counts. That claim is false — sync IS
    /// succeeding — so this verdict gets its own honest narration and
    /// NEVER counts as a loss episode: no loss clock, no wake-LOSS emit
    /// (the cluster-gone consult keys on those), no [`EndedOutage`]
    /// bookkeeping. The (freshness-filtered) primary-leg redial cadence
    /// stays alive — a dead primary→observer leg is one legitimate cause
    /// of this state and the redial is its heal; the per-leg filter
    /// guarantees a delivering leg is never touched.
    Degraded {
        /// Names the named primary, its keepalive-silence age, and the
        /// data-plane freshness for the log.
        reason: String,
    },
}

/// What the coordinator should do this iteration, returned from
/// [`LostVisibilityReporter::observe`]. The reporter owns the cadence
/// (lost-since clock + the ~60s recurrence), the coordinator owns the
/// action (trigger the reconnect port). NEVER an exit — visibility loss
/// is not a run verdict (BUG-B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDirective {
    /// Visibility is fine (or just regained) — nothing to do.
    Idle,
    /// Visibility is currently lost AND a reconnect attempt is due this
    /// iteration (the first lost loop, then once per recurrence interval).
    /// The coordinator should trigger the [`super::reconnect::TunnelReconnector`]
    /// with its current roster. The same cadence as the recurrence report,
    /// so a single clock drives both "remind the operator" and "retry the
    /// tunnel".
    ReconnectDue,
    /// Visibility is currently lost but no attempt is due yet (the
    /// inter-recurrence wait) — keep observing, do not re-fire the rebuild.
    WaitingToRetry,
}

/// What [`LostVisibilityReporter::observe`] tells the coordinator this
/// iteration: the reconnect-cadence directive (unchanged semantics) plus,
/// on the loop where a LOGGED loss episode ends, the [`EndedOutage`]
/// signal to forward to the periodic stats reporter.
#[derive(Debug, Clone, Copy)]
pub struct ObserveOutcome {
    pub directive: RetryDirective,
    /// `Some` exactly on the regain iteration of an outage whose loss WAS
    /// logged on the wake stream (≥ [`WAKE_LOSS_THRESHOLD`] down). A blip
    /// never produces this.
    pub ended_logged_outage: Option<EndedOutage>,
}

/// Report-state machine for the observer's connection visibility.
///
/// Single writer (the coordinator's run loop, single-threaded LocalSet),
/// so no synchronisation. It owns ONLY the report cadence + the
/// wake-stream loss policy; it carries no transport, no exit decision, no
/// authority.
#[derive(Debug, Default)]
pub struct LostVisibilityReporter {
    /// `Some(t)` while visibility is currently lost — the instant the
    /// CURRENT loss episode began. `None` while visible.
    lost_since: Option<Instant>,
    /// The last instant a "connection lost / still retrying" report was
    /// emitted, so recurrence fires at most once per [`REPORT_RECURRENCE`].
    last_report: Option<Instant>,
    /// `Some(t)` once the CURRENT loss episode has been reported on the
    /// wake stream (it stayed down past [`WAKE_LOSS_THRESHOLD`]) — the
    /// instant of the LAST wake-stream loss emit, from which the next
    /// [`WAKE_LOSS_RECURRENCE`] deadline derives. Reset on regain. The ONE
    /// state owning "has this outage been reported" (it gates the
    /// reconnection note + the [`EndedOutage`] signal) AND the repeat
    /// cadence — never two parallel gates.
    last_wake_emit: Option<Instant>,
    /// The latest lost-reason + mesh-liveness evidence observed while
    /// down, so the threshold emit (which fires from the deadline arm, not
    /// from an `observe` call) carries the same gated wording as the
    /// full-log banner.
    last_lost_context: Option<(String, MeshLiveness)>,
    /// `Some(t)` while the keepalive-addressing DEGRADED state persists —
    /// the instant the CURRENT degraded episode began. Mutually exclusive
    /// with `lost_since` by construction: each verdict arm clears the
    /// other episode's clocks. NEVER feeds the loss machinery (no
    /// [`EndedOutage`], no reconnection note, no `last_wake_emit`
    /// advance) — the cluster-gone consult and the outage bookkeeping
    /// key on the LOSS clocks only.
    degraded_since: Option<Instant>,
    /// The last instant the degraded full-log WARN was emitted, so the
    /// recurrence fires at most once per [`REPORT_RECURRENCE`].
    last_degraded_report: Option<Instant>,
    /// The instant of the LAST wake-stream DEGRADED emit (the honest
    /// "primary keepalive not reaching this observer; data plane live"
    /// line), threshold/recurrence-spaced exactly like the loss policy
    /// but on its OWN clock — it must never advance `last_wake_emit`, the
    /// LOSS-emit clock that gates the outage/note bookkeeping (and that the
    /// degraded-episode veto pin observes via `wake_emit_instant`).
    last_degraded_wake_emit: Option<Instant>,
    /// The latest degraded reason observed, so the threshold emit (which
    /// fires from the deadline arm, between `observe` calls) carries the
    /// live wording.
    last_degraded_context: Option<String>,
    /// The shared reconnection-note slot (see [`WakeNoteSlot`]). The
    /// reporter is the single writer; the wake emitters flush it.
    note: WakeNoteSlot,
}

impl LostVisibilityReporter {
    /// Build a reporter writing reconnection notes into `note` — the same
    /// shared slot every wake-stream emitter in the process flushes.
    pub fn new(note: WakeNoteSlot) -> Self {
        Self {
            note,
            ..Self::default()
        }
    }

    /// Feed the current visibility verdict and learn what to do this
    /// iteration. On the FIRST loop where visibility is lost, emit the
    /// FULL-LOG "connection lost — observer continues passively, retrying
    /// reconnect" report AND return [`RetryDirective::ReconnectDue`];
    /// while still lost, re-emit + signal a fresh attempt at most once per
    /// [`REPORT_RECURRENCE`] (otherwise [`RetryDirective::WaitingToRetry`]);
    /// on the loop visibility is REGAINED, emit a FULL-LOG "reconnected"
    /// report, clear the loss state, and return [`RetryDirective::Idle`].
    ///
    /// None of those diagnostics touch the wake stream: the wake-stream
    /// loss is emitted at the [`WAKE_LOSS_THRESHOLD`] mark and then once
    /// per [`WAKE_LOSS_RECURRENCE`] while still down (via
    /// [`Self::on_wake_deadline`], also checked here as a backstop), and a
    /// regain after a LOGGED loss parks the reconnection note + returns
    /// the [`EndedOutage`] signal — never a wake emit of its own.
    ///
    /// NEVER returns an exit signal — a lost-visibility observer keeps
    /// observing. This is the entire BUG-B contract: the observer reports,
    /// retries the tunnel rebuild, and does not reap the run. The single
    /// `due` decision drives BOTH the full-log report and the reconnect
    /// attempt, so they share one ~60s clock.
    pub fn observe(&mut self, visibility: &Visibility, now: Instant) -> ObserveOutcome {
        match visibility {
            Visibility::Visible => {
                let ended_logged_outage = self.end_loss_episode(now);
                self.clear_degraded_state();
                ObserveOutcome {
                    directive: RetryDirective::Idle,
                    ended_logged_outage,
                }
            }
            Visibility::Degraded { reason } => {
                // Data plane LIVE ⇒ NOT a connection loss: any loss
                // episode ends here exactly as on a regain — frames
                // arriving and applying IS the connection — with the same
                // logged-outage note / late-stats bookkeeping. The
                // keepalive-class gap is then narrated on its OWN cadence
                // (full-log WARN per [`REPORT_RECURRENCE`]; the wake
                // stream only at the threshold marks via
                // [`Self::on_wake_deadline`], with honest wording and on
                // a clock the cluster-gone consult never reads). The
                // redial cadence stays alive: a dead primary→observer
                // leg is one legitimate cause of this state and the
                // freshness-filtered roster guarantees the rebuild can
                // only ever touch non-delivering legs (the
                // run_20260611_200548 per-leg-honesty contract).
                let ended_logged_outage = self.end_loss_episode(now);
                let first = self.degraded_since.is_none();
                self.degraded_since.get_or_insert(now);
                self.last_degraded_context = Some(reason.clone());
                self.on_wake_deadline(now);
                let due = first
                    || self
                        .last_degraded_report
                        .is_none_or(|last| now.duration_since(last) >= REPORT_RECURRENCE);
                let directive = if due {
                    let since = self.degraded_since.expect("set above");
                    let degraded_secs = now.duration_since(since).as_secs();
                    // FULL-LOG only; the wake stream learns of a
                    // persistent gap solely via the threshold emit.
                    tracing::warn!(
                        %reason,
                        degraded_secs,
                        "primary keepalive not reaching this observer; data plane live — \
                         frames from the run keep arriving and applying, so this is a \
                         role-addressing gap or a dead primary→observer leg, NOT a \
                         connection loss (no reconnect+sync outage is in progress); the \
                         primary-leg redial keeps retrying on this cadence and never \
                         touches a delivering leg"
                    );
                    self.last_degraded_report = Some(now);
                    RetryDirective::ReconnectDue
                } else {
                    RetryDirective::WaitingToRetry
                };
                ObserveOutcome {
                    directive,
                    ended_logged_outage,
                }
            }
            Visibility::Lost {
                reason,
                mesh_liveness,
            } => {
                // A degraded episode that collapses into a real loss (the
                // data plane went quiet too) starts the loss clocks
                // fresh; the degraded cadence state is over.
                self.clear_degraded_state();
                let first = self.lost_since.is_none();
                self.lost_since.get_or_insert(now);
                // Keep the wake-threshold emit's context fresh (the
                // deadline arm fires between observes) and run the
                // cadence check as a backstop — `on_wake_deadline` is
                // self-spacing (the next deadline derives from the last
                // emit), so the dedicated deadline arm and this backstop
                // can never double-emit inside one cadence window.
                self.last_lost_context = Some((reason.clone(), *mesh_liveness));
                self.on_wake_deadline(now);
                let due = first
                    || self
                        .last_report
                        .is_none_or(|last| now.duration_since(last) >= REPORT_RECURRENCE);
                let directive = if due {
                    let since = self.lost_since.expect("set above");
                    let lost_secs = now.duration_since(since).as_secs();
                    // Gate the reassurance on POSITIVE CRDT evidence. The
                    // observer may only assert the mesh "runs autonomously"
                    // when its last snapshot still shows live
                    // worker-secondaries; otherwise it stays NEUTRAL — the
                    // observer cannot tell an ssh blip from an all-nodes
                    // teardown from its own transport view alone. See
                    // [`MeshLiveness`]. FULL-LOG only: the wake stream
                    // learns of the loss solely via the 5-minute threshold
                    // emit in [`Self::on_wake_deadline`].
                    match mesh_liveness {
                        MeshLiveness::KnownAlive { alive_count } => {
                            tracing::warn!(
                                %reason,
                                lost_secs,
                                alive_secondaries = *alive_count,
                                "observer lost connection to the run — the last CRDT snapshot \
                                 still shows {alive_count} live worker-secondary member(s), so the \
                                 compute mesh is running autonomously over its direct links; the \
                                 observer carries zero authority and stays a passive monitor, \
                                 rebuilding its tunnel + retrying reconnect (~60s cadence). The \
                                 run's verdict comes from the primary, never from the observer's \
                                 own view."
                            );
                        }
                        MeshLiveness::Unknown => {
                            tracing::warn!(
                                %reason,
                                lost_secs,
                                "observer lost connection to the run — compute-mesh state UNKNOWN: \
                                 the observer holds NO live worker-secondary in its last CRDT \
                                 snapshot, so it CANNOT confirm the mesh survived (this is also \
                                 how an all-nodes teardown would look). The observer carries zero \
                                 authority and stays a passive monitor, rebuilding its tunnel + \
                                 retrying reconnect (~60s cadence); the run's verdict comes from \
                                 the primary, never from the observer's own view."
                            );
                        }
                    }
                    self.last_report = Some(now);
                    RetryDirective::ReconnectDue
                } else {
                    RetryDirective::WaitingToRetry
                };
                ObserveOutcome {
                    directive,
                    ended_logged_outage: None,
                }
            }
        }
    }

    /// End any current LOSS episode (the regain transition shared by the
    /// `Visible` and `Degraded` arms — frames arriving and applying IS
    /// the connection): emit the FULL-LOG "reconnected" diagnostic, and
    /// — iff the loss WAS reported on the wake stream — park the
    /// reconnection note + return the [`EndedOutage`] signal for the
    /// periodic reporter's late-run decision. Clears every loss clock;
    /// a no-op (returning `None`) when no loss episode is open.
    fn end_loss_episode(&mut self, now: Instant) -> Option<EndedOutage> {
        let mut ended_logged_outage = None;
        if let Some(since) = self.lost_since.take() {
            let lost_secs = now.duration_since(since).as_secs();
            // FULL-LOG diagnostic — immediate, regardless of the
            // wake threshold (the wake stream stays silent here).
            tracing::warn!(
                lost_secs,
                "observer reconnected — visibility regained; resuming passive \
                 observation of the run"
            );
            if self.last_wake_emit.is_some() {
                // The loss WAS reported on the wake stream, so the
                // operator must eventually learn it ended — but a
                // reconnection is never a wake event of its own:
                // park the note to ride the next wake-stream log,
                // and hand the coordinator the ended-outage signal
                // for the periodic reporter's late-run decision.
                self.note.set(format!(
                    "observer connection to the run was restored after {lost_secs}s of \
                     lost visibility (the loss was reported earlier on this stream)"
                ));
                ended_logged_outage = Some(EndedOutage {
                    down_since: since.into_std(),
                });
            }
        }
        self.last_wake_emit = None;
        self.last_lost_context = None;
        self.last_report = None;
        ended_logged_outage
    }

    /// Clear the degraded-episode cadence state (the `Visible` and
    /// `Lost` transitions both end a degraded episode). Touches NO loss
    /// clock.
    fn clear_degraded_state(&mut self) {
        self.degraded_since = None;
        self.last_degraded_report = None;
        self.last_degraded_wake_emit = None;
        self.last_degraded_context = None;
    }

    /// The NEXT wake-stream deadline: while the connection is down, the
    /// LOSS cadence (`Some(lost_since + 5min)` before the episode's first
    /// wake emit, then `Some(last_emit + 10min)` for each repeat —
    /// [`WAKE_LOSS_RECURRENCE`]); while the keepalive-addressing DEGRADED
    /// state persists, the SAME threshold/recurrence shape on the
    /// degraded clocks (the two episodes are mutually exclusive); `None`
    /// while visible. Derived from STORED instants (the
    /// persistent-deadline law): the coordinator's select arm rebuilds
    /// `sleep_until` from this value every iteration, so sibling arm
    /// activity never resets the clock.
    pub fn wake_deadline(&self) -> Option<Instant> {
        match (self.lost_since, self.last_wake_emit) {
            (Some(since), None) => return Some(since + WAKE_LOSS_THRESHOLD),
            (Some(_), Some(last_emit)) => return Some(last_emit + WAKE_LOSS_RECURRENCE),
            (None, _) => {}
        }
        match (self.degraded_since, self.last_degraded_wake_emit) {
            (Some(since), None) => Some(since + WAKE_LOSS_THRESHOLD),
            (Some(_), Some(last_emit)) => Some(last_emit + WAKE_LOSS_RECURRENCE),
            (None, _) => None,
        }
    }

    /// Emit the wake-stream loss event if the next cadence deadline has
    /// been reached: no successful reconnect-and-sync for
    /// [`WAKE_LOSS_THRESHOLD`] (the first), then one repeat per elapsed
    /// [`WAKE_LOSS_RECURRENCE`] while still down. Self-spacing — the
    /// deadline always derives from the last emit, so no matter how often
    /// the deadline arm or the `observe` backstop call this (the ~60s
    /// full-log/reconnect cadence included), the wake stream sees at most
    /// one line per cadence window. The wording carries the same
    /// positive-CRDT-evidence gate as the full-log banner (see
    /// [`MeshLiveness`]). The emit is itself a wake-stream host: a note
    /// still pending from an EARLIER outage rides it.
    pub fn on_wake_deadline(&mut self, now: Instant) {
        let Some(deadline) = self.wake_deadline() else {
            return;
        };
        if now < deadline {
            return;
        }
        let Some(since) = self.lost_since else {
            // DEGRADED episode (the two are mutually exclusive): the
            // honest wake line, threshold/recurrence-spaced on the
            // degraded clock. Deliberately NOT `last_wake_emit` — that
            // is the LOSS clock the cluster-gone consult and the
            // outage/note bookkeeping key on, and a degraded state is
            // not an outage.
            let since = self
                .degraded_since
                .expect("wake_deadline() is Some only while lost or degraded");
            self.last_degraded_wake_emit = Some(now);
            let degraded_secs = now.duration_since(since).as_secs();
            let reason = self
                .last_degraded_context
                .clone()
                .unwrap_or_else(|| "primary keepalive-silent".to_string());
            tracing::warn!(
                target: IMPORTANT_TARGET,
                %reason,
                degraded_secs,
                "primary keepalive not reaching this observer; data plane live — the \
                 named primary's keepalive/re-point class has not reached this observer \
                 for {degraded_secs}s while CRDT sync continues over the delivering legs \
                 (a role-addressing gap or a dead primary→observer leg, NOT a connection \
                 loss); the primary-leg redial keeps retrying"
            );
            // A wake-stream host like any other: a pending note rides it.
            self.note.flush_after_host();
            return;
        };
        self.last_wake_emit = Some(now);
        let lost_secs = now.duration_since(since).as_secs();
        let (reason, mesh_liveness) = self
            .last_lost_context
            .clone()
            .unwrap_or_else(|| ("visibility lost".to_string(), MeshLiveness::Unknown));
        match mesh_liveness {
            MeshLiveness::KnownAlive { alive_count } => {
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    %reason,
                    lost_secs,
                    alive_secondaries = alive_count,
                    "observer connection to the run has been down for {lost_secs}s with no \
                     successful reconnect+sync — the last CRDT snapshot still shows \
                     {alive_count} live worker-secondary member(s), so the compute mesh is \
                     running autonomously over its direct links; the observer keeps retrying \
                     reconnect. The run's verdict comes from the primary, never from the \
                     observer's own view."
                );
            }
            MeshLiveness::Unknown => {
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    %reason,
                    lost_secs,
                    "observer connection to the run has been down for {lost_secs}s with no \
                     successful reconnect+sync — compute-mesh state UNKNOWN: the observer holds \
                     NO live worker-secondary in its last CRDT snapshot, so it CANNOT confirm \
                     the mesh survived. The observer keeps retrying reconnect; the run's \
                     verdict comes from the primary, never from the observer's own view."
                );
            }
        }
        // The loss emit is a wake-stream host like any other: an unflushed
        // note from a previous outage rides it rather than waiting longer.
        self.note.flush_after_host();
    }

    /// The instant of the LAST wake-stream LOSS emit of the CURRENT episode,
    /// or `None` if the episode has not yet been logged on the wake stream
    /// (or visibility is fine, or the episode is DEGRADED not lost — that
    /// rides `last_degraded_wake_emit`, a separate clock). Reset to `None`
    /// on regain.
    ///
    /// Test-only observation handle for the loss-vs-degraded clock split
    /// (the cluster-gone consult no longer keys on this — it runs its own
    /// `CLUSTER_GONE_CONSULT_INTERVAL` cadence in the coordinator). The
    /// loss-emit clock itself remains live (it gates the outage/note
    /// bookkeeping via `last_wake_emit`); this accessor only exposes it for
    /// the degraded-episode veto pin.
    #[cfg(test)]
    pub(crate) fn wake_emit_instant(&self) -> Option<Instant> {
        self.last_wake_emit
    }

    /// Whether the reporter currently considers visibility lost. Test/
    /// diagnostic accessor — not part of any exit decision.
    #[cfg(test)]
    pub fn is_lost(&self) -> bool {
        self.lost_since.is_some()
    }

    /// Whether the reporter is currently in the keepalive-addressing
    /// DEGRADED state. Test/diagnostic accessor.
    #[cfg(test)]
    pub(crate) fn is_degraded(&self) -> bool {
        self.degraded_since.is_some()
    }

    /// Whether the CURRENT loss episode has been reported on the wake
    /// stream (at least once). Test/diagnostic accessor.
    #[cfg(test)]
    pub(crate) fn loss_logged(&self) -> bool {
        self.last_wake_emit.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_capture::{CapturedEvent, ImportantCapture, capture_important};

    fn lost(reason: &str) -> Visibility {
        Visibility::Lost {
            reason: reason.to_string(),
            mesh_liveness: MeshLiveness::Unknown,
        }
    }

    /// `t0 + secs` — the simulated-time helper every wake-policy test
    /// drives `observe` / `on_wake_deadline` with (the same injected-now
    /// pattern as the `IdleDetector` tests).
    fn at(t0: Instant, secs: u64) -> Instant {
        t0 + Duration::from_secs(secs)
    }

    /// Capture EVERY tracing event (any target) — the full-log view. The
    /// wake-stream view is `capture_important` (target-filtered); the
    /// full-log diagnostics now emit WITHOUT the importance target, so
    /// asserting on them needs the unfiltered capture.
    fn capture_all(body: impl FnOnce()) -> Vec<CapturedEvent> {
        use tracing_subscriber::Registry;
        use tracing_subscriber::layer::SubscriberExt;

        let capture = ImportantCapture::default();
        let subscriber = Registry::default().with(capture.clone());
        tracing::subscriber::with_default(subscriber, body);
        capture.events()
    }

    #[test]
    fn observe_never_signals_exit_while_lost() {
        // The contract is that `observe` returns a `RetryDirective` (Idle /
        // ReconnectDue / WaitingToRetry) — there is NO variant by which a
        // lost-visibility observer is told to exit. This test pins the
        // type-level guarantee: feeding repeated losses only updates
        // internal report state + asks for reconnects, never a terminal.
        let t0 = Instant::now();
        let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
        let d = r.observe(&lost("fleet empty"), t0).directive;
        assert!(r.is_lost());
        assert_eq!(
            d,
            RetryDirective::ReconnectDue,
            "the first lost loop must request a reconnect attempt"
        );
        // The immediately-following lost loop is inside the recurrence
        // window, so no fresh attempt is due — but the observer keeps
        // observing (no exit variant exists).
        let d2 = r.observe(&lost("fleet empty"), at(t0, 1)).directive;
        assert!(r.is_lost(), "still lost — observer keeps observing, no exit");
        assert_eq!(
            d2,
            RetryDirective::WaitingToRetry,
            "a second lost loop within the recurrence window waits, does not re-fire"
        );
    }

    #[test]
    fn first_loss_requests_reconnect() {
        // The owner directive: a lost observer must TRY to rebuild its
        // tunnel + reconnect. The first lost observation surfaces the
        // ReconnectDue directive the coordinator acts on (trigger the
        // TunnelReconnector). This is the unit pin of "the observer drives
        // the reconnect", independent of the wall-clock recurrence.
        let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
        assert_eq!(
            r.observe(&lost("no reachable peer"), Instant::now()).directive,
            RetryDirective::ReconnectDue,
        );
    }

    #[test]
    fn regaining_visibility_clears_loss_state() {
        let t0 = Instant::now();
        let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
        assert_eq!(
            r.observe(&lost("primary silent"), t0).directive,
            RetryDirective::ReconnectDue
        );
        assert!(r.is_lost());
        assert_eq!(
            r.observe(&Visibility::Visible, at(t0, 1)).directive,
            RetryDirective::Idle
        );
        assert!(!r.is_lost(), "regained visibility clears the loss episode");
    }

    #[test]
    fn visible_throughout_stays_clear() {
        let t0 = Instant::now();
        let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
        assert_eq!(
            r.observe(&Visibility::Visible, t0).directive,
            RetryDirective::Idle
        );
        assert_eq!(
            r.observe(&Visibility::Visible, at(t0, 1)).directive,
            RetryDirective::Idle
        );
        assert!(!r.is_lost());
    }

    #[test]
    fn unknown_liveness_emits_neutral_not_autonomy_on_full_log() {
        // The misdirection fix: with NO positive CRDT evidence the mesh is
        // alive, the reporter must NEVER assert the mesh "runs autonomously"
        // — it must emit the neutral "compute-mesh state UNKNOWN" line. This
        // is the exact shape of the proven all-nodes-SIGTERM failure, where
        // the old unconditional banner reassured the operator while every
        // compute peer was dead. The banner is a FULL-LOG diagnostic now —
        // it must stay immediate there and NOT touch the wake stream.
        let t0 = Instant::now();
        let events = capture_all(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            r.observe(
                &Visibility::Lost {
                    reason: "no reachable peer".to_string(),
                    mesh_liveness: MeshLiveness::Unknown,
                },
                t0,
            );
        });
        assert!(
            events.iter().any(|e| e.message.contains("UNKNOWN")),
            "unknown mesh liveness must emit the neutral UNKNOWN line: {events:?}"
        );
        assert!(
            !events.iter().any(|e| e.message.contains("autonomous")),
            "the observer must NOT assert mesh autonomy it cannot verify: {events:?}"
        );
        // And the SAME first-loss observation produces nothing wake-worthy.
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            r.observe(&lost("no reachable peer"), t0);
        });
        assert!(
            wake.is_empty(),
            "an immediate loss is a full-log diagnostic, never a wake event: {wake:?}"
        );
    }

    #[test]
    fn known_alive_liveness_emits_autonomy_reassurance_on_full_log() {
        // The reassurance is legitimate ONLY when the last CRDT snapshot
        // still shows live worker-secondaries — then the observer CAN verify
        // the mesh is up. Pin that the positive-evidence branch keeps the
        // "runs autonomously" reassurance AND surfaces the live count, on
        // the full log (the wake stream is threshold-gated separately).
        let events = capture_all(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            r.observe(
                &Visibility::Lost {
                    reason: "named primary silent".to_string(),
                    mesh_liveness: MeshLiveness::KnownAlive { alive_count: 14 },
                },
                Instant::now(),
            );
        });
        assert!(
            events.iter().any(|e| e.message.contains("autonomous")),
            "positive CRDT liveness justifies the autonomy reassurance: {events:?}"
        );
        assert!(
            events.iter().any(|e| e
                .fields
                .get("alive_secondaries")
                .is_some_and(|v| v.contains("14"))),
            "the autonomy banner must surface the live worker-secondary count: {events:?}"
        );
        assert!(
            !events.iter().any(|e| e.message.contains("UNKNOWN")),
            "the positive-evidence branch must not emit the neutral UNKNOWN line: {events:?}"
        );
    }

    // ── wake-stream policy (the owner's loss-wake semantics) ──

    #[test]
    fn blip_under_threshold_is_wake_silent_forever() {
        // Rule 1 + 2: a 3-minute blip produces NOTHING on the wake stream —
        // no loss (before, during, after) and no reconnection note, ever.
        let t0 = Instant::now();
        let note = WakeNoteSlot::default();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(note.clone());
            r.observe(&lost("fleet empty"), t0);
            r.observe(&lost("fleet empty"), at(t0, 60));
            r.observe(&lost("fleet empty"), at(t0, 120));
            let outcome = r.observe(&Visibility::Visible, at(t0, 180));
            assert!(
                outcome.ended_logged_outage.is_none(),
                "a blip is not a logged outage — no late-periodic signal"
            );
            // Long after the blip: still nothing latent.
            r.observe(&Visibility::Visible, at(t0, 3600));
        });
        assert!(
            wake.is_empty(),
            "a sub-threshold blip must produce ZERO wake-stream output: {wake:?}"
        );
        assert!(!note.is_pending(), "no reconnection note for a blip");
    }

    #[test]
    fn loss_logged_once_at_threshold_then_spaced_by_recurrence() {
        // Rule 1: the loss reaches the wake stream AT the 5-minute mark —
        // not at loss time — and inside the following 10-minute cadence
        // window nothing repeats, no matter how often the ~60s arm/backstop
        // fire.
        let t0 = Instant::now();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            r.observe(&lost("fleet empty"), t0);
            // Just before the mark: nothing (deadline arm not yet due).
            r.on_wake_deadline(at(t0, 299));
            assert!(!r.loss_logged(), "299s < threshold — not logged yet");
            assert_eq!(
                r.wake_deadline(),
                Some(t0 + WAKE_LOSS_THRESHOLD),
                "first deadline derives from the STORED loss instant"
            );
            // The mark: the deadline arm fires.
            r.on_wake_deadline(at(t0, 300));
            assert!(r.loss_logged());
            assert_eq!(
                r.wake_deadline(),
                Some(at(t0, 300) + WAKE_LOSS_RECURRENCE),
                "after the first emit the deadline re-arms at the 10-minute \
                 recurrence, derived from the last emit"
            );
            // Still down inside the cadence window: the observe backstop +
            // stray deadline calls must NOT re-emit.
            r.observe(&lost("fleet empty"), at(t0, 360));
            r.on_wake_deadline(at(t0, 400));
            r.on_wake_deadline(at(t0, 899));
        });
        let losses: Vec<_> = wake
            .iter()
            .filter(|e| e.message.contains("has been down for"))
            .collect();
        assert_eq!(
            losses.len(),
            1,
            "exactly one wake loss event per cadence window: {wake:?}"
        );
        assert_eq!(
            wake.len(),
            1,
            "nothing else reaches the wake stream while down: {wake:?}"
        );
    }

    #[test]
    fn observe_backstop_logs_loss_without_deadline_arm() {
        // The threshold check also runs inside `observe` (latched), so an
        // observe at/after the mark logs the loss even if the select arm
        // never got polled — and the two paths can never double-emit.
        let t0 = Instant::now();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            r.observe(&lost("fleet empty"), t0);
            r.observe(&lost("fleet empty"), at(t0, 301));
            r.on_wake_deadline(at(t0, 302));
        });
        let losses: Vec<_> = wake
            .iter()
            .filter(|e| e.message.contains("has been down for"))
            .collect();
        assert_eq!(losses.len(), 1, "backstop + arm emit exactly once: {wake:?}");
    }

    #[test]
    fn wake_loss_line_carries_mesh_liveness_gate() {
        // The wake loss emit reuses the misdirection-fix gate: autonomy
        // reassurance only on positive CRDT evidence, neutral UNKNOWN
        // otherwise.
        let t0 = Instant::now();
        let known = capture_important(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            r.observe(
                &Visibility::Lost {
                    reason: "named primary silent".to_string(),
                    mesh_liveness: MeshLiveness::KnownAlive { alive_count: 7 },
                },
                t0,
            );
            r.on_wake_deadline(at(t0, 300));
        });
        assert!(
            known.iter().any(|e| e.message.contains("autonomous")
                && e.fields
                    .get("alive_secondaries")
                    .is_some_and(|v| v.contains("7"))),
            "positive evidence: the wake loss line reassures with the count: {known:?}"
        );
        let unknown = capture_important(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            r.observe(&lost("fleet empty"), t0);
            r.on_wake_deadline(at(t0, 300));
        });
        assert!(
            unknown.iter().any(|e| e.message.contains("UNKNOWN")),
            "no evidence: the wake loss line stays neutral: {unknown:?}"
        );
    }

    #[test]
    fn reconnection_note_rides_next_host_exactly_once() {
        // Rule 2 (the 7-minute case, no periodic elapsed): nothing emits at
        // reconnect; the note is parked and rides the next wake-stream host
        // exactly once.
        let t0 = Instant::now();
        let note = WakeNoteSlot::default();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(note.clone());
            r.observe(&lost("fleet empty"), t0);
            r.on_wake_deadline(at(t0, 300)); // loss logged (1 event)
            let outcome = r.observe(&Visibility::Visible, at(t0, 420));
            assert!(
                outcome.ended_logged_outage.is_some(),
                "a logged outage ending surfaces the EndedOutage signal"
            );
        });
        assert_eq!(
            wake.len(),
            1,
            "the regain itself emits NOTHING on the wake stream: {wake:?}"
        );
        assert!(note.is_pending(), "the reconnection note is parked");

        // The next host flush carries the note exactly once.
        let ride = capture_important(|| {
            note.flush_after_host();
            note.flush_after_host(); // second host: no duplicate
        });
        assert_eq!(ride.len(), 1, "note attaches exactly once: {ride:?}");
        assert!(
            ride[0].message.contains("restored after 420s"),
            "the note names the outage duration: {ride:?}"
        );
        assert!(!note.is_pending());
    }

    #[test]
    fn two_outages_short_then_long_only_second_wakes() {
        // A <5min blip followed by a >5min outage: only the second produces
        // the loss event + the note.
        let t0 = Instant::now();
        let note = WakeNoteSlot::default();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(note.clone());
            // Outage 1: 3 minutes — silent.
            r.observe(&lost("fleet empty"), t0);
            let o1 = r.observe(&Visibility::Visible, at(t0, 180));
            assert!(o1.ended_logged_outage.is_none());
            assert!(!note.is_pending(), "a blip parks no note");
            // Outage 2: starts at +200s, logged at +500s, ends at +560s.
            r.observe(&lost("fleet empty"), at(t0, 200));
            r.on_wake_deadline(at(t0, 500));
            let o2 = r.observe(&Visibility::Visible, at(t0, 560));
            assert!(o2.ended_logged_outage.is_some());
        });
        let losses: Vec<_> = wake
            .iter()
            .filter(|e| e.message.contains("has been down for"))
            .collect();
        assert_eq!(losses.len(), 1, "only the second outage wakes: {wake:?}");
        assert!(note.is_pending(), "only the second outage parks a note");
    }

    #[test]
    fn never_regained_loss_repeats_at_recurrence_and_has_no_note_ever() {
        // Loss logged, connection never returns: the wake stream sees the
        // 5-minute loss event and then one repeat per 10-minute cadence
        // window — never the ~60s tick rate — and no reconnection note.
        let t0 = Instant::now();
        let note = WakeNoteSlot::default();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(note.clone());
            r.observe(&lost("fleet empty"), t0);
            r.on_wake_deadline(at(t0, 300));
            for m in 6..120 {
                r.observe(&lost("fleet empty"), at(t0, m * 60));
                r.on_wake_deadline(at(t0, m * 60));
            }
        });
        // Emits: +300s, then every 600s (900, 1500, …, 6900) — 12 in two
        // hours of minutely ticks, not ~114.
        assert_eq!(
            wake.len(),
            12,
            "one loss event per cadence window, nothing more: {wake:?}"
        );
        assert!(
            wake.iter().all(|e| e.message.contains("has been down for")),
            "every wake event of a never-regained loss is a loss line: {wake:?}"
        );
        assert!(!note.is_pending(), "no regain — no note");
    }

    #[test]
    fn owner_repro_25_minute_outage_wakes_thrice_not_per_minute() {
        // The owner's live observation (13:31–13:40): one loss line per
        // ~60s reporter tick on the wake stream. Spec: the FIRST warning at
        // 5 minutes of no successful reconnect+sync, then repeats at a
        // 10-MINUTE cadence — a 25-minute outage wakes the operator at
        // ~+5, ~+15 and ~+25, i.e. exactly 3 emissions, not 25, not 1.
        // This replays the observed sequence: visibility lost, the
        // coordinator's ~60s recheck tick driving observe + the deadline
        // arm every minute, for 25 minutes.
        let t0 = Instant::now();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            r.observe(
                &Visibility::Lost {
                    reason: "no reachable peer".to_string(),
                    mesh_liveness: MeshLiveness::KnownAlive { alive_count: 4 },
                },
                t0,
            );
            for m in 1..=25 {
                r.observe(
                    &Visibility::Lost {
                        reason: "no reachable peer".to_string(),
                        mesh_liveness: MeshLiveness::KnownAlive { alive_count: 4 },
                    },
                    at(t0, m * 60),
                );
                r.on_wake_deadline(at(t0, m * 60));
            }
        });
        let down_secs: Vec<&str> = wake
            .iter()
            .map(|e| {
                e.message
                    .split("down for ")
                    .nth(1)
                    .and_then(|rest| rest.split('s').next())
                    .expect("every wake event is a loss line")
            })
            .collect();
        assert_eq!(
            down_secs,
            vec!["300", "900", "1500"],
            "exactly three wake emissions, at the +5/+15/+25 boundaries: {wake:?}"
        );
    }

    #[test]
    fn ended_outage_signal_carries_loss_instant() {
        // The EndedOutage hands the periodic reporter the DOWN-SINCE
        // instant so the grid owner can decide whether an occurrence
        // elapsed inside the down window.
        let t0 = Instant::now();
        let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
        r.observe(&lost("fleet empty"), t0);
        r.on_wake_deadline(at(t0, 300));
        let outcome = r.observe(&Visibility::Visible, at(t0, 720));
        let ended = outcome.ended_logged_outage.expect("logged outage ended");
        assert_eq!(ended.down_since, t0.into_std());
    }

    #[test]
    fn pending_note_rides_a_subsequent_loss_event() {
        // The wake loss emit is itself a host: a note left unflushed from
        // an earlier outage rides the NEXT outage's loss event instead of
        // waiting forever.
        let t0 = Instant::now();
        let note = WakeNoteSlot::default();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(note.clone());
            r.observe(&lost("fleet empty"), t0);
            r.on_wake_deadline(at(t0, 300));
            r.observe(&Visibility::Visible, at(t0, 400)); // note parked
            r.observe(&lost("fleet empty"), at(t0, 500));
            r.on_wake_deadline(at(t0, 800)); // second loss logged — hosts the note
        });
        assert_eq!(wake.len(), 3, "loss, loss, ridden note: {wake:?}");
        assert!(
            wake[2].message.contains("restored after"),
            "the parked note rides the second loss event: {wake:?}"
        );
        assert!(!note.is_pending());
    }

    /// The persistent-deadline law: the 5-minute threshold timer must fire
    /// under CONSTANT sibling tick activity. This drives the EXACT select
    /// shape the coordinator uses — `sleep_until` rebuilt every iteration
    /// from the STORED `wake_deadline()` — against a 10ms always-ready
    /// sibling under the paused clock, and pins that the loss is logged at
    /// the 5-minute virtual mark anyway (a per-iteration `sleep` arm would
    /// be reset by every sibling win and never fire — the #324 lesson).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn wake_deadline_fires_under_constant_tick_activity() {
        let start = Instant::now();
        let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
        r.observe(&lost("fleet empty"), Instant::now());

        let mut busy = tokio::time::interval(Duration::from_millis(10));
        let logged_at = loop {
            tokio::select! {
                _ = busy.tick() => {
                    // Constant sibling activity; the deadline arm below must
                    // still fire at the stored instant.
                }
                _ = async {
                    match r.wake_deadline() {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                } => {
                    r.on_wake_deadline(Instant::now());
                    assert!(r.loss_logged(), "deadline arm logs the loss");
                    break Instant::now();
                }
            }
        };
        let elapsed = logged_at.duration_since(start);
        assert!(
            elapsed >= WAKE_LOSS_THRESHOLD,
            "never before the 5-minute mark: {elapsed:?}"
        );
        assert!(
            elapsed < WAKE_LOSS_THRESHOLD + Duration::from_secs(1),
            "fires AT the mark despite constant sibling ticks (a resettable \
             sleep would never fire): {elapsed:?}"
        );
    }

    // ── the keepalive-addressing DEGRADED state (the late-joined-
    //    observer keepalive blackout, owner logs 2026-06-11) ──

    fn degraded(reason: &str) -> Visibility {
        Visibility::Degraded {
            reason: reason.to_string(),
        }
    }

    /// A degraded observation is NOT a loss: no loss episode opens, the
    /// honest full-log WARN fires once per [`REPORT_RECURRENCE`] (never
    /// per tick), and the redial cadence stays alive (ReconnectDue on the
    /// same recurrence — the run_20260611_200548 per-leg heal must keep
    /// firing; its roster filter guarantees only non-delivering legs are
    /// ever rebuilt).
    #[test]
    fn degraded_warns_honestly_on_recurrence_and_keeps_redial_cadence() {
        let t0 = Instant::now();
        let events = capture_all(|| {
            let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
            let first = r.observe(&degraded("keepalive gap"), t0);
            assert_eq!(
                first.directive,
                RetryDirective::ReconnectDue,
                "the first degraded loop drives the primary-leg redial"
            );
            assert!(!r.is_lost(), "degraded is NOT a loss episode");
            assert!(r.is_degraded());
            // Inside the recurrence window: wait, no re-warn, no re-fire.
            let second = r.observe(&degraded("keepalive gap"), at(t0, 1));
            assert_eq!(second.directive, RetryDirective::WaitingToRetry);
            // Past the recurrence: warn + redial again.
            let third = r.observe(&degraded("keepalive gap"), at(t0, 61));
            assert_eq!(third.directive, RetryDirective::ReconnectDue);
        });
        let warns: Vec<_> = events
            .iter()
            .filter(|e| {
                e.message
                    .contains("primary keepalive not reaching this observer")
            })
            .collect();
        assert_eq!(
            warns.len(),
            2,
            "one honest WARN per recurrence window, never per tick: {events:?}"
        );
        assert!(
            !events.iter().any(|e| e.message.contains("lost connection")),
            "a degraded state must never narrate as a connection loss: {events:?}"
        );
    }

    /// The escalation VETO pin: a degraded episode's wake emit rides its
    /// OWN clock — the LOSS-emit clock
    /// [`LostVisibilityReporter::wake_emit_instant`] (the gate for the
    /// outage/note bookkeeping) must NEVER advance, and the wake line must
    /// carry the honest wording, never the "has been down for" outage claim.
    #[test]
    fn degraded_wake_emit_never_advances_the_loss_emit_clock() {
        let t0 = Instant::now();
        let note = WakeNoteSlot::default();
        let wake = capture_important(|| {
            let mut r = LostVisibilityReporter::new(note.clone());
            r.observe(&degraded("keepalive gap"), t0);
            // Before the threshold: wake-silent.
            r.on_wake_deadline(at(t0, 299));
            assert_eq!(
                r.wake_emit_instant(),
                None,
                "no LOSS wake emit exists in a degraded episode"
            );
            // The threshold mark: the degraded wake line fires…
            r.observe(&degraded("keepalive gap"), at(t0, 300));
            r.on_wake_deadline(at(t0, 300));
            // …and the LOSS wake-emit clock still reads None.
            assert_eq!(
                r.wake_emit_instant(),
                None,
                "the LOSS-emit clock must NOT advance on a degraded wake emit"
            );
            // Self-spacing on the degraded clock: nothing inside the
            // 10-minute window repeats.
            r.on_wake_deadline(at(t0, 400));
            r.on_wake_deadline(at(t0, 899));
        });
        assert_eq!(
            wake.len(),
            1,
            "exactly one degraded wake line per cadence window: {wake:?}"
        );
        assert!(
            wake[0]
                .message
                .contains("primary keepalive not reaching this observer"),
            "the wake line carries the honest addressing-gap wording: {wake:?}"
        );
        assert!(
            !wake[0].message.contains("has been down for"),
            "the wake line must never claim a connection outage: {wake:?}"
        );
        assert!(!note.is_pending(), "no reconnection note — nothing was lost");
    }

    /// A LOGGED loss that transitions into the degraded state (frames
    /// started arriving again, keepalives still missing) ENDS the outage
    /// exactly like a regain: the [`EndedOutage`] signal fires, the
    /// reconnection note is parked, and the loss clocks clear — the data
    /// plane IS the connection.
    #[test]
    fn logged_loss_ending_in_degraded_parks_note_and_signals_ended_outage() {
        let t0 = Instant::now();
        let note = WakeNoteSlot::default();
        let mut r = LostVisibilityReporter::new(note.clone());
        r.observe(&lost("fleet empty"), t0);
        r.on_wake_deadline(at(t0, 300)); // loss logged on the wake stream
        let outcome = r.observe(&degraded("keepalive gap"), at(t0, 400));
        assert!(
            outcome.ended_logged_outage.is_some(),
            "frames arriving again ends the logged outage"
        );
        assert!(note.is_pending(), "the reconnection note is parked");
        assert!(!r.is_lost());
        assert!(r.is_degraded());
    }

    /// The reverse transition: a degraded episode that collapses into a
    /// genuine loss (the data plane went quiet too) starts the loss
    /// clocks FRESH — the connection-down age counts from the
    /// transition, and the degraded cadence state is over.
    #[test]
    fn degraded_collapsing_into_loss_starts_a_fresh_loss_episode() {
        let t0 = Instant::now();
        let mut r = LostVisibilityReporter::new(WakeNoteSlot::default());
        r.observe(&degraded("keepalive gap"), t0);
        let d = r.observe(&lost("fleet empty"), at(t0, 100)).directive;
        assert_eq!(
            d,
            RetryDirective::ReconnectDue,
            "the first lost loop after a degraded episode re-fires the rebuild"
        );
        assert!(r.is_lost());
        assert!(!r.is_degraded(), "the degraded episode is over");
        assert_eq!(
            r.wake_deadline(),
            Some(at(t0, 100) + WAKE_LOSS_THRESHOLD),
            "the loss wake threshold counts from the TRANSITION instant, \
             not from the degraded episode's start"
        );
    }
}
