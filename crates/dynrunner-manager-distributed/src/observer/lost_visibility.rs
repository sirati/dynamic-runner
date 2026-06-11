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
//!   1. A loss reaches the wake stream ONLY once it has been continuously
//!      down for [`WAKE_LOSS_THRESHOLD`] (5 minutes). A shorter blip
//!      produces NOTHING on the wake stream, ever — the full-log
//!      diagnostics above stay immediate and untouched.
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
/// stay neutral (see [`MeshLiveness`]).
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
    /// Latch: the CURRENT loss episode has been reported on the wake
    /// stream (it stayed down past [`WAKE_LOSS_THRESHOLD`]). Reset on
    /// regain. Gates both "emit the wake loss exactly once" and "only a
    /// logged loss earns a reconnection note".
    loss_logged: bool,
    /// The latest lost-reason + mesh-liveness evidence observed while
    /// down, so the threshold emit (which fires from the deadline arm, not
    /// from an `observe` call) carries the same gated wording as the
    /// full-log banner.
    last_lost_context: Option<(String, MeshLiveness)>,
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
    /// loss is emitted exactly once at the [`WAKE_LOSS_THRESHOLD`] mark
    /// (via [`Self::on_wake_deadline`], also checked here as a backstop),
    /// and a regain after a LOGGED loss parks the reconnection note +
    /// returns the [`EndedOutage`] signal — never a wake emit of its own.
    ///
    /// NEVER returns an exit signal — a lost-visibility observer keeps
    /// observing. This is the entire BUG-B contract: the observer reports,
    /// retries the tunnel rebuild, and does not reap the run. The single
    /// `due` decision drives BOTH the full-log report and the reconnect
    /// attempt, so they share one ~60s clock.
    pub fn observe(&mut self, visibility: &Visibility, now: Instant) -> ObserveOutcome {
        match visibility {
            Visibility::Visible => {
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
                    if self.loss_logged {
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
                self.loss_logged = false;
                self.last_lost_context = None;
                self.last_report = None;
                ObserveOutcome {
                    directive: RetryDirective::Idle,
                    ended_logged_outage,
                }
            }
            Visibility::Lost {
                reason,
                mesh_liveness,
            } => {
                let first = self.lost_since.is_none();
                self.lost_since.get_or_insert(now);
                // Keep the wake-threshold emit's context fresh (the
                // deadline arm fires between observes) and run the
                // threshold check as a backstop — `on_wake_deadline` is
                // latched, so the dedicated deadline arm and this backstop
                // can never double-emit.
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

    /// The wake-stream loss deadline: `Some(lost_since + 5min)` while the
    /// connection is down and the loss has not yet been logged on the wake
    /// stream; `None` otherwise. Derived from the STORED loss instant
    /// (the persistent-deadline law): the coordinator's select arm
    /// rebuilds `sleep_until` from this value every iteration, so sibling
    /// arm activity never resets the clock.
    pub fn wake_deadline(&self) -> Option<Instant> {
        match (self.lost_since, self.loss_logged) {
            (Some(since), false) => Some(since + WAKE_LOSS_THRESHOLD),
            _ => None,
        }
    }

    /// Emit the wake-stream loss event if the threshold has been reached:
    /// the connection has been continuously down for
    /// [`WAKE_LOSS_THRESHOLD`]. Latched — exactly one wake loss per loss
    /// episode, no matter how often the deadline arm or the `observe`
    /// backstop call this. The wording carries the same
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
        self.loss_logged = true;
        let since = self
            .lost_since
            .expect("wake_deadline() is Some only while lost");
        let lost_secs = now.duration_since(since).as_secs();
        let threshold_secs = WAKE_LOSS_THRESHOLD.as_secs();
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
                    "observer connection to the run has been down for ≥{threshold_secs}s — the \
                     last CRDT snapshot still shows {alive_count} live worker-secondary \
                     member(s), so the compute mesh is running autonomously over its direct \
                     links; the observer keeps retrying reconnect. The run's verdict comes from \
                     the primary, never from the observer's own view."
                );
            }
            MeshLiveness::Unknown => {
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    %reason,
                    lost_secs,
                    "observer connection to the run has been down for ≥{threshold_secs}s — \
                     compute-mesh state UNKNOWN: the observer holds NO live worker-secondary in \
                     its last CRDT snapshot, so it CANNOT confirm the mesh survived. The \
                     observer keeps retrying reconnect; the run's verdict comes from the \
                     primary, never from the observer's own view."
                );
            }
        }
        // The loss emit is a wake-stream host like any other: an unflushed
        // note from a previous outage rides it rather than waiting longer.
        self.note.flush_after_host();
    }

    /// Whether the reporter currently considers visibility lost. Test/
    /// diagnostic accessor — not part of any exit decision.
    #[cfg(test)]
    pub fn is_lost(&self) -> bool {
        self.lost_since.is_some()
    }

    /// Whether the CURRENT loss episode has been reported on the wake
    /// stream. Test/diagnostic accessor.
    #[cfg(test)]
    pub(crate) fn loss_logged(&self) -> bool {
        self.loss_logged
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
    fn loss_logged_exactly_once_at_threshold_mark() {
        // Rule 1: the loss reaches the wake stream exactly once, AT the
        // 5-minute mark — not at loss time, not repeated afterwards.
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
                "deadline derives from the STORED loss instant"
            );
            // The mark: the deadline arm fires.
            r.on_wake_deadline(at(t0, 300));
            assert!(r.loss_logged());
            assert_eq!(
                r.wake_deadline(),
                None,
                "a logged loss parks the deadline arm"
            );
            // Still down much later: the observe backstop + a stray
            // deadline call must NOT re-emit.
            r.observe(&lost("fleet empty"), at(t0, 360));
            r.on_wake_deadline(at(t0, 400));
        });
        let losses: Vec<_> = wake
            .iter()
            .filter(|e| e.message.contains("has been down for"))
            .collect();
        assert_eq!(
            losses.len(),
            1,
            "exactly one wake loss event per episode: {wake:?}"
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
    fn never_regained_loss_has_no_note_ever() {
        // Loss logged, connection never returns: the single loss event is
        // all the wake stream ever sees — no reconnection note.
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
        assert_eq!(wake.len(), 1, "one loss event, nothing more: {wake:?}");
        assert!(!note.is_pending(), "no regain — no note");
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
}
