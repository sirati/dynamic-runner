//! `PullCoordinator` — the single-flight, load-balanced, probe-first
//! anti-entropy PULL state machine.
//!
//! # The ONE concern
//!
//! The DISCIPLINED PULL MECHANISM: when a role detects it is behind a peer
//! (an inbound digest, or its own recovery tick, shows `is_behind`), it
//! must run AT MOST ONE probe→pull cycle at a time and pick the
//! least-loaded peer that can actually help. This module owns that entire
//! decision — the single-flight FSM (`Idle → Probing → Pulling →`
//! cooldown), the smallest-inbox-among-ahead target selection, the 1s
//! selection window (with the zero-replies-then-first-reply fallback), the
//! 30s no-answer re-probe, the 1-minute rebalance re-probe, and the
//! `FAIL → next target` fallback.
//!
//! It REPLACES the eager `anti_entropy::reconcile_against_peer` /
//! `plan_recovery_pull` immediate-pull — those fired one full-snapshot pull
//! per inbound digest with NO single-flight, so a perpetually-`is_behind`
//! replica under churn flooded the mesh with snapshot-package frames (the
//! 50 GB primary RSS leak). With this coordinator a behind replica
//! initiates pulls at the cooldown rate, never one-per-digest, and never
//! more than one in flight.
//!
//! # The boundary (what the role sees) — design-first
//!
//! - WHICH MODULE OWNS THIS CONCERN: this one. The FSM + selection + timers
//!   live here ONCE; primary / secondary / observer re-implement none of
//!   it (exactly the `anti_entropy` / `snapshot_stream` precedent).
//! - API SURFACE crossing the boundary: a role drives the coordinator with
//!   four pure methods — [`PullCoordinator::note_behind] (idempotent
//!   trigger; a NoOp while already Probing/Pulling = single-flight),
//!   [`PullCoordinator::on_probe_reply`], [`PullCoordinator::on_fail`],
//!   and [`PullCoordinator::tick`] — each taking `now: Instant` and
//!   returning a [`PullDirective`] vocabulary the role TRANSLATES into its
//!   own `send_to` edge. It also exposes [`PullCoordinator::wake_deadline`]
//!   so the loop's persistent-deadline arm parks correctly. The role also
//!   answers an inbound probe with [`probe_reply_for`] (a free fn — the
//!   responder side carries no FSM).
//! - WHAT CALLERS KNOW about the internals: NOTHING. They never see the
//!   `State` enum, the candidate list, or the window math. A directive is
//!   `Probe` (broadcast my digest) or `PullFrom { target }` (request a
//!   snapshot stream from this peer); the role owns building the actual
//!   wire frames (the digest it folds, the stream cursor it tracks in its
//!   `InboundSnapshotStreams`, the role-typed `Destination`) — so the
//!   coordinator stays free of frame construction AND of the snapshot-RPC
//!   bookkeeping, the same clean split `anti_entropy` keeps between the
//!   pure decision and the role's `send_to`.
//!
//! The coordinator holds NO clock of its own and NO `tokio` dependency: it
//! is driven by an injected `now: Instant`, so it is fully unit-testable
//! without a runtime, and the role's loop owns the single persistent-
//! deadline arm that calls [`PullCoordinator::tick`] (never a per-iteration
//! `sleep` that a sibling arm would reset — the watchdog-fires-under-load
//! law).

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, RangeDigest, StateDigest,
};

use crate::anti_entropy::role_destination;
use crate::snapshot_stream::InboundSnapshotStreams;

// ── Pull cadence constants (the owner's protocol) ──
//
// 1s selection window, 30s no-answer re-probe, 60s rebalance. These are the
// production values AND the test values: the in-crate integration tests
// (the multi-node relocate/relocation-handoff loops) drive a real-time heal
// and converge well within their seconds-scale budgets with the 1s window;
// the unit tests reference these constants SYMBOLICALLY (never hardcoded),
// so they exercise the exact production cadence. Shrinking them under
// `cfg(test)` was REJECTED: a sub-second window makes the pull arm fire
// every few ms while a freshly-joining node is `is_behind` everyone, and
// that probe churn starves the setup handshake of co-scheduled multi-node
// test clusters — the calm 1s/30s/60s cadence is exactly what keeps the
// pull path quiet during bring-up.

/// The 1-second selection window: after emitting a probe, the coordinator
/// collects [`PullProbeReply`](dynrunner_protocol_primary_secondary::DistributedMessage::PullProbeReply)s
/// for this long, then picks the smallest-inbox AHEAD responder. If the
/// window elapses with ZERO replies, the FIRST reply that subsequently
/// arrives is chosen (the protocol's first-answer fallback) — so a fleet
/// that is slow to answer is not stuck probe-only forever.
pub const SELECTION_WINDOW: Duration = Duration::from_secs(1);

/// No-answer re-probe: if a probe has been outstanding this long with no
/// usable (ahead) reply and the coordinator has not entered `Pulling`, it
/// re-broadcasts a fresh probe. Covers a probe whose every reply was
/// `ahead == false` (no one can help right now) and a probe that reached a
/// momentarily-empty mesh — the next probe re-evaluates against a
/// (hopefully) recovered fleet.
pub const REPROBE_AFTER: Duration = Duration::from_secs(30);

/// Rebalance re-probe: after pulling from ONE source for this long, the
/// coordinator re-broadcasts a probe so the source MAY change (the peer it
/// chose a minute ago may now be the most loaded). Keeps the load-balancing
/// honest over a long transfer / a long divergence.
pub const REBALANCE_AFTER: Duration = Duration::from_secs(60);

/// A reply collected during the selection window (or the first reply after
/// it). Only `ahead` responders are retained as candidates (Decision A: the
/// ahead-filter — never burn a cycle pulling from a peer that cannot help);
/// among them the SMALLEST `inbox_size` wins.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    responder_id: String,
    /// The responder's declared role (carried so the role-typed pull
    /// `Destination` is `Observer(id)` for an observer responder,
    /// `Secondary(id)` otherwise — the same role-addressing the eager pull
    /// used). The probe reply does not carry the responder's role bit
    /// explicitly; the coordinator learns it from the reply's `sender_id`
    /// against the role hint the caller passes (see [`ProbeReply`]).
    responder_is_observer: bool,
    inbox_size: u64,
    /// The responder's piggybacked task-ledger [`RangeDigest`] (P1). Carried
    /// per-candidate so that when THIS candidate is committed as the pull
    /// target — at window-end, via the first-answer fallback, OR via the
    /// `on_fail` fall-to-next path — the role can compute the divergent
    /// range-set against its OWN range digest and stream only those buckets.
    /// The coordinator never reads its internals (it is pure data threaded
    /// to the role through the `PullFrom` directive); the FSM stays free of
    /// the fold concern.
    ///
    /// `Box`ed: a `RangeDigest` is `RANGE_COUNT × (u64 + u32)` ≈ 3 KiB, so
    /// inlining it would bloat every `Candidate` (and the `State` enum that
    /// holds one as the `Pulling` target) — boxing keeps the FSM states
    /// pointer-sized while the digest lives on the heap until the role reads
    /// it once at pull time.
    range_digest: Box<RangeDigest>,
}

/// One probe reply, as the role hands it to [`PullCoordinator::on_probe_reply`]
/// after decoding a `PullProbeReply` frame. Pure data — no FSM.
pub struct ProbeReply<'a> {
    /// The responding peer's id (the reply frame's `sender_id`).
    pub responder_id: &'a str,
    /// The responder's declared role (the role hint the caller resolves
    /// from its membership / role view, so the pull is typed correctly).
    pub responder_is_observer: bool,
    /// The responder's reported inbox depth.
    pub inbox_size: u64,
    /// Whether the responder is ahead of the requester (the reply's `ahead`
    /// bit). Non-`ahead` replies are dropped (never a pull candidate).
    pub ahead: bool,
    /// The responder's piggybacked task-ledger [`RangeDigest`] (P1 Decision
    /// A). Retained on the candidate so the eventual pull to this responder
    /// can be narrowed to the divergent buckets. A pre-field responder's
    /// reply carries the all-zero default, which yields no narrowing and
    /// falls back to the all-ranges full stream (the data-loss fail-safe).
    /// `Box`ed end-to-end (the wire variant boxes it too) to keep the ~3 KiB
    /// digest off the by-value stack-move paths; the role hands the boxed
    /// value straight off the decoded `PullProbeReply` frame.
    pub range_digest: Box<RangeDigest>,
}

/// What the coordinator asks the role to do next. The role TRANSLATES each
/// into its own `send_to` edge; the coordinator never builds a wire frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullDirective {
    /// Broadcast a `PullProbe` carrying the local digest to the DIRECT mesh
    /// neighbours (`Destination::All`, which the ingress never relays). The
    /// role folds its own current digest onto the frame.
    Probe,
    /// Send a `RequestSnapshotStream` to this peer (resume-from-last-good).
    /// The role mints the stream id + resume cursor from its
    /// `InboundSnapshotStreams` tracker and types the `Destination` off
    /// `target_is_observer`. The `target_range_digest` is the chosen
    /// responder's piggybacked [`RangeDigest`]: the role compares it to its
    /// OWN range digest (`ClusterState::tasks_range_digest`) to compute the
    /// divergent buckets it stamps on the request's `task_ranges` (P1), so
    /// only the divergent ranges stream. The coordinator threads it through
    /// without inspecting it (the fold/compare is a `cluster_state`
    /// concern); the role owns the comparison + frame stamp.
    PullFrom {
        target_id: String,
        target_is_observer: bool,
        /// `Box`ed for the same reason as `Candidate::range_digest`: a
        /// ~3 KiB `RangeDigest` inlined here would make every `PullDirective`
        /// (returned by value from `note_behind`/`tick`/`on_*`) carry it on
        /// the stack; boxing keeps the directive small.
        target_range_digest: Box<RangeDigest>,
    },
}

/// The single-flight state. Never observed by a caller (private) — the
/// public surface is the directive vocabulary + `wake_deadline`.
#[derive(Debug)]
enum State {
    /// No pull in flight. `note_behind` here starts a probe.
    Idle,
    /// A probe is outstanding. `since` stamps the broadcast; the selection
    /// window ends at `since + SELECTION_WINDOW`. `candidates` accumulates
    /// every AHEAD reply seen so far (de-duplicated by responder id — a
    /// peer that replies twice updates its inbox reading, never doubles).
    /// At the window end they are SORTED smallest-inbox-first; the head
    /// becomes the pull target and the tail becomes the `Pulling`
    /// `ordered_rest` so a `PullFail` falls to the next without a re-probe.
    /// A committed target leaves this state (→ `Pulling`), so a straggler
    /// reply that arrives post-commit lands while `Pulling` and
    /// `on_probe_reply` ignores it — no "selected" flag needed.
    ///
    /// `window_closed` is set by the window-end `tick` when the 1s window
    /// elapsed with NO candidate (still Probing, awaiting the first-answer
    /// fallback / the re-probe). It exists ONLY so [`wake_deadline`] can
    /// re-target the persistent arm from the (now-past) window-end deadline
    /// to the re-probe deadline — without it the arm would hot-loop on the
    /// elapsed window instant between 1s and 30s.
    Probing {
        since: Instant,
        candidates: Vec<Candidate>,
        window_closed: bool,
    },
    /// Pulling from `target`. `since` stamps the source selection (the
    /// 1-minute rebalance clock). `ordered` is the remaining
    /// smallest-inbox-first candidate list AFTER `target`, so a `PullFail`
    /// falls to the next one without a re-probe. `current_stream` is the
    /// in-flight pull's stream id, set by the role when it issues the pull
    /// (so a `PullFail` for a STALE stream is ignored).
    Pulling {
        target: Candidate,
        since: Instant,
        ordered_rest: Vec<Candidate>,
        current_stream: Option<String>,
    },
}

/// The single-flight, probe-first, load-balanced anti-entropy pull driver.
/// One per role coordinator (role-agnostic — primary, secondary, observer
/// each hold one). See the module doc for the boundary contract.
pub struct PullCoordinator {
    node_id: String,
    state: State,
}

impl PullCoordinator {
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            state: State::Idle,
        }
    }

    /// The requester id stamped on this coordinator's probes / pulls (the
    /// node's own id). Exposed so the role's frame builders read it once.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// IDEMPOTENT trigger: the role detected (via an inbound digest or its
    /// recovery tick) that the local replica is behind some peer. Starts a
    /// fresh probe cycle iff Idle; a NoOp while already Probing or Pulling
    /// — this collapse to a SINGLE in-flight cycle is what kills the
    /// one-pull-per-digest storm. Returns `Some(PullDirective::Probe)` on
    /// the cold (Idle) trigger, `None` otherwise.
    pub fn note_behind(&mut self, now: Instant) -> Option<PullDirective> {
        match self.state {
            State::Idle => {
                self.state = State::Probing {
                    since: now,
                    candidates: Vec::new(),
                    window_closed: false,
                };
                Some(PullDirective::Probe)
            }
            // Already Probing or Pulling — single-flight: ignore.
            _ => None,
        }
    }

    /// Record one probe reply. Returns `Some(PullFrom)` when this reply
    /// resolves a target (the zero-replies-then-first-reply fallback: the
    /// window already elapsed, so the FIRST usable reply is chosen
    /// immediately); otherwise `None` — the reply is folded into the
    /// smallest-inbox running best and the regular `tick` resolves the
    /// window. A non-`ahead` reply is dropped (never a candidate). A reply
    /// arriving while Idle/Pulling (a straggler from an old probe) is
    /// ignored.
    pub fn on_probe_reply(&mut self, now: Instant, reply: &ProbeReply<'_>) -> Option<PullDirective> {
        if !reply.ahead {
            return None;
        }
        let State::Probing {
            since, candidates, ..
        } = &mut self.state
        else {
            return None;
        };
        // De-dup by responder id (a peer that replies twice updates its
        // reading rather than appearing twice in the sorted list).
        if let Some(existing) = candidates
            .iter_mut()
            .find(|c| c.responder_id == reply.responder_id)
        {
            existing.inbox_size = reply.inbox_size;
            existing.responder_is_observer = reply.responder_is_observer;
            // Write through the existing box (no fresh allocation on a
            // re-reply update).
            *existing.range_digest = (*reply.range_digest).clone();
        } else {
            candidates.push(Candidate {
                responder_id: reply.responder_id.to_string(),
                responder_is_observer: reply.responder_is_observer,
                inbox_size: reply.inbox_size,
                range_digest: reply.range_digest.clone(),
            });
        }
        // First-answer fallback: if the 1s window has ALREADY elapsed when
        // this (first usable) reply lands, commit it NOW rather than wait
        // for another tick — the protocol's "0 replies in the window ⇒ the
        // first subsequent reply is chosen". When the window has NOT
        // elapsed we keep collecting; `tick` resolves at the window end.
        if now.duration_since(*since) >= SELECTION_WINDOW {
            return self.commit_best(now);
        }
        None
    }

    /// A pull TARGET sent a `PullFail` for `stream_id`: its direct link to
    /// us dropped, so it could not serve our `RequestSnapshotStream`. Drop
    /// the dead target and fall to the NEXT candidate in the
    /// smallest-inbox-ordered list (the FAIL→next-target fallback). Returns
    /// `Some(PullFrom)` for the next target, or `None` when the list is
    /// exhausted — in which case the coordinator returns to Idle and the
    /// next `note_behind` (the role keeps detecting divergence) re-probes.
    /// A fail for a STALE stream (not the one in flight) is ignored.
    pub fn on_fail(&mut self, now: Instant, stream_id: &str) -> Option<PullDirective> {
        let State::Pulling {
            current_stream,
            ordered_rest,
            ..
        } = &mut self.state
        else {
            return None;
        };
        // Only the in-flight stream's fail advances the target; a fail for
        // an abandoned stream is a stale echo.
        if current_stream.as_deref() != Some(stream_id) {
            return None;
        }
        if let Some(next) = pop_front(ordered_rest) {
            let rest = std::mem::take(ordered_rest);
            let directive = PullDirective::PullFrom {
                target_id: next.responder_id.clone(),
                target_is_observer: next.responder_is_observer,
                target_range_digest: next.range_digest.clone(),
            };
            self.state = State::Pulling {
                target: next,
                since: now,
                ordered_rest: rest,
                current_stream: None,
            };
            Some(directive)
        } else {
            // No fallback target left — quiesce. The role's next
            // divergence detection re-probes from scratch.
            self.state = State::Idle;
            None
        }
    }

    /// Record the stream id the role minted for the in-flight pull, so a
    /// later `PullFail` can be matched to exactly this attempt (and a stale
    /// fail ignored). Called by the role right after it issues the
    /// `PullFrom` directive's `RequestSnapshotStream`. A NoOp unless
    /// Pulling.
    pub fn note_pull_stream(&mut self, stream_id: &str) {
        if let State::Pulling { current_stream, .. } = &mut self.state {
            *current_stream = Some(stream_id.to_string());
        }
    }

    /// The in-flight pull's stream DELIVERED its terminal (`done`) package:
    /// the transfer is over (whether or not every package decoded cleanly —
    /// a WARN-dropped package leaves the replica still behind, which the
    /// role's own re-detection handles). Return the coordinator to Idle so
    /// the cycle ends: a node that is now CONVERGED stays silent
    /// (`note_behind` is a NoOp on a non-behind digest), and a node still
    /// behind (a dropped package) re-probes on its very next divergence
    /// detection — NOT after the 60s rebalance. Without this an in-flight
    /// pull would pin the FSM in `Pulling` until the rebalance timer, so a
    /// converged node would never go quiescent and a malformed-package node
    /// would wait a full minute to retry. Matches only the in-flight stream
    /// id (a stale `done` echo is ignored); a NoOp unless Pulling.
    pub fn on_pull_done(&mut self, stream_id: &str) {
        if let State::Pulling { current_stream, .. } = &self.state
            && current_stream.as_deref() == Some(stream_id)
        {
            self.state = State::Idle;
        }
    }

    /// Drive the coordinator's timers against `now`. Returns the directives
    /// due this tick:
    /// - In Probing: if the 1s selection window has elapsed and a candidate
    ///   was found, commit it (→ `PullFrom`). If the window elapsed with no
    ///   candidate, do nothing yet (await the first-answer fallback in
    ///   `on_probe_reply`), but if the probe has been outstanding past
    ///   `REPROBE_AFTER` with still no candidate, re-broadcast (→ `Probe`).
    /// - In Pulling: if we have been on one source past `REBALANCE_AFTER`,
    ///   re-broadcast a probe so the source may change (→ `Probe`), folding
    ///   back to Probing.
    ///
    /// The role calls this from ONE persistent-deadline `select!` arm
    /// (parked on [`Self::wake_deadline`]); it never resets the deadline
    /// from another arm, so the timers fire under constant loop activity.
    pub fn tick(&mut self, now: Instant) -> Vec<PullDirective> {
        match &mut self.state {
            State::Idle => Vec::new(),
            State::Probing { since, .. } => {
                if now.duration_since(*since) >= SELECTION_WINDOW {
                    // Window over: commit the best candidate if we have one
                    // (→ Pulling). Otherwise mark the window closed so the
                    // wake deadline re-targets to the re-probe instant
                    // instead of hot-looping on the past window-end instant.
                    if let Some(d) = self.commit_best(now) {
                        return vec![d];
                    }
                    if let State::Probing { window_closed, .. } = &mut self.state {
                        *window_closed = true;
                    }
                }
                // Still Probing (no candidate committed). Re-probe once the
                // probe has been outstanding too long with no usable answer.
                if let State::Probing { since, .. } = &self.state
                    && now.duration_since(*since) >= REPROBE_AFTER
                {
                    self.state = State::Probing {
                        since: now,
                        candidates: Vec::new(),
                        window_closed: false,
                    };
                    return vec![PullDirective::Probe];
                }
                Vec::new()
            }
            State::Pulling { since, target, .. } => {
                if now.duration_since(*since) >= REBALANCE_AFTER {
                    // Rebalance: re-probe; the source may change. Fold back
                    // to a fresh Probing cycle.
                    tracing::debug!(
                        node = %self.node_id,
                        current_source = %target.responder_id,
                        "pull-coordinator rebalance: re-probing after \
                         {}s on one source (the source may change)",
                        REBALANCE_AFTER.as_secs(),
                    );
                    self.state = State::Probing {
                        since: now,
                        candidates: Vec::new(),
                        window_closed: false,
                    };
                    return vec![PullDirective::Probe];
                }
                Vec::new()
            }
        }
    }

    /// The next instant [`Self::tick`] must be driven at, or `None` when no
    /// timer is armed (Idle). The role's loop parks its single
    /// pull-coordinator arm on this PERSISTENT deadline (a `sleep_until`,
    /// not a relative `sleep`), so it fires under constant sibling-arm
    /// activity.
    pub fn wake_deadline(&self) -> Option<Instant> {
        match &self.state {
            State::Idle => None,
            State::Probing {
                since,
                window_closed,
                ..
            } => {
                if *window_closed {
                    // The 1s window already elapsed with no candidate; the
                    // next due event is the re-probe. Targeting the past
                    // window-end instant here would hot-loop the arm.
                    Some(*since + REPROBE_AFTER)
                } else {
                    // Window not yet resolved: the window end gates the next
                    // wake (always sooner than the re-probe).
                    Some(*since + SELECTION_WINDOW)
                }
            }
            State::Pulling { since, .. } => Some(*since + REBALANCE_AFTER),
        }
    }

    /// Commit the SMALLEST-INBOX ahead candidate as the pull target,
    /// transition Probing → Pulling (stamping the rebalance clock at `now`,
    /// the commit instant), and return the `PullFrom` directive. The
    /// remaining candidates — sorted smallest-inbox-first AFTER the target —
    /// become the `ordered_rest` fallback list, so a `PullFail` falls to the
    /// next without a re-probe (the FAIL→next-target protocol step). `None`
    /// when there is no candidate (no ahead responder) — the coordinator
    /// stays Probing so the first-answer fallback / re-probe still apply.
    fn commit_best(&mut self, now: Instant) -> Option<PullDirective> {
        let State::Probing { candidates, .. } = &mut self.state else {
            return None;
        };
        if candidates.is_empty() {
            // No ahead candidate — keep Probing; first-answer fallback or
            // re-probe handles it.
            return None;
        }
        let mut ordered = std::mem::take(candidates);
        // Smallest-inbox-first; ties broken by responder id for a stable,
        // reproducible order (no `HashMap`-iteration nondeterminism leaks
        // into target selection).
        ordered.sort_by(|a, b| {
            a.inbox_size
                .cmp(&b.inbox_size)
                .then_with(|| a.responder_id.cmp(&b.responder_id))
        });
        let target = ordered.remove(0);
        let directive = PullDirective::PullFrom {
            target_id: target.responder_id.clone(),
            target_is_observer: target.responder_is_observer,
            target_range_digest: target.range_digest.clone(),
        };
        // The rebalance clock starts at the COMMIT instant (`now`), not the
        // probe broadcast — the 1-minute source-stickiness is measured from
        // when this source actually started serving. The tail is the
        // smallest-inbox-ordered fallback list for `on_fail`.
        self.state = State::Pulling {
            target,
            since: now,
            ordered_rest: ordered,
            current_stream: None,
        };
        Some(directive)
    }

    /// Test introspection: the current state discriminant as a `&str`
    /// (`"idle"`/`"probing"`/`"pulling"`), so the unit tests assert the FSM
    /// without exposing the private `State` enum.
    #[cfg(test)]
    pub(crate) fn state_name(&self) -> &'static str {
        match self.state {
            State::Idle => "idle",
            State::Probing { .. } => "probing",
            State::Pulling { .. } => "pulling",
        }
    }

    /// Test introspection: the current pull target id when Pulling.
    #[cfg(test)]
    pub(crate) fn pull_target(&self) -> Option<&str> {
        match &self.state {
            State::Pulling { target, .. } => Some(&target.responder_id),
            _ => None,
        }
    }
}

/// Pop the first element of a Vec (front), shifting the rest down. Small —
/// the candidate list is at most the direct-neighbour count.
fn pop_front<T>(v: &mut Vec<T>) -> Option<T> {
    if v.is_empty() {
        None
    } else {
        Some(v.remove(0))
    }
}

/// The RESPONDER side (no FSM): given the local digest and a probe's
/// carried `requester_digest`, compute the `ahead` bit a `PullProbeReply`
/// reports — `true` iff the LOCAL replica holds ledger data the requester
/// lacks, i.e. `requester_digest.is_behind(local_digest)`. This is the
/// cheap correctness filter (Decision A) computed responder-side from the
/// digest the probe carried, so the requester need not hold every peer's
/// digest. Pure function — the role pairs it with its own inbox depth +
/// `send_to` edge to build the reply.
pub fn probe_reply_ahead(local_digest: &StateDigest, requester_digest: &StateDigest) -> bool {
    requester_digest.is_behind(local_digest)
}

// ─── Pull-model frame construction (the WIRE side of the one concern) ───
//
// These free functions are the frame-construction half of the disciplined
// pull: they turn a [`PullDirective`] (or an inbound probe) into a typed
// wire frame + its role-bearing [`Destination`], typed through the SAME
// `anti_entropy::role_destination` core every other snapshot-RPC edge uses
// (no re-implemented addressing). They live HERE (not on the FSM struct) so
// the `PullCoordinator` stays pure policy (no `DistributedMessage`
// dependency) while the wire shape stays inside the ONE pull module. The
// role supplies its identity + digest + `InboundSnapshotStreams` cursor +
// `send_to` edge; it re-implements none of the frame shape.

/// Build the `PullProbe` broadcast a [`PullDirective::Probe`] asks for: the
/// requester's id + current digest, addressed to [`Destination::All`] (the
/// ingress local-fans an inbound `All` and never relays it, so this reaches
/// only the DIRECT neighbours — the protocol's direct-neighbours-only
/// broadcast).
pub fn pull_probe<I>(node_id: &str, timestamp: f64, digest: StateDigest) -> DistributedMessage<I> {
    DistributedMessage::PullProbe {
        target: None,
        sender_id: node_id.to_string(),
        timestamp,
        digest,
    }
}

/// Build the `PullProbeReply` a node sends back to a probe's `requester`:
/// its own inbox depth + the responder-side `ahead` bit + the responder's
/// task-ledger [`RangeDigest`] (P1 Decision A — piggybacked so the requester
/// computes the divergent buckets with no extra round-trip). Typed DIRECTLY
/// at the requester (`Secondary(id)`/`Observer(id)`) — a reply that cannot
/// reach the requester directly is simply lost (the requester's 30s
/// re-probe recovers), so it is never relayed. The role folds its own
/// `cluster_state.tasks_range_digest()` and passes it here; this function
/// only frames it (the fold is a `cluster_state` concern).
pub fn pull_probe_reply<I>(
    node_id: &str,
    timestamp: f64,
    requester: &str,
    requester_is_observer: bool,
    inbox_size: u64,
    ahead: bool,
    range_digest: Box<RangeDigest>,
) -> (Destination, DistributedMessage<I>) {
    let dst = role_destination(requester, requester_is_observer);
    let frame = DistributedMessage::PullProbeReply {
        target: None,
        sender_id: node_id.to_string(),
        timestamp,
        requester: requester.to_string(),
        inbox_size,
        ahead,
        range_digest,
    };
    (dst, frame)
}

/// Build the `RequestSnapshotStream` a [`PullDirective::PullFrom`] asks for,
/// RESUMING the interrupted stream toward `target` (same stream id +
/// `resume_after` cursor) via the requester's `streams` tracker — NOT a
/// fresh from-package-0 transfer. Returns the role-typed pull destination,
/// the request frame, and the minted `stream_id` so the role can hand the
/// id back to [`PullCoordinator::note_pull_stream`] (matching a later
/// `PullFail` to exactly this attempt). This is the SAME
/// `RequestSnapshotStream`/`request_params` path the eager
/// `anti_entropy::reconcile_against_peer` used — the chunk transfer is
/// unchanged; only the TRIGGER (single-flight, probe-selected) differs.
// A frame-builder with an inherently wide, FLAT parameter surface (it
// mirrors the `RequestSnapshotStream` wire frame's fields one-to-one);
// grouping them into a struct would be artificial ceremony that hides the
// 1:1 frame mapping. The `task_ranges` slice is the role-computed divergent
// bucket set (see [`divergent_ranges_for_pull`]).
#[allow(clippy::too_many_arguments)]
pub fn pull_request<I>(
    requester_id: &str,
    requester_is_observer: bool,
    requester_can_be_primary: bool,
    target_id: &str,
    target_is_observer: bool,
    task_ranges: Vec<u16>,
    streams: &mut InboundSnapshotStreams,
    timestamp: f64,
) -> (Destination, DistributedMessage<I>, String) {
    let dst = role_destination(target_id, target_is_observer);
    let (stream_id, resume_after) = streams.request_params(target_id);
    // P1 range-scoped delta: `task_ranges` is the set of buckets in which the
    // chosen responder holds task data this requester lacks (the per-bucket
    // image of `StateDigest::is_behind`'s task rule), computed by the role
    // via [`divergent_ranges_for_pull`]. The responder's
    // `SnapshotStreamPlan` filters its keys to these buckets, so a one-task
    // change re-pulls ~one bucket. An EMPTY set (converged, OR a legacy
    // responder's all-zero digest) means ALL ranges — the P0 full stream,
    // the data-loss fail-safe: a missing delta NEVER drops a divergent
    // range, it only forgoes the narrowing.
    let frame = DistributedMessage::RequestSnapshotStream {
        target: None,
        sender_id: requester_id.to_string(),
        timestamp,
        stream_id: stream_id.clone(),
        resume_after,
        task_ranges,
        is_observer: requester_is_observer,
        can_be_primary: requester_can_be_primary,
    };
    (dst, frame, stream_id)
}

/// The divergent bucket set for a pull: the buckets in which the chosen
/// `target` responder holds task data the `requester` lacks (the per-bucket
/// image of `StateDigest::is_behind`'s task rule). The role computes this
/// from its own range digest + the responder's piggybacked one, then hands
/// the slice to [`pull_request`]. A thin re-export of
/// [`RangeDigest::divergent_ranges`] so the role names ONE pull-model
/// vocabulary (it never reaches into the wire type directly).
pub fn divergent_ranges_for_pull(
    requester_range_digest: &RangeDigest,
    target_range_digest: &RangeDigest,
) -> Vec<u16> {
    requester_range_digest.divergent_ranges(target_range_digest)
}

/// Build the `PullFail` a pull responder sends when it could not serve a
/// `RequestSnapshotStream` because its DIRECT link to the requester dropped.
/// Typed `Secondary(requester)`/`Observer(requester)`; the ingress
/// role-miss relay forwards it toward the requester's recognized holder when
/// the direct leg is gone (the indirect-delivery contract this frame exists
/// for — the one pull-model frame that IS relayed).
pub fn pull_fail<I>(
    node_id: &str,
    timestamp: f64,
    requester: &str,
    requester_is_observer: bool,
    stream_id: &str,
) -> (Destination, DistributedMessage<I>) {
    let dst = role_destination(requester, requester_is_observer);
    let frame = DistributedMessage::PullFail {
        target: None,
        sender_id: node_id.to_string(),
        timestamp,
        requester: requester.to_string(),
        stream_id: stream_id.to_string(),
    };
    (dst, frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    fn reply<'a>(id: &'a str, inbox: u64, ahead: bool) -> ProbeReply<'a> {
        ProbeReply {
            responder_id: id,
            responder_is_observer: false,
            inbox_size: inbox,
            ahead,
            // The pull-coordinator FSM threads the range digest opaquely; its
            // CONTENT is exercised by the cluster_state range_digest tests +
            // the differential delta≡full test, not the FSM unit tests, so
            // the default (all-zero) digest suffices here.
            range_digest: Box::new(RangeDigest::default()),
        }
    }

    /// A `PullFrom` directive for `target` carrying the default range digest
    /// — the FSM tests assert target SELECTION, not the range-set content
    /// (which the cluster_state tests own), so they compare against the
    /// default-digest directive.
    fn pull_from(target: &str) -> PullDirective {
        PullDirective::PullFrom {
            target_id: target.to_string(),
            target_is_observer: false,
            target_range_digest: Box::new(RangeDigest::default()),
        }
    }

    /// SINGLE-FLIGHT: concurrent `note_behind` triggers collapse into ONE
    /// probe cycle. The first emits a `Probe`; every subsequent trigger
    /// while Probing/Pulling is a NoOp — the storm-killer invariant.
    #[test]
    fn note_behind_is_idempotent_single_flight() {
        let mut pc = PullCoordinator::new("me");
        let now = t0();
        assert_eq!(pc.note_behind(now), Some(PullDirective::Probe));
        assert_eq!(pc.state_name(), "probing");
        // Ten more triggers within the window — all NoOps (single-flight).
        for i in 1..=10 {
            assert_eq!(
                pc.note_behind(now + Duration::from_millis(i * 10)),
                None,
                "a note_behind while Probing must NOT start a second cycle"
            );
        }
        assert_eq!(pc.state_name(), "probing");
    }

    /// SMALLEST-INBOX-AMONG-AHEAD selection: three ahead replies + one
    /// not-ahead; after the window the smallest-inbox AHEAD responder is the
    /// pull target, and the not-ahead (even with the smallest inbox) is
    /// never chosen.
    #[test]
    fn selects_smallest_inbox_among_ahead() {
        let mut pc = PullCoordinator::new("me");
        let start = t0();
        pc.note_behind(start);
        // Not-ahead with the smallest inbox of all — must be filtered out.
        assert_eq!(pc.on_probe_reply(start, &reply("low-but-behind", 1, false)), None);
        // Ahead candidates: inbox 9, 4, 7. Smallest ahead = "b" (4).
        assert_eq!(pc.on_probe_reply(start, &reply("a", 9, true)), None);
        assert_eq!(pc.on_probe_reply(start, &reply("b", 4, true)), None);
        assert_eq!(pc.on_probe_reply(start, &reply("c", 7, true)), None);
        assert_eq!(pc.state_name(), "probing");
        // Window elapses → tick commits the smallest-inbox ahead target.
        let directives = pc.tick(start + SELECTION_WINDOW);
        assert_eq!(directives, vec![pull_from("b")]);
        assert_eq!(pc.state_name(), "pulling");
        assert_eq!(pc.pull_target(), Some("b"));
    }

    /// AHEAD-FILTER: if EVERY reply is not-ahead, no pull is committed (we
    /// are caught up, or the others are behind too); the coordinator stays
    /// Probing and re-probes after the no-answer deadline.
    #[test]
    fn no_ahead_reply_commits_no_pull() {
        let mut pc = PullCoordinator::new("me");
        let start = t0();
        pc.note_behind(start);
        pc.on_probe_reply(start, &reply("a", 1, false));
        pc.on_probe_reply(start, &reply("b", 2, false));
        // Window elapses with no ahead candidate → still Probing.
        assert!(pc.tick(start + SELECTION_WINDOW).is_empty());
        assert_eq!(pc.state_name(), "probing");
        // Past the re-probe deadline → a fresh Probe.
        let d = pc.tick(start + REPROBE_AFTER);
        assert_eq!(d, vec![PullDirective::Probe]);
        assert_eq!(pc.state_name(), "probing");
    }

    /// FIRST-ANSWER FALLBACK: zero replies arrive within the 1s window;
    /// the FIRST reply that lands AFTER the window is chosen immediately
    /// (not held for another tick).
    #[test]
    fn first_reply_after_empty_window_is_chosen() {
        let mut pc = PullCoordinator::new("me");
        let start = t0();
        pc.note_behind(start);
        // No replies during the window.
        assert!(pc.tick(start + SELECTION_WINDOW).is_empty());
        assert_eq!(pc.state_name(), "probing");
        // First reply AFTER the window → committed on arrival.
        let after = start + SELECTION_WINDOW + Duration::from_millis(250);
        let d = pc.on_probe_reply(after, &reply("late", 5, true));
        assert_eq!(d, Some(pull_from("late")));
        assert_eq!(pc.pull_target(), Some("late"));
    }

    /// A straggler ahead reply that arrives AFTER a target was committed
    /// within the window cannot retro-change the choice (no re-selection
    /// mid-pull).
    #[test]
    fn straggler_after_commit_is_ignored() {
        let mut pc = PullCoordinator::new("me");
        let start = t0();
        pc.note_behind(start);
        pc.on_probe_reply(start, &reply("chosen", 3, true));
        pc.tick(start + SELECTION_WINDOW); // commits "chosen"
        assert_eq!(pc.pull_target(), Some("chosen"));
        // A smaller-inbox straggler after commit — ignored.
        let d = pc.on_probe_reply(
            start + SELECTION_WINDOW + Duration::from_millis(10),
            &reply("smaller", 0, true),
        );
        assert_eq!(d, None);
        assert_eq!(pc.pull_target(), Some("chosen"));
    }

    /// FAIL→NEXT-TARGET: with multiple ahead candidates, a `PullFail` for
    /// the in-flight stream falls to the NEXT smallest-inbox candidate
    /// WITHOUT a re-probe (the protocol's smallest-inbox-ordered fallback
    /// list). A fail for a STALE stream is ignored. The list exhausts to
    /// Idle, after which the next divergence re-probes.
    #[test]
    fn fail_falls_to_next_smallest_inbox_target() {
        let mut pc = PullCoordinator::new("me");
        let start = t0();
        pc.note_behind(start);
        // Three ahead candidates with inbox 2, 5, 8 → order t2, t5, t8.
        pc.on_probe_reply(start, &reply("t8", 8, true));
        pc.on_probe_reply(start, &reply("t2", 2, true));
        pc.on_probe_reply(start, &reply("t5", 5, true));
        pc.tick(start + SELECTION_WINDOW);
        assert_eq!(pc.pull_target(), Some("t2"), "smallest inbox first");
        pc.note_pull_stream("me/0");
        // A fail for a STALE stream is ignored.
        assert_eq!(pc.on_fail(start, "stale"), None);
        assert_eq!(pc.pull_target(), Some("t2"));
        // The in-flight stream's fail → fall to the next smallest (t5).
        assert_eq!(pc.on_fail(start, "me/0"), Some(pull_from("t5")));
        assert_eq!(pc.pull_target(), Some("t5"));
        pc.note_pull_stream("me/1");
        // Next fail → t8.
        assert_eq!(pc.on_fail(start, "me/1"), Some(pull_from("t8")));
        assert_eq!(pc.pull_target(), Some("t8"));
        pc.note_pull_stream("me/2");
        // List exhausted → Idle; the next divergence re-probes.
        assert_eq!(pc.on_fail(start, "me/2"), None);
        assert_eq!(pc.state_name(), "idle");
        assert_eq!(pc.note_behind(start), Some(PullDirective::Probe));
    }

    /// PULL DONE → Idle → re-probe on next divergence (not after rebalance):
    /// the in-flight stream's terminal package returns the FSM to Idle so a
    /// node still behind (a WARN-dropped package) re-probes immediately on
    /// its next `note_behind`, and a converged node stays quiescent. A
    /// `done` for a STALE stream is ignored.
    #[test]
    fn pull_done_returns_to_idle_and_allows_immediate_reprobe() {
        let mut pc = PullCoordinator::new("me");
        let start = t0();
        pc.note_behind(start);
        pc.on_probe_reply(start, &reply("src", 1, true));
        pc.tick(start + SELECTION_WINDOW);
        pc.note_pull_stream("me/0");
        assert_eq!(pc.state_name(), "pulling");
        // A `done` for a STALE stream is ignored.
        pc.on_pull_done("stale");
        assert_eq!(pc.state_name(), "pulling");
        // The in-flight stream's terminal package → Idle.
        pc.on_pull_done("me/0");
        assert_eq!(pc.state_name(), "idle");
        // Still behind (the package was WARN-dropped) → the NEXT divergence
        // re-probes immediately, NOT after the 60s rebalance.
        assert_eq!(
            pc.note_behind(start + Duration::from_millis(1)),
            Some(PullDirective::Probe)
        );
    }

    /// 1-MINUTE REBALANCE: after pulling from one source past
    /// `REBALANCE_AFTER`, the tick re-broadcasts a probe (the source may
    /// change) and folds back to Probing.
    #[test]
    fn rebalance_reprobes_after_one_minute() {
        let mut pc = PullCoordinator::new("me");
        let start = t0();
        pc.note_behind(start);
        pc.on_probe_reply(start, &reply("src", 1, true));
        pc.tick(start + SELECTION_WINDOW);
        assert_eq!(pc.state_name(), "pulling");
        // The rebalance clock starts at the COMMIT instant (start +
        // SELECTION_WINDOW), not the probe broadcast. Before that + 60s —
        // nothing.
        let commit = start + SELECTION_WINDOW;
        // Halfway through the rebalance window (symbolic — adapts to the
        // cfg(test)-scaled constant) → still Pulling, no re-probe yet.
        assert!(pc.tick(commit + REBALANCE_AFTER / 2).is_empty());
        assert_eq!(pc.state_name(), "pulling");
        // Past the rebalance deadline (commit + REBALANCE_AFTER) → re-probe.
        let d = pc.tick(commit + REBALANCE_AFTER + Duration::from_millis(1));
        assert_eq!(d, vec![PullDirective::Probe]);
        assert_eq!(pc.state_name(), "probing");
    }

    /// WAKE DEADLINE drives the persistent-deadline arm: None when Idle, the
    /// window-end when Probing, the rebalance instant when Pulling.
    #[test]
    fn wake_deadline_tracks_state() {
        let mut pc = PullCoordinator::new("me");
        assert_eq!(pc.wake_deadline(), None, "Idle arms no timer");
        let start = t0();
        pc.note_behind(start);
        assert_eq!(pc.wake_deadline(), Some(start + SELECTION_WINDOW));
        pc.on_probe_reply(start, &reply("x", 1, true));
        pc.tick(start + SELECTION_WINDOW);
        // The rebalance clock (and thus the Pulling wake deadline) is
        // measured from the COMMIT instant (start + SELECTION_WINDOW), not
        // the probe broadcast.
        let commit = start + SELECTION_WINDOW;
        assert_eq!(pc.wake_deadline(), Some(commit + REBALANCE_AFTER));
    }

    /// THE STORM REPRO: a perpetually-`is_behind` node fed a fresh
    /// `note_behind` on EVERY simulated tick (the churn that flipped the
    /// digest fold every transition) must initiate pulls bounded by the
    /// COOLDOWN, NOT one-per-trigger, and never more than one in flight.
    /// Replays the storm shape end to end on the coordinator: thousands of
    /// triggers + replies over simulated minutes, asserting the
    /// probe-initiation count is O(time / cooldown), not O(triggers).
    #[test]
    fn perpetual_behind_bounds_pull_initiation_by_cooldown() {
        let mut pc = PullCoordinator::new("me");
        let base = t0();
        let mut probes_emitted = 0usize;
        let mut pulls_issued = 0usize;
        let mut in_flight = false;
        // 10 simulated minutes at a 10ms churn cadence = 60_000 triggers —
        // the eager pull would fire ~60_000 snapshot pulls (one per
        // divergent digest). Drive note_behind EVERY step (perpetual
        // is_behind), answer each probe with one ahead reply, and run the
        // tick each step.
        let total_steps = 60_000u64;
        let step = Duration::from_millis(10);
        // One closure folds either path's directive into the counters so a
        // commit via the first-answer fallback (`on_probe_reply`) and a
        // commit via the window-end (`tick`) are both counted.
        let apply = |d: PullDirective,
                     probes: &mut usize,
                     pulls: &mut usize,
                     in_flight: &mut bool,
                     pc: &mut PullCoordinator| {
            match d {
                PullDirective::Probe => {
                    *probes += 1;
                    *in_flight = false;
                }
                PullDirective::PullFrom { .. } => {
                    *pulls += 1;
                    // At most one pull in flight at any time.
                    assert!(!*in_flight, "a second pull was issued while one was in flight");
                    *in_flight = true;
                    pc.note_pull_stream("me/stream");
                }
            }
        };
        for i in 0..total_steps {
            let now = base + step * (i as u32);
            // Perpetual divergence: the role would call note_behind on every
            // inbound digest. Single-flight must absorb all but the cold one.
            if let Some(d) = pc.note_behind(now) {
                apply(d, &mut probes_emitted, &mut pulls_issued, &mut in_flight, &mut pc);
            }
            // A direct neighbour answers the outstanding probe (ahead).
            // Feeding a reply each step models a fast fleet; only matters
            // while Probing — and may commit the pull via the first-answer
            // fallback once the window has elapsed.
            if pc.state_name() == "probing"
                && let Some(d) = pc.on_probe_reply(now, &reply("donor", 2, true))
            {
                apply(d, &mut probes_emitted, &mut pulls_issued, &mut in_flight, &mut pc);
            }
            // The role's persistent-deadline arm fires tick.
            let directives = pc.tick(now);
            for d in directives {
                apply(d, &mut probes_emitted, &mut pulls_issued, &mut in_flight, &mut pc);
            }
            // The in-flight pull "completes": the role's divergence keeps
            // firing note_behind, but single-flight + the rebalance cooldown
            // gate the next probe. We do NOT call on_fail (the happy path).
        }
        // The whole run spans `total_steps * step` of SIMULATED time. The
        // probe/pull initiations are bounded by ~elapsed/cooldown, NOT by
        // the 60_000 triggers. Computed in MILLISECONDS so the bound adapts
        // to the `cfg(test)`-scaled `REBALANCE_AFTER` (a per-second compute
        // would divide by zero at the sub-second test cadence). After each
        // pull commits, the cycle stays `Pulling` until the rebalance
        // re-probe (we never call `on_pull_done` here — the happy in-flight
        // path), so the gap between successive initiations is one full
        // rebalance window: pulls ≈ elapsed / REBALANCE_AFTER.
        let elapsed_ms = total_steps * (step.as_millis() as u64); // simulated span
        let cooldown_ms = REBALANCE_AFTER.as_millis() as u64;
        // Generous slack: +2 for the cold start + the trailing partial
        // window, ×2 headroom for the window+reprobe interplay between
        // rebalance cycles. Still DRAMATICALLY sub-linear in `total_steps`.
        let cooldown_bound = ((elapsed_ms / cooldown_ms + 2) * 2) as usize;
        assert!(
            pulls_issued <= cooldown_bound,
            "pull initiations ({pulls_issued}) must be bounded by the cooldown \
             (~{cooldown_bound} over {elapsed_ms}ms sim, cooldown {cooldown_ms}ms), \
             NOT one-per-trigger ({total_steps}); the storm-killer invariant"
        );
        assert!(
            (pulls_issued as f64) < (total_steps as f64) * 0.1,
            "the storm-killer must keep pull initiations FAR below the trigger \
             count: {pulls_issued} pulls for {total_steps} perpetual-behind triggers"
        );
        assert!(pulls_issued >= 1, "the cold trigger must have issued at least one pull");
    }
}
