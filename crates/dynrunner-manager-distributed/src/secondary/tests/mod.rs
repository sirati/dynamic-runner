//! Tests for the secondary coordinator.
//!
//! Post-unification the secondary is a PURE NON-AUTHORITY node: it
//! manages its own workers, mirrors the replicated CRDT, and reports
//! its own workers' outcomes to whoever holds the primary role. It
//! never dispatches authoritatively, never originates a terminal CRDT
//! mutation, and never drives a phase machine. The tests here exercise
//! exactly that surface; every authority-mirror test (command_channel,
//! phase_lifecycle_callback, keyed_outputs_apply_race,
//! retry_bucket_cascade, cascade, singleton_type_shift,
//! result_data_plumbing, promoted_primary_quiesce_gate) was retired
//! with the authority mirror it tested.
//!
//! Split per-concern:
//!
//! - [`processing`] — basic worker-processing integration tests
//!   (initial-assignment + dispatch loop + StageFile pre-flight).
//! - [`peer_mesh_watchdog`] — peer-mesh-formation watchdog
//!   (degraded-mode + healthy-mesh non-regression).
//! - [`r1`] — R1 promotion-threshold tests + cold-start no-primary
//!   tests + post-promotion peer-message dispatch test.
//! - [`honest_liveness`] — `run_election_tick`'s honest-by-source
//!   `need_election`: a transient blip (route up, staleness < backstop)
//!   does NOT elect, a dead link arms fast via leg (A), a wedged-but-
//!   routable primary elects via the patient backstop (leg B), and a
//!   resumed primary message cancels an in-flight election.
//! - [`failover_membership`] — BUG H: `run_election_tick`'s leg (C) arms the
//!   failover election when the current primary LEAVES the transport
//!   `MembershipView`, so an IDLE survivor (no `send_to_primary`, leg (A)
//!   silent) elects on primary-death without waiting the leg-(B) backstop;
//!   gated on `primary_last_seen.is_some()` (no relocation-window false-arm),
//!   self-cancelling on a flicker, and a gap-closure control proving the
//!   election is membership-armed.
//! - [`failover_lone_survivor`] — lone-survivor failover convergence: the
//!   failover-quorum denominator (`live_peer_ids`) intersects `peer_keepalives`
//!   with the live transport `MembershipView`, so a peer that DEPARTED
//!   membership on a simultaneous kill stops inflating the quorum within one
//!   pump cycle (the fast signal) instead of lingering the full `peer_timeout`
//!   (300s) reaper window — the lone survivor's quorum shrinks to 1 and it
//!   self-promotes; a still-present peer is NOT over-shrunk (split-brain safe).
//! - [`failover_multi_survivor`] — multi-survivor failover convergence under
//!   abrupt-crash membership-eviction divergence: the lex-lowest survivor
//!   re-polls (`TimeoutQuery` re-emitted each waiting Suspecting tick) so a
//!   peer that observes the dead primary AFTER the first query still gets
//!   counted, and a peer that has NOT yet observed the death correctly refuses
//!   to confirm (split-brain safety) until it does — the candidate never
//!   wedges on a cached stale "primary still alive" answer.
//! - [`failover_second_round`] — SECOND-failover convergence: candidate
//!   selection (`lowest_alive` over `live_peer_ids()`) excludes a peer that is
//!   DEAD-but-still-lingering in `peer_keepalives` (the 300s reaper window)
//!   because `live_peer_ids` intersects the live transport membership
//!   (`has_peer`) — the SAME seam the lone-survivor quorum-denominator fix
//!   shrank. So a survivor of the FIRST failover does NOT defer to the first
//!   failover's dead lowest-id primary on the SECOND failover; it self-leads
//!   and promotes (vs. wedging the full reaper window). A still-MEMBER lowest-id
//!   peer is correctly still deferred to (no over-exclude).
//! - [`deferrer_membership_death`] — #331 deferrer-side death-observation
//!   on membership-departure: a peer asked to lend its failover agreement
//!   (`TimeoutResponse` / `PromotionConfirm`) treats the current primary's
//!   transport membership-departure as its own death observation — the
//!   deferrer-side twin of arming leg (C) — so failover converges without
//!   each survivor waiting out its frame-silence death deadline. A
//!   non-primary departure and a departure+rejoin blip change nothing.
//! - [`keepalive_recognition`] — primary-vs-peer keepalive routing: a
//!   current-primary keepalive refreshes `primary_last_seen`; any other
//!   peer's keepalive feeds `peer_keepalives`.
//! - [`keepalive_emission`] — `send_keepalive` fans ONE keepalive out
//!   exactly once (reaching the meshed primary once, not twice), fires
//!   even when the mesh is degraded, and is suppressed for observers.
//! - [`late_joiner_observer`] — late-joiner observer-mode scenario.
//! - [`mesh_ready_reannounce`] — pairwise mesh confirmation: a
//!   genuinely-applied `PrimaryChanged` (either receive arm, either
//!   reason) re-announces `MeshReady` to the NEW primary; a stale-epoch
//!   NoOp re-announces nothing; an unsettled mesh defers to the
//!   watchdog's terminal report.
//! - [`late_joiner_accept_emits_peer_joined`] — receive-side
//!   PeerJoined emission contract for the late-joiner accept path,
//!   carrying the joiner's ACTUAL role.
//! - [`observer_announcer_wireup`] — observer announcer production
//!   wiring contract (the separate resource-provider capability).
//! - [`panik_integration`] — panik self-departure + worker teardown.
//! - [`memprofile_hook`] — sampler lifecycle hooks.
//! - [`terminal_ack`] — #352 app-level delivery confirmation: the
//!   blackholed-but-live-leg repro (transport-Ok send retained awaiting
//!   `TerminalAck`; ack-timeout replays with the same seq; the ack is the
//!   only drop site; failover-arming inputs untouched).
//! - [`replay_backoff`] — the replay-flood repro: an unACKed retained
//!   report replays on the per-entry exponential backoff schedule
//!   (`ack_timeout` → 2× → 4× … capped), at most once per seq per drain
//!   pass, with the route-restored edge (`drain_report_replays_now`)
//!   retrying promptly; the wake deadline is the min `next_due`.

#![cfg(test)]

mod anti_entropy_heal;
mod custom_messages;
mod deferrer_membership_death;
mod failover_beacon_union;
mod failover_lone_survivor;
mod failover_membership;
mod failover_multi_survivor;
mod failover_second_round;
mod firstbind_orphan;
mod generation_gate;
mod graceful_drain;
mod hold_probe_responder;
mod honest_liveness;
mod keepalive_emission;
mod keepalive_recognition;
mod late_joiner_accept_emits_peer_joined;
mod late_joiner_observer;
mod memprofile_hook;
mod mesh_ready_reannounce;
mod observer_announcer_wireup;
mod panik_integration;
mod peer_mesh_watchdog;
mod processing;
mod r1;
mod relocation_handoff_race;
mod replay_backoff;
mod run_config_responder;
mod terminal_ack;
mod voting_candidate_death;
mod worker_custom_messages;
