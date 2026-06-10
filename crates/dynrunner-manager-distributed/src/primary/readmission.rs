//! Frame-ingest RE-ADMISSION of a removed-but-provably-alive member.
//!
//! # Concern (and ONLY this concern)
//!
//! A member removed from the replicated membership (`PeerRemoved` —
//! e.g. a keepalive-timeout declaration) whose AUTHENTICATED frames
//! keep arriving at the primary is provably alive: the removal was
//! false (the production face: a flood-starved primary declared live
//! peers silent while their keepalives sat in its backed-up inbox).
//! The removed node does NOT know it was removed — its `cluster_state`
//! marks itself `Dead` only via the broadcast it observes, and it never
//! re-sends a `SecondaryWelcome` — so re-admission cannot require it to
//! act. THIS seam is the automatic recovery: every inbound frame's
//! sender is checked against the replicated membership BEFORE any other
//! handling, and a frame from a `Dead` sender re-admits it through the
//! existing lattice (a `PeerJoined` at `member_gen + 1` — removal at
//! generation N, rejoin at generation N+1; see `apply_peer_joined`).
//!
//! # Why the primary, and why frame-ingest
//!
//! The primary is the sole authoritative author of membership
//! (`PeerRemoved` originates only here / at a node's own self-departure
//! announce), so the re-admission bump is authored at the same
//! altitude — peers converge via the broadcast exactly as they do for a
//! removal. Frame-ingest is the seam because an arriving authenticated
//! frame IS the proof of life: pre-fix it hit the stale-role/ignore
//! path (`record_keepalive` no-ops for an unknown id) and the evidence
//! was discarded forever.
//!
//! # What a re-admission restores
//!
//! 1. The replicated ledger: `PeerJoined { member_gen: dead_gen + 1 }`
//!    carrying the advertisement preserved on the capability `Departed`
//!    tombstone (the exact capability the member departed with),
//!    applied locally + broadcast through the canonical origination
//!    path so every replica re-admits.
//! 2. The primary-local roster: a metadata-only `Operational`
//!    connection entry rebuilt from the replicated capacity record —
//!    the SAME seed shape the promotion hydrate uses
//!    (`reconstruct_secondaries_from_cluster_state`), single-peer — plus
//!    a fresh keepalive seed so the death clock counts from
//!    re-admission.
//! 3. The worker roster + dispatch: via the established capacity-growth
//!    reaction (`react_to_capacity_growth` — the SOLE worker-roster
//!    builder + the decoupled `TasksAdded` recheck emit), so dispatch
//!    re-evaluates against the recovered capacity with no direct call
//!    (the dispatch-decoupling law).
//!
//! # What does NOT trigger it
//!
//! - A frame from this node itself (loopback).
//! - A `SecondaryFatalError` — that frame's own meaning is "I am
//!   dying"; re-admitting on it would immediately re-remove.
//! - Frames from live or never-joined senders (the common case — one
//!   cheap `peer_state` lookup, no other cost on the hot path).
//!
//! A genuinely dead peer sends nothing, so it can never be re-admitted
//! by this seam; a removal→re-admission cycle requires BOTH a full
//! silence window (the removal) AND a fresh authenticated frame (the
//! re-admission), so no tight churn loop exists.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::state::{SecondaryConnection, SecondaryConnectionState};

use super::PrimaryCoordinator;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Re-admit `msg`'s sender if it is a REMOVED member whose
    /// authenticated frame just arrived (proof of life). Called from the
    /// `dispatch_message` preamble BEFORE `record_keepalive`, so the
    /// triggering frame itself lands in the restored roster entry's
    /// death clock. No-op for live / never-joined / self senders (one
    /// `peer_state` lookup).
    pub(super) async fn maybe_readmit_sender(&mut self, msg: &DistributedMessage<I>) {
        let sender = msg.sender_id();
        if sender.is_empty() || sender == self.config.node_id {
            return;
        }
        // "I am dying" is not proof of ongoing life — re-admitting on it
        // would self-contradict (the handler immediately re-removes).
        if msg.msg_type() == MessageType::SecondaryFatalError {
            return;
        }
        let Some(ticket) = self.cluster_state.removed_peer_readmission(sender) else {
            return;
        };
        let sender = sender.to_string();
        tracing::warn!(
            peer = %sender,
            to_gen = ticket.member_gen,
            msg_type = ?msg.msg_type(),
            "frame from a REMOVED member arrived — it is provably alive; \
             re-admitting it into the replicated membership at the next \
             generation (the removal was false or the member recovered)"
        );
        // (1) Replicated ledger: the generation-advancing `PeerJoined`,
        // restoring the advertisement preserved on the Departed
        // tombstone. Local apply + broadcast through the canonical
        // origination path — every replica re-admits via the same
        // `apply_peer_joined` rule.
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PeerJoined {
            peer_id: sender.clone(),
            is_observer: ticket.is_observer,
            can_be_primary: ticket.can_be_primary,
            // Stamped at the origination choke point.
            cap_version: Default::default(),
            member_gen: ticket.member_gen,
        }])
        .await;
        // (2) Primary-local roster: restore the single peer's connection
        // + keepalive entry from the replicated capacity record — the
        // same metadata-only Operational seed the promotion hydrate
        // builds (`reconstruct_secondaries_from_cluster_state`), scoped
        // to one id so the live roster's other entries (their keepalive
        // clocks, their silence streaks) stay untouched.
        self.restore_roster_entry(&sender, ticket.is_observer, ticket.can_be_primary);
        // (3) Worker roster + dispatch recheck: the established
        // capacity-growth reaction (idempotent wholesale rebuild from
        // the CRDT + the decoupled `TasksAdded` emit).
        self.react_to_capacity_growth();
    }

    /// Restore ONE re-admitted peer's `self.secondaries` +
    /// `self.secondary_keepalives` entries from the replicated capacity
    /// ledger — the single-peer counterpart of the promotion hydrate's
    /// `reconstruct_secondaries_from_cluster_state` (the same
    /// metadata-only `Operational` typestate walk; the peer is reached
    /// over the unified mesh, so no `QuicConnection` handle exists or is
    /// needed). A missing capacity record (never advertised — should not
    /// happen for a previously-welcomed member) restores only the
    /// keepalive seed and logs the gap.
    fn restore_roster_entry(&mut self, id: &str, is_observer: bool, can_be_primary: bool) {
        match self.cluster_state.secondary_capacity(id) {
            Some(cap) => {
                let conn = SecondaryConnection::new(id.to_string())
                    .receive_welcome(
                        cap.worker_count,
                        cap.resources.clone(),
                        String::new(),
                        0,
                        None,
                        is_observer,
                        can_be_primary,
                    )
                    .receive_cert_exchange(String::new(), None, None, 0, None)
                    .begin_peer_discovery()
                    .peers_ready()
                    .assignments_sent();
                self.secondaries
                    .insert(id.to_string(), SecondaryConnectionState::Operational(conn));
            }
            None => {
                tracing::warn!(
                    peer = %id,
                    "re-admitted member has no replicated capacity record; \
                     restoring its keepalive clock only (it will re-enter \
                     the worker roster when its capacity lands)"
                );
            }
        }
        // Fresh silence streak from the moment of re-admission — the
        // same treatment `seed_keepalive` gives a bootstrap welcome.
        self.seed_keepalive(id);
    }
}
