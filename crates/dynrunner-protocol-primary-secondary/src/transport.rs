use std::time::Duration;

use dynrunner_core::{Identifier, MessageReceiver};

use crate::DistributedMessage;
use crate::address::PeerId;
use crate::messages::timestamp_now;

/// Default bootstrap-RPC budget for [`PeerTransport::join_running_cluster`].
///
/// 10 s matches the pre-Step-8 per-peer QUIC dial budget (cf.
/// `transport-quic/src/peer/dial.rs`'s happy-eyeballs total) plus
/// rendezvous slack: a healthy responder hands back a snapshot in
/// milliseconds once the QUIC handshake completes; the entire
/// budget is for the rendezvous + handshake to land. Shorter
/// budgets (5 s) round-tripped fine on a LAN but were tight on
/// fabric where the first dial attempt's UDP must traverse a
/// firewall before falling back to WSS — both consumer teams hit
/// this on Krater (cf. lower-id-dials commentary in
/// `transport-quic/src/peer/mod.rs`). Longer budgets (>15 s) start
/// to mask transport bugs (a peer that never replies should be
/// caller-observable, not silently retried).
///
/// Caller-overridable via the `timeout` parameter on
/// [`PeerTransport::join_running_cluster`].
pub const DEFAULT_JOIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Error from [`PeerTransport::join_running_cluster`].
#[derive(Debug)]
pub enum JoinError {
    /// `connect_to_peers` ran but no peer became reachable within
    /// the per-peer-connect slice of the bootstrap budget.
    NoReachablePeer,
    /// At least one peer was reachable and we sent the snapshot
    /// request, but no `ClusterSnapshot` reply arrived within the
    /// budget. The transport may have received other live messages
    /// during the window — those are dropped (logged at `warn`).
    /// Bootstrap is a single-RPC contract; the caller drives any
    /// retry policy.
    Timeout,
    /// `send_to_peer` returned an error while delivering the
    /// snapshot request. The wrapped string is the transport's
    /// error message verbatim.
    SendFailed(String),
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoReachablePeer => f.write_str(
                "join_running_cluster: no seed peer became reachable within the connect window",
            ),
            Self::Timeout => f.write_str(
                "join_running_cluster: no ClusterSnapshot reply within the bootstrap timeout",
            ),
            Self::SendFailed(e) => write!(f, "join_running_cluster: failed to send request: {e}"),
        }
    }
}

impl std::error::Error for JoinError {}

/// Primary-side transport for the legacy per-secondary writer fan-out.
///
/// # Status — Step 11 audit
///
/// Kept alive on the recv side because the setup-phase recv loops on
/// both ends (`primary/connect.rs::wait_for_connections` and
/// `primary/lifecycle.rs::wait_for_mesh_ready`) interleave setup
/// frames (`SecondaryWelcome`, `CertExchange`) with runtime frames
/// (`MeshReady`, `Keepalive`, `ClusterMutation`, etc.) during the
/// setup window. The narrow-typed `SetupBootstrap` (Step 10) covers
/// the setup-frame send side only; the recv-side filter still needs
/// the full `DistributedMessage` shape that this trait carries, plus
/// the per-secondary `send_to` for `InitialAssignment` / `StageFile`
/// / `TaskAssignment` / broadcasts that aren't part of `SetupBootstrap`.
///
/// Sibling `PrimaryTransport` (the secondary-side legacy unicast +
/// recv shape) deleted in Step 11; it was a zero-method marker over
/// `MessageSender + MessageReceiver` and disappeared cleanly. This
/// trait carries real methods (`send_to`, `broadcast`) backed by
/// production impls (`NetworkServer` and `ChannelSecondaryTransportEnd`),
/// so it stays — deleting it would require a recv-loop redesign.
///
/// # Concern
///
/// Addressing-by-secondary-id (`send_to`) sits on top of the base
/// `MessageSender`/`MessageReceiver` shape; the trait keeps it as a
/// protocol-level addition without leaking the transport-level
/// `connections: HashMap` into call sites.
pub trait SecondaryTransport<I: Identifier>: MessageReceiver<DistributedMessage<I>> {
    /// Send a message to a specific secondary.
    fn send_to(
        &mut self,
        secondary_id: &str,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Broadcast a message to every connected secondary.
    ///
    /// Implementations must drain pending new connections before iterating so a
    /// secondary whose handshake completed since the last poll is not silently
    /// skipped. Per-peer failures are returned as `(secondary_id, error)`
    /// pairs; the broadcast itself succeeds (`Ok(())`) when every peer's
    /// outgoing channel accepted the message. Callers choose the log severity
    /// for partial failures (e.g. `debug` for high-cadence keepalives, `warn`
    /// for low-cadence control messages).
    fn broadcast(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), Vec<(String, String)>>>;
}

// `PrimaryTransport<I>` (secondary-side legacy unicast + recv shape)
// deleted in Step 11 of the transport-unification refactor. It was a
// zero-method marker over `MessageSender<DistributedMessage<I>> +
// MessageReceiver<DistributedMessage<I>>` with a blanket impl; the
// bound now expresses itself directly at every former call site.
// `SecondaryTransport` (above) stays because it carries real methods
// (`send_to`, `broadcast`).

/// Transport trait for peer-to-peer communication between secondaries.
///
/// Supports broadcasting to all connected peers, sending to a specific peer,
/// and receiving messages from any peer.
pub trait PeerTransport<I: Identifier> {
    /// Broadcast a message to all connected peers.
    fn broadcast(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Send a message to a specific peer.
    fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Receive the next message from any peer.
    fn recv_peer(&mut self) -> impl std::future::Future<Output = Option<DistributedMessage<I>>>;

    /// Try to receive a message without blocking. Returns `None` if no message is available.
    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>>;

    /// The number of connected peers.
    fn peer_count(&self) -> usize;

    /// Whether `id` is currently a member of this transport's mesh —
    /// i.e. the transport holds a connection (or the equivalent
    /// addressable entry) keyed by that peer-id right now.
    ///
    /// This is the per-id companion to [`Self::peer_count`] (the
    /// cardinality): `peer_count` answers "how many peers", `has_peer`
    /// answers "is THIS peer one of them". The membership predicate is
    /// what the rendezvous / cutover work needs so it can rest on
    /// "the primary is a connected peer" rather than on the
    /// always-present uplink leg.
    ///
    /// REQUIRED (no default). A blanket default would have to pick a
    /// constant answer that ignores the connection map, which is
    /// silently wrong for every real transport — a membership predicate
    /// that never reflects membership is a bug waiting to happen. Each
    /// impl answers from its own real connection/writer table.
    fn has_peer(&self, id: &PeerId) -> bool;

    /// Deliverability, as opposed to direct membership: can this
    /// transport deliver a directed frame to `id` by ANY path right now
    /// — a direct connection, OR a relay through a connected forwarder
    /// it has not blacklisted for `id`?
    ///
    /// `has_route(id)` ⊇ `has_peer(id)`: a direct member is always
    /// routable; a peer whose direct wire died may STILL be routable via
    /// relay. The distinction is load-bearing (BUG 3.3): reading
    /// [`Self::has_peer`] where the question is "can my frames reach
    /// it / can its frames reach me" declares a relay-covered link dead
    /// — the no-route-for-sends vs recovered-for-liveness flip-flop.
    /// Consumers asking about DELIVERY (the egress no-route gate, the
    /// death-evidence membership reads) take this; consumers asking
    /// about the DIRECT wire (redial decisions, broadcast fan-out
    /// honesty) keep `has_peer`.
    ///
    /// Default: `has_peer` — exact for transports with no relay layer
    /// (every connection is the only path). Relay-capable transports
    /// (`PeerNetwork`) override with the Router-backed predicate.
    fn has_route(&self, id: &PeerId) -> bool {
        self.has_peer(id)
    }

    /// Whether this transport can DELIVER beyond its direct connections
    /// — i.e. whether [`Self::has_route`] can exceed [`Self::has_peer`]
    /// (a Router-backed relay layer). Published alongside the
    /// connected/unroutable sets so a detached membership-view reader
    /// answers `has_route` with THIS transport's real semantics instead
    /// of inferring relay capability that a stub/direct-only transport
    /// does not have. Default: `false` (has_route == has_peer);
    /// Router-backed transports override to `true`.
    fn relay_capable(&self) -> bool {
        false
    }

    /// The finite set of peer ids this transport currently KNOWS it
    /// cannot deliver to by any path (no direct connection AND every
    /// connected forwarder blacklisted for them). The published
    /// projection of [`Self::has_route`] for detached membership-view
    /// readers: when [`Self::relay_capable`] is `true`, an id outside
    /// this set is routable iff the connected set contains it or any
    /// other peer. Default: empty — exact for transports with no relay
    /// blacklist.
    fn unroutable_ids(&self) -> Vec<PeerId> {
        Vec::new()
    }

    /// Enumerate the currently-connected peer ids — the live id-set
    /// behind [`Self::peer_count`] (the cardinality) and [`Self::has_peer`]
    /// (the per-id predicate). Role-agnostic (transport ⊥ roles): it
    /// returns bare [`PeerId`]s, never a role; the folded bootstrap
    /// primary appears like any other connected member.
    ///
    /// Used by a role-aware wrapper that must PUBLISH the live membership
    /// to a detached send-handle (which cannot borrow the by-value
    /// transport). The wrapper reads this each pump cycle and republishes
    /// the whole set — so the published view is a live read, never a
    /// hand-maintained shadow.
    ///
    /// Default: empty. A transport whose detached handles never need the
    /// per-id membership view (the no-peer arm, mock fixtures) compiles
    /// unchanged; the cardinality [`Self::peer_count`] stays the
    /// authoritative count regardless. Transports that DO back a published
    /// membership view (`PeerNetwork`, the channel mesh) override to
    /// enumerate their real connection table.
    fn connected_ids(&self) -> Vec<PeerId> {
        Vec::new()
    }

    /// The transport's inbound ingest-edge clocks
    /// ([`crate::freshness::IngestEdges`]): per-peer last-ARRIVAL
    /// (recorded by the connection read loops the moment a decoded
    /// frame enters the transport's inbound queue) and per-peer
    /// last-DRAINED (recorded as `recv_peer`/`try_recv_peer` pulls it
    /// back out). Cloneable Arc-backed handles — a detached liveness
    /// reader samples them on its own cadence while the transport's
    /// reader tasks keep writing, so the arrival clock stays honest
    /// even when the consumer side is starved.
    ///
    /// `None` (the default) means this transport cannot observe a
    /// frame's arrival any earlier than its own `recv_peer` (e.g. the
    /// channel transport, whose inbound queue is filled directly by the
    /// sending peer) — liveness readers then fall back to their
    /// downstream clocks and the ingest-health gate stays inactive.
    /// Transports with independent read-loop tasks (the QUIC/WSS
    /// `PeerNetwork`, the submitter's `TunneledPeerTransport`) override
    /// with `Some` of their owned pair.
    fn ingest_edges(&self) -> Option<crate::freshness::IngestEdges> {
        None
    }

    /// Connect to peers from the peer list received from primary.
    fn connect_to_peers(
        &mut self,
        peers: &[crate::PeerConnectionInfo],
    ) -> impl std::future::Future<Output = ()>;

    /// Fold any asynchronously-completed connection registrations into
    /// the membership view that [`Self::peer_count`] / [`Self::has_peer`]
    /// / [`Self::connected_ids`] report.
    ///
    /// Transports whose dials register lazily (the QUIC `PeerNetwork`:
    /// dial tasks complete on spawned tasks and park the accepted
    /// connection on a channel that is only drained inside `&mut self`
    /// entry points like `send`/`recv`) override this with that drain.
    /// Pre-wired transports (channel mesh, mocks) keep the no-op
    /// default.
    ///
    /// Exists for membership POLL LOOPS that hold `&mut self` but call
    /// none of the draining entry points — most importantly
    /// [`Self::join_running_cluster`]'s step-2 rendezvous gate, which
    /// spins on `peer_count()` between sleeps: without this fold the
    /// gate can never observe a dial that completed after
    /// `connect_to_peers` returned, and the bootstrap dies with
    /// [`JoinError::NoReachablePeer`] against a perfectly reachable
    /// seed.
    fn sync_membership(&mut self) {}

    /// The local node's own peer-id, used as the bootstrap RPC's
    /// return address (the `sender_id` of the
    /// [`DistributedMessage::RequestClusterSnapshot`] frame
    /// [`Self::join_running_cluster`] constructs) and to skip the
    /// joiner's own entry when iterating the seed list.
    ///
    /// Default impl returns the empty string: transports whose join
    /// path never needs a self-identifying return address (any
    /// transport that pre-wires its
    /// mesh) compile cleanly without overriding. A transport that
    /// participates in the snapshot-bootstrap rendezvous overrides to
    /// return its real id so the responder's `PeerJoined` broadcast
    /// carries the truthful joiner id.
    fn local_id(&self) -> &str {
        ""
    }

    /// Bootstrap-from-snapshot orchestration for a late-joiner / fresh
    /// observer.
    ///
    /// Semantics — single-RPC contract:
    /// 1. Wire up the peer mesh by calling [`Self::connect_to_peers`]
    ///    with `seed`. Transports that pre-wire (channel,
    ///    [`crate::PeerTransport`] mocks) treat this as a no-op.
    /// 2. Wait briefly for at least one peer to register as connected
    ///    by polling [`Self::peer_count`] up to a small slice of the
    ///    total budget. If no peer is reachable, surface
    ///    [`JoinError::NoReachablePeer`] — the caller decides between
    ///    abort vs. retry-with-a-different-seed.
    /// 3. Construct a [`DistributedMessage::RequestClusterSnapshot`]
    ///    envelope tagged with [`Self::local_id`] as the responder's
    ///    return address and `is_observer` declaring the joiner's own
    ///    role (so the responder's `PeerJoined` broadcast carries the
    ///    truth instead of assuming observer).
    /// 4. Send it via [`Self::send_to_peer`] to EVERY reachable non-self
    ///    seed id (multi-responder fan-out). The receiver-side handler in
    ///    `secondary/dispatch.rs` — and now the primary
    ///    (`primary::task::mutation::handle_request_cluster_snapshot`) —
    ///    accepts the request from any peer (`cluster_state` is
    ///    replicated) and replies with a unicast
    ///    [`DistributedMessage::ClusterSnapshot`].
    ///
    ///    The request is addressed by concrete peer-id, not by role: a
    ///    cold-start joiner cannot resolve `Destination::Primary` yet
    ///    (it has observed no `PrimaryChanged`). Fanning to ALL seeds —
    ///    rather than the first that accepts — is the primary-preferred /
    ///    completeness fix: the first reachable seed may be a secondary
    ///    whose own roster is incomplete (the pre-mesh
    ///    `secondary_capacities` desync), so a SINGLE reply could
    ///    bootstrap the joiner from a partial snapshot. Collecting every
    ///    responder's snapshot and letting the caller `restore()` each
    ///    one heals via the idempotent lattice (the union is complete iff
    ///    ANY responder — the primary above all — was complete).
    ///
    /// 5. Drive [`Self::recv_peer`] inside a `tokio::time::timeout`,
    ///    COLLECTING every [`DistributedMessage::ClusterSnapshot`] that
    ///    arrives until either one reply per peer the request was sent to
    ///    has been gathered or the bootstrap budget expires. Messages
    ///    OTHER than `ClusterSnapshot` received in the window are logged
    ///    at `warn` and dropped — the cluster's CRDT-merge guarantees the
    ///    next live broadcast (or a follow-up snapshot) covers anything
    ///    dropped here.
    ///
    /// Returns the COLLECTED snapshot payloads (the `snapshot_json` of
    /// each `ClusterSnapshot` reply) as a `Vec<String>` — at least one on
    /// `Ok`. The caller decodes each into its own concrete
    /// `ClusterStateSnapshot<I>` and `restore()`s each (the idempotent
    /// lattice unions them). The protocol crate stays free of
    /// `ClusterStateSnapshot<I>` — the wire-side `String` keeps `I`
    /// erased at the transport boundary; see the dispatch.rs commentary
    /// on the same direction-of-dependency point.
    ///
    /// **Single concern**: bootstrap rendezvous + snapshot RPC. The
    /// caller's concern is cluster-state restoration (one `restore` per
    /// returned payload) and any retry policy if `Err` comes back. The
    /// loop above never touches `ClusterState` directly — the dependency
    /// edge is preserved (protocol crate does not depend on
    /// manager-distributed).
    fn join_running_cluster(
        &mut self,
        seed: &[crate::PeerConnectionInfo],
        timeout: Duration,
        is_observer: bool,
        can_be_primary: bool,
    ) -> impl std::future::Future<Output = Result<Vec<String>, JoinError>>
    where
        I: 'static,
    {
        async move {
            // Step 1: dial. No-op for pre-wired transports (channel
            // mesh, tests); real work for `PeerNetwork`.
            self.connect_to_peers(seed).await;

            // Step 2: rendezvous gate. Wait until at least one peer
            // connection has registered. Bound by a
            // fraction of the total budget so the snapshot recv
            // gets the lion's share. Polling cadence is 25 ms —
            // tight enough that a ~100 ms QUIC handshake is observed
            // within ~4 ticks; loose enough that the busy-wait cost
            // is negligible on the bootstrap path.
            let connect_budget = timeout / 4;
            let connect_deadline = tokio::time::Instant::now() + connect_budget;
            let local_id = self.local_id().to_owned();
            // Wait for the mesh to register at least one connection.
            // For pre-wired transports (channel mesh) this is true
            // synchronously after connect_to_peers returns; for
            // PeerNetwork the dial races land asynchronously through
            // new_conn_rx and the next peer_count > 0 observation is
            // the proxy for "at least one peer is up". The transport
            // doesn't expose a per-id "is THIS id connected?"
            // predicate today (only peer_count, the cardinality),
            // so we drive the rendezvous on cardinality and then
            // (Step 3+4) send the request to EVERY non-self seed
            // (multi-responder fan-out). Any peer can answer per
            // dispatch.rs's RequestClusterSnapshot handler; collecting
            // all replies and merging them via the idempotent lattice
            // heals an incomplete responder.
            loop {
                // Fold completed dial registrations into the membership
                // view first: `peer_count()` is `&self` and lazily-
                // registering transports (the QUIC `PeerNetwork`) only
                // fold inside `&mut self` entry points this poll loop
                // otherwise never calls — see `sync_membership`'s doc.
                self.sync_membership();
                if self.peer_count() > 0 {
                    break;
                }
                if tokio::time::Instant::now() >= connect_deadline {
                    return Err(JoinError::NoReachablePeer);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }

            // Step 3+4: send the request to EVERY reachable non-self
            // seed (multi-responder fan-out). The joiner's cold-start role
            // table can't resolve `Destination::Primary` yet (no
            // `PrimaryChanged` observed), and any peer can answer per the
            // dispatch.rs / primary snapshot handlers. We address the
            // transport directly by peer-id (no role resolution — the id
            // IS the host). Fanning to ALL seeds (not first-success) is
            // the completeness fix: the first reachable seed may be a
            // secondary holding an incomplete roster, so a single reply
            // could bootstrap from a partial snapshot. Collecting every
            // responder's snapshot and `restore()`-ing each (idempotent
            // lattice) heals — the union is complete iff ANY responder
            // (the primary above all) was complete. Per-peer send errors
            // (`no route`, `outgoing channel closed`) are tolerated; a
            // partially-stale seed (a peer retired between file-write and
            // this dial) just doesn't get a request, and the remaining
            // live peers still answer.
            let mut send_err: Option<String> = None;
            let mut sent_count: usize = 0;
            for peer in seed {
                if peer.secondary_id == local_id {
                    continue;
                }
                let request = DistributedMessage::RequestClusterSnapshot {
                    target: None,
                    sender_id: local_id.clone(),
                    timestamp: timestamp_now(),
                    // The joiner declares its own role + capability so the
                    // responder broadcasts a truthful `PeerJoined` rather
                    // than assuming observer / incapable.
                    is_observer,
                    can_be_primary,
                };
                match self.send_to_peer(&peer.secondary_id, request).await {
                    Ok(()) => {
                        sent_count += 1;
                    }
                    Err(e) => {
                        send_err = Some(e);
                    }
                }
            }
            if sent_count == 0 {
                return Err(JoinError::SendFailed(
                    send_err.unwrap_or_else(|| "no seed peer accepted send".into()),
                ));
            }

            // Step 5: collect ClusterSnapshot replies. Gather every reply
            // that arrives until we have one per peer we sent to OR the
            // bootstrap budget expires. Non-ClusterSnapshot frames in this
            // window are dropped with a warn log — see method-doc. A
            // budget expiry / inbound-close with at least one snapshot
            // already collected is success (the caller unions them); zero
            // collected surfaces `Timeout` — the operator-visible signal
            // is identical to the cold-start no-reply case.
            let recv_budget = timeout.saturating_sub(connect_budget);
            let recv_deadline = tokio::time::Instant::now() + recv_budget;
            let mut snapshots: Vec<String> = Vec::with_capacity(sent_count);
            loop {
                if snapshots.len() >= sent_count {
                    // Every peer we sent to has answered; no point waiting
                    // out the rest of the budget.
                    return Ok(snapshots);
                }
                let remaining =
                    recv_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return if snapshots.is_empty() {
                        Err(JoinError::Timeout)
                    } else {
                        Ok(snapshots)
                    };
                }
                let recv = tokio::time::timeout(remaining, self.recv_peer()).await;
                match recv {
                    Err(_) => {
                        // Budget expired mid-recv.
                        return if snapshots.is_empty() {
                            Err(JoinError::Timeout)
                        } else {
                            Ok(snapshots)
                        };
                    }
                    Ok(None) => {
                        // Transport's inbound channel closed: no more
                        // messages will ever arrive. Return whatever we
                        // collected; empty surfaces as timeout (identical
                        // operator-visible signal, cause shows up in the
                        // transport's own teardown logs).
                        return if snapshots.is_empty() {
                            Err(JoinError::Timeout)
                        } else {
                            Ok(snapshots)
                        };
                    }
                    // Accept the reply REGARDLESS of its Phase-C target
                    // stamp. The responder's coordinator egress stamps
                    // every frame with the resolved role-typed return
                    // address (`anti_entropy::reply_destination` →
                    // `Some(Destination::Observer(<this id>))` /
                    // `Some(Destination::Secondary(<this id>))`), and the
                    // frame's ARRIVAL on this node's wire already
                    // satisfies the host-addressing — the stamp is only
                    // the mesh-pump's slot-demux hint, and the bootstrap
                    // window has no role slots (the same never-drop-on-
                    // stamp ingress rule as `Mesh::route_incoming`).
                    // `None` (a pre-stamp transport or test double) is
                    // equally accepted. A `target: None` pattern here
                    // dropped every production reply as "non-
                    // ClusterSnapshot … kind=ClusterSnapshot" until the
                    // budget expired — the gateway late-joiner Test-1a
                    // bootstrap timeout.
                    Ok(Some(DistributedMessage::ClusterSnapshot {
                        snapshot_json, ..
                    })) => {
                        snapshots.push(snapshot_json);
                        continue;
                    }
                    Ok(Some(other)) => {
                        tracing::warn!(
                            kind = ?other.msg_type(),
                            target = ?other.target(),
                            "join_running_cluster: dropped non-ClusterSnapshot frame in bootstrap window"
                        );
                        continue;
                    }
                }
            }
        }
    }
}
