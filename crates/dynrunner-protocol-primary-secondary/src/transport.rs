use std::time::Duration;

use dynrunner_core::{Identifier, MessageReceiver};

use crate::DistributedMessage;
use crate::address::PeerId;
use crate::messages::timestamp_now;

/// Default bootstrap-RPC budget for [`PeerTransport::join_running_cluster`].
///
/// 60 s: a quarter goes to the dial rendezvous (`timeout / 4` — covers
/// the QUIC-then-WSS happy-eyeballs fabric dial both consumer teams
/// needed on Krater, cf. `transport-quic/src/peer/dial.rs` and the
/// lower-id-dials commentary in `transport-quic/src/peer/mod.rs`); the
/// remaining 45 s is the reply-collection window, which the re-request
/// cadence (`recv_budget / 3` capped at [`JOIN_REREQUEST_CAP`] = 5 s)
/// slices into ~9 fan-out rounds. The budget is sized for the REPLY to
/// LAND, not just for the request to be heard: a production bootstrap
/// (gateway joiner into a busy run) collects many multi-MB snapshot
/// packages over WAN legs, and the
/// previous 10 s budget (7.5 s recv, 3 fan-outs) expired while replies
/// were still in flight — the joiner died `Timeout` against responders
/// that had already answered. A silent responder is still
/// caller-observable, not masked: the `Timeout` error carries
/// `requests_sent` / `fan_outs`, and every fan-out round names its
/// per-peer send failures.
///
/// Caller-overridable via the `timeout` parameter on
/// [`PeerTransport::join_running_cluster`].
pub const DEFAULT_JOIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Cap on the bootstrap re-request cadence inside
/// [`PeerTransport::join_running_cluster`]'s reply-wait window.
///
/// The snapshot request is RE-FANNED periodically until the bootstrap
/// deadline (the realised interval is `recv_budget / 3` capped here, so
/// every budget gets at least two re-request opportunities). The first
/// fan-out fires the instant the FIRST mesh leg registers, which makes
/// it a one-shot race against leg establishment (later seeds get at
/// best a relay) and against responder-seat churn — a joiner dialing
/// inside a primary-promotion window can lose EVERY first-shot request
/// while its legs keep delivering gossip. Re-requesting is safe by
/// design: a responder still holding the stream RESUMES it from the
/// carried cursor, and the joiner's `PeerJoined` originates through
/// idempotent CRDT apply — a duplicate request is a cheap reposition,
/// never a state change.
const JOIN_REREQUEST_CAP: Duration = Duration::from_secs(5);

/// Error from [`PeerTransport::join_running_cluster`].
#[derive(Debug)]
pub enum JoinError {
    /// `connect_to_peers` ran but no peer became reachable within
    /// the per-peer-connect slice of the bootstrap budget.
    NoReachablePeer,
    /// At least one peer was reachable and snapshot-stream requests were
    /// sent — re-fanned periodically until the deadline (see
    /// [`JOIN_REREQUEST_CAP`]) — but no `SnapshotStreamPackage` ever
    /// arrived within the budget. Live `ClusterMutation` gossip received
    /// in the window is buffered (not dropped), but gossip alone cannot
    /// bootstrap an empty mirror, so zero packages is still a failed
    /// join. The carried counters say how hard the bootstrap tried; the
    /// caller drives any whole-bootstrap retry policy.
    Timeout {
        /// Total individual snapshot requests sent across all fan-outs.
        requests_sent: usize,
        /// Number of fan-out rounds driven before the deadline.
        fan_outs: usize,
    },
    /// `send_to_peer` returned an error while delivering the
    /// snapshot request. The wrapped string is the transport's
    /// error message verbatim.
    SendFailed(String),
    /// The transport's [`PeerTransport::local_id`] is EMPTY, so the
    /// bootstrap RPC has no return address: every responder would reply
    /// to peer `""` (undeliverable / mis-routed), the joiner's mesh legs
    /// would be registered ANONYMOUSLY by the receivers' first-frame
    /// identification, and the responder-originated `PeerJoined` would
    /// record a phantom `""` member. A join without an identity can only
    /// produce a half-joined zombie, so it is refused UP FRONT — loud
    /// and terminal — instead of soaking the bootstrap budget. A
    /// transport that participates in the snapshot-bootstrap rendezvous
    /// MUST override `local_id` (see its doc).
    MissingLocalIdentity,
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoReachablePeer => f.write_str(
                "join_running_cluster: no seed peer became reachable within the connect window",
            ),
            Self::Timeout {
                requests_sent,
                fan_outs,
            } => write!(
                f,
                "join_running_cluster: no SnapshotStreamPackage within the bootstrap timeout \
                 ({requests_sent} snapshot requests sent across {fan_outs} fan-outs, re-sent \
                 until the deadline; a cluster mid primary-promotion can leave every request \
                 unanswered for the whole window)"
            ),
            Self::SendFailed(e) => write!(f, "join_running_cluster: failed to send request: {e}"),
            Self::MissingLocalIdentity => f.write_str(
                "join_running_cluster: the transport's local_id() is empty — the bootstrap \
                 RPC would carry no return address (replies route to peer \"\" and the \
                 joiner's legs register anonymously); the joining transport must override \
                 PeerTransport::local_id with its real peer-id",
            ),
        }
    }
}

impl std::error::Error for JoinError {}

/// Successful outcome of [`PeerTransport::join_running_cluster`].
///
/// `payloads` are the collected snapshot-stream package payloads
/// (base64-wrapped CBOR partial snapshots — opaque at this layer; the
/// protocol crate stays free of `ClusterStateSnapshot<I>`). The caller
/// decodes each and `restore()`s each — the idempotent lattice unions
/// duplicates and partials regardless of order.
///
/// `live_frames` is the live `ClusterMutation` gossip that arrived
/// DURING the bootstrap window. Previously these frames were
/// warn-dropped ("the next broadcast covers it" — except one-shot facts
/// it never re-covers); now they are returned so the caller applies
/// them after restoring `payloads`. By the CRDT join's commutativity +
/// idempotence, apply-after-restore reaches exactly the state
/// concurrent application would. Cadence / announce traffic
/// (keepalives, digests, election frames) is still not buffered — a
/// joiner is not yet a participant in those exchanges and they carry no
/// replicated facts.
#[derive(Debug)]
pub struct JoinBootstrap<I> {
    pub payloads: Vec<String>,
    pub live_frames: Vec<DistributedMessage<I>>,
}

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
    /// [`DistributedMessage::RequestSnapshotStream`] frame
    /// [`Self::join_running_cluster`] constructs) and to skip the
    /// joiner's own entry when iterating the seed list.
    ///
    /// Default impl returns the empty string: transports whose join
    /// path never needs a self-identifying return address (any
    /// transport that pre-wires its
    /// mesh) compile cleanly without overriding. A transport that
    /// participates in the snapshot-bootstrap rendezvous MUST override
    /// to return its real id: the value is the request's `sender_id` —
    /// the address every responder REPLIES to, the key the remote
    /// accept loop registers this node's mesh leg under (first-frame
    /// identification), and the id the responder's `PeerJoined`
    /// broadcast records. [`Self::join_running_cluster`] refuses an
    /// empty identity up front ([`JoinError::MissingLocalIdentity`]):
    /// a de-role refactor once dropped the QUIC override and production
    /// joiners bootstrapped as the phantom peer `""` — replies
    /// undeliverable, membership recording a nameless member.
    fn local_id(&self) -> &str {
        ""
    }

    /// Bootstrap-from-snapshot orchestration for a late-joiner / fresh
    /// observer.
    ///
    /// Semantics — re-requesting RPC (one bootstrap, many request
    /// fan-outs):
    /// 1. Wire up the peer mesh by calling [`Self::connect_to_peers`]
    ///    with `seed`. Transports that pre-wire (channel,
    ///    [`crate::PeerTransport`] mocks) treat this as a no-op.
    /// 2. Wait briefly for at least one peer to register as connected
    ///    by polling [`Self::peer_count`] up to a small slice of the
    ///    total budget. If no peer is reachable, surface
    ///    [`JoinError::NoReachablePeer`] — the caller decides between
    ///    abort vs. retry-with-a-different-seed.
    /// 3. Construct a [`DistributedMessage::RequestSnapshotStream`]
    ///    envelope tagged with [`Self::local_id`] as the responder's
    ///    return address, a per-(join, responder) minted `stream_id`,
    ///    and `is_observer` declaring the joiner's own role (so the
    ///    responder's `PeerJoined` broadcast carries the truth instead
    ///    of assuming observer).
    /// 4. Send it via [`Self::send_to_peer`] to EVERY reachable non-self
    ///    seed id (multi-responder fan-out). The receiver-side stream
    ///    responders (primary, secondary router, observer) accept the
    ///    request from any peer (`cluster_state` is replicated) and
    ///    answer with a sequence of unicast bounded
    ///    [`DistributedMessage::SnapshotStreamPackage`] frames, one per
    ///    responder-loop wakeup.
    ///
    ///    The request is addressed by concrete peer-id, not by role: a
    ///    cold-start joiner cannot resolve `Destination::Primary` yet
    ///    (it has observed no `PrimaryChanged`). Fanning to ALL seeds —
    ///    rather than the first that accepts — is the primary-preferred /
    ///    completeness fix: the first reachable seed may be a secondary
    ///    whose own roster is incomplete (the pre-mesh
    ///    `secondary_capacities` desync), so a SINGLE stream could
    ///    bootstrap the joiner from a partial mirror. Collecting every
    ///    responder's packages and letting the caller `restore()` each
    ///    one heals via the idempotent lattice (the union is complete iff
    ///    ANY responder — the primary above all — was complete).
    ///
    /// 5. Drive [`Self::recv_peer`] inside a `tokio::time::timeout`,
    ///    COLLECTING every [`DistributedMessage::SnapshotStreamPackage`]
    ///    that arrives until either every responder the request reached
    ///    has delivered its `done` package or the bootstrap budget
    ///    expires. Live `ClusterMutation` gossip received in the window
    ///    is BUFFERED and returned on [`JoinBootstrap::live_frames`]
    ///    (pre-stream it was warn-dropped, which lost one-shot facts);
    ///    cadence/announce traffic is debug-ignored as before.
    ///
    ///    The fan-out is RE-SENT on a fixed cadence (`recv_budget / 3`,
    ///    capped at [`JOIN_REREQUEST_CAP`]) until the deadline, skipping
    ///    responders whose stream completed. A re-request to a responder
    ///    with an INCOMPLETE stream repeats the SAME `stream_id` and
    ///    carries the last seen package `cursor` as `resume_after`, so
    ///    the responder resumes from where the stream broke instead of
    ///    restarting (the resume-from-cursor shape of the old re-request
    ///    cadence). The first fan-out fires the instant the FIRST leg
    ///    registers, so it races leg establishment and responder-seat
    ///    churn — re-requesting until the deadline heals both within the
    ///    same budget. Responders are idempotent under duplicates
    ///    (per-stream reposition + CRDT `PeerJoined`).
    ///
    /// Returns the collected [`JoinBootstrap`]: every package payload
    /// (opaque base64-CBOR partial snapshots — at least one on `Ok`)
    /// plus the buffered live gossip. The caller decodes each payload
    /// into its own concrete `ClusterStateSnapshot<I>` and `restore()`s
    /// each, then applies the gossip (the idempotent lattice makes the
    /// ordering immaterial). The protocol crate stays free of
    /// `ClusterStateSnapshot<I>` — the wire-side `String` keeps `I`
    /// erased at the transport boundary; see the dispatch.rs commentary
    /// on the same direction-of-dependency point.
    ///
    /// **Single concern**: bootstrap rendezvous + snapshot-stream RPC.
    /// The caller's concern is cluster-state restoration (one `restore`
    /// per returned payload, then the gossip) and any retry policy if
    /// `Err` comes back. The loop above never touches `ClusterState`
    /// directly — the dependency edge is preserved (protocol crate does
    /// not depend on manager-distributed).
    fn join_running_cluster(
        &mut self,
        seed: &[crate::PeerConnectionInfo],
        timeout: Duration,
        is_observer: bool,
        can_be_primary: bool,
    ) -> impl std::future::Future<Output = Result<JoinBootstrap<I>, JoinError>>
    where
        I: 'static,
    {
        async move {
            // Step 0: identity gate. The bootstrap RPC's `sender_id` is
            // the joiner's return address AND the id receivers key its
            // mesh legs under (first-frame identification) AND the
            // member id the responders' `PeerJoined` records. An EMPTY
            // id poisons all three — replies route to peer `""`, the
            // legs register anonymously, membership records a phantom —
            // and the failure is otherwise SILENT (a lucky direct leg
            // can even deliver one reply, sliding the joiner into a
            // half-joined zombie instead of a loud error). Refuse it
            // before any wire work.
            if self.local_id().is_empty() {
                return Err(JoinError::MissingLocalIdentity);
            }
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
            // dispatch.rs's RequestSnapshotStream handler; collecting
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

            // Step 3+4+5: periodic request fan-out + reply collection,
            // ONE loop until the bootstrap deadline.
            //
            // Each fan-out sends the request to EVERY reachable non-self
            // seed that has not answered yet (multi-responder fan-out).
            // The joiner's cold-start role table can't resolve
            // `Destination::Primary` yet (no `PrimaryChanged` observed),
            // and any peer can answer per the dispatch.rs / primary
            // snapshot handlers. We address the transport directly by
            // peer-id (no role resolution — the id IS the host). Fanning
            // to ALL seeds (not first-success) is the completeness fix:
            // the first reachable seed may be a secondary holding an
            // incomplete roster, so a single reply could bootstrap from a
            // partial snapshot. Collecting every responder's snapshot and
            // `restore()`-ing each (idempotent lattice) heals — the union
            // is complete iff ANY responder (the primary above all) was
            // complete. Per-peer send errors (`no route`, `outgoing
            // channel closed`) are tolerated; a peer the fan-out misses
            // (a leg still dialing, a retired seed) is simply retried on
            // the next round.
            //
            // WHY periodic (not one-shot): the first fan-out fires the
            // instant the FIRST leg registers, racing both leg
            // establishment (an unconnected seed gets at best a relayed
            // request whose reply has no return wire to this brand-new
            // joiner yet) and responder-seat churn (a joiner dialing
            // inside a primary-promotion window can lose EVERY first-shot
            // request while gossip keeps arriving). All of those losses
            // are transient; re-requesting until the deadline heals them
            // within the SAME bootstrap budget. Responders are idempotent
            // under duplicate requests (digest-keyed snapshot-serialize
            // cache + CRDT-idempotent `PeerJoined` origination).
            let recv_budget = timeout.saturating_sub(connect_budget);
            let recv_deadline = tokio::time::Instant::now() + recv_budget;
            // Cadence: a third of the reply budget so every budget gets
            // at least two re-request opportunities, capped for long
            // budgets. The `max(1ms)` floor only guards degenerate
            // caller-supplied budgets from a zero-interval spin.
            let rerequest_interval = (recv_budget / 3)
                .min(JOIN_REREQUEST_CAP)
                .max(Duration::from_millis(1));
            let mut next_fan_out = tokio::time::Instant::now();
            let mut fan_outs: usize = 0;
            let mut requests_sent: usize = 0;
            let mut send_err: Option<String> = None;
            // Per-responder stream progress, keyed by the peer a request
            // REACHED (entry created on first successful send). Holds the
            // stream_id minted for that responder (repeated verbatim on
            // every re-request so a responder still holding the stream
            // RESUMES instead of restarting), the last package cursor
            // (echoed as `resume_after`), and the done latch. Packages
            // answering a stale stream_id (a previous join attempt) still
            // contribute their payload — restore is idempotent — but
            // never advance this accounting.
            struct StreamProgress {
                stream_id: String,
                cursor: Option<String>,
                done: bool,
            }
            // Join-epoch mint: unique per call within this process, so a
            // retried whole-bootstrap never aliases a previous attempt's
            // stream ids. Combined with the cluster-unique `local_id` +
            // responder id, the minted id is unique without an RNG (this
            // deterministic runtime deliberately has none).
            static JOIN_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let join_epoch = JOIN_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut streams: std::collections::HashMap<String, StreamProgress> = Default::default();
            let mut payloads: Vec<String> = Vec::new();
            // Live gossip buffered (not dropped) during the window; see
            // `JoinBootstrap::live_frames`. Bounded so a pathological
            // gossip storm cannot balloon the joiner's memory — overflow
            // degrades to the pre-stream drop behaviour, counted + warned.
            const JOIN_LIVE_BUFFER_CAP: usize = 8192;
            let mut live_frames: Vec<DistributedMessage<I>> = Vec::new();
            let mut live_dropped: usize = 0;
            loop {
                if !streams.is_empty() && streams.values().all(|p| p.done) {
                    // Every responder we reached has streamed to `done`;
                    // no point waiting out the rest of the budget.
                    if live_dropped > 0 {
                        tracing::warn!(
                            live_dropped,
                            "join_running_cluster: live-gossip buffer overflowed; \
                             dropped frames heal via anti-entropy"
                        );
                    }
                    return Ok(JoinBootstrap {
                        payloads,
                        live_frames,
                    });
                }
                let now = tokio::time::Instant::now();
                if now >= recv_deadline {
                    // Budget expired. At least one package collected is
                    // success (the caller unions the partials and the
                    // post-join anti-entropy cadence resumes the rest
                    // from the cursor); zero collected surfaces `Timeout`
                    // — the operator-visible signal is identical to the
                    // cold-start no-reply case, and the carried counters
                    // say how hard the bootstrap tried.
                    return if payloads.is_empty() {
                        Err(JoinError::Timeout {
                            requests_sent,
                            fan_outs,
                        })
                    } else {
                        Ok(JoinBootstrap {
                            payloads,
                            live_frames,
                        })
                    };
                }
                if now >= next_fan_out {
                    fan_outs += 1;
                    for peer in seed {
                        if peer.secondary_id == local_id
                            || streams
                                .get(&peer.secondary_id)
                                .is_some_and(|p| p.done)
                        {
                            continue;
                        }
                        // Mint once per responder; a re-request to an
                        // incomplete stream repeats the SAME id and
                        // resumes from the last seen cursor.
                        let (stream_id, resume_after) = match streams.get(&peer.secondary_id) {
                            Some(p) => (p.stream_id.clone(), p.cursor.clone()),
                            None => (
                                format!("{local_id}/join{join_epoch}/{}", peer.secondary_id),
                                None,
                            ),
                        };
                        let request = DistributedMessage::RequestSnapshotStream {
                            target: None,
                            sender_id: local_id.clone(),
                            timestamp: timestamp_now(),
                            stream_id: stream_id.clone(),
                            resume_after,
                            // The joiner declares its own role + capability
                            // so the responder broadcasts a truthful
                            // `PeerJoined` rather than assuming observer /
                            // incapable.
                            is_observer,
                            can_be_primary,
                        };
                        match self.send_to_peer(&peer.secondary_id, request).await {
                            Ok(()) => {
                                requests_sent += 1;
                                streams.entry(peer.secondary_id.clone()).or_insert(
                                    StreamProgress {
                                        stream_id,
                                        cursor: None,
                                        done: false,
                                    },
                                );
                            }
                            Err(e) => {
                                send_err = Some(e);
                            }
                        }
                    }
                    // First-shot contract preserved: a FIRST fan-out that
                    // reaches nobody (e.g. a seed list naming only self)
                    // fails fast as `SendFailed` instead of burning the
                    // budget. Later fan-outs are best-effort — a transient
                    // all-sends-failed round is exactly what the next
                    // round heals.
                    if fan_outs == 1 && streams.is_empty() {
                        return Err(JoinError::SendFailed(
                            send_err.unwrap_or_else(|| "no seed peer accepted send".into()),
                        ));
                    }
                    next_fan_out = now + rerequest_interval;
                }
                // Wait for the next frame, bounded by whichever comes
                // first: the re-request tick or the bootstrap deadline.
                let wait_until = next_fan_out.min(recv_deadline);
                let remaining = wait_until.saturating_duration_since(tokio::time::Instant::now());
                let recv = tokio::time::timeout(remaining, self.recv_peer()).await;
                match recv {
                    Err(_) => {
                        // Re-request tick or deadline elapsed mid-recv;
                        // the loop head decides which.
                        continue;
                    }
                    Ok(None) => {
                        // Transport's inbound channel closed: no more
                        // messages will ever arrive. Return whatever we
                        // collected; empty surfaces as timeout (identical
                        // operator-visible signal, cause shows up in the
                        // transport's own teardown logs).
                        return if payloads.is_empty() {
                            Err(JoinError::Timeout {
                                requests_sent,
                                fan_outs,
                            })
                        } else {
                            Ok(JoinBootstrap {
                                payloads,
                                live_frames,
                            })
                        };
                    }
                    // Accept the package REGARDLESS of its Phase-C target
                    // stamp. The responder's coordinator egress stamps
                    // every frame with the resolved role-typed return
                    // address, and the frame's ARRIVAL on this node's
                    // wire already satisfies the host-addressing — the
                    // stamp is only the mesh-pump's slot-demux hint, and
                    // the bootstrap window has no role slots (the same
                    // never-drop-on-stamp ingress rule as
                    // `Mesh::route_incoming`). `None` (a pre-stamp
                    // transport or test double) is equally accepted. A
                    // `target: None` pattern here once dropped every
                    // production reply until the budget expired — the
                    // gateway late-joiner Test-1a bootstrap timeout.
                    Ok(Some(DistributedMessage::SnapshotStreamPackage {
                        sender_id,
                        stream_id,
                        cursor,
                        payload,
                        done,
                        ..
                    })) => {
                        payloads.push(payload);
                        // Progress accounting only for the stream WE
                        // minted for this responder; a stale-stream
                        // package contributed its payload above and is
                        // otherwise ignored.
                        if let Some(p) = streams.get_mut(&sender_id)
                            && p.stream_id == stream_id
                        {
                            if cursor.is_some() {
                                p.cursor = cursor;
                            }
                            p.done |= done;
                        }
                        continue;
                    }
                    Ok(Some(other)) => {
                        // Live replicated-state gossip is BUFFERED for the
                        // caller (pre-stream it was warn-dropped, losing
                        // one-shot facts until anti-entropy healed them);
                        // cadence / announce traffic stays ignored — a
                        // joiner is not yet a participant in those
                        // exchanges and they carry no replicated facts.
                        if matches!(
                            other.msg_type(),
                            crate::messages::MessageType::ClusterMutation
                        ) {
                            if live_frames.len() < JOIN_LIVE_BUFFER_CAP {
                                live_frames.push(other);
                            } else {
                                live_dropped += 1;
                            }
                        } else {
                            tracing::debug!(
                                kind = ?other.msg_type(),
                                target = ?other.target(),
                                "join_running_cluster: ignored non-gossip frame in bootstrap window"
                            );
                        }
                        continue;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PeerConnectionInfo;

    /// A transport that LOOKS healthy (one connected peer, sends accepted)
    /// but keeps the trait-default empty `local_id` — the exact shape the
    /// QUIC `PeerNetwork` regressed into when a de-role refactor deleted
    /// its override. Records whether any wire work happened so the guard
    /// test can pin "refused BEFORE the wire".
    struct AnonymousTransport {
        sends: usize,
    }

    impl PeerTransport<String> for AnonymousTransport {
        async fn broadcast(&mut self, _msg: DistributedMessage<String>) -> Result<(), String> {
            self.sends += 1;
            Ok(())
        }
        async fn send_to_peer(
            &mut self,
            _peer_id: &str,
            _msg: DistributedMessage<String>,
        ) -> Result<(), String> {
            self.sends += 1;
            Ok(())
        }
        async fn recv_peer(&mut self) -> Option<DistributedMessage<String>> {
            std::future::pending().await
        }
        fn try_recv_peer(&mut self) -> Option<DistributedMessage<String>> {
            None
        }
        fn peer_count(&self) -> usize {
            1
        }
        fn has_peer(&self, _id: &PeerId) -> bool {
            true
        }
        async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
    }

    /// An identity-less transport must be refused at the join ENTRY —
    /// the typed [`JoinError::MissingLocalIdentity`], zero requests sent,
    /// no budget soaked. Pre-guard, this shape sent anonymous requests
    /// (`sender_id: ""`) whose replies could never route back: the
    /// bootstrap either died `Timeout` at its deadline or — when one
    /// lucky direct leg delivered a reply addressed to peer `""` — slid
    /// into a half-joined state under a phantom identity with NO error
    /// at all (the production silent-failure shape).
    #[tokio::test]
    async fn join_refuses_an_empty_local_identity_up_front() {
        let mut transport = AnonymousTransport { sends: 0 };
        let seed = vec![PeerConnectionInfo {
            secondary_id: "secondary-0".into(),
            cert: String::new(),
            ipv4: Some("127.0.0.1".into()),
            ipv6: None,
            port: 1,
            is_observer: false,
            liveness_port: None,
        }];
        let started = std::time::Instant::now();
        let result = transport
            .join_running_cluster(&seed, Duration::from_secs(30), true, false)
            .await;
        assert!(
            matches!(result, Err(JoinError::MissingLocalIdentity)),
            "an empty local_id must be refused with the typed error, got {result:?}"
        );
        assert_eq!(
            transport.sends, 0,
            "the refusal must precede any wire work — no anonymous request may leave"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the refusal is immediate, never a budget soak"
        );
    }
}
