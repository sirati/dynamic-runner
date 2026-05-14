use std::time::Duration;

use dynrunner_core::{Identifier, MessageReceiver};

use crate::address::{Address, Role, RoleChangeHookRegistrar, Scope};
use crate::messages::timestamp_now;
use crate::DistributedMessage;

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
pub trait SecondaryTransport<I: Identifier>:
    MessageReceiver<DistributedMessage<I>>
{
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

    /// Connect to peers from the peer list received from primary.
    fn connect_to_peers(
        &mut self,
        peers: &[crate::PeerConnectionInfo],
    ) -> impl std::future::Future<Output = ()>;

    /// Send a message via role-aware addressing.
    ///
    /// Default implementation routes the well-known address shapes to
    /// the existing primitives:
    ///   - `Address::Peer(id)` → `send_to_peer(id, msg)`
    ///   - `Address::Broadcast(Scope::Mesh)` → `broadcast(msg)`
    ///   - `Address::Role(role)` → resolve `role` through the transport's
    ///     write-through cache (Step 2), wrap `msg` in a
    ///     [`DistributedMessage::RoleAddressed`] envelope, ship via
    ///     `send_to_peer` to the resolved holder. The wrapper is what
    ///     lets the receiver detect misaddress and relay-and-hint in
    ///     Step 4 — the receiver inspects `intended_role` against its
    ///     own cache. Cache-cold lookups return `Err`: a sender that
    ///     hasn't yet observed `PromotePrimary` cannot route by role,
    ///     and silently fanning out would mask the design defect.
    ///
    /// Step 5 lifts `Address::Broadcast(Scope::AllSecondaries)` from the
    /// pre-migration `Err`-return to the same fan-out shape as
    /// `Scope::Mesh`: a primary calling `send(Broadcast(AllSecondaries))`
    /// from its peer-mesh vantage already has every-peer-is-a-secondary
    /// (the primary is not its own peer), so the wire effect is
    /// identical to `broadcast(msg)`. The semantic distinction the
    /// `Scope` enum encodes — "exclude the current primary holder" —
    /// only matters for a SECONDARY caller (who'd otherwise broadcast
    /// to a peer set that includes the primary); no such caller exists
    /// today, so the default delegates to `broadcast` and the
    /// per-impl override path stays open for the future
    /// secondary-broadcasts-to-non-primary-peers use case.
    fn send(
        &mut self,
        addr: Address,
        msg: DistributedMessage<I>,
    ) -> impl std::future::Future<Output = Result<(), String>> {
        async move {
            match addr {
                Address::Peer(id) => self.send_to_peer(&id, msg).await,
                Address::Broadcast(Scope::Mesh)
                | Address::Broadcast(Scope::AllSecondaries) => self.broadcast(msg).await,
                Address::Role(role) => {
                    // Resolve via the write-through cache (Step 2).
                    // Cache-cold is a hard error here: Step 4 lands
                    // the receiver-side relay-and-hint that warms
                    // the cache through observation; until the
                    // sender's `ClusterState` has fired its first
                    // `PromotePrimary` hook, the safe behaviour is
                    // to surface "no route" to the caller rather
                    // than broadcast or guess.
                    let holder = self.peer_for_role(&role).ok_or_else(|| {
                        format!(
                            "Address::Role({role:?}) unresolvable: role-table cache empty for \
                             this role; cluster_state has not yet observed PromotePrimary \
                             (or the equivalent role-assignment mutation)"
                        )
                    })?;
                    // Wrap in the role-addressed envelope so the
                    // receiver can detect misaddress and relay
                    // (Step 4). `sender_id` lets a misaddress hint
                    // travel back; we read it through `local_id`
                    // rather than threading it through every send
                    // call site (see trait doc for the design
                    // tradeoff).
                    let envelope = DistributedMessage::RoleAddressed {
                        sender_id: self.local_id().to_owned(),
                        timestamp: timestamp_now(),
                        intended_role: role,
                        payload: Box::new(msg),
                        attempts: 0,
                    };
                    self.send_to_peer(&holder, envelope).await
                }
            }
        }
    }

    /// Attach this transport's write-through role cache to the
    /// authoritative [`RoleTable`] owner. The registrar is the
    /// downstream `ClusterState` (or a test fixture implementing
    /// [`RoleChangeHookRegistrar`]).
    ///
    /// Default impl is a no-op: transports that don't keep a
    /// role-cache (e.g. `NoPeerTransport`, or the channel transport
    /// in tests that never exercise role addressing) compile cleanly
    /// without overriding. Real transports override to register a
    /// hook that writes their local `HashMap<Role, String>` cache
    /// whenever the authoritative table mutates — that's how Step 3
    /// gets a lock-free read of "who is primary now" on the send
    /// hot path.
    ///
    /// The registration is one-shot; callers invoke this once at
    /// coordinator construction.
    fn register_with_cluster_state(&self, _registrar: &mut dyn RoleChangeHookRegistrar) {}

    /// Look up the current id of whoever holds `role` per this
    /// transport's local write-through cache.
    ///
    /// Default impl returns `None` — transports without a cache
    /// silently report "no holder", which is the safe answer
    /// upstream (Step 3's role dispatch will surface `None` as a
    /// no-route error, not a mis-send).
    ///
    /// Real transports override to read their cached map populated
    /// by the hook registered via [`Self::register_with_cluster_state`].
    /// The returned `String` is a clone — the cache stays locked for
    /// the minimum window.
    fn peer_for_role(&self, _role: &Role) -> Option<String> {
        None
    }

    /// The local node's own peer-id, used as the `sender_id` field
    /// of envelopes the transport constructs internally — currently
    /// the `RoleAddressed` wrapper produced by [`Self::send`] when
    /// dispatching an `Address::Role(_)` send.
    ///
    /// Default impl returns the empty string. The reason for the
    /// default (over making this a required method) is that not
    /// every `PeerTransport` impl exercises role addressing — the
    /// `NoPeerTransport` arm is the canonical example — and forcing
    /// them to plumb an id just to satisfy the trait would be
    /// noise. Real impls (`ChannelPeerTransport`, `PeerNetwork`,
    /// `EitherPeerTransport`) override; both already stash the
    /// local id (channel transport via `peer_mesh`'s id parameter,
    /// `PeerNetwork.peer_id` field).
    ///
    /// The alternative — threading `sender_id` as a parameter on
    /// every `send` call site — was rejected because every call
    /// site would have to know about it; the transport already
    /// knows. The empty-string default is only observable on the
    /// role-addressing path, and a misaddress hint travelling back
    /// to an empty-string sender id is a no-op (the receiver's
    /// `send_to_peer("")` errors out cleanly) — the failure mode
    /// is contained to "cache stays cold", not "cluster
    /// corruption".
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
    ///    return address.
    /// 4. Send it via [`Self::send`] with `Address::Peer(<first
    ///    reachable seed id>)`. The receiver-side handler in
    ///    `secondary/dispatch.rs` accepts the request from any peer
    ///    (`cluster_state` is replicated, so any responder's snapshot
    ///    is valid bootstrap material) and replies with a unicast
    ///    [`DistributedMessage::ClusterSnapshot`].
    ///
    ///    NOTE: the original plan called for `Address::Role(Role::Primary)`
    ///    dispatch here so the receiver-side relay (Step 4) would
    ///    forward to whichever peer holds the primary role. That
    ///    shape doesn't work for a cold-start joiner — its role
    ///    cache is empty (no `PromotePrimary` mutation has been
    ///    observed yet), so the Step-3 `Address::Role(_)` dispatch
    ///    surfaces "role-table cache empty" and the request never
    ///    leaves the wire. Sending unicast to the first reachable
    ///    seed peer is equivalent semantically (any peer can
    ///    answer per the dispatch.rs comment) and works regardless
    ///    of cache state. The role cache is then warmed by the
    ///    `restore` path on the cluster state once the joiner
    ///    deserialises the returned snapshot — by `current_primary`
    ///    triggering [`crate::address::RoleTable.primary`] update +
    ///    role-change-hook fire.
    ///
    /// 5. Drive [`Self::recv_peer`] inside a `tokio::time::timeout`
    ///    until a [`DistributedMessage::ClusterSnapshot`] arrives, or
    ///    the bootstrap budget expires. Messages OTHER than
    ///    `ClusterSnapshot` received in the bootstrap window are
    ///    logged at `warn` and dropped — bootstrap is a one-shot
    ///    rendezvous, the cluster's CRDT-merge guarantees the next
    ///    live broadcast (or a follow-up snapshot) covers anything
    ///    dropped here.
    ///
    /// Returns the snapshot's serialized JSON payload (the
    /// `snapshot_json` field on the wire frame). The caller decodes
    /// it into their own concrete `ClusterStateSnapshot<I>` and
    /// passes that to `ClusterState::restore`. The protocol crate
    /// stays free of `ClusterStateSnapshot<I>` — the wire-side
    /// `String` keeps `I` erased at the transport boundary; see
    /// the dispatch.rs commentary on the same direction-of-
    /// dependency point.
    ///
    /// **Single concern**: bootstrap rendezvous + snapshot RPC. The
    /// caller's concern is cluster-state restoration (one `restore`
    /// call) and any retry policy if `Err` comes back. The 5-step
    /// loop above never touches `ClusterState` directly — the
    /// dependency edge is preserved (protocol crate does not depend
    /// on manager-distributed).
    fn join_running_cluster(
        &mut self,
        seed: &[crate::PeerConnectionInfo],
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<String, JoinError>>
    where
        I: 'static,
    {
        async move {
            // Step 1: dial. No-op for pre-wired transports (channel
            // mesh, tests); real work for `PeerNetwork`.
            self.connect_to_peers(seed).await;

            // Step 2: rendezvous gate. Walk the seed list, pick the
            // first id whose connection registered. Bound by a
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
            // attempt the unicast send against each non-self seed
            // id in order until one succeeds. Any peer can answer
            // per dispatch.rs's RequestClusterSnapshot handler, so
            // first-success-wins is correct on the join flow.
            loop {
                if self.peer_count() > 0 {
                    break;
                }
                if tokio::time::Instant::now() >= connect_deadline {
                    return Err(JoinError::NoReachablePeer);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }

            // Step 3+4: send the request. We use Address::Peer
            // (unicast to a reachable seed) rather than Address::Role
            // (Role::Primary) — see method-doc rationale (cold-start
            // role cache is empty). Iterate seed in order; the first
            // send_to_peer that returns Ok stops the loop. Send-side
            // errors (`no route`, `outgoing channel closed`) are
            // tolerated and we move on to the next candidate so a
            // partially-stale seed (one of the peers retired between
            // file-write and this dial) still bootstraps via the
            // remaining live peers.
            let mut send_err: Option<String> = None;
            let mut sent_to: Option<String> = None;
            for peer in seed {
                if peer.secondary_id == local_id {
                    continue;
                }
                let request = DistributedMessage::RequestClusterSnapshot {
                    sender_id: local_id.clone(),
                    timestamp: timestamp_now(),
                };
                match self
                    .send(Address::Peer(peer.secondary_id.clone()), request)
                    .await
                {
                    Ok(()) => {
                        sent_to = Some(peer.secondary_id.clone());
                        break;
                    }
                    Err(e) => {
                        send_err = Some(e);
                    }
                }
            }
            if sent_to.is_none() {
                return Err(JoinError::SendFailed(
                    send_err.unwrap_or_else(|| "no seed peer accepted send".into()),
                ));
            }

            // Step 5: wait for the ClusterSnapshot reply. Non-
            // ClusterSnapshot frames received in this window are
            // dropped with a warn log — see method-doc.
            let recv_budget = timeout.saturating_sub(connect_budget);
            let recv_deadline = tokio::time::Instant::now() + recv_budget;
            loop {
                let remaining =
                    recv_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(JoinError::Timeout);
                }
                let recv = tokio::time::timeout(remaining, self.recv_peer()).await;
                match recv {
                    Err(_) => return Err(JoinError::Timeout),
                    Ok(None) => {
                        // Transport's inbound channel closed: no more
                        // messages will ever arrive. Surface as
                        // timeout — the operator-visible signal is
                        // identical ("no snapshot in window") and the
                        // cause shows up in the transport's own
                        // teardown logs.
                        return Err(JoinError::Timeout);
                    }
                    Ok(Some(DistributedMessage::ClusterSnapshot { snapshot_json, .. })) => {
                        return Ok(snapshot_json);
                    }
                    Ok(Some(other)) => {
                        tracing::warn!(
                            kind = ?other.msg_type(),
                            "join_running_cluster: dropped non-ClusterSnapshot frame in bootstrap window"
                        );
                        continue;
                    }
                }
            }
        }
    }
}
