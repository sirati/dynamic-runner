//! Compose an authoritative `PrimaryCoordinator` over an mpsc loopback
//! to a co-located `SecondaryCoordinator`.
//!
//! Single concern: build a real `PrimaryCoordinator` whose
//! per-secondary `SecondaryTransport` writer for THIS NODE'S OWN
//! secondary is an in-process mpsc loopback, so
//! `transport.send_to(local_secondary_id, TaskAssignment)` reaches the
//! co-located secondary's inbound exactly like any remote secondary.
//! The promotion sites (failover election + bootstrap `PromotePrimary`)
//! call this to stand up the canonical coordinator instead of running
//! the secondary's reimpl-mirror primary machine.
//!
//! Faithful adaptation of the in-process wiring the pyo3 distributed
//! manager already performs (`managers/distributed/run.rs:279-357,554`):
//! a `(pri→sec, sec→pri)` mpsc pair per secondary, the per-secondary
//! writer registered in the primary's `outgoing` map under the
//! secondary's id, and a `sec→pri` forwarder feeding the primary's
//! single `incoming_rx`. The only deviation: there is exactly one
//! loopback secondary (the local node), and its writer is keyed by the
//! node's OWN secondary_id.
//!
//! ## Boundary
//!
//! This constructor owns ONLY the primary-side channel wiring (the
//! `ChannelSecondaryTransportEnd` aggregator). The caller owns:
//!
//!   * creating the two mpsc pairs and handing the secondary side
//!     (`ChannelPrimaryTransportEnd { tx: sec→pri, rx: pri→sec }`) to
//!     the co-located `SecondaryCoordinator` so its untouched
//!     `process_tasks` loop reads from the same `pri→sec` channel this
//!     primary writes to;
//!   * spawning the `sec→pri` forwarder into `incoming_rx` (the caller
//!     can fan-out / tap exactly as the pyo3 reference does);
//!   * registering the SAME `pri→sec` writer into the node's
//!     `peer_transport` shared-outgoing view when remote secondaries
//!     are present (mirroring `run.rs:297-300` — BOTH the legacy
//!     `outgoing` map and the tunneled peer view), so `Address::Role`
//!     / `send_to_peer` dispatch reaches the loopback secondary too;
//!   * after construction: calling
//!     [`PrimaryCoordinator::hydrate_from_cluster_state`] to seed the
//!     pool/ledger from the replicated CRDT, then spawning
//!     [`PrimaryCoordinator::operational_loop`] on the same `LocalSet`
//!     that runs the secondary's `process_tasks`.
//!
//! Keeping channel creation + forwarder + peer-view registration with
//! the caller mirrors the pyo3 reference verbatim and keeps this
//! constructor a single pure concern (primary-side aggregator wiring),
//! with no knowledge of the secondary's transport type or the peer
//! transport's internal shared-writer table.

use std::collections::HashMap;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use dynrunner_transport_channel::secondary_transport::ChannelSecondaryTransportEnd;
use tokio::sync::mpsc;

use super::config::PrimaryConfig;
use super::PrimaryCoordinator;

impl<P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<ChannelSecondaryTransportEnd<I>, P, S, E, I>
{
    /// Stand up an authoritative `PrimaryCoordinator` wired to the
    /// node's co-located `SecondaryCoordinator` over an mpsc loopback.
    ///
    /// `local_secondary_id` is the registration key: the `pri→sec`
    /// writer is inserted into the primary's `outgoing` map under it,
    /// so `transport.send_to(local_secondary_id, ..)` routes a
    /// `TaskAssignment` to the loopback secondary's inbound. There is
    /// NO echo back to the primary — the channel is unidirectional
    /// (primary→secondary); the reverse direction is the caller's
    /// independent `sec→pri` forwarder into `incoming_rx`.
    ///
    /// `incoming_rx` is the primary's single aggregated inbound. The
    /// caller has already wired the `sec→pri` forwarder onto it (and
    /// may have multiplexed remote secondaries / a peer-view tap onto
    /// the same receiver). The constructor does not touch it beyond
    /// storing it on the aggregator.
    ///
    /// The remote-secondary path is served by `peer_transport` exactly
    /// as the legacy `new()` coordinator's is; the caller registers the
    /// loopback writer into the peer-view as well (see the module
    /// boundary note) so role-addressed sends reach it too.
    ///
    /// After this returns the caller MUST, in order:
    ///   1. [`PrimaryCoordinator::hydrate_from_cluster_state`] — seed
    ///      the pool / `pre_owned_in_flight` ledger from the replicated
    ///      `cluster_state` (the loopback secondary's already-in-flight
    ///      work is owned here as remote-`InFlight`, never re-offered);
    ///   2. [`PrimaryCoordinator::operational_loop`] on the same
    ///      `LocalSet` as the secondary's `process_tasks`.
    pub fn compose_with_local_secondary(
        config: PrimaryConfig,
        local_secondary_id: String,
        pri_to_sec_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
        peer_transport: P,
        scheduler: S,
        estimator: E,
    ) -> Self {
        // Register the loopback writer under the node's OWN secondary
        // id. This is the load-bearing detail: the co-located secondary
        // is addressed by `send_to(local_secondary_id, ..)` exactly like
        // any remote one — the only difference is the transport impl
        // (in-process mpsc vs QUIC). The primary's dispatch / assignment
        // code stays agnostic; it sees one more entry in `outgoing`.
        let mut outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>> =
            HashMap::new();
        outgoing.insert(local_secondary_id, pri_to_sec_tx);

        let transport = ChannelSecondaryTransportEnd {
            outgoing,
            incoming_rx,
        };

        // Delegate to the canonical constructor: the composed primary is
        // a fully ordinary `PrimaryCoordinator` from here on — same
        // cluster_state wiring, same lifecycle dispatchers, same
        // operational loop. Composition is purely a transport-wiring
        // concern; nothing downstream branches on "am I composed".
        Self::new(config, transport, peer_transport, scheduler, estimator)
    }
}
