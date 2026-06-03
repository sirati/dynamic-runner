//! Peer-mesh constructors. Build one [`ChannelPeerTransport`] per
//! peer-id and wire their outboxes either fully-connected
//! ([`peer_mesh`]) or per a caller-supplied undirected adjacency
//! list ([`peer_mesh_with_adjacency`]).

use std::collections::{HashMap, HashSet};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, Router};
use tokio::sync::mpsc;

use crate::peer_transport::ChannelPeerTransport;

/// Build the full set of unordered pairs `(a, b)` where `a < b` in
/// input order, given a peer-id list. The shared subroutine behind
/// the all-to-all [`peer_mesh`] adjacency.
fn all_undirected_pairs(ids: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(ids.len() * ids.len().saturating_sub(1) / 2);
    for (i, a) in ids.iter().enumerate() {
        for b in &ids[i + 1..] {
            out.push((a.clone(), b.clone()));
        }
    }
    out
}

/// Wire up an all-to-all peer mesh for the given ids and return one
/// [`ChannelPeerTransport`] per id, in input order. Each transport's
/// outbox map contains every other peer's inbox sender.
///
/// Delegates to [`peer_mesh_with_adjacency`] with the full set of
/// unordered pairs so a single adjacency wiring path serves both
/// fully-connected and partial-mesh test fixtures.
pub fn peer_mesh<I: Identifier>(peer_ids: &[String]) -> Vec<ChannelPeerTransport<I>> {
    let links = all_undirected_pairs(peer_ids);
    peer_mesh_with_adjacency(peer_ids, &links)
}

/// Wire up a partial peer mesh defined by an explicit adjacency list
/// and return one [`ChannelPeerTransport`] per id, in `peer_ids`
/// input order. Each `(a, b)` link is **undirected**: a's outbox gains
/// b's sender AND b's outbox gains a's sender.
///
/// # Panics
///
/// - Panics if a link references an id not present in `peer_ids` —
///   silently dropping it would half-wire a partition the test
///   thought was symmetric.
/// - Panics if the same unordered pair appears more than once
///   (including the directed-style `(a, b) + (b, a)` form). A
///   misconfigured fixture that accidentally lists a link twice would
///   half-wire a partition test exactly the way a real bug would, so
///   we refuse to construct the mesh rather than mask the typo.
pub fn peer_mesh_with_adjacency<I: Identifier>(
    peer_ids: &[String],
    links: &[(String, String)],
) -> Vec<ChannelPeerTransport<I>> {
    // Allocate inbox + outbox-sender for each peer.
    let mut inboxes: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>> = HashMap::new();
    let mut receivers: HashMap<String, mpsc::UnboundedReceiver<DistributedMessage<I>>> =
        HashMap::new();
    for id in peer_ids {
        let (tx, rx) = mpsc::unbounded_channel();
        inboxes.insert(id.clone(), tx);
        receivers.insert(id.clone(), rx);
    }

    // Build the per-peer outgoing tables from the undirected adjacency
    // list. We key by the canonical (lo, hi) ordering so a duplicate
    // — whether listed twice in the same direction or once each
    // direction — surfaces as a panic rather than silent re-insert.
    let mut outgoing: HashMap<
        String,
        HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    > = peer_ids
        .iter()
        .map(|id| (id.clone(), HashMap::new()))
        .collect();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for (a, b) in links {
        assert!(
            a != b,
            "peer_mesh_with_adjacency: self-link '{a}' is not allowed"
        );
        assert!(
            inboxes.contains_key(a),
            "peer_mesh_with_adjacency: link references unknown peer '{a}'"
        );
        assert!(
            inboxes.contains_key(b),
            "peer_mesh_with_adjacency: link references unknown peer '{b}'"
        );
        let canonical = if a <= b {
            (a.clone(), b.clone())
        } else {
            (b.clone(), a.clone())
        };
        assert!(
            seen.insert(canonical.clone()),
            "peer_mesh_with_adjacency: duplicate link {canonical:?} (adjacency is undirected; \
             list each unordered pair at most once)"
        );
        outgoing
            .get_mut(a)
            .expect("peer table allocated above")
            .insert(b.clone(), inboxes[b].clone());
        outgoing
            .get_mut(b)
            .expect("peer table allocated above")
            .insert(a.clone(), inboxes[a].clone());
    }

    let mut transports = Vec::with_capacity(peer_ids.len());
    for id in peer_ids {
        let incoming_rx = receivers
            .remove(id)
            .expect("inbox was inserted above for every id");
        let outgoing_for_peer = outgoing
            .remove(id)
            .expect("outgoing table allocated for every id");
        transports.push(ChannelPeerTransport {
            local_id: id.clone(),
            incoming_rx,
            outgoing: outgoing_for_peer,
            router: Router::new(id.clone()),
            last_outcome: None,
        });
    }
    transports
}
