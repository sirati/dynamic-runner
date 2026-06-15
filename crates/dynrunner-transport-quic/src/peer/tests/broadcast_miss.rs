//! `broadcast_miss_*` — broadcast honesty (#363): a KNOWN peer (in the
//! authoritative `peer_dial_info` roster) with no live `connections`
//! entry silently misses every broadcast; the transport names the gap
//! with a WARN, rate-limited to once per peer per OUTAGE (the
//! reconnect tracker's `first_broadcast_miss` latch, reset by the
//! heal).
//!
//! Observability-only: `broadcast`'s return type / delivery semantics
//! are unchanged. These tests pin (a) the once-per-outage shape — one
//! WARN on the first missed broadcast of an outage, silence on every
//! further broadcast of the SAME outage, and (b) the re-arm — a heal
//! followed by a fresh outage earns a fresh WARN, while a connected
//! peer never warns.

use std::sync::{Arc, Mutex};

use super::super::PeerNetwork;
use super::TestId;
use super::log_capture::{CaptureLayer, CapturedEvent};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerTransport,
};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// A distinctive non-routable address so the missed peer can never
/// incidentally connect.
const UNROUTABLE_IPV4: &str = "10.255.255.254";
const UNROUTABLE_PORT: u16 = 59124;

/// The captured broadcast-miss WARNs (matched on the message text the
/// `broadcast` sweep emits — the tracker's own "peer disconnect
/// observed" / milestone WARNs do not contain it).
fn miss_events(records: &Arc<Mutex<Vec<CapturedEvent>>>) -> Vec<CapturedEvent> {
    records
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.message.contains("broadcast missed known peer"))
        .cloned()
        .collect()
}

/// Build a peer (NOT self) with an unroutable address, registered in
/// `peer_dial_info` but absent from `connections` — the exact
/// known-but-unconnected shape the broadcast-miss sweep names.
fn unreachable_peer_info() -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: "peer-z".into(),
        cert: String::new(),
        ipv4: Some(UNROUTABLE_IPV4.into()),
        ipv6: None,
        port: UNROUTABLE_PORT,
        is_observer: false,
        liveness_port: None,
        slurm_job_id: None,
    }
}

fn keepalive() -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: "peer-a".into(),
        timestamp: 1.0,
        secondary_id: "peer-a".into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn broadcast_miss_warns_once_per_outage() {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let info = unreachable_peer_info();
            peer_a
                .peer_dial_info
                .insert(info.secondary_id.clone(), info);

            // Mesh-forming window: known but not yet TRACKED as
            // disconnected (no reconcile tick has run). A broadcast
            // here stays silent — a dial may still be in flight, and
            // the gate only names peers the tracker already considers
            // in outage.
            peer_a.broadcast(keepalive()).await.unwrap();
            assert!(
                miss_events(&records).is_empty(),
                "untracked known-but-unconnected peer must not warn \
                 (mesh-forming window); got {:#?}",
                miss_events(&records)
            );

            // The reconcile tick tracks the peer as disconnected
            // (peer_dial_info ∖ connections).
            peer_a.process_reconnect_tick();

            // First missed broadcast of the outage: exactly one WARN.
            peer_a.broadcast(keepalive()).await.unwrap();
            let events = miss_events(&records);
            assert_eq!(
                events.len(),
                1,
                "exactly one broadcast-miss WARN on the first missed \
                 broadcast of an outage; got {events:#?}"
            );
            assert!(
                events[0].fields.contains("peer-z"),
                "the WARN must name the missed peer; got fields {:?}",
                events[0].fields
            );

            // Every further broadcast of the SAME outage: silent.
            peer_a.broadcast(keepalive()).await.unwrap();
            peer_a.broadcast(keepalive()).await.unwrap();
            assert_eq!(
                miss_events(&records).len(),
                1,
                "a persistently-down peer warns once per outage, not per \
                 broadcast; got {:#?}",
                miss_events(&records)
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn broadcast_miss_rearms_after_heal_and_fresh_outage() {
    let records: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let info = unreachable_peer_info();
            let peer_id = info.secondary_id.clone();
            peer_a.peer_dial_info.insert(peer_id.clone(), info);

            // Outage #1: tracked, one WARN on the first missed broadcast.
            peer_a.process_reconnect_tick();
            peer_a.broadcast(keepalive()).await.unwrap();
            assert_eq!(miss_events(&records).len(), 1, "outage #1 warns once");

            // Heal: inject a live connection entry (the held `_rx`
            // keeps the channel open so the broadcast send succeeds);
            // the reconcile tick observes the reconnect and clears the
            // tracked entry — and with it the per-outage latch.
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            peer_a.connections.insert(peer_id.clone(), tx);
            peer_a.process_reconnect_tick();

            // Connected peer: broadcasts reach it, no new WARN.
            peer_a.broadcast(keepalive()).await.unwrap();
            assert_eq!(
                miss_events(&records).len(),
                1,
                "a connected peer must never warn; got {:#?}",
                miss_events(&records)
            );

            // Outage #2: the connection drops again; once re-tracked,
            // the next missed broadcast earns a FRESH WARN.
            peer_a.connections.remove(&peer_id);
            peer_a.process_reconnect_tick();
            peer_a.broadcast(keepalive()).await.unwrap();
            assert_eq!(
                miss_events(&records).len(),
                2,
                "a fresh outage re-arms the once-per-outage WARN; got {:#?}",
                miss_events(&records)
            );
        })
        .await;
}
