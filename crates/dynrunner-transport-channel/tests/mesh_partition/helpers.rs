//! Shared fixtures and the pump driver for the mesh_partition
//! integration scenarios. Imports + helper functions lifted verbatim
//! from the pre-split mesh_partition.rs (lines 38-285); items are
//! `pub(crate)` so the scenario sub-files can reach them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::{DistributedMessage, KeepaliveRole, PeerTransport};
use dynrunner_transport_channel::{ChannelPeerTransport, peer_mesh_with_adjacency};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct TestId(pub(crate) String);

/// Convert a `&[&str]` of peer ids into the owned-`String` shape the
/// public mesh constructor wants.
pub(crate) fn ids(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// Convert a `&[(&str, &str)]` adjacency list into the owned-`String`
/// pair shape the public mesh constructor wants.
pub(crate) fn links(items: &[(&str, &str)]) -> Vec<(String, String)> {
    items
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// Build a partial mesh keyed by peer-id for clean borrow-by-key
/// access in the pump driver. The construction order is the order
/// the caller passed in `peer_ids`; we re-key into a `HashMap` after
/// the fact since `peer_mesh_with_adjacency` returns a `Vec`.
pub(crate) fn build_mesh(
    peer_ids: &[&str],
    link_pairs: &[(&str, &str)],
) -> HashMap<String, ChannelPeerTransport<TestId>> {
    let id_vec = ids(peer_ids);
    let link_vec = links(link_pairs);
    let transports = peer_mesh_with_adjacency::<TestId>(&id_vec, &link_vec);
    id_vec.into_iter().zip(transports).collect()
}

/// Sample payload — a `Keepalive` is the cheapest non-routing
/// `DistributedMessage` variant and round-trips identically through
/// every relay decision.
pub(crate) fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: sender.to_string(),
        timestamp: 1.0,
        secondary_id: sender.to_string(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// One captured `tracing` event from the relay log target. The
/// `target` field is redundant given the layer pre-filters on
/// target == `"dynrunner_relay"`, but keeping it keeps the trace
/// shape symmetric with `dynrunner-transport-quic`'s
/// `CapturedEvent` and future-proofs scenarios that want to relax
/// the pre-filter.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CapturedRecord {
    pub(crate) target: String,
    pub(crate) message: String,
}

/// Field visitor that pulls the formatted message body out of an
/// `Event`. Modeled on the same shape used in
/// `dynrunner-transport-quic/src/peer/tests.rs` — the `tracing`
/// macros encode the message as a field named `message`.
struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// `tracing-subscriber` layer that appends every relay-target event
/// it sees to a shared buffer. We pre-filter on the layer side
/// (target == `"dynrunner_relay"`) so unrelated events from other
/// crates' instrumentation don't dilute the trace.
struct RelayCaptureLayer {
    records: Arc<Mutex<Vec<CapturedRecord>>>,
}

impl<S: tracing::Subscriber> Layer<S> for RelayCaptureLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target().to_string();
        if target != "dynrunner_relay" {
            return;
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        if let Ok(mut buf) = self.records.lock() {
            buf.push(CapturedRecord {
                target,
                message: visitor.0,
            });
        }
    }
}

/// Run `body` with a thread-local `tracing` subscriber that captures
/// every `dynrunner_relay`-target event. Returns `(body_output,
/// captured_records)`. Use this in scenarios that need to assert on
/// the relay-path log trace (e.g. the originator-drop log in
/// scenario 4).
///
/// Caveat: `set_default` installs the subscriber thread-locally. The
/// scenarios in this file run under `#[tokio::test]`'s default
/// `current_thread` runtime and do not `tokio::spawn` to other
/// threads, so every event surfaces through this layer. A future
/// scenario that crosses thread boundaries would need
/// `set_global_default` (and serial-execution gating against other
/// tests).
pub(crate) async fn with_relay_log_capture<F, T>(body: F) -> (T, Vec<CapturedRecord>)
where
    F: std::future::Future<Output = T>,
{
    let records: Arc<Mutex<Vec<CapturedRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = RelayCaptureLayer {
        records: records.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);
    let result = body.await;
    let captured = records.lock().unwrap().clone();
    (result, captured)
}

/// Pump every transport's `recv_peer` round-robin (with a tiny
/// per-call timeout) until `done` returns `true` or the deadline
/// expires. Returns `true` iff `done` succeeded before the deadline.
///
/// Captured deliveries are appended to `delivered` keyed by recipient
/// peer-id so the closure can inspect them. The transports map is
/// borrowed exclusively for the duration of each iteration; no two
/// `recv_peer` calls overlap.
pub(crate) async fn pump_until_with_deadline<F>(
    transports: &mut HashMap<String, ChannelPeerTransport<TestId>>,
    delivered: &mut HashMap<String, Vec<DistributedMessage<TestId>>>,
    deadline: Instant,
    mut done: F,
) -> bool
where
    F: FnMut(&HashMap<String, Vec<DistributedMessage<TestId>>>) -> bool,
{
    // Per-transport recv timeout. Tiny so a quiescent inbox doesn't
    // hold up the round-robin; the outer wall-clock deadline still
    // bounds total time. `recv_peer`'s sole `.await` point is on
    // `mpsc::UnboundedReceiver::recv()` which is cancellation-safe,
    // so dropping the future on timeout leaves the receiver intact.
    let recv_slice = Duration::from_millis(5);
    // Iterate by sorted key so the trace is reproducible across
    // platforms — the standard library's HashMap iteration is
    // randomized, and the relay invariants we assert depend on
    // forwarder-id ordering rather than iteration order, so this is
    // a no-op for correctness; it just stabilises the trace order.
    let keys: Vec<String> = {
        let mut ks: Vec<String> = transports.keys().cloned().collect();
        ks.sort();
        ks
    };
    loop {
        let mut progressed = false;
        for k in &keys {
            if let Some(t) = transports.get_mut(k) {
                // Async `recv_peer` is the only path that forwards
                // Relay envelopes (Router::process_inbound, vs the
                // sync `try_recv_peer` which drops them).
                match tokio::time::timeout(recv_slice, t.recv_peer()).await {
                    Ok(Some(msg)) => {
                        delivered.entry(k.clone()).or_default().push(msg);
                        progressed = true;
                    }
                    Ok(None) => {
                        // Inbox closed — peer gone. Skip on
                        // subsequent iterations by leaving the
                        // entry alone; HashMap removal would race
                        // with the borrow.
                    }
                    Err(_) => {
                        // Timeout: empty inbox or only routing
                        // envelopes that resolved without
                        // delivering. Move on.
                    }
                }
            }
        }
        if done(delivered) {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        if !progressed {
            tokio::task::yield_now().await;
        }
    }
}

/// Pump with the standard 5s wall-clock abort deadline.
pub(crate) async fn pump_until<F>(
    transports: &mut HashMap<String, ChannelPeerTransport<TestId>>,
    delivered: &mut HashMap<String, Vec<DistributedMessage<TestId>>>,
    done: F,
) -> bool
where
    F: FnMut(&HashMap<String, Vec<DistributedMessage<TestId>>>) -> bool,
{
    pump_until_with_deadline(
        transports,
        delivered,
        Instant::now() + Duration::from_secs(5),
        done,
    )
    .await
}

/// Convenience: pump until the named recipient has at least one
/// delivered message.
pub(crate) async fn pump_until_received(
    transports: &mut HashMap<String, ChannelPeerTransport<TestId>>,
    delivered: &mut HashMap<String, Vec<DistributedMessage<TestId>>>,
    target: &str,
) -> bool {
    let target = target.to_string();
    pump_until(transports, delivered, |d| {
        d.get(&target).map(|v| !v.is_empty()).unwrap_or(false)
    })
    .await
}
