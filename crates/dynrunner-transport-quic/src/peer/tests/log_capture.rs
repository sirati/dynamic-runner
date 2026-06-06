//! Shared `tracing` log-capture layer + the `pump_b_until` driver
//! used by the silent-reconnect scenario. Items are `pub(crate)` to
//! the test tree so the scenario file can use them without
//! duplicating the capture setup.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer};

/// One captured `tracing::Event` reduced to the two fields the
/// silent-reconnect assertions care about: the event's `target`
/// metadata (so we can scope to `dynrunner_relay`) and its
/// formatted message text (so we can match on substrings).
///
/// Visiting fields is the only way to extract the message body
/// from an `Event` without a full `fmt::Layer`. The `tracing`
/// macros encode the message as a field named `message` whose
/// value is rendered through `record_debug` for the typical
/// `tracing::warn!("...")` / `tracing::info!(target: "...", "...")`
/// invocations on the relay path.
#[derive(Debug, Clone)]
pub(crate) struct CapturedEvent {
    pub(crate) target: String,
    pub(crate) message: String,
}

/// `tracing` field visitor: extract the formatted message body of
/// an event into a `String`. The macros render the message field
/// via `record_debug`; `record_str` is wired up too for forward
/// compatibility with future tracing versions that prefer it.
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

/// `tracing-subscriber` Layer that appends every `Event` it sees
/// (regardless of target / level) to a shared buffer. Captured
/// records are inspected after the scenario completes — the
/// silent-reconnect property is "the only relay-path log lines
/// between partition and heal are exactly the two state-transition
/// observers; nothing anywhere mentions redial/reconnect", which
/// requires looking at every event rather than pre-filtering by
/// target inside the layer.
pub(crate) struct CaptureLayer {
    pub(crate) records: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target().to_string();
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        // Lock can poison if a concurrent test panics, but this
        // layer is only ever installed via `set_default` for the
        // duration of a single test on a current_thread runtime —
        // a poisoned mutex here means the scenario already failed,
        // so we just swallow the error rather than masking it.
        if let Ok(mut buf) = self.records.lock() {
            buf.push(CapturedEvent {
                target,
                message: visitor.0,
            });
        }
    }
}

/// Round-robin pump that calls `try_recv_peer` on each of the
/// passed peers until either `done(received_count)` returns
/// `true` or the wall-clock deadline expires. We use
/// `try_recv_peer` per peer instead of `recv_peer` because each
/// async call would borrow `&mut peer` exclusively for the
/// duration of the `await`, blocking the round-robin from
/// advancing other peers' state.
///
/// Caveat: `try_recv_peer` runs the **synchronous** Router path
/// which drops Relay envelopes that aren't for self with a warn
/// (see `Router::process_inbound_sync`). For a forwarder C, that
/// would defeat the test. So in this scenario the forwarder
/// (peer-c) is driven by a dedicated `recv_peer()` task spawned
/// inside the LocalSet — see `silent_reconnect_*` below.
///
/// Returns `Some(n)` with `n` = number of payload messages
/// delivered to peer-b on success; `None` on timeout.
pub(crate) async fn pump_b_until<F>(
    peer_b: &mut PeerNetwork<TestId>,
    peer_a_drain: &mut PeerNetwork<TestId>,
    deadline: std::time::Instant,
    mut done: F,
) -> Option<usize>
where
    F: FnMut(usize) -> bool,
{
    let mut received = 0usize;
    while std::time::Instant::now() < deadline {
        // Cooperative tick — yields to the runtime so accept-loop
        // tasks, redial dial tasks, and the forwarder's recv_peer
        // task can make progress.
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Drain freshly-accepted connections on A so the redial's
        // `AcceptedPeer` is observable via `peer_count()` without
        // having to call `recv_peer` (which would consume a
        // payload and complicate the assertion logic below).
        peer_a_drain.drain_new_connections();
        // Drain B's accept-loop pending registrations the same
        // way before we look at its incoming inbox.
        peer_b.drain_new_connections();
        while let Some(msg) = <PeerNetwork<TestId> as PeerTransport<TestId>>::try_recv_peer(peer_b)
        {
            // Sanity: the relayed envelope's inner is delivered
            // unwrapped (Router::process_inbound_sync's Relay-for-
            // self arm). Anything else means the Router or accept
            // loop misrouted.
            assert!(
                matches!(msg, DistributedMessage::Keepalive {
    target: None, .. }),
                "unexpected delivered variant on peer-b: {msg:?}"
            );
            received += 1;
            if done(received) {
                return Some(received);
            }
        }
        if done(received) {
            return Some(received);
        }
    }
    None
}
