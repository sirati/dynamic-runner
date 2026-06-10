//! Router-level integration tests (sender-side dispatch, inbound
//! relay/backoff handling, redial-cooldown gate). All public-API tests
//! for `Router<I>` live here; helper-function tests for the pure
//! routing primitives (`pick_relay`, `route_send`, `forward_step`,
//! `handle_backoff`) live alongside those primitives in `relay/mod.rs`
//! (now `relay/forwarding_tests.rs`).
//!
//! Submodule layout:
//!   - [`send`] — `send_to_peer` outbound dispatch + redial-cooldown gate.
//!   - [`inbound`] — `process_inbound` forwarding, receiver-side relay
//!     observation, backoff retry/propagate/drop, non-routing pass-through.
//!   - [`inbound_sync`] — sync-recv `process_inbound_sync` semantics.
//!   - [`prune`] — TTL eviction of outgoing-relay state and the
//!     forwarder blacklist.
//!
//! Shared helpers (`keepalive`, `conns_with_log`, `clocks_at`) live in
//! this file and are imported by each submodule via `use super::*`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;

use super::*;
use crate::messages::{DistributedMessage, KeepaliveRole};
use crate::relay::testing::{DispatchedRecord, RecordingChannel};

mod inbound;
mod inbound_sync;
mod prune;
mod routability;
mod send;

/// Trivial Identifier impl so we can build messages without
/// pulling in the real cluster types.
fn keepalive(sender: &str) -> DistributedMessage<()> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// Build a connection map populated with a `RecordingChannel` per
/// id, sharing one log buffer.
fn conns_with_log(
    ids: &[&str],
    log: &Rc<RefCell<Vec<DispatchedRecord<()>>>>,
) -> HashMap<String, RecordingChannel<()>> {
    ids.iter()
        .map(|id| {
            (
                id.to_string(),
                RecordingChannel::new(id.to_string(), log.clone()),
            )
        })
        .collect()
}

fn clocks_at(now: Instant, wire: f64) -> Clocks {
    Clocks { now, wire }
}
