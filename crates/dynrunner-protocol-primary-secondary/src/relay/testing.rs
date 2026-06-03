//! Testing helpers for the relay/router stack.
//!
//! Lives outside `#[cfg(test)]` so downstream test crates (the
//! channel transport's mesh-partition suite, the QUIC silent-
//! reconnect integration test) can re-use the recording
//! [`OutboundChannel`] impl without copying it. Production code does
//! not reference any item here; the dead-code-eliminated cost in a
//! release binary is the trait vtables + struct layout for
//! [`RecordingChannel`], i.e. nothing measurable.

use std::cell::RefCell;
use std::rc::Rc;

use dynrunner_core::Identifier;

use crate::messages::DistributedMessage;
use crate::relay::channel::OutboundChannel;

/// An [`OutboundChannel`] that captures every dispatched message into
/// a shared `Vec`. The clone-cheap `Rc<RefCell<...>>` carrier lets a
/// test driver hold one read end while many `RecordingChannel`s
/// (one per peer in a connection map) push into the same log, so
/// the test can assert on the full wire trace at the end.
///
/// `dispatch` returns `Err` when the channel is `disabled` — used to
/// simulate a dead per-peer mpsc without needing a real tokio
/// runtime in the unit tests.
#[derive(Clone)]
pub struct RecordingChannel<I: Identifier> {
    log: Rc<RefCell<Vec<DispatchedRecord<I>>>>,
    addressee: String,
    disabled: Rc<RefCell<bool>>,
}

/// One row in the dispatch log: which peer the message was addressed
/// to (the connection-map key), and the message itself.
#[derive(Debug, Clone)]
pub struct DispatchedRecord<I: Identifier> {
    pub addressee: String,
    pub msg: DistributedMessage<I>,
}

impl<I: Identifier> RecordingChannel<I> {
    /// Construct a recorder addressing peer `addressee` that writes
    /// into the shared log.
    pub fn new(addressee: String, log: Rc<RefCell<Vec<DispatchedRecord<I>>>>) -> Self {
        Self {
            log,
            addressee,
            disabled: Rc::new(RefCell::new(false)),
        }
    }

    /// Disable this channel — subsequent `dispatch` calls return
    /// `Err`, simulating a dead mpsc without tearing down the test
    /// fixture.
    pub fn disable(&self) {
        *self.disabled.borrow_mut() = true;
    }
}

impl<I: Identifier> OutboundChannel<I> for RecordingChannel<I> {
    fn dispatch(&self, msg: DistributedMessage<I>) -> Result<(), ()> {
        if *self.disabled.borrow() {
            return Err(());
        }
        self.log.borrow_mut().push(DispatchedRecord {
            addressee: self.addressee.clone(),
            msg,
        });
        Ok(())
    }
}

/// Build a fresh shared log buffer.
pub fn new_log<I: Identifier>() -> Rc<RefCell<Vec<DispatchedRecord<I>>>> {
    Rc::new(RefCell::new(Vec::new()))
}
