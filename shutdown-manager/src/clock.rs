//! Single concern: an abstract sleep primitive so the state machine
//! is testable without `thread::sleep`.
//!
//! The trait surface is one method: `sleep(Duration)`. Production
//! impl forwards to the stdlib. Tests use a `FakeClock` that records
//! durations slept and never blocks.

use std::time::Duration;

pub trait Clock {
    fn sleep(&self, dur: Duration);
}

/// Production clock: real wall-time `thread::sleep`.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealClock;

impl Clock for RealClock {
    fn sleep(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}
