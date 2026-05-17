//! Single concern: the atomic shutdown flag set by SIGTERM/SIGCONT
//! handlers and polled by the main loop.
//!
//! Async-signal-safety: `AtomicBool::store` on x86_64 lowers to a
//! single `mov` (with the `SeqCst` ordering we use, plus an `mfence`
//! after — still async-signal-safe; the kernel does not interrupt
//! between the instructions of a signal handler in a way the userland
//! cares about). No locks, no allocation, no heap.
//!
//! The flag is intentionally *level-triggered* (latching): once set,
//! it stays set. That's the right semantics — multiple SIGTERMs from
//! a flapping supervisor must collapse into one shutdown.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Shareable shutdown flag. Cloning yields an additional handle to the
/// same atomic.
#[derive(Clone, Debug, Default)]
pub struct ShutdownFlag {
    inner: Arc<AtomicBool>,
}

impl ShutdownFlag {
    pub fn new() -> Self {
        Self::default()
    }

    /// True once any handler has fired.
    pub fn is_set(&self) -> bool {
        self.inner.load(Ordering::SeqCst)
    }

    /// Set the flag. Called from signal handlers (async-signal-safe) and
    /// from tests via [`Self::set_for_test`].
    pub fn set(&self) {
        self.inner.store(true, Ordering::SeqCst);
    }

    /// Test-only alias for [`Self::set`] making intent explicit in tests.
    #[doc(hidden)]
    pub fn set_for_test(&self) {
        self.set();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_clear() {
        let f = ShutdownFlag::new();
        assert!(!f.is_set());
    }

    #[test]
    fn set_is_observable() {
        let f = ShutdownFlag::new();
        f.set_for_test();
        assert!(f.is_set());
    }

    #[test]
    fn clone_shares_state() {
        let a = ShutdownFlag::new();
        let b = a.clone();
        assert!(!b.is_set());
        a.set_for_test();
        assert!(b.is_set());
    }
}
