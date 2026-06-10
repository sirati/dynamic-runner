//! Tunnel-child storage: the seam between the concern-blind
//! establishment engine ([`establish_one_tunnel_inner`](super::establish::establish_one_tunnel_inner))
//! and the two ways a verified `ssh -N -R` `Child` handle is held for
//! later teardown.
//!
//! # Single concern
//!
//! ONE concern: own a verified tunnel `Child` from the moment it passes
//! the establishment gate until [`SlurmPreparation::cleanup`](super::pipeline::SlurmPreparation::cleanup)
//! reaps it, and expose exactly the lifecycle operations the
//! establishment engine + cleanup need — `commit` one child, `drain`
//! them all. The engine never learns WHICH storage shape it is feeding;
//! it calls [`TunnelStore::commit`] with the secondary id + child and is
//! done.
//!
//! # The two shapes (no special-casing in the engine)
//!
//! - [`SharedTunnelVec`]: the cohort-setup + single-respawn paths. Each
//!   secondary establishes ONCE; the child is APPENDED to an anonymous
//!   shared `Vec<Child>`. There is no prior child for the id to displace
//!   (a fresh node), so the id is not even consulted — append is the
//!   whole contract, exactly as the pre-registry code did.
//!
//! - [`PerSecondaryTunnelRegistry`]: the observer-reconnect path. A
//!   reconnect REBUILDS an EXISTING secondary's dropped tunnel, so its
//!   child is keyed BY secondary id and REPLACES any prior child for that
//!   id (the displaced child is reaped). This per-id keying is what lets
//!   the reconnect cadence ask "is this secondary's tunnel still alive?"
//!   ([`PerSecondaryTunnelRegistry::is_alive`]) and NO-OP a re-fire on a
//!   healthy forward — the fix for the rc=255 release+rebind loop that
//!   blinded the observer (defect (a)). The Vec could not answer that
//!   question (anonymous, append-only, accumulating dead lingerers).
//!
//! Both shapes drain-and-terminate identically at cleanup, so the
//! `SlurmPreparation` teardown reaps either without knowing which it
//! holds.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::process::Child;
use tokio::sync::Mutex;

use super::ssh::terminate_child;

/// The commit-and-drain seam the establishment engine writes through.
///
/// `commit` is the ONLY call the engine makes; it hands over a verified
/// child keyed by secondary id and forgets it. `drain_and_terminate`
/// is the cleanup-side reap. Both `async` because draining SIGTERM/waits
/// the children and the registry replace may terminate a displaced one.
pub(super) trait TunnelStore {
    /// Take ownership of a verified tunnel `Child` for `secondary_id`.
    ///
    /// [`SharedTunnelVec`] appends (the id is irrelevant — fresh node,
    /// no prior child). [`PerSecondaryTunnelRegistry`] inserts keyed by
    /// id, terminating any displaced prior child for that id.
    async fn commit(&self, secondary_id: &str, child: Child);

    /// Reap every held child (SIGTERM → 5s → SIGKILL via
    /// [`terminate_child`]) and empty the store. Idempotent: a second
    /// call is a harmless no-op.
    async fn drain_and_terminate(&self);
}

/// The anonymous append-only child set used by the cohort-setup and
/// single-respawn paths. A newtype over the same `Arc<Mutex<Vec<Child>>>`
/// the pre-registry code threaded directly, so the shared cleanup Vec on
/// [`SlurmPreparation`](super::pipeline::SlurmPreparation) is unchanged —
/// only the access goes through the [`TunnelStore`] seam now.
#[derive(Clone)]
pub(super) struct SharedTunnelVec {
    tunnels: Arc<Mutex<Vec<Child>>>,
}

impl SharedTunnelVec {
    pub(super) fn new(tunnels: Arc<Mutex<Vec<Child>>>) -> Self {
        Self { tunnels }
    }
}

impl TunnelStore for SharedTunnelVec {
    async fn commit(&self, _secondary_id: &str, child: Child) {
        // Append: a fresh node has no prior child to displace, so the id
        // is not consulted — same effect as the pre-registry `push`.
        self.tunnels.lock().await.push(child);
    }

    async fn drain_and_terminate(&self) {
        let mut guard = self.tunnels.lock().await;
        for mut child in guard.drain(..) {
            terminate_child(&mut child).await;
        }
    }
}

/// Per-secondary tunnel registry for the observer-reconnect path: one
/// live `Child` per secondary id, REPLACED (old reaped) on each rebuild.
///
/// This is the layer that gives the reconnect cadence its success
/// signal. The cadence re-fires every ~60s while the observer's
/// visibility is lost; without per-id keying it would blindly
/// release+rebind a HEALTHY forward every tick (rc=255 loop / self-kill /
/// child accumulation). With the registry the reconnect path first asks
/// [`Self::is_alive`]: a live child ⇒ NO-OP; only an EXITED child ⇒
/// release + rebind, whose fresh child then [`Self::commit`]-replaces the
/// dead entry.
#[derive(Clone)]
pub(super) struct PerSecondaryTunnelRegistry {
    children: Arc<Mutex<HashMap<String, Child>>>,
}

impl PerSecondaryTunnelRegistry {
    pub(super) fn new(children: Arc<Mutex<HashMap<String, Child>>>) -> Self {
        Self { children }
    }

    /// Whether `secondary_id` currently has a LIVE tunnel child.
    ///
    /// `Some(child)` whose `try_wait()` yields `Ok(None)` ⇒ the process
    /// has not exited ⇒ the forward is up ⇒ `true`. An EXITED child
    /// (`Ok(Some(status))`), a `try_wait` error, or no entry at all ⇒
    /// `false` (a rebuild is warranted). `try_wait` is non-blocking and
    /// reaps a dead child's zombie as a side effect.
    pub(super) async fn is_alive(&self, secondary_id: &str) -> bool {
        let mut guard = self.children.lock().await;
        match guard.get_mut(secondary_id) {
            Some(child) => matches!(child.try_wait(), Ok(None)),
            None => false,
        }
    }
}

impl TunnelStore for PerSecondaryTunnelRegistry {
    async fn commit(&self, secondary_id: &str, child: Child) {
        // Replace the entry; reap any displaced prior child so a stale
        // (dead OR superseded) handle never lingers — this is the child
        // accumulation the anonymous Vec suffered, fixed at the owner of
        // the handles.
        let displaced = {
            let mut guard = self.children.lock().await;
            guard.insert(secondary_id.to_owned(), child)
        };
        if let Some(mut old) = displaced {
            terminate_child(&mut old).await;
        }
    }

    async fn drain_and_terminate(&self) {
        // Collect-then-terminate so the lock is not held across the
        // per-child SIGTERM waits (mirrors `SharedTunnelVec`'s drain).
        let drained: Vec<Child> = {
            let mut guard = self.children.lock().await;
            guard.drain().map(|(_, child)| child).collect()
        };
        for mut child in drained {
            terminate_child(&mut child).await;
        }
    }
}
