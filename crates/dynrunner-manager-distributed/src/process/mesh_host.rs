//! [`MeshHost`] — WHERE the mesh (transport + pump) executes, and how it is
//! torn down.
//!
//! # Concern
//!
//! ONE concern: own the EXECUTOR of the live wire turn. The [`super::Mesh`]
//! (which owns the by-value transport) and the [`super::pump`] that drives it
//! used to be spawned on the same `current_thread` runtime as the
//! coordinators — so a coordinator stall (a saturated operational loop, a
//! long synchronous excursion) starved the WIRE itself: keepalives un-emitted,
//! inbound frames un-ingested, ACK servicing frozen (the production
//! coordinator-saturation incident: an hour of unacked report replays).
//! `MeshHost` makes "where the mesh runs" an explicit composition-site
//! decision with two flavors:
//!
//! - [`MeshHost::on_dedicated_thread`] — the **mesh runtime**: a dedicated
//!   `std::thread` running its own `current_thread` tokio runtime + `LocalSet`
//!   that CONSTRUCTS the transport, wraps it in a [`super::Mesh`], and drives
//!   [`super::pump::run_pump`] until shutdown. Wire QoS (keepalive emission
//!   through queued egress, frame ingest, accept/dial/redial tickers) then
//!   survives any coordinator-side stall. Used by every process that hosts a
//!   REAL network transport (the SLURM/network secondary, the submitter
//!   primary, the observer late-joiner).
//! - [`MeshHost::on_local_set`] — the pump on the CALLER's `LocalSet`,
//!   exactly the pre-split shape. Used by the in-process
//!   `--multi-computer local` channel mesh (a pure-`mpsc` transport with no
//!   socket IO: there is no wire QoS to protect, and the N+1 in-process
//!   nodes would otherwise each spawn an OS thread) and by unit fixtures.
//!
//! # Why construction happens ON the mesh thread (not before)
//!
//! Tokio IO resources register with the driver of the runtime that CREATES
//! them, and the transports are deliberately `!Send` (`Rc`/`RefCell` writer
//! tables, `spawn_local`-ed accept/read/write/redial tasks at construction).
//! So the dedicated-thread flavor takes a CONSTRUCT closure and runs it on
//! the mesh runtime's `LocalSet`; the transport never exists anywhere else.
//! Only `Send` channel handles cross back: the [`MeshControlHandle`] (role
//! registration / retag / wind-down) and the closure's `Send` extras (e.g.
//! cert/port material the composition threads onward).
//!
//! # The boundary stays channel-shaped
//!
//! Coordinators already reach the mesh ONLY through their `MeshClient`
//! (queued egress) and `RoleInbox` (channel ingress), and the node mutates it
//! only through the [`MeshControlHandle`] — tokio channels wake across
//! runtimes natively, so NOTHING on the coordinator side changes shape when
//! the pump moves threads. Role trios are minted by the pump's `Register`
//! control arm (which republishes membership BEFORE replying, so a
//! coordinator's first `has_peer`-gated egress never sees an empty view —
//! the R5 invariant, now enforced by the reply ordering instead of spawn
//! ordering).
//!
//! # Isolation invariant (enforced by dependency direction)
//!
//! Nothing on the mesh runtime ever touches Python: this crate and the
//! transport crates have no `pyo3` dependency, and the construct closure's
//! contract (documented on [`MeshHost::on_dedicated_thread`]) forbids
//! capturing anything that re-enters Python. The mesh runtime's tokio
//! `Handle` is PRIVATE to the thread body — it is never exposed, so no other
//! module can spawn onto it.
//!
//! # Shutdown
//!
//! [`MeshHost::stop`] first drains any still-queued egress THROUGH the pump
//! (`MeshControlHandle::wind_down` — the NEW-C final-keepalive guarantee),
//! then stops the executor: the local flavor aborts the pump task (its
//! ingress arm parks on the transport inbound, which stays open while any
//! peer is connected — awaiting it would hang); the thread flavor signals
//! the mesh runtime, which drops the pump future + `LocalSet` (transport,
//! accept loops, tickers) + runtime, and is then JOINED with a bounded
//! grace. The same teardown runs from `Drop` (best-effort) so early-error,
//! graceful-abort, and panik paths that unwind the composition scope never
//! leak the thread.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::PeerTransport;

use super::mesh::Mesh;
use super::pump::{self, MeshControlHandle};

/// Bounded grace for joining the mesh runtime thread after the shutdown
/// signal. The thread's exit path is non-blocking (the select resolves on
/// the signal, then `LocalSet` + runtime drop synchronously), so the normal
/// join completes in milliseconds; the grace only bounds a pathological
/// wedge (which is reported loudly and then detached rather than wedging
/// the whole process teardown).
const MESH_THREAD_JOIN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// A hosted, running mesh: the pump owns the [`Mesh`] on some executor; this
/// handle owns the [`MeshControlHandle`] and the executor's stop lever.
///
/// Composition sites build it (picking the executor flavor), register roles
/// through [`MeshHost::control`], and hand it to the `Node`, which stops it
/// at wind-down.
pub struct MeshHost<I: Identifier> {
    control: MeshControlHandle<I>,
    runner: HostRunner,
}

/// How the pump is hosted — the stop lever per flavor.
enum HostRunner {
    /// Pump task on the caller's `LocalSet` (the pre-split shape).
    Local(tokio::task::JoinHandle<()>),
    /// Pump + transport on the dedicated mesh runtime thread.
    Thread(MeshThread),
}

/// The dedicated mesh runtime thread's teardown handle: the shutdown signal,
/// the thread-exit notification, and the join handle. `teardown` is
/// idempotent and also runs from `Drop` so no path leaks the thread.
struct MeshThread {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    done: std::sync::mpsc::Receiver<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl MeshThread {
    /// Signal shutdown and join the thread within [`MESH_THREAD_JOIN_GRACE`].
    ///
    /// The done-channel wait (rather than a bare `join`) is what makes the
    /// join BOUNDED: a wedged mesh thread is reported loudly and detached
    /// instead of hanging the process teardown forever.
    fn teardown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            // An Err means the thread already exited (pump done / construct
            // failed) — the done-channel wait below still resolves.
            let _ = tx.send(());
        }
        let Some(handle) = self.thread.take() else {
            return;
        };
        match self.done.recv_timeout(MESH_THREAD_JOIN_GRACE) {
            // `Disconnected` means the thread exited without sending (a
            // panic unwound past the send) — joinable either way.
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                if handle.join().is_err() {
                    tracing::error!("mesh runtime: thread panicked; mesh torn down by unwind");
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                tracing::error!(
                    grace_s = MESH_THREAD_JOIN_GRACE.as_secs(),
                    "mesh runtime: thread did not exit within the join grace after \
                     the shutdown signal; detaching it so process teardown is not \
                     wedged (the thread leaks until process exit)"
                );
            }
        }
    }
}

impl Drop for MeshThread {
    fn drop(&mut self) {
        self.teardown();
    }
}

impl<I: Identifier> MeshHost<I> {
    /// Host the pump on the CALLER's `LocalSet` (the pre-split shape).
    ///
    /// For transports with no socket IO — the in-process channel mesh — and
    /// for unit fixtures: there is no wire QoS for a dedicated thread to
    /// protect there. The mesh may already carry registered role slots
    /// (fixtures mint trios synchronously via `Mesh::register_local_role`
    /// before hosting); production sites register through
    /// [`MeshHost::control`] either way.
    ///
    /// Must be called from within a `LocalSet` (the pump is `spawn_local`-ed).
    /// The pump's entry membership-publish runs before any later-spawned
    /// coordinator is first polled — the same R5 ordering `Node::run`'s
    /// in-line pump spawn used to provide.
    pub fn on_local_set<Tr>(mesh: Mesh<I, Tr>) -> Self
    where
        Tr: PeerTransport<I> + 'static,
    {
        let (control, control_rx) = pump::control_channel::<I>();
        let handle = tokio::task::spawn_local(pump::run_pump(mesh, control_rx));
        Self {
            control,
            runner: HostRunner::Local(handle),
        }
    }

    /// Spawn the dedicated **mesh runtime** thread: run `construct` on its
    /// own `current_thread` runtime + `LocalSet` to build the transport
    /// (every socket/timer it creates registers with THAT runtime's driver),
    /// wrap it in a [`Mesh`], start the pump, and hand back this host plus
    /// the closure's `Send` extras.
    ///
    /// `construct` runs entirely on the mesh runtime. Its contract:
    ///
    /// - it may `spawn_local` transport-lifetime tasks (accept loops,
    ///   bring-up dials, listener-keepalive parks) — they die when the host
    ///   is stopped (the `LocalSet` drops);
    /// - it must NOT touch Python or capture anything that re-enters Python
    ///   (the mesh runtime never blocks on the GIL — that is the entire
    ///   point of the split);
    /// - everything it returns besides the transport must be `Send` (it
    ///   crosses back to the coordinator runtime).
    ///
    /// Errors if the thread/runtime cannot be spawned or `construct` fails;
    /// the thread is joined before returning in both cases.
    pub async fn on_dedicated_thread<Tr, X, F, Fut>(construct: F) -> Result<(Self, X), String>
    where
        Tr: PeerTransport<I> + 'static,
        X: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(Tr, X), String>> + 'static,
    {
        let (handshake_tx, handshake_rx) =
            tokio::sync::oneshot::channel::<Result<(MeshControlHandle<I>, X), String>>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        let thread = std::thread::Builder::new()
            .name("dynrunner-mesh".to_string())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = handshake_tx.send(Err(format!(
                            "mesh runtime: failed to build tokio runtime: {e}"
                        )));
                        let _ = done_tx.send(());
                        return;
                    }
                };
                let local = tokio::task::LocalSet::new();
                rt.block_on(local.run_until(async move {
                    let (transport, extras) = match construct().await {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = handshake_tx.send(Err(e));
                            return;
                        }
                    };
                    let mesh = Mesh::new(transport);
                    let (control, control_rx) = pump::control_channel::<I>();
                    if handshake_tx.send(Ok((control, extras))).is_err() {
                        // The spawner is gone; nothing will ever register a
                        // role or stop this mesh — exit (drops the transport).
                        return;
                    }
                    tokio::select! {
                        _ = pump::run_pump(mesh, control_rx) => {
                            tracing::info!(
                                "mesh runtime: pump exited (transport inbound closed)"
                            );
                        }
                        // Resolves on the stop signal AND on a dropped-without-
                        // send handle (a leaked host) — shutdown either way.
                        _ = shutdown_rx => {
                            tracing::debug!("mesh runtime: shutdown signalled");
                        }
                    }
                }));
                // `local` (every spawn_local-ed transport task) and `rt` drop
                // here, synchronously; then notify the joiner.
                let _ = done_tx.send(());
            })
            .map_err(|e| format!("mesh runtime: failed to spawn thread: {e}"))?;

        let mut thread = MeshThread {
            shutdown: Some(shutdown_tx),
            done: done_rx,
            thread: Some(thread),
        };
        match handshake_rx.await {
            Ok(Ok((control, extras))) => Ok((
                Self {
                    control,
                    runner: HostRunner::Thread(thread),
                },
                extras,
            )),
            Ok(Err(e)) => {
                // Construction failed ON the mesh runtime; join its exit.
                thread.teardown();
                Err(e)
            }
            Err(_) => {
                thread.teardown();
                Err(
                    "mesh runtime: thread exited before completing the construction handshake"
                        .to_string(),
                )
            }
        }
    }

    /// The pump's control handle: role registration (minting the
    /// `(slot, client, inbox)` trio), in-place retag, and the wind-down
    /// drain. The ONLY mesh-mutation surface a composition site or the
    /// `Node` holds.
    pub fn control(&self) -> &MeshControlHandle<I> {
        &self.control
    }

    /// Whether this mesh runs on a DEDICATED runtime thread
    /// ([`Self::on_dedicated_thread`]) rather than the caller's `LocalSet`
    /// ([`Self::on_local_set`]).
    ///
    /// The composition flavor IS the coordinator-executor decision: a
    /// dedicated-thread mesh marks a REAL-network node (the SLURM/network
    /// secondary, the submitter primary) where a promoted primary co-resides
    /// in-process with a live secondary and MUST run its loop on its own thread
    /// so a primary CPU burst cannot starve the secondary (see
    /// [`super::run`]'s `Node::run` + the `coordinator_host` executor). A
    /// `LocalSet`-hosted mesh marks the in-process `--multi-computer local`
    /// node, whose pure-`mpsc` mesh AND every co-located role deliberately
    /// share ONE runtime (no wire QoS to protect, and the single-runtime model
    /// is load-bearing for the in-process harness's cooperative scheduling) —
    /// there the primary stays on the shared `LocalSet`.
    pub fn runs_on_dedicated_thread(&self) -> bool {
        matches!(self.runner, HostRunner::Thread(_))
    }

    /// Stop the hosted mesh: drain any still-queued egress THROUGH the pump
    /// (`wind_down` — so a final keepalive/completion broadcast queued in
    /// the same sync step as the headline role resolving is applied, not
    /// discarded), then stop the executor — abort the local pump task, or
    /// signal + bounded-join the mesh runtime thread.
    pub async fn stop(self) {
        self.control.wind_down().await;
        drop(self.control);
        match self.runner {
            HostRunner::Local(handle) => {
                // The pump's ingress arm parks on the transport inbound,
                // which stays open while any peer is connected — abort, as
                // `Node::run` always has.
                handle.abort();
                let _ = handle.await;
            }
            HostRunner::Thread(mut thread) => thread.teardown(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::LocalRole;
    use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};
    use dynrunner_protocol_primary_secondary::{DistributedMessage, KeepaliveRole};
    use dynrunner_transport_channel::peer_mesh;

    type TestId = String;

    fn keepalive(sender: &str) -> DistributedMessage<TestId> {
        DistributedMessage::Keepalive {
            target: None,
            sender_id: sender.to_string(),
            timestamp: 1.0,
            secondary_id: sender.to_string(),
            active_workers: 0,
            emitter_role: KeepaliveRole::Secondary,
        }
    }

    /// Thread flavor end-to-end: construct on the mesh runtime, register two
    /// roles through the control channel from the coordinator runtime, send a
    /// loopback frame through the cross-thread egress queue, receive it on
    /// the cross-thread inbox, and stop (joining the thread).
    #[tokio::test]
    async fn dedicated_thread_hosts_register_send_and_stop() {
        let (host, extra) = MeshHost::<TestId>::on_dedicated_thread(|| async {
            // A 2-node channel mesh: the SIBLING transport is parked on the
            // mesh runtime so node-1's inbound stays OPEN (a 1-node mesh's
            // inbound closes immediately and the pump exits before the
            // second register can be served — the production transports
            // keep their inbound open via their own accept/read tasks).
            let mut nodes = peer_mesh::<TestId>(&["node-1".to_string(), "node-2".to_string()]);
            let sibling = nodes.remove(1);
            let transport = nodes.remove(0);
            tokio::task::spawn_local(async move {
                let _sibling = sibling;
                std::future::pending::<()>().await;
            });
            Ok((transport, 7u32))
        })
        .await
        .expect("mesh runtime spawn");
        assert_eq!(extra, 7, "construct extras cross the handshake");

        let (sec_slot, sec_client, _sec_inbox) = host
            .control()
            .register(LocalRole::Secondary, PeerId::from("node-1"))
            .await
            .expect("register secondary");
        let (obs_slot, _obs_client, mut obs_inbox) = host
            .control()
            .register(LocalRole::Observer, PeerId::from("node-1"))
            .await
            .expect("register observer");

        // Egress is queued on the coordinator runtime, applied by the pump on
        // the mesh runtime, loopback-delivered to the observer slot, and read
        // back here — the full cross-runtime channel boundary in one pass.
        sec_client
            .send(
                Destination::Observer(PeerId::from("node-1")),
                keepalive("node-1"),
            )
            .expect("queued egress");
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), obs_inbox.recv())
            .await
            .expect("loopback delivery within bound")
            .expect("inbox open");
        assert_eq!(frame.sender_id(), "node-1");

        drop(sec_slot);
        drop(obs_slot);
        host.stop().await;
    }

    /// A failing construct closure surfaces its error through the handshake
    /// and the thread is joined (no leak, no hang).
    #[tokio::test]
    async fn dedicated_thread_construct_failure_propagates() {
        let result = MeshHost::<TestId>::on_dedicated_thread(|| async {
            Err::<
                (
                    dynrunner_transport_channel::ChannelPeerTransport<TestId>,
                    (),
                ),
                String,
            >("construct exploded".to_string())
        })
        .await;
        assert_eq!(result.err().as_deref(), Some("construct exploded"));
    }

    /// Local flavor: pre-registered mesh (the fixture shape), pump on the
    /// caller's LocalSet, stop aborts it.
    #[tokio::test]
    async fn local_set_hosts_pre_registered_mesh() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let transport = peer_mesh::<TestId>(&["node-1".to_string()]).remove(0);
                let mut mesh = Mesh::new(transport);
                let (slot, client, mut inbox) =
                    mesh.register_local_role(LocalRole::Secondary, PeerId::from("node-1"));
                let host = MeshHost::on_local_set(mesh);
                client
                    .send(
                        Destination::Secondary(PeerId::from("node-1")),
                        keepalive("node-1"),
                    )
                    .expect("queued egress");
                let frame = tokio::time::timeout(std::time::Duration::from_secs(5), inbox.recv())
                    .await
                    .expect("loopback delivery within bound")
                    .expect("inbox open");
                assert_eq!(frame.sender_id(), "node-1");
                drop(slot);
                host.stop().await;
            })
            .await;
    }

    /// The executor-flavor predicate the coordinator host keys on (see
    /// `process::run::coordinator_host` + `Node::run`): a dedicated-thread mesh
    /// reports `true` (the promoted primary co-resides with a live secondary →
    /// isolate its loop on its own thread); a `LocalSet`-hosted mesh reports
    /// `false` (the in-process node shares one runtime). Pinning this predicate
    /// pins the decision that routes the production relocation path to the
    /// thread executor and the in-process / paused-clock path to the shared
    /// `LocalSet`.
    #[tokio::test]
    async fn runs_on_dedicated_thread_distinguishes_the_two_flavors() {
        // Thread flavor: a real dedicated mesh runtime thread.
        let (thread_host, _extra) = MeshHost::<TestId>::on_dedicated_thread(|| async {
            let mut nodes = peer_mesh::<TestId>(&["node-1".to_string(), "node-2".to_string()]);
            let sibling = nodes.remove(1);
            let transport = nodes.remove(0);
            tokio::task::spawn_local(async move {
                let _sibling = sibling;
                std::future::pending::<()>().await;
            });
            Ok((transport, ()))
        })
        .await
        .expect("mesh runtime spawn");
        assert!(
            thread_host.runs_on_dedicated_thread(),
            "an on_dedicated_thread mesh must select the dedicated-thread primary executor"
        );
        thread_host.stop().await;

        // Local flavor: pump on the caller's LocalSet.
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let transport = peer_mesh::<TestId>(&["node-1".to_string()]).remove(0);
                let mesh = Mesh::new(transport);
                let local_host = MeshHost::on_local_set(mesh);
                assert!(
                    !local_host.runs_on_dedicated_thread(),
                    "an on_local_set mesh must keep the primary on the shared LocalSet"
                );
                local_host.stop().await;
            })
            .await;
    }
}
