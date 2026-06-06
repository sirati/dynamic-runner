//! The mesh-pump — the single OWNER of the live [`Mesh`] turn, plus the
//! control channel through which [`super::Node::run`] mutates the role set
//! without ever touching the mesh directly.
//!
//! # Concern
//!
//! ONE concern: own the [`Mesh`] and keep it turning. The coordinators
//! reach the mesh only through their `MeshClient` (egress, QUEUED — M4) and
//! `RoleInbox` (ingress, fed by the pump). The pump is the SOLE owner of the
//! live wire turn:
//!
//! - **egress drain** — pull each queued `LocalDispatch` and apply it
//!   (`Mesh::apply_local_dispatch` → loopback-or-remote demux),
//! - **ingress route** — receive each inbound wire frame, dial off it if it
//!   is a `PeerInfo` (RV-2), then route it to the right local slot(s)
//!   (`Mesh::recv_dial_and_route`),
//! - **membership publish** — republish the LIVE transport membership into
//!   the detached clients' view, once per tick,
//! - **control** — serve register / retag / clear requests from
//!   [`super::Node::run`] (the ONLY mesh mutations the node performs go
//!   through this channel, so the pump stays the mesh's sole owner and the
//!   node never needs a concurrent `&mut Mesh`).
//!
//! These run as ONE `select!` so a queued egress send never starves while
//! the pump awaits an inbound (M4 / BUG-2), AND so a lifecycle register/retag
//! interleaves with the steady drains without a second owner of the mesh.
//!
//! # Why the pump OWNS the mesh (the borrow architecture)
//!
//! The pump's drains (`recv_peer`, `apply_local_dispatch`) need `&mut Mesh`
//! continuously, and the node's lifecycle ops (`register_local_role`,
//! `retag_local_role`) also need `&mut Mesh`. Two concurrent `&mut Mesh`
//! owners is impossible. So the pump OWNS the mesh by value and the node
//! mutates it ONLY through [`MeshControl`] — a request/response channel. The
//! mesh is single-owned (no `RefCell`-across-`await`, no deadlock), and
//! every mesh concern (drain + mutate) is serialized through the one pump
//! `select!`.
//!
//! # Why the pump is THIN (H6)
//!
//! ALL routing lives in [`Mesh`]. The pump never classifies a frame, never
//! re-derives membership, never decides loopback-vs-remote. It only chooses
//! WHICH mesh method to call on a ready event.
//!
//! # The E0499 resolution
//!
//! `Mesh::next_local_dispatch` and `Mesh::recv_peer` are BOTH `&mut self`,
//! so they cannot be two arms of one `select!` (each future would hold a
//! `&mut Mesh` across the await — a double borrow, B-SECONDARY's flag). The
//! pump OWNS the egress-queue receiver disjointly (`Mesh::take_local_dispatch_rx`),
//! so the egress-drain future borrows only that receiver — never `&mut
//! Mesh`. The ingress arm (`recv_dial_and_route`) is then the SOLE `&mut
//! Mesh` future in the `select!`; the egress handler + the control handler
//! take `&mut Mesh` only after the select drops the ingress future.

use std::sync::Arc;
use std::time::Duration;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::PeerTransport;
use dynrunner_protocol_primary_secondary::address::PeerId;
use tokio::sync::{mpsc, oneshot};

use super::mesh::Mesh;
use super::mesh_client::{LocalDispatch, MeshClient, RoleInbox};
use super::role::LocalRole;
use super::role_slot::RoleSlot;

/// How often the pump republishes the live transport membership into the
/// detached clients' [`super::MembershipView`].
///
/// The view is read by strand/failover guards that tolerate a ≤1-cycle
/// stale count (no guard declares DEATH off it — deaths key on keepalive
/// timers + the send-result failover probe). 100 ms keeps the staleness
/// bound well under any guard's decision granularity while costing one cheap
/// transport read per tick.
const MEMBERSHIP_PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

/// One mesh-mutation request from [`super::Node::run`] to the pump.
///
/// The node never holds `&mut Mesh` (the pump owns it); every register /
/// retag / clear it needs rides this channel, and the pump applies it inside
/// its `select!` so the mutation is serialized with the drains. The minted
/// trio (register) rides back on the `reply` oneshot.
/// The capability trio minted by a register request: the slot `Arc` (the
/// node's teardown lever), the egress client, and the ingress inbox.
pub type RoleTrio<I> = (Arc<RoleSlot<I>>, MeshClient<I>, RoleInbox<I>);

pub enum MeshControl<I: Identifier> {
    /// Register a fresh local role and mint its [`RoleTrio`]. Used by the
    /// promotion build (mint the Primary trio) and by the node's bootstrap
    /// composition. Boxed so this large-payload variant does not bloat every
    /// `MeshControl` (the `Retag` variant is tiny).
    Register {
        role: LocalRole,
        peer_id: PeerId,
        reply: oneshot::Sender<RoleTrio<I>>,
    },
    /// Retag a live slot from `old`→`new` role IN PLACE, moving the mesh
    /// `Weak` between role fields (D-RETAG / H5). Used by the
    /// submitter-primary→observer swap.
    Retag { old: LocalRole, new: LocalRole },
    /// Defensive final egress drain on a clean wind-down. The node sends
    /// this AFTER its headline role resolved and BEFORE it aborts the pump,
    /// so any egress queued in the same sync step as the run-future
    /// resolving (e.g. a final keepalive / completion broadcast) is applied
    /// through the mesh rather than discarded by the abort. The pump drains
    /// what is queued NOW (bounded `try_recv`, never awaits a fresh item)
    /// and `ack`s — the node awaits that ack so the drain provably precedes
    /// the abort.
    WindDown { ack: oneshot::Sender<()> },
}

/// The node's handle to send [`MeshControl`] requests to the pump.
///
/// Cloneable; held by [`super::Node::run`]. A send failing means the pump
/// has exited (the mesh is gone) — the node is winding down.
#[derive(Clone)]
pub struct MeshControlHandle<I: Identifier> {
    tx: mpsc::UnboundedSender<MeshControl<I>>,
}

impl<I: Identifier> MeshControlHandle<I> {
    /// Register a role and await the minted trio. `None` if the pump has
    /// exited (mesh gone).
    pub async fn register(&self, role: LocalRole, peer_id: PeerId) -> Option<RoleTrio<I>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(MeshControl::Register {
                role,
                peer_id,
                reply,
            })
            .ok()?;
        rx.await.ok()
    }

    /// Request an in-place role retag (primary→observer swap). Best-effort:
    /// a closed pump means the mesh is already gone.
    pub fn retag(&self, old: LocalRole, new: LocalRole) {
        let _ = self.tx.send(MeshControl::Retag { old, new });
    }

    /// Request a defensive final egress drain and AWAIT it.
    ///
    /// Called once at wind-down, before the node aborts the pump: the pump
    /// drains every egress item queued NOW (bounded — `try_recv` to empty,
    /// it never awaits a fresh item) and applies each through the mesh, then
    /// acks. Awaiting the ack makes the drain provably precede the abort, so
    /// a final keepalive / completion broadcast queued in the same sync step
    /// as the headline role's run future resolving is NOT lost. A closed
    /// pump (mesh already gone) yields immediately — nothing left to drain.
    pub async fn wind_down(&self) {
        let (ack, rx) = oneshot::channel();
        if self.tx.send(MeshControl::WindDown { ack }).is_err() {
            return;
        }
        let _ = rx.await;
    }
}

/// Run the mesh-pump, OWNING `mesh` for the whole run.
///
/// Returns when BOTH the egress queue and the transport inbound close (the
/// process-teardown signal). The control channel's `control_rx` lets the
/// node register/retag/clear; when every [`MeshControlHandle`] is dropped
/// the control arm yields `None` and stops being polled (the drains keep the
/// pump alive until the wire closes).
///
/// The pump publishes membership immediately on entry so the detached
/// clients see a live count from their first read (BUG-4 substrate:
/// membership + the registered slots are seeded before the first inbound).
pub async fn run_pump<I, Tr>(
    mut mesh: Mesh<I, Tr>,
    mut control_rx: mpsc::UnboundedReceiver<MeshControl<I>>,
) where
    I: Identifier,
    Tr: PeerTransport<I>,
{
    // Take the egress receiver to OWN it disjointly from `&mut mesh` (E0499).
    let Some(mut dispatch_rx): Option<mpsc::UnboundedReceiver<LocalDispatch<I>>> =
        mesh.take_local_dispatch_rx()
    else {
        debug_assert!(
            false,
            "run_pump: the egress-queue receiver was already taken — a second \
             pump on the same mesh is a composition bug"
        );
        return;
    };

    // Seed the detached clients' view from the live transport before the
    // first coordinator read (BUG-4).
    //
    // ORDERING INVARIANT (R5 — entry publish precedes any coordinator
    // egress): this runs synchronously BEFORE the loop's first await, and
    // the node spawns this pump before any coordinator (`run.rs`), so the
    // membership view is truthful from a coordinator's FIRST read. A
    // coordinator's first `has_peer`-gated egress (e.g. the secondary setup
    // Welcome) must never observe an empty `MembershipView`. Keep this
    // publish synchronous-before-await and ahead of any coordinator spawn.
    mesh.publish_membership();

    let mut ticker = tokio::time::interval(MEMBERSHIP_PUBLISH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // The egress queue / control channel each close independently; once a
    // `recv()` yields `None` it stays closed forever (left polled it would
    // busy-spin the select), so we gate each arm on its own open flag.
    let mut egress_open = true;
    let mut control_open = true;

    loop {
        tokio::select! {
            // Egress first (biased): a queued send that has been waiting must
            // not be starved by a steady inbound stream (M4).
            biased;

            // CONTROL: owned receiver — borrows only `control_rx`. The
            // handler then mutates the mesh (register/retag) — serialized
            // with the drains, single-owner.
            maybe_ctl = control_rx.recv(), if control_open => {
                match maybe_ctl {
                    Some(MeshControl::Register { role, peer_id, reply }) => {
                        let trio = mesh.register_local_role(role, peer_id);
                        // Republish so the just-registered role's client sees
                        // the live membership without waiting a tick.
                        mesh.publish_membership();
                        // A dropped reply means the node aborted the build —
                        // the minted slot's Arc drops, the Weak self-prunes.
                        let _ = reply.send(trio);
                    }
                    Some(MeshControl::Retag { old, new }) => {
                        mesh.retag_local_role(old, new);
                    }
                    Some(MeshControl::WindDown { ack }) => {
                        // Defensive final egress drain (NEW-C): apply every
                        // item queued NOW so a final keepalive / completion
                        // broadcast isn't lost when the node aborts the pump.
                        // Bounded — `try_recv` to empty, never awaiting a
                        // fresh item — so it cannot block the wind-down.
                        while let Ok(item) = dispatch_rx.try_recv() {
                            if let Err(reason) = mesh.apply_local_dispatch(item).await {
                                tracing::debug!(%reason, "mesh-pump: wind-down egress apply returned an error");
                            }
                        }
                        // Ack so the node's `wind_down().await` returns only
                        // after the drain — the drain provably precedes the
                        // abort. A dropped receiver (node already gone) is
                        // fine; the drain still happened.
                        let _ = ack.send(());
                    }
                    None => control_open = false,
                }
            }

            // EGRESS: owned receiver — borrows only `dispatch_rx`.
            maybe_item = dispatch_rx.recv(), if egress_open => {
                match maybe_item {
                    Some(item) => {
                        if let Err(reason) = mesh.apply_local_dispatch(item).await {
                            tracing::debug!(%reason, "mesh-pump: egress apply returned an error");
                        }
                    }
                    None => egress_open = false,
                }
            }

            // INGRESS: the SOLE `&mut mesh` future in this select. Self-
            // contained (dial + route inside one `&mut Mesh` call), so its
            // borrow is released on return.
            handled = mesh.recv_dial_and_route() => {
                if !handled {
                    // Transport inbound closed — the wire is gone. Tear down.
                    break;
                }
            }

            // MEMBERSHIP: republish the live transport read into the view.
            _ = ticker.tick() => {
                mesh.publish_membership();
            }
        }

        // Both the egress queue and the control channel are closed: every
        // coordinator + the node have dropped their ends. Drain the inbound
        // until the wire closes (so a coordinator awaiting its teardown frame
        // still gets it), then exit.
        if !egress_open && !control_open && !mesh.recv_dial_and_route().await {
            break;
        }
    }
}

/// Build the pump's control channel, returning the node's handle + the
/// receiver the pump owns. Minted in one place so the pairing can't
/// mismatch.
pub fn control_channel<I: Identifier>() -> (
    MeshControlHandle<I>,
    mpsc::UnboundedReceiver<MeshControl<I>>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    (MeshControlHandle { tx }, rx)
}
