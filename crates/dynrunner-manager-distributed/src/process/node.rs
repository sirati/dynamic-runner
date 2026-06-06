//! [`Node`] — the OS-process role composition shell.
//!
//! # Concern
//!
//! One OS process hosts the runner's role composition for a single peer:
//! at most one primary + one secondary + one observer coordinator, the
//! [`Mesh`] that demuxes the wire to them, and the lifecycle channels that
//! drive promotion (a secondary becoming primary) and demotion (an old
//! primary winding down). `Node` OWNS all of that and is the single
//! teardown lever: dropping a role's [`RoleEntry`] drops its
//! `Arc<RoleSlot>`, so the mesh `Weak` stops upgrading and the slot
//! auto-dies (clarification H4).
//!
//! It is named `Node` (not `Process`) to avoid colliding with
//! [`std::process`] (maint-M2); the file stays `process/node.rs` under the
//! `process/` module.
//!
//! # Scope of THIS file (foundation only)
//!
//! This is the SKELETON: the struct shape, [`RoleEntry`], the typed
//! [`PromotionSignal`], and the channel plumbing (`promotion`/`demote`
//! senders handed out, receivers held). It deliberately does NOT define
//! `Node::run`, does NOT compose or spawn the coordinators, and does NOT
//! build a primary on a promotion signal — those are the later
//! coordinator-rewire + node-wiring waves, which fill the `#[allow(
//! dead_code)] // TODO(C-NODE)` parts once the coordinators drop their
//! transport generic and take a [`MeshClient`] + [`RoleInbox`].
//!
//! # Boundary
//!
//! `Node` names the [`Mesh`] (which owns the by-value transport) and the
//! coordinator types by GENERIC parameter — it never reaches into a
//! coordinator's internals nor the transport's. A role "exists" iff its
//! `Option<RoleEntry<_>>` is `Some` (clarification H3: one nullable per
//! role, not a quad of parallel `Option`s).

use std::sync::Arc;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{PeerTransport, PrimaryChangeReason};
use tokio::sync::mpsc;

use super::mesh::Mesh;
use super::role_slot::RoleSlot;
use crate::cluster_state::ClusterStateSnapshot;

/// One live local role: its coordinator paired with the `Arc<RoleSlot>`
/// the mesh demuxes to (clarification H3).
///
/// Binding the two into ONE struct makes "a role exists" a single
/// nullable (`Option<RoleEntry<_>>`) rather than a drift-prone quad of
/// `Option<Coordinator>` + `Option<Arc<RoleSlot>>` + `Weak` + `RoleInbox`.
/// The `Arc<RoleSlot>` here is the teardown lever: dropping the
/// `RoleEntry` drops the `Arc`, and the mesh `Weak` then fails to upgrade
/// (H4). The matching `MeshClient` + `RoleInbox` live INSIDE the
/// coordinator `C` (minted together with the slot by
/// [`Mesh::register_local_role`], clarification M3); the entry holds only
/// the slot so the `Node` keeps the teardown handle.
///
/// `C` is the coordinator type. It is a GENERIC parameter (not a concrete
/// coordinator) because the coordinators have not yet dropped their
/// transport generic; the node-wiring wave instantiates `C` with the
/// rewired `PrimaryCoordinator<…>` / `SecondaryCoordinator<…>` /
/// `ObserverCoordinator<…>` once those land.
pub struct RoleEntry<C, I: Identifier> {
    /// The role's coordinator, owned BY VALUE (it runs via `&mut self` /
    /// a consuming `run`, so it cannot be `Arc`'d).
    // TODO(C-NODE): the node-wiring wave drives `coordinator.run(..)` on a
    // `LocalSet` alongside the mesh-pump; until then the field only exists
    // to hold the value + pair it with the slot.
    #[allow(dead_code)]
    pub coordinator: C,
    /// The `Arc<RoleSlot>` whose `Weak` the mesh holds. Dropping this
    /// (with the rest of the entry) is role teardown (H4).
    pub slot: Arc<RoleSlot<I>>,
}

/// A typed promotion request handed to the [`Node`] — the secondary NEVER
/// constructs a primary itself (SUPREME-LAW #3 & #7); it only SIGNALS, and
/// the `Node` does the build on its own loop.
///
/// Typed end-to-end (no strings): the `reason` distinguishes an election
/// win from a transferred primary, the `epoch` carries the role-table
/// epoch the signal was raised at, and the `snapshot` is the promoting
/// host's converged `cluster_state` captured ATOMICALLY at the
/// promotion-fire instant (the same `&mut self` apply that fires the
/// signal). Carrying the snapshot ON the signal — rather than a
/// shared-mutable cell the `Node` reads later — keeps it coherent with its
/// trigger and owned (`Send`): there is no "secondary writes before Node
/// reads" ordering coupling. The `Node` hands the snapshot straight to the
/// caller's [`super::PromotedPrimaryBuilder`], which seeds the freshly-built
/// primary via `seed_from_promotion_snapshot`.
///
/// Generic over `I` because the snapshot is `ClusterStateSnapshot<I>`. Not
/// `Copy`/`PartialEq`/`Eq` (the snapshot is a `HashMap`-bearing payload);
/// `Clone` so a test fixture can keep a copy after asserting on the fired
/// signal.
#[derive(Debug, Clone)]
pub struct PromotionSignal<I: Identifier> {
    /// Why this host is being promoted — an election win
    /// (`fire_local_promotion`) or a transferred primary (submitter
    /// relocate). Carried so the node-wiring wave can branch the build/seed
    /// path without re-deriving it from cluster state.
    pub reason: PrimaryChangeReason,
    /// The role-table epoch at which the promotion was raised.
    pub epoch: u64,
    /// The promoting host's converged `cluster_state` snapshot, captured at
    /// the promotion-fire instant. The `Node` threads it to the
    /// [`super::PromotedPrimaryBuilder`] so the built primary resumes from
    /// the right replicated generation rather than empty state.
    pub snapshot: ClusterStateSnapshot<I>,
}

/// The OS-process role composition shell (see the module docs).
///
/// Generic over the identifier `I`, the transport `Tr` (owned by the
/// [`Mesh`], never named by a coordinator), and the three coordinator
/// types `P`/`S`/`O`. The coordinator types are parameters — not the
/// concrete coordinators — so this skeleton compiles BEFORE the
/// coordinators drop their transport generic; the node-wiring wave pins
/// them to the rewired coordinator types.
pub struct Node<I, Tr, P, S, O>
where
    I: Identifier,
    Tr: PeerTransport<I>,
{
    /// The role-demux mesh over the by-value transport — the ONLY thing in
    /// this process that touches the wire. Coordinators reach it solely
    /// through their `MeshClient` / `RoleInbox`.
    pub mesh: Mesh<I, Tr>,
    /// The primary coordinator, if one runs here. `Some` after a
    /// bootstrap-submitter build or a promotion build.
    // TODO(C-NODE): populated by the node-wiring wave's register + build.
    #[allow(dead_code)]
    pub primary: Option<RoleEntry<P, I>>,
    /// The secondary coordinator, if one runs here.
    #[allow(dead_code)]
    pub secondary: Option<RoleEntry<S, I>>,
    /// The observer coordinator, if one runs here (after a
    /// submitter-primary→observer swap or a cold-join observer).
    #[allow(dead_code)]
    pub observer: Option<RoleEntry<O, I>>,
    /// Promotion ingress: the secondary signals here on a self-named
    /// election/transfer; the node-wiring wave's loop drains this and
    /// builds the primary (§1.3). The matching sender is handed to the
    /// secondary at construction.
    // TODO(C-NODE): drained by `Node::run`'s promotion arm.
    #[allow(dead_code)]
    pub promotion_rx: mpsc::UnboundedReceiver<PromotionSignal<I>>,
    /// Demote ingress: the BUG-6 role-change hook signals here on ANY
    /// self→other primary-register flip (apply OR restore/merge heal); the
    /// node-wiring wave's loop drains this and drops `self.primary` (§1.5).
    // TODO(C-NODE): drained by `Node::run`'s demote arm.
    #[allow(dead_code)]
    pub demote_rx: mpsc::UnboundedReceiver<()>,
}

impl<I, Tr, P, S, O> Node<I, Tr, P, S, O>
where
    I: Identifier,
    Tr: PeerTransport<I>,
{
    /// Build a fresh node shell around a `mesh`, with no roles yet live.
    ///
    /// Returns the node plus the two ingress SENDERS the node hands out:
    /// `promotion_tx` is installed on the secondary (mirror of
    /// `register_panik_signal_rx`) so a self-named promotion signals the
    /// node; `demote_tx` is installed on the BUG-6 role-change hook so a
    /// self→other primary flip signals teardown. Both are best-effort: a
    /// dropped receiver means the node is winding down.
    ///
    /// The roles start `None`; the node-wiring wave registers them on the
    /// mesh (minting each `(slot, client, inbox)` trio) and builds the
    /// coordinators with `client + inbox`.
    pub fn new(
        mesh: Mesh<I, Tr>,
    ) -> (
        Self,
        mpsc::UnboundedSender<PromotionSignal<I>>,
        mpsc::UnboundedSender<()>,
    ) {
        let (promotion_tx, promotion_rx) = mpsc::unbounded_channel();
        let (demote_tx, demote_rx) = mpsc::unbounded_channel();
        let node = Self {
            mesh,
            primary: None,
            secondary: None,
            observer: None,
            promotion_rx,
            demote_rx,
        };
        (node, promotion_tx, demote_tx)
    }

    /// Compose a primary role onto this node (builder form). The `slot` is the
    /// `Arc<RoleSlot>` minted alongside the coordinator's `MeshClient`/`RoleInbox`
    /// by [`Mesh::register_local_role`]; the node holds it as the teardown
    /// lever (H4). Used by the composition site (pyo3 / the test harness) that
    /// builds the bootstrap submitter's primary.
    pub fn with_primary(mut self, coordinator: P, slot: Arc<RoleSlot<I>>) -> Self {
        self.primary = Some(RoleEntry { coordinator, slot });
        self
    }

    /// Compose a secondary role onto this node (builder form). See
    /// [`Self::with_primary`].
    pub fn with_secondary(mut self, coordinator: S, slot: Arc<RoleSlot<I>>) -> Self {
        self.secondary = Some(RoleEntry { coordinator, slot });
        self
    }

    /// Compose an observer role onto this node (builder form, cold-join). See
    /// [`Self::with_primary`].
    pub fn with_observer(mut self, coordinator: O, slot: Arc<RoleSlot<I>>) -> Self {
        self.observer = Some(RoleEntry { coordinator, slot });
        self
    }
}
