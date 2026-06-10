//! Outbound notifications fired from the CRDT apply path.
//!
//! Single concern: the synchronous role-change hook firing
//! (`fire_role_change_hooks`) plus the three best-effort
//! dispatcher-channel emit / install pairs:
//!   - peer-lifecycle (`PeerLifecycleEvent`)
//!   - fulfillability-matcher trigger (`MatcherTriggerEvent`)
//!   - task-completion (`TaskCompletedEvent`)
//!
//! All emit-side methods are non-blocking, infallible, and silently
//! drop on missing or closed receivers; the install-side methods are
//! the channel sender's only entry point. See the per-method docs for
//! the CCD-9 "apply path never crosses node boundaries" contract.

use dynrunner_core::Identifier;

use super::ClusterState;
use crate::fulfillability_matcher::MatcherTriggerEvent;
use crate::peer_lifecycle::PeerLifecycleEvent;
use crate::task_completed::TaskCompletedEvent;
use crate::worker_signal::WorkerMgmtSignal;

impl<I: Identifier> ClusterState<I> {
    /// Fire every registered hook against the current [`RoleTable`].
    /// Invoked from `apply` immediately AFTER any mutation that
    /// touches the table, so registrants see post-state values.
    /// `pub(super)` so sibling sub-modules (`snapshot::restore` on
    /// the late-joiner merge path, the `apply_peer` rules) can fire
    /// it after their own role-table mutations; external triggering
    /// would let a hook fire on a state it does not describe so
    /// visibility stops at the sub-module tree.
    pub(super) fn fire_role_change_hooks(&self) {
        for hook in &self.role_change_hooks {
            hook(&self.role_table);
        }
    }

    /// Enqueue a [`PeerLifecycleEvent`] onto the dispatcher channel.
    ///
    /// Non-blocking, infallible (errors are silently dropped): the
    /// receiver-gone case happens during clean shutdown when the
    /// coordinator's dispatcher task has already exited, and the
    /// no-sender-installed case happens in unit tests that exercise
    /// the apply path in isolation. Both are non-events from the
    /// apply path's perspective — it MUST NOT panic, block, or
    /// surface a user-visible error on emit, because the broadcast
    /// happens-before observation is the only contract the CRDT
    /// promises and the dispatcher channel is strictly best-effort
    /// observation on top.
    ///
    /// CCD-9 invariant: this method must never invoke a listener
    /// directly. Listener invocation happens off the apply task on
    /// the dispatcher; the channel is the only synchronization
    /// crossing.
    pub(crate) fn emit_lifecycle_event(&self, event: PeerLifecycleEvent) {
        if let Some(tx) = &self.lifecycle_tx {
            // `send` on `UnboundedSender` only fails when the
            // receiver is dropped; silent drop matches the
            // "best-effort observation" contract documented above.
            let _ = tx.send(event);
        }
    }

    /// Attach the dispatcher's sender end so subsequent
    /// `emit_lifecycle_event` calls route events through the
    /// coordinator's dispatcher task.
    ///
    /// Called by the coordinator at `new()` time after building the
    /// (sender, receiver) pair; the receiver is then handed to
    /// [`crate::peer_lifecycle::run_peer_lifecycle_dispatcher`] when
    /// the coordinator's tokio runtime is live. Re-installation
    /// replaces the prior sender silently — the only legitimate
    /// caller is the owning coordinator, and a coordinator that
    /// re-installs is signalling "the prior dispatcher is gone, use
    /// the new one"; we have no use case for stacking dispatchers.
    pub fn install_lifecycle_sender(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<PeerLifecycleEvent>,
    ) {
        self.lifecycle_tx = Some(tx);
    }

    /// Enqueue a [`MatcherTriggerEvent`] onto the matcher-pipeline channel.
    ///
    /// Same best-effort / non-blocking / non-panicking contract as
    /// [`Self::emit_lifecycle_event`] — no installed sender or a
    /// closed receiver is a silent drop; the matcher pipeline is a
    /// strictly-observational layer on top of the CRDT.
    ///
    /// CCD-9 invariant: this method must never invoke the matcher
    /// directly. Matcher invocation happens off the apply task in the
    /// operational `select!` loop; the channel is the only
    /// synchronization crossing.
    ///
    /// TODO (E1): the apply rule for
    /// `ClusterMutation::PeerResourceHoldingsUpdated` is the only
    /// legitimate production caller. Until that variant + its apply
    /// rule land, the only paths that invoke this are tests (via the
    /// `trigger_fulfillability_matcher_for_test` shim below).
    #[allow(dead_code)]
    pub(crate) fn emit_matcher_trigger(&self, event: MatcherTriggerEvent) {
        if let Some(tx) = &self.matcher_trigger_tx {
            let _ = tx.send(event);
        }
    }

    /// Attach the matcher-pipeline sender end so subsequent
    /// `emit_matcher_trigger` calls route events through the
    /// coordinator's operational-loop drain.
    ///
    /// Called by the coordinator at `new()` time after building the
    /// (sender, receiver) pair; the receiver is then consumed by
    /// [`crate::fulfillability_matcher::drain_matcher_batch`] from
    /// inside the `select!` loop. Same re-installation semantics as
    /// [`Self::install_lifecycle_sender`].
    pub fn install_matcher_trigger_sender(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<MatcherTriggerEvent>,
    ) {
        self.matcher_trigger_tx = Some(tx);
    }

    /// Enqueue a [`WorkerMgmtSignal`] onto the worker-management bus.
    ///
    /// Same best-effort / non-blocking / non-panicking contract as
    /// [`Self::emit_matcher_trigger`] — no installed sender or a closed
    /// receiver is a silent drop; the worker-management bus is a
    /// strictly-decoupled signal layer, so the emit side (phase/task
    /// management) MUST NOT panic, block, or surface an error on emit.
    ///
    /// Decoupling invariant: this method must never invoke worker
    /// management directly. Worker management's reaction happens off the
    /// emit path in its operational `select!` loop; the bus is the only
    /// synchronization crossing.
    ///
    /// Emitted from the phase layer (`fire_initial_phase_starts` →
    /// `PhaseStartedNeedsWorkers`; the per-phase proceed-or-fail decision
    /// → `RunShouldFail`) and from the dispatch-decoupling call sites; the
    /// consuming `select!` arm in the operational loop reacts off this
    /// path.
    pub(crate) fn emit_worker_mgmt(&self, signal: WorkerMgmtSignal) {
        if let Some(tx) = &self.worker_mgmt_tx {
            let _ = tx.send(signal);
        }
    }

    /// Attach the worker-management bus sender end so subsequent
    /// `emit_worker_mgmt` calls route signals through worker
    /// management's operational-loop drain.
    ///
    /// Called by worker management at wire-up time after building the
    /// (sender, receiver) pair; the receiver is then consumed by
    /// [`crate::worker_signal::recv_worker_signal_batch`] from inside
    /// the `select!` loop. Same re-installation semantics as
    /// [`Self::install_lifecycle_sender`].
    pub fn install_worker_mgmt_sender(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<WorkerMgmtSignal>,
    ) {
        self.worker_mgmt_tx = Some(tx);
    }

    /// Enqueue a [`TaskCompletedEvent`] onto the dispatcher channel.
    ///
    /// Same best-effort / non-blocking / non-panicking contract as
    /// [`Self::emit_lifecycle_event`]: a missing or closed receiver is
    /// a silent drop. The dispatcher channel is strictly observational
    /// on top of the CRDT — the broadcast happens-before observation
    /// is the only contract the CRDT promises.
    ///
    /// CCD-9 invariant: this method must never invoke a listener
    /// directly. Listener invocation happens off the apply task on
    /// the dispatcher; the channel is the only synchronization
    /// crossing.
    pub(crate) fn emit_task_completed_event(&self, event: TaskCompletedEvent) {
        if let Some(tx) = &self.task_completed_tx {
            // `send` on `UnboundedSender` only fails when the
            // receiver is dropped; silent drop matches the
            // "best-effort observation" contract documented above.
            let _ = tx.send(event);
        }
    }

    /// Attach the dispatcher's sender end so subsequent
    /// `emit_task_completed_event` calls route events through the
    /// coordinator's dispatcher task.
    ///
    /// Same re-installation semantics as
    /// [`Self::install_lifecycle_sender`].
    pub fn install_task_completed_sender(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<TaskCompletedEvent>,
    ) {
        self.task_completed_tx = Some(tx);
    }
}
