//! Per-variant command handlers for the secondary-side command channel.
//! The `handle_secondary_command` entry is the single match called from
//! the `process_tasks` `select!`; each arm forwards to an `apply_*`
//! method on `SecondaryCoordinator` so the mutation's state-machine
//! semantics stay co-located with the rest of the coordinator's state.
//!
//! Mirror of `primary::command_channel::handler` — the same dispatch
//! shape, just routed onto the secondary's `primary_pending` pool /
//! `primary_failed` ledger / `apply_and_broadcast_mutations` primitive.
//! `apply_spawn_tasks` already lives in `secondary/primary/spawn_tasks.rs`
//! (extracted before this command-channel split); the dispatch arm
//! delegates there. The remaining three `apply_*` methods are siblings
//! in `secondary/primary/{fail_permanent,reinject_task,update_preferred_secondaries}.rs`.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCommand;
use crate::secondary::SecondaryCoordinator;

/// Dispatch one received command to its secondary-side handler. Single
/// line at the `select!` call site in `process_tasks` keeps the loop's
/// match arm transport-shape-pure.
///
/// `command_rx` threads the operational-loop's command-channel receiver
/// into the `FailPermanent` cascade so a callback-issued `spawn_tasks`
/// fired by an `on_phase_end` running inside `apply_fail_permanent`'s
/// recursive `note_primary_item_failed` step applies inline. Mirrors
/// the primary-side `handle_primary_command` threading 1:1.
pub(in crate::secondary) async fn handle_secondary_command<PT, P, M, S, E, I>(
    coordinator: &mut SecondaryCoordinator<PT, P, M, S, E, I>,
    command: PrimaryCommand<I>,
    command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
) where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    match command {
        PrimaryCommand::FailPermanent {
            hash,
            error,
            reason,
            reply,
        } => {
            let result = coordinator
                .apply_fail_permanent(hash, error, reason, command_rx)
                .await;
            let _ = reply.send(result);
        }
        PrimaryCommand::ReinjectTask { hash, reply } => {
            let result = coordinator.apply_reinject_task(hash).await;
            let _ = reply.send(result);
        }
        PrimaryCommand::UpdatePreferredSecondaries {
            hash,
            secondaries,
            reply,
        } => {
            let result = coordinator
                .apply_update_preferred_secondaries(hash, secondaries)
                .await;
            let _ = reply.send(result);
        }
        PrimaryCommand::SpawnTasks { tasks, reply } => {
            let result = coordinator.apply_spawn_tasks(tasks).await;
            let _ = reply.send(result);
        }
    }
}
