//! `select!`-arm adapters for the [`super::Node::run`] lifecycle loop.
//!
//! # Concern
//!
//! ONE concern: keep the [`super::Node::run`] `select!` arms readable by
//! adapting each optional channel / join handle into an arm that parks
//! forever when its source is absent (so a missing channel makes the arm
//! inert rather than resolving spuriously), and normalises a panicked/
//! aborted task into a structured `Failed` terminal.

use dynrunner_core::Identifier;
use tokio::sync::{mpsc, oneshot};

use super::outcome::{ObserverJoinHandle, ObserverRunResult, SecondaryJoinHandle, SecondaryRunResult};
use super::outcome::RunTerminal;
use crate::primary::{PrimaryRunOutcome, RunError};

/// `recv` on an `Option<Receiver>`, parking forever when `None` so the arm
/// is inert rather than resolving on a missing channel.
pub(super) async fn recv_opt<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> Option<T> {
    rx.recv().await
}

pub(super) async fn recv_primary<I: Identifier>(
    rx: &mut Option<oneshot::Receiver<PrimaryRunOutcome<I>>>,
) -> Option<PrimaryRunOutcome<I>> {
    match rx.as_mut() {
        Some(r) => match r.await {
            Ok(v) => {
                *rx = None;
                Some(v)
            }
            Err(_) => {
                *rx = None;
                None
            }
        },
        None => std::future::pending().await,
    }
}

pub(super) async fn join_secondary(
    h: &mut Option<SecondaryJoinHandle>,
) -> Option<SecondaryRunResult> {
    match h.as_mut() {
        Some(handle) => {
            let r = handle.await;
            *h = None;
            Some(r.unwrap_or_else(|e| {
                (
                    RunTerminal::Failed {
                        // A panicked/aborted task is an UNEXPECTED non-clean
                        // exit, not the stay-local-primary swallow case — type
                        // it structured so the boundary raises.
                        error: RunError::FatalPolicyExit {
                            reason: format!("secondary task panicked/aborted: {e}"),
                        },
                    },
                    0,
                )
            }))
        }
        None => std::future::pending().await,
    }
}

pub(super) async fn join_opt_run(
    h: &mut Option<ObserverJoinHandle>,
) -> Option<Result<ObserverRunResult, tokio::task::JoinError>> {
    match h.as_mut() {
        Some(handle) => {
            let r = handle.await;
            *h = None;
            Some(r)
        }
        None => std::future::pending().await,
    }
}
