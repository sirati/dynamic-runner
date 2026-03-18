use std::collections::HashMap;

use db_comm_api_base::{Identifier, MessageReceiver, MessageSender};
use db_primary_secondary_comm::{DistributedMessage, SecondaryTransport};
use db_manager_runner_comm::{Command, Response};
use tokio::sync::mpsc;

// ── Manager ↔ Runner channel transport ──

/// Manager-side endpoint backed by tokio mpsc channels.
pub struct ChannelManagerEnd {
    cmd_tx: mpsc::UnboundedSender<Command>,
    resp_rx: mpsc::UnboundedReceiver<Response>,
}

/// Runner-side endpoint backed by tokio mpsc channels.
pub struct ChannelRunnerEnd {
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    resp_tx: mpsc::UnboundedSender<Response>,
}

/// Create a pair of channel endpoints for in-process testing.
pub fn channel_pair() -> (ChannelManagerEnd, ChannelRunnerEnd) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (resp_tx, resp_rx) = mpsc::unbounded_channel();
    (
        ChannelManagerEnd { cmd_tx, resp_rx },
        ChannelRunnerEnd { cmd_rx, resp_tx },
    )
}

impl MessageSender<Command> for ChannelManagerEnd {
    async fn send(&mut self, msg: Command) -> Result<(), String> {
        self.cmd_tx.send(msg).map_err(|e| e.to_string())
    }
}

impl MessageReceiver<Response> for ChannelManagerEnd {
    async fn recv(&mut self) -> Option<Response> {
        self.resp_rx.recv().await
    }

}

impl MessageReceiver<Command> for ChannelRunnerEnd {
    async fn recv(&mut self) -> Option<Command> {
        self.cmd_rx.recv().await
    }
}

impl MessageSender<Response> for ChannelRunnerEnd {
    async fn send(&mut self, msg: Response) -> Result<(), String> {
        self.resp_tx.send(msg).map_err(|e| e.to_string())
    }
}

// ── Primary ↔ Secondary channel transport ──

/// Channel-based transport for the primary side of distributed coordination.
///
/// Holds per-secondary outgoing senders and a single incoming receiver
/// that aggregates messages from all secondaries.
pub struct ChannelSecondaryTransportEnd<I: Identifier> {
    pub outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    pub incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ChannelSecondaryTransportEnd<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.incoming_rx.recv().await
    }
}

impl<I: Identifier> SecondaryTransport<I> for ChannelSecondaryTransportEnd<I> {
    async fn send_to(&mut self, secondary_id: &str, msg: DistributedMessage<I>) -> Result<(), String> {
        if let Some(tx) = self.outgoing.get(secondary_id) {
            tx.send(msg).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

/// Channel-based transport for the secondary side of distributed coordination.
///
/// Sends to the primary and receives from it via unbounded mpsc channels.
pub struct ChannelPrimaryTransportEnd<I: Identifier> {
    pub tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    pub rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for ChannelPrimaryTransportEnd<I> {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.tx.send(msg).map_err(|e| e.to_string())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ChannelPrimaryTransportEnd<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_roundtrip() {
        let (mut manager, mut runner) = channel_pair();

        manager
            .send(Command::ProcessTask {
                relative_path: "test/bin".into(),
            })
            .await
            .unwrap();

        let cmd = runner.recv().await.unwrap();
        match cmd {
            Command::ProcessTask { relative_path } => {
                assert_eq!(relative_path, "test/bin");
            }
            _ => panic!("expected ProcessTask"),
        }
    }

    #[tokio::test]
    async fn response_roundtrip() {
        let (mut manager, mut runner) = channel_pair();

        runner
            .send(Response::Done {
                result_data: Some(b"2:5".to_vec()),
            })
            .await
            .unwrap();

        let resp = manager.recv().await.unwrap();
        match resp {
            Response::Done { result_data } => {
                assert_eq!(result_data.unwrap(), b"2:5");
            }
            _ => panic!("expected Done"),
        }
    }

    #[tokio::test]
    async fn stop_command() {
        let (mut manager, mut runner) = channel_pair();

        manager.send(Command::Stop).await.unwrap();

        let cmd = runner.recv().await.unwrap();
        assert!(matches!(cmd, Command::Stop));
    }

    #[tokio::test]
    async fn multiple_responses() {
        let (mut manager, mut runner) = channel_pair();

        runner.send(Response::Ready).await.unwrap();
        runner
            .send(Response::PhaseUpdate {
                phase_name: "ANGR_1".into(),
            })
            .await
            .unwrap();
        runner.send(Response::Keepalive).await.unwrap();

        let r1 = manager.recv().await.unwrap();
        assert!(matches!(r1, Response::Ready));
        let r2 = manager.recv().await.unwrap();
        assert!(matches!(r2, Response::PhaseUpdate { .. }));
        let r3 = manager.recv().await.unwrap();
        assert!(matches!(r3, Response::Keepalive));
    }

    #[tokio::test]
    async fn runner_disconnect_returns_none() {
        let (manager, mut runner) = channel_pair();

        // Drop the manager end
        drop(manager);

        // Runner should get a send error
        let result = runner.send(Response::Ready).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn manager_disconnect_returns_none() {
        let (mut manager, runner) = channel_pair();

        // Drop the runner end
        drop(runner);

        // Manager recv should return None (disconnected)
        let resp = manager.recv().await;
        assert!(resp.is_none());
    }
}
