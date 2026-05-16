//! Manager ↔ Runner channel transport.
//!
//! In-process tokio-mpsc pair for the manager-side `Command` /
//! `Response` protocol. Used by tests that need a fully in-process
//! manager/worker pair without spawning subprocesses.

use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::{Command, Response};
use tokio::sync::mpsc;

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
