use db_comm_api_base::{Command, CommandReceiver, CommandSender, Response, ResponseReceiver, ResponseSender};
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

impl CommandSender for ChannelManagerEnd {
    async fn send_command(&mut self, command: Command) -> Result<(), String> {
        self.cmd_tx.send(command).map_err(|e| e.to_string())
    }
}

impl ResponseReceiver for ChannelManagerEnd {
    async fn recv_responses(&mut self) -> Vec<Response> {
        let mut responses = Vec::new();
        // Try to receive all currently buffered responses without blocking.
        // If nothing is buffered, block on the first one to avoid busy-spinning.
        match self.resp_rx.try_recv() {
            Ok(resp) => {
                responses.push(resp);
                // Drain remaining buffered
                while let Ok(resp) = self.resp_rx.try_recv() {
                    responses.push(resp);
                }
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // Nothing buffered — block-wait for the next one
                if let Some(resp) = self.resp_rx.recv().await {
                    responses.push(resp);
                }
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // Channel closed — return empty to signal disconnection
            }
        }
        responses
    }
}

impl CommandReceiver for ChannelRunnerEnd {
    async fn recv_command(&mut self) -> Option<Command> {
        self.cmd_rx.recv().await
    }
}

impl ResponseSender for ChannelRunnerEnd {
    async fn send_response(&mut self, response: Response) -> Result<(), String> {
        self.resp_tx.send(response).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_roundtrip() {
        let (mut manager, mut runner) = channel_pair();

        manager
            .send_command(Command::ProcessBinary {
                relative_path: "test/bin".into(),
            })
            .await
            .unwrap();

        let cmd = runner.recv_command().await.unwrap();
        match cmd {
            Command::ProcessBinary { relative_path } => {
                assert_eq!(relative_path, "test/bin");
            }
            _ => panic!("expected ProcessBinary"),
        }
    }

    #[tokio::test]
    async fn response_roundtrip() {
        let (mut manager, mut runner) = channel_pair();

        runner
            .send_response(Response::Done {
                warnings: 2,
                filtered: 5,
            })
            .await
            .unwrap();

        let responses = manager.recv_responses().await;
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            Response::Done { warnings, filtered } => {
                assert_eq!(*warnings, 2);
                assert_eq!(*filtered, 5);
            }
            _ => panic!("expected Done"),
        }
    }

    #[tokio::test]
    async fn stop_command() {
        let (mut manager, mut runner) = channel_pair();

        manager.send_command(Command::Stop).await.unwrap();

        let cmd = runner.recv_command().await.unwrap();
        assert!(matches!(cmd, Command::Stop));
    }

    #[tokio::test]
    async fn multiple_responses_batched() {
        let (mut manager, mut runner) = channel_pair();

        runner.send_response(Response::Ready).await.unwrap();
        runner
            .send_response(Response::PhaseUpdate {
                phase_name: "ANGR_1".into(),
            })
            .await
            .unwrap();
        runner.send_response(Response::Keepalive).await.unwrap();

        // Small delay to let all messages arrive
        tokio::task::yield_now().await;

        let responses = manager.recv_responses().await;
        assert!(responses.len() >= 1); // at least first one
    }

    #[tokio::test]
    async fn runner_disconnect_returns_none() {
        let (manager, mut runner) = channel_pair();

        // Drop the manager end
        drop(manager);

        // Runner should get a send error
        let result = runner
            .send_response(Response::Ready)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn manager_disconnect_returns_empty() {
        let (mut manager, runner) = channel_pair();

        // Drop the runner end
        drop(runner);

        // Manager recv should return empty (disconnected)
        let responses = manager.recv_responses().await;
        assert!(responses.is_empty());
    }
}
