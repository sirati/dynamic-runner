use db_comm_api_base::{ErrorType, Response, ResponseSender, RunnerEndpoint};

/// Output from a successful task execution.
pub struct TaskOutput {
    pub warnings: u32,
    pub filtered: u32,
}

/// Error from a failed task execution.
pub struct TaskError {
    pub error_type: ErrorType,
    pub message: String,
}

/// Trait for the actual task execution logic.
///
/// Implementations are provided by Python (via PyO3) or by Rust test harnesses.
/// The executor receives the relative path to process and a handle to send
/// phase updates and keepalives during execution.
///
/// Generic over `S` (the ResponseSender) to avoid dyn-compatibility issues
/// with async traits.
pub trait TaskExecutor<S: ResponseSender> {
    fn execute(
        &self,
        relative_path: &str,
        status_sender: &mut S,
    ) -> impl std::future::Future<Output = Result<TaskOutput, TaskError>>;
}

/// The runner's main loop. Transport-agnostic: takes any RunnerEndpoint.
///
/// 1. Sends Ready
/// 2. Waits for commands
/// 3. For each ProcessBinary: executes the task, sends Done/Error
/// 4. On Stop or connection close: exits
pub async fn runner_main_loop<E: RunnerEndpoint>(
    endpoint: &mut E,
    executor: &impl TaskExecutor<E>,
) {
    // Send Ready
    if endpoint.send_response(Response::Ready).await.is_err() {
        return;
    }

    loop {
        match endpoint.recv_command().await {
            Some(db_comm_api_base::Command::Stop) => break,
            Some(db_comm_api_base::Command::ProcessBinary { relative_path }) => {
                match executor.execute(&relative_path, endpoint).await {
                    Ok(output) => {
                        let _ = endpoint
                            .send_response(Response::Done {
                                warnings: output.warnings,
                                filtered: output.filtered,
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = endpoint
                            .send_response(Response::Error {
                                error_type: e.error_type,
                                message: e.message,
                            })
                            .await;
                    }
                }
            }
            None => break, // Connection closed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db_comm_api_base::{Command, CommandSender, ResponseReceiver};
    use db_transport_channel::{ChannelRunnerEnd, channel_pair};

    struct EchoExecutor;

    impl TaskExecutor<ChannelRunnerEnd> for EchoExecutor {
        async fn execute(
            &self,
            relative_path: &str,
            status_sender: &mut ChannelRunnerEnd,
        ) -> Result<TaskOutput, TaskError> {
            // Send a phase update
            let _ = status_sender
                .send_response(Response::PhaseUpdate {
                    phase_name: "PROCESSING".into(),
                })
                .await;

            if relative_path == "fail" {
                return Err(TaskError {
                    error_type: ErrorType::Recoverable,
                    message: "intentional failure".into(),
                });
            }

            Ok(TaskOutput {
                warnings: 0,
                filtered: 0,
            })
        }
    }

    #[tokio::test]
    async fn runner_processes_task_and_stops() {
        let (mut manager, mut runner) = channel_pair();
        let executor = EchoExecutor;

        let runner_handle = tokio::spawn(async move {
            runner_main_loop(&mut runner, &executor).await;
        });

        // Should receive Ready
        let responses = manager.recv_responses().await;
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], Response::Ready));

        // Send a task
        manager
            .send_command(Command::ProcessBinary {
                relative_path: "test/bin".into(),
            })
            .await
            .unwrap();

        // Collect all responses until we see Done
        let mut all = Vec::new();
        loop {
            let responses = manager.recv_responses().await;
            if responses.is_empty() {
                break;
            }
            all.extend(responses);
            if all.iter().any(|r| matches!(r, Response::Done { .. })) {
                break;
            }
        }

        let has_phase = all
            .iter()
            .any(|r| matches!(r, Response::PhaseUpdate { .. }));
        let has_done = all.iter().any(|r| matches!(r, Response::Done { .. }));
        assert!(has_phase, "expected PhaseUpdate");
        assert!(has_done, "expected Done");

        // Send stop
        manager.send_command(Command::Stop).await.unwrap();
        runner_handle.await.unwrap();
    }

    #[tokio::test]
    async fn runner_handles_failure() {
        let (mut manager, mut runner) = channel_pair();
        let executor = EchoExecutor;

        let runner_handle = tokio::spawn(async move {
            runner_main_loop(&mut runner, &executor).await;
        });

        // Ready
        let _ = manager.recv_responses().await;

        // Send failing task
        manager
            .send_command(Command::ProcessBinary {
                relative_path: "fail".into(),
            })
            .await
            .unwrap();

        // Collect responses until we get Error
        let mut all = Vec::new();
        loop {
            let responses = manager.recv_responses().await;
            if responses.is_empty() {
                break;
            }
            all.extend(responses);
            if all.iter().any(|r| matches!(r, Response::Error { .. })) {
                break;
            }
        }

        let error = all
            .iter()
            .find(|r| matches!(r, Response::Error { .. }))
            .unwrap();
        match error {
            Response::Error {
                error_type,
                message,
            } => {
                assert_eq!(*error_type, ErrorType::Recoverable);
                assert_eq!(message, "intentional failure");
            }
            _ => unreachable!(),
        }

        manager.send_command(Command::Stop).await.unwrap();
        runner_handle.await.unwrap();
    }

    #[tokio::test]
    async fn runner_exits_on_connection_close() {
        let (manager, mut runner) = channel_pair();
        let executor = EchoExecutor;

        let runner_handle = tokio::spawn(async move {
            runner_main_loop(&mut runner, &executor).await;
        });

        // Drop manager (close connection) — runner should see Ready send fail or recv None
        drop(manager);

        // Runner should exit cleanly
        runner_handle.await.unwrap();
    }
}
