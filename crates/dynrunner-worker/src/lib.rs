use dynrunner_core::{ErrorType, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::{Command, Response, RunnerEndpoint};

/// Output from a successful task execution.
pub struct TaskOutput {
    pub result_data: Option<Vec<u8>>,
}

/// Error from a failed task execution.
pub struct TaskError {
    pub error_type: ErrorType,
    pub message: String,
}

/// Trait for the actual task execution logic.
///
/// Implementations are provided by Python (via PyO3) or by Rust test harnesses.
/// The executor receives the relative path to process, the optional
/// per-task payload (FR-3 — `TaskInfo.payload` serialised as a JSON
/// string, `None` for the legacy file-only wire), the optional
/// locally-resolved on-disk location of the file (`None` for
/// LocalManager dispatches and any distributed dispatch that didn't
/// trigger extraction-cache resolution; `Some(p)` means "open `p`
/// directly; treat `relative_path` purely as the wire identifier
/// used for output-tree mirroring"), and a handle to send phase
/// updates and keepalives during execution.
///
/// Generic over `S` (a `MessageSender<Response>`) to avoid dyn-compatibility issues
/// with async traits.
pub trait TaskExecutor<S: MessageSender<Response>> {
    fn execute(
        &self,
        relative_path: &str,
        payload: Option<&str>,
        resolved_path: Option<&str>,
        status_sender: &mut S,
    ) -> impl std::future::Future<Output = Result<TaskOutput, TaskError>>;
}

/// The runner's main loop. Transport-agnostic: takes any RunnerEndpoint.
///
/// 1. Sends Ready
/// 2. Waits for commands
/// 3. For each ProcessTask: executes the task, sends Done/Error
/// 4. On Stop or connection close: exits
pub async fn runner_main_loop<E: RunnerEndpoint>(
    endpoint: &mut E,
    executor: &impl TaskExecutor<E>,
) {
    // Send Ready
    if endpoint.send(Response::Ready).await.is_err() {
        return;
    }

    loop {
        match MessageReceiver::<Command>::recv(endpoint).await {
            Some(Command::Stop) => break,
            Some(Command::ProcessTask {
                relative_path,
                payload,
                resolved_path,
                // `predecessor_outputs` is plumbed from the manager
                // through the wire codec to the worker's user code
                // via the PyO3 bridge; the framework-side worker
                // loop here is opaque to its contents.
                ..
            }) => {
                match executor
                    .execute(
                        &relative_path,
                        payload.as_deref(),
                        resolved_path.as_deref(),
                        endpoint,
                    )
                    .await
                {
                    Ok(output) => {
                        let _ = endpoint
                            .send(Response::Done {
                                result_data: output.result_data,
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = endpoint
                            .send(Response::Error {
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
    use dynrunner_transport_channel::{ChannelRunnerEnd, channel_pair};

    struct EchoExecutor;

    impl TaskExecutor<ChannelRunnerEnd> for EchoExecutor {
        async fn execute(
            &self,
            relative_path: &str,
            _payload: Option<&str>,
            _resolved_path: Option<&str>,
            status_sender: &mut ChannelRunnerEnd,
        ) -> Result<TaskOutput, TaskError> {
            let _ = status_sender
                .send(Response::PhaseUpdate {
                    phase_name: "PROCESSING".into(),
                })
                .await;

            if relative_path == "fail" {
                return Err(TaskError {
                    error_type: ErrorType::Recoverable,
                    message: "intentional failure".into(),
                });
            }

            Ok(TaskOutput { result_data: None })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runner_processes_task_and_stops() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
        let (mut manager, mut runner) = channel_pair();
        let executor = EchoExecutor;

        let runner_handle = tokio::task::spawn_local(async move {
            runner_main_loop(&mut runner, &executor).await;
        });

        let resp = manager.recv().await.unwrap();
        assert!(matches!(resp, Response::Ready));

        manager
            .send(Command::ProcessTask {
                relative_path: "test/bin".into(),
                payload: None,
                resolved_path: None,
                predecessor_outputs: std::collections::BTreeMap::new(),
            })
            .await
            .unwrap();

        let mut all = Vec::new();
        while let Some(r) = manager.recv().await {
            let is_done = matches!(r, Response::Done { .. });
            all.push(r);
            if is_done {
                break;
            }
        }

        let has_phase = all
            .iter()
            .any(|r| matches!(r, Response::PhaseUpdate { .. }));
        let has_done = all.iter().any(|r| matches!(r, Response::Done { .. }));
        assert!(has_phase, "expected PhaseUpdate");
        assert!(has_done, "expected Done");

        manager.send(Command::Stop).await.unwrap();
        runner_handle.await.unwrap();
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runner_handles_failure() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
        let (mut manager, mut runner) = channel_pair();
        let executor = EchoExecutor;

        let runner_handle = tokio::task::spawn_local(async move {
            runner_main_loop(&mut runner, &executor).await;
        });

        let _ = manager.recv().await;

        manager
            .send(Command::ProcessTask {
                relative_path: "fail".into(),
                payload: None,
                resolved_path: None,
                predecessor_outputs: std::collections::BTreeMap::new(),
            })
            .await
            .unwrap();

        let mut all = Vec::new();
        while let Some(r) = manager.recv().await {
            let is_error = matches!(r, Response::Error { .. });
            all.push(r);
            if is_error {
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

        manager.send(Command::Stop).await.unwrap();
        runner_handle.await.unwrap();
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runner_exits_on_connection_close() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
        let (manager, mut runner) = channel_pair();
        let executor = EchoExecutor;

        let runner_handle = tokio::task::spawn_local(async move {
            runner_main_loop(&mut runner, &executor).await;
        });

        drop(manager);

        runner_handle.await.unwrap();
        }).await;
    }
}
