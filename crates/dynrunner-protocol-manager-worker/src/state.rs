use std::marker::PhantomData;

use dynrunner_core::{ErrorType, MessageReceiver, MessageSender, TaskResult};
use crate::command::{Command, Response};

/// Composite trait: a manager endpoint can send commands and receive responses.
pub trait ManagerEndpoint: MessageSender<Command> + MessageReceiver<Response> {}
impl<T: MessageSender<Command> + MessageReceiver<Response>> ManagerEndpoint for T {}

/// Composite trait: a runner endpoint can receive commands and send responses.
pub trait RunnerEndpoint: MessageReceiver<Command> + MessageSender<Response> {}
impl<T: MessageReceiver<Command> + MessageSender<Response>> RunnerEndpoint for T {}

// --- ZST state tags ---
pub struct Unconnected;
pub struct WaitingForReady;
pub struct Idle;
pub struct Processing;
pub struct Stopped;

/// The manager's view of one runner's protocol state.
///
/// Generic over `M: ManagerEndpoint` (which is `MessageSender<Command> + MessageReceiver<Response>`).
/// State transitions consume `self` and return the next state, enforcing
/// valid transitions at compile time.
pub struct RunnerProtocol<State, M: ManagerEndpoint> {
    _state: PhantomData<State>,
    transport: M,
}

impl<M: ManagerEndpoint> RunnerProtocol<Unconnected, M> {
    /// Transition: Unconnected -> WaitingForReady
    pub fn connect(transport: M) -> RunnerProtocol<WaitingForReady, M> {
        RunnerProtocol {
            _state: PhantomData,
            transport,
        }
    }
}

impl<M: ManagerEndpoint> RunnerProtocol<WaitingForReady, M> {
    /// Poll for the Ready response.
    /// Returns Idle on success, NotYet if still waiting, or Disconnected.
    pub async fn wait_ready(mut self) -> WaitReadyResult<M> {
        match self.transport.recv().await {
            Some(Response::Ready) => {
                WaitReadyResult::Ready(RunnerProtocol {
                    _state: PhantomData,
                    transport: self.transport,
                })
            }
            Some(_) => {
                // Got a non-Ready response, keep waiting
                WaitReadyResult::NotYet(self)
            }
            None => {
                // Connection closed
                WaitReadyResult::Disconnected(RunnerProtocol {
                    _state: PhantomData,
                    transport: self.transport,
                })
            }
        }
    }
}

pub enum WaitReadyResult<M: ManagerEndpoint> {
    Ready(RunnerProtocol<Idle, M>),
    NotYet(RunnerProtocol<WaitingForReady, M>),
    Disconnected(RunnerProtocol<Stopped, M>),
}

impl<M: ManagerEndpoint> RunnerProtocol<Idle, M> {
    /// Transition: Idle -> Processing (send ProcessTask command).
    ///
    /// `payload` is the consumer's opaque per-task data forwarded
    /// from `TaskInfo.payload`; pass `None` for tasks with no
    /// payload (the wire then collapses to the legacy `<path>\n`
    /// form). When `Some`, the worker can read it via the
    /// `payload` field of the parsed `ProcessTask` command and
    /// skip a per-task filesystem read entirely (the FR-3 use
    /// case for `uses_file_based_items=False` consumers).
    pub async fn assign_task(
        mut self,
        relative_path: String,
        payload: Option<String>,
    ) -> AssignResult<M> {
        let cmd = Command::ProcessTask {
            relative_path,
            payload,
        };
        match self.transport.send(cmd).await {
            Ok(()) => AssignResult::Assigned(RunnerProtocol {
                _state: PhantomData,
                transport: self.transport,
            }),
            Err(e) => AssignResult::SendFailed {
                error: e,
                protocol: RunnerProtocol {
                    _state: PhantomData,
                    transport: self.transport,
                },
            },
        }
    }

    /// Transition: Idle -> Stopped (send Stop command)
    pub async fn stop(mut self) -> RunnerProtocol<Stopped, M> {
        let _ = self.transport.send(Command::Stop).await;
        RunnerProtocol {
            _state: PhantomData,
            transport: self.transport,
        }
    }
}

pub enum AssignResult<M: ManagerEndpoint> {
    Assigned(RunnerProtocol<Processing, M>),
    SendFailed {
        error: String,
        protocol: RunnerProtocol<Stopped, M>,
    },
}

impl<M: ManagerEndpoint> RunnerProtocol<Processing, M> {
    /// Poll for the next response from the runner.
    ///
    /// Returns Completed on done/error, StillRunning on phase/keepalive
    /// updates, or Disconnected if the connection closed.
    pub async fn poll_status(mut self) -> PollResult<M> {
        match self.transport.recv().await {
            None => {
                // Phase D: a worker process dying mid-task without
                // sending a final Error response is most likely an
                // environment glitch (OOM-killer, host crash, signal)
                // or a worker-process bug. Either way, retrying is
                // the safe default — repeated Recoverable failures
                // still get caught by the retry-pass exhaustion logic
                // (`MAX_RETRY_ATTEMPTS`). Pre-Phase-D this hardcoded
                // NonRecoverable, which prevented retry on the
                // common case (worker crashed in user code without
                // catching the exception).
                PollResult::Disconnected {
                    result: TaskResult::error(
                        ErrorType::Recoverable,
                        "Worker connection closed".into(),
                    ),
                    protocol: RunnerProtocol {
                        _state: PhantomData,
                        transport: self.transport,
                    },
                }
            }
            Some(response) => match response {
                Response::Done { result_data } => {
                    // `result_data` is forwarded as opaque bytes to the manager
                    // and surfaced via `LocalManager::task_results()` for the
                    // Python-side task-specific aggregator (M6).
                    PollResult::Completed {
                        result: TaskResult::ok(),
                        result_data,
                        protocol: RunnerProtocol {
                            _state: PhantomData,
                            transport: self.transport,
                        },
                    }
                }
                Response::Error {
                    error_type,
                    message,
                } => {
                    let needs_restart = error_type == ErrorType::NonRecoverable;
                    let result = TaskResult::error(error_type, message);
                    if needs_restart {
                        PollResult::Disconnected {
                            result,
                            protocol: RunnerProtocol {
                                _state: PhantomData,
                                transport: self.transport,
                            },
                        }
                    } else {
                        PollResult::Completed {
                            result,
                            result_data: None,
                            protocol: RunnerProtocol {
                                _state: PhantomData,
                                transport: self.transport,
                            },
                        }
                    }
                }
                Response::WorkerException {
                    exception_type,
                    message,
                    traceback,
                    error_type,
                } => {
                    // `error_type` controls restart-vs-recover. Legacy
                    // wire (no field set) defaults to NonRecoverable
                    // — matches the original "worker process is
                    // corrupt, restart it" semantic. Newer senders
                    // can set Recoverable to attach a traceback to a
                    // user-task failure WITHOUT killing the worker
                    // (the formatted body still contains
                    // exception_type + message + traceback so the
                    // consumer's WARN log shows the full stack).
                    let category = error_type.unwrap_or(ErrorType::NonRecoverable);
                    let needs_restart = category == ErrorType::NonRecoverable;
                    let result = TaskResult::error(
                        category,
                        format!("{exception_type}: {message}\n{traceback}"),
                    );
                    if needs_restart {
                        PollResult::Disconnected {
                            result,
                            protocol: RunnerProtocol {
                                _state: PhantomData,
                                transport: self.transport,
                            },
                        }
                    } else {
                        PollResult::Completed {
                            result,
                            result_data: None,
                            protocol: RunnerProtocol {
                                _state: PhantomData,
                                transport: self.transport,
                            },
                        }
                    }
                }
                Response::PhaseUpdate { phase_name } => {
                    PollResult::StillRunning {
                        protocol: self,
                        phase_update: Some(phase_name),
                        got_keepalive: false,
                    }
                }
                Response::Keepalive => {
                    PollResult::StillRunning {
                        protocol: self,
                        phase_update: None,
                        got_keepalive: true,
                    }
                }
                Response::Ready => {
                    // Spurious ready during processing — ignore
                    PollResult::StillRunning {
                        protocol: self,
                        phase_update: None,
                        got_keepalive: false,
                    }
                }
            },
        }
    }
}

pub enum PollResult<M: ManagerEndpoint> {
    Completed {
        result: TaskResult,
        /// Opaque task-specific payload returned by the worker on `done:<bytes>`.
        /// The runner does not interpret these bytes — Python decodes them.
        result_data: Option<Vec<u8>>,
        protocol: RunnerProtocol<Idle, M>,
    },
    StillRunning {
        protocol: RunnerProtocol<Processing, M>,
        phase_update: Option<String>,
        got_keepalive: bool,
    },
    Disconnected {
        result: TaskResult,
        protocol: RunnerProtocol<Stopped, M>,
    },
}

/// Runtime enum wrapper for storing runners in a collection.
///
/// Each worker may be in a different protocol state, so we need a runtime
/// enum to hold them in a `Vec`.
pub enum RunnerProtocolState<M: ManagerEndpoint> {
    Unconnected,
    WaitingForReady(RunnerProtocol<WaitingForReady, M>),
    Idle(RunnerProtocol<Idle, M>),
    Processing(RunnerProtocol<Processing, M>),
    Stopped(RunnerProtocol<Stopped, M>),
    /// Temporarily taken out for a state transition.
    Transitioning,
}

impl<M: ManagerEndpoint> RunnerProtocolState<M> {
    pub fn take_idle(&mut self) -> Option<RunnerProtocol<Idle, M>> {
        match std::mem::replace(self, RunnerProtocolState::Transitioning) {
            RunnerProtocolState::Idle(p) => Some(p),
            other => {
                *self = other;
                None
            }
        }
    }

    pub fn take_processing(&mut self) -> Option<RunnerProtocol<Processing, M>> {
        match std::mem::replace(self, RunnerProtocolState::Transitioning) {
            RunnerProtocolState::Processing(p) => Some(p),
            other => {
                *self = other;
                None
            }
        }
    }

    pub fn take_waiting(&mut self) -> Option<RunnerProtocol<WaitingForReady, M>> {
        match std::mem::replace(self, RunnerProtocolState::Transitioning) {
            RunnerProtocolState::WaitingForReady(p) => Some(p),
            other => {
                *self = other;
                None
            }
        }
    }

    pub fn is_idle(&self) -> bool {
        matches!(self, RunnerProtocolState::Idle(_))
    }

    pub fn is_processing(&self) -> bool {
        matches!(self, RunnerProtocolState::Processing(_))
    }

    pub fn is_stopped(&self) -> bool {
        matches!(self, RunnerProtocolState::Stopped(_))
    }
}
