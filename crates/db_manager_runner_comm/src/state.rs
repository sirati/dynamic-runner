use std::marker::PhantomData;

use db_comm_api_base::{Command, ErrorType, ManagerEndpoint, Response, TaskResult};

// --- ZST state tags ---
pub struct Unconnected;
pub struct WaitingForReady;
pub struct Idle;
pub struct Processing;
pub struct Stopped;

/// The manager's view of one runner's protocol state.
///
/// Generic over `M: ManagerEndpoint` (which is `CommandSender + ResponseReceiver`).
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
        let responses = self.transport.recv_responses().await;
        for response in &responses {
            if matches!(response, Response::Ready) {
                return WaitReadyResult::Ready(RunnerProtocol {
                    _state: PhantomData,
                    transport: self.transport,
                });
            }
        }
        if responses.is_empty() {
            return WaitReadyResult::Disconnected(RunnerProtocol {
                _state: PhantomData,
                transport: self.transport,
            });
        }
        WaitReadyResult::NotYet(self)
    }
}

pub enum WaitReadyResult<M: ManagerEndpoint> {
    Ready(RunnerProtocol<Idle, M>),
    NotYet(RunnerProtocol<WaitingForReady, M>),
    Disconnected(RunnerProtocol<Stopped, M>),
}

impl<M: ManagerEndpoint> RunnerProtocol<Idle, M> {
    /// Transition: Idle -> Processing (send ProcessBinary command)
    pub async fn assign_task(mut self, relative_path: String) -> AssignResult<M> {
        let cmd = Command::ProcessBinary { relative_path };
        match self.transport.send_command(cmd).await {
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
        let _ = self.transport.send_command(Command::Stop).await;
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
    /// Poll for task completion.
    ///
    /// Returns Completed on done/error, StillRunning if only phase/keepalive
    /// updates were received, or Disconnected if the connection closed.
    pub async fn poll_status(mut self) -> PollResult<M> {
        let responses = self.transport.recv_responses().await;

        if responses.is_empty() {
            return PollResult::Disconnected {
                result: TaskResult::error(
                    ErrorType::NonRecoverable,
                    "Worker connection closed".into(),
                ),
                protocol: RunnerProtocol {
                    _state: PhantomData,
                    transport: self.transport,
                },
            };
        }

        let mut phase_updates = Vec::new();
        let mut got_keepalive = false;

        for response in responses {
            match response {
                Response::Done { warnings, filtered } => {
                    return PollResult::Completed {
                        result: TaskResult::ok(warnings, filtered),
                        protocol: RunnerProtocol {
                            _state: PhantomData,
                            transport: self.transport,
                        },
                        phase_updates,
                    };
                }
                Response::Error {
                    error_type,
                    message,
                } => {
                    let needs_restart = error_type == ErrorType::NonRecoverable;
                    let result = TaskResult::error(error_type, message);
                    if needs_restart {
                        return PollResult::Disconnected {
                            result,
                            protocol: RunnerProtocol {
                                _state: PhantomData,
                                transport: self.transport,
                            },
                        };
                    }
                    return PollResult::Completed {
                        result,
                        protocol: RunnerProtocol {
                            _state: PhantomData,
                            transport: self.transport,
                        },
                        phase_updates,
                    };
                }
                Response::PickledError {
                    exception_type,
                    message,
                    traceback,
                } => {
                    return PollResult::Disconnected {
                        result: TaskResult::error(
                            ErrorType::NonRecoverable,
                            format!("{exception_type}: {message}\n{traceback}"),
                        ),
                        protocol: RunnerProtocol {
                            _state: PhantomData,
                            transport: self.transport,
                        },
                    };
                }
                Response::PhaseUpdate { phase_name } => {
                    phase_updates.push(phase_name);
                }
                Response::Keepalive => {
                    got_keepalive = true;
                }
                Response::Ready => {}
            }
        }

        PollResult::StillRunning {
            protocol: self,
            phase_updates,
            got_keepalive,
        }
    }
}

pub enum PollResult<M: ManagerEndpoint> {
    Completed {
        result: TaskResult,
        protocol: RunnerProtocol<Idle, M>,
        phase_updates: Vec<String>,
    },
    StillRunning {
        protocol: RunnerProtocol<Processing, M>,
        phase_updates: Vec<String>,
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
