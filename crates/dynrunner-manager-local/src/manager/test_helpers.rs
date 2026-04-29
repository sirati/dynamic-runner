//! Shared test fixtures for the local manager. Compiled only under
//! `#[cfg(test)]` so they never enter the production binary.

use std::collections::HashMap;

use dynrunner_core::{
    BinaryInfo, ErrorType, MessageReceiver, MessageSender, PhaseId, ResourceKind, ResourceMap,
    TypeId, WorkerId,
};
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_scheduler_api::ResourceEstimator;
use dynrunner_transport_channel::{channel_pair, ChannelManagerEnd, ChannelRunnerEnd};
use serde::{Deserialize, Serialize};

use super::{LocalManagerConfig, WorkerFactory};

/// Minimal serializable identifier used by every manager test.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) struct TestId(pub String);

pub(super) struct FixedEstimator(pub u64);

impl ResourceEstimator for FixedEstimator {
    fn estimate(&self, _binary_size: u64) -> ResourceMap {
        ResourceMap::from([(ResourceKind::memory(), self.0)])
    }
}

pub(super) fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
    BinaryInfo {
        path: std::path::PathBuf::from(name),
        size,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
    }
}

/// A simple factory that spawns fake worker tasks that auto-respond.
pub(super) struct FakeWorkerFactory {
    pub mode: FakeWorkerMode,
}

#[derive(Clone)]
pub(super) enum FakeWorkerMode {
    AlwaysSucceed,
    AlwaysOom,
    FailThenSucceed,
}

impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: WorkerId,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        let mode = self.mode.clone();
        tokio::task::spawn_local(fake_worker_loop(runner_end, mode));
        Ok((manager_end, None))
    }
}

pub(super) async fn fake_worker_loop(mut runner: ChannelRunnerEnd, mode: FakeWorkerMode) {
    let _ = runner.send(Response::Ready).await;

    let mut task_count = 0u32;
    loop {
        match MessageReceiver::<Command>::recv(&mut runner).await {
            Some(Command::Stop) => break,
            Some(Command::ProcessTask { .. }) => {
                task_count += 1;
                match &mode {
                    FakeWorkerMode::AlwaysSucceed => {
                        let _ = runner.send(Response::Done { result_data: None }).await;
                    }
                    FakeWorkerMode::AlwaysOom => {
                        let _ = runner
                            .send(Response::Error {
                                error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                                message: "out of memory".into(),
                            })
                            .await;
                    }
                    FakeWorkerMode::FailThenSucceed => {
                        if task_count == 1 {
                            let _ = runner
                                .send(Response::Error {
                                    error_type: ErrorType::Recoverable,
                                    message: "transient failure".into(),
                                })
                                .await;
                        } else {
                            let _ = runner.send(Response::Done { result_data: None }).await;
                        }
                    }
                }
            }
            None => break,
        }
    }
}

/// Default config for tests that don't care about the per-test
/// LocalManagerConfig knobs. 1 GiB memory, 300 MiB low-water-mark,
/// no stage timeouts, no resource-pressure phase, no stuck-worker
/// reporter.
pub(super) fn test_config(num_workers: u32) -> LocalManagerConfig {
    LocalManagerConfig {
        num_workers,
        max_resources: ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]),
        always_restart_worker: false,
        restart_predicate: None,
        retry_max_attempts: 1,
        print_pid: false,
        memuse_log_path: None,
        stage_timeouts: HashMap::new(),
        low_resource_thresholds: ResourceMap::from([(ResourceKind::memory(), 300 * 1024 * 1024)]),
        resource_check_interval: std::time::Duration::from_millis(100),
        phase_status_log_intervals: Vec::new(),
    }
}
