//! Unit tests for the Manager↔Runner channel transport. Loaded by
//! `lib.rs` only under `#[cfg(test)]`. Covers send/recv round-trips
//! plus the disconnect semantics on either side.

use crate::manager_runner::channel_pair;
use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::{Command, Response};

#[tokio::test]
async fn command_roundtrip() {
    let (mut manager, mut runner) = channel_pair();

    manager
        .send(Command::ProcessTask {
            relative_path: "test/bin".into(),
            payload: None,
            resolved_path: None,
            predecessor_outputs: std::collections::BTreeMap::new(),
        })
        .await
        .unwrap();

    let cmd = runner.recv().await.unwrap();
    match cmd {
        Command::ProcessTask { relative_path, .. } => {
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
