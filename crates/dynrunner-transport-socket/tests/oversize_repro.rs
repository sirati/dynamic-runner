//! #364 repro + gate: an oversize `done:<payload>` frame through the
//! REAL worker→manager socketpair channel and the REAL protocol state
//! machine. Writer side emulates the Python worker: a blocking
//! `std::os::unix::net::UnixStream` doing one big `write_all` (Python
//! `socket.sendall`) from a dedicated OS thread.
//!
//! Production capture (the defect): the worker published a ~55MB
//! string; the frame fully transferred; the consuming side dropped it
//! with NO reply; the worker stayed wedged forever and the task
//! stranded. Pre-fix, this channel happily delivered an arbitrarily
//! large frame into memory, leaving the drop-or-strand decision to
//! whatever sat downstream. The gate pinned here:
//!
//! * an over-limit frame is REJECTED LOUDLY — `poll_status` resolves
//!   (bounded time, no wedge) to a NonRecoverable failure naming the
//!   actual size and the limit, which rides the existing
//!   restart-the-worker machinery (the worker is released, never
//!   stranded);
//! * the writer's sendall COMPLETES (the manager drains the frame —
//!   the stream stays frame-aligned rather than wedging the sender);
//! * an under-limit large payload (8 MiB) still flows end-to-end
//!   intact (legitimate large outputs are not broken).

use std::io::Write;
use std::os::fd::FromRawFd;
use std::time::Duration;

use dynrunner_core::ErrorType;
use dynrunner_protocol_manager_worker::MAX_RESPONSE_FRAME_BYTES;
use dynrunner_protocol_manager_worker::state::{
    AssignResult, PollResult, RunnerProtocol, WaitReadyResult,
};
use dynrunner_transport_socket::socketpair::create_socketpair;

/// Drive the real manager-side protocol up to `Processing` against a
/// blocking writer thread that has already sent `ready\n`.
async fn into_processing(
    manager: dynrunner_transport_socket::socketpair::SocketpairManagerEnd,
) -> RunnerProtocol<dynrunner_protocol_manager_worker::state::Processing, dynrunner_transport_socket::socketpair::SocketpairManagerEnd>
{
    let waiting = RunnerProtocol::connect(manager);
    let idle = match waiting.wait_ready().await {
        WaitReadyResult::Ready(idle) => idle,
        _ => panic!("worker did not become ready"),
    };
    match idle
        .assign_task("some/task".into(), None, None, Default::default())
        .await
    {
        AssignResult::Assigned(processing) => processing,
        AssignResult::SendFailed { error, .. } => panic!("assign failed: {error}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn oversize_done_frame_fails_loud_and_releases_worker() {
    let (manager, child_fd) = create_socketpair().unwrap();

    // One byte over the cap once the `done:` prefix and trailing
    // newline are counted.
    let payload_len = MAX_RESPONSE_FRAME_BYTES; // + "done:" + "\n" > cap
    let expected_line_len = payload_len + b"done:".len() + 1;

    // Blocking writer thread = the Python worker's sendall.
    let writer = std::thread::spawn(move || {
        let mut sock = unsafe { std::os::unix::net::UnixStream::from_raw_fd(child_fd) };
        sock.write_all(b"ready\n").unwrap();
        let mut line = Vec::with_capacity(payload_len + 16);
        line.extend_from_slice(b"done:");
        line.resize(line.len() + payload_len, b'A');
        line.push(b'\n');
        let start = std::time::Instant::now();
        sock.write_all(&line).unwrap();
        sock.flush().unwrap();
        eprintln!("writer: finished sendall in {:?}", start.elapsed());
        // Keep the socket open: the production worker stays alive,
        // blocked in its next read. The manager must reject WITHOUT
        // needing the worker to hang up.
        sock
    });

    let processing = into_processing(manager).await;

    let poll = tokio::time::timeout(Duration::from_secs(60), processing.poll_status()).await;
    match poll {
        Ok(PollResult::Disconnected { result, .. }) => {
            // The loud reject: NonRecoverable + names actual size and
            // the limit. `Disconnected` is the protocol's
            // restart-the-worker outcome — the existing machinery
            // kills/respawns the subprocess, releasing the wedge.
            assert!(!result.success);
            assert_eq!(result.error_type, Some(ErrorType::NonRecoverable));
            let msg = result.error_message.expect("reject carries a message");
            assert!(
                msg.contains(&expected_line_len.to_string()),
                "message must name the actual frame size: {msg}"
            );
            assert!(
                msg.contains(&MAX_RESPONSE_FRAME_BYTES.to_string()),
                "message must name the limit: {msg}"
            );
        }
        Ok(PollResult::Completed { result_data, .. }) => panic!(
            "oversize frame must be rejected, not delivered \
             ({} bytes accepted) — this is the pre-fix defect shape",
            result_data.map(|d| d.len()).unwrap_or(0)
        ),
        Ok(PollResult::StillRunning { .. }) => {
            panic!("oversize frame must resolve the poll, not idle through it")
        }
        Err(_) => panic!("WEDGED: poll_status did not complete within 60s"),
    }

    // The frame fully transferred — the manager drained it, so the
    // worker-side sendall returned (the worker is not wedged on a
    // full socket buffer).
    let _sock = writer.join().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn under_limit_8mb_done_frame_flows_end_to_end() {
    let (manager, child_fd) = create_socketpair().unwrap();

    let payload_len = 8 * 1024 * 1024;
    let writer = std::thread::spawn(move || {
        let mut sock = unsafe { std::os::unix::net::UnixStream::from_raw_fd(child_fd) };
        sock.write_all(b"ready\n").unwrap();
        let mut line = Vec::with_capacity(payload_len + 16);
        line.extend_from_slice(b"done:");
        line.resize(line.len() + payload_len, b'B');
        line.push(b'\n');
        sock.write_all(&line).unwrap();
        sock.flush().unwrap();
    });

    let processing = into_processing(manager).await;

    let poll = tokio::time::timeout(Duration::from_secs(60), processing.poll_status()).await;
    match poll {
        Ok(PollResult::Completed {
            result,
            result_data,
            ..
        }) => {
            assert!(result.success);
            let data = result_data.expect("payload present");
            assert_eq!(data.len(), payload_len, "payload must arrive intact");
            assert!(data.iter().all(|&b| b == b'B'));
        }
        Ok(other) => panic!(
            "under-limit payload must complete normally, got {}",
            match other {
                PollResult::Disconnected { result, .. } =>
                    format!("Disconnected({:?})", result.error_message),
                PollResult::StillRunning { .. } => "StillRunning".into(),
                PollResult::Completed { .. } => unreachable!(),
            }
        ),
        Err(_) => panic!("WEDGED: poll_status did not complete within 60s"),
    }
    writer.join().unwrap();
}
