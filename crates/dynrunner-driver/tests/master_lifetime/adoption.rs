//! Adopt-then-disconnect partial cleanup test (locked design point
//! b). An adopt-handle's `disconnect()` releases its runtime
//! forwards via `ssh -O cancel -R` but leaves the underlying daemon
//! alive — that is the contract a peer handle expects.

use std::time::{Duration, Instant};

use dynrunner_driver::ssh_master::SshMaster;

use crate::helpers::{
    AuthorizedKey, make_config, pick_free_port, pid_alive, serialise, sshd_reachable,
};

/// Locked design point (b): adopt-master `disconnect()` runs
/// `ssh -O cancel -R` per forwarded_ports entry. PARTIAL cleanup —
/// the master is still alive after disconnect.
///
/// Topology:
///   1. Spawn master via handle A.
///   2. Adopt the same control socket via handle B.
///   3. Register a runtime forward via B.add_forward(...).
///   4. B.disconnect(): forward must be released AND master (handle
///      A's daemon PID) must still be alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adopt_disconnect_partial_cleanup() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — adopt-disconnect contract unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let master_a = SshMaster::spawn(make_config(&authorized)).expect("spawn A");
    let pid_a = master_a.master_pid().expect("pid A");
    assert!(pid_alive(pid_a));

    let cp = master_a.control_path().to_path_buf();
    let target_b = master_a.target().clone();

    // adopt() with minimal flags — locked point (k): the master
    // responds via the unix socket regardless of identity / config
    // flags. We pass the same target the master was spawned with
    // for telemetry consistency.
    let mut master_b = SshMaster::adopt(cp.clone(), target_b).expect("adopt B");
    assert!(!master_b.is_spawned(), "B is adopt-master");
    assert!(master_a.is_spawned(), "A is spawn-master");

    // Pick two free localhost ports for the forward. We don't
    // actually need the forward to *work* end-to-end; we just need
    // it registered against the master so disconnect()'s
    // `ssh -O cancel -R` has something to release.
    let local_port = pick_free_port();
    let remote_port = pick_free_port();

    master_b
        .add_forward(local_port, remote_port)
        .expect("register runtime forward via adopt-handle");

    // disconnect() exercises the per-forward `ssh -O cancel -R`
    // cleanup path.
    master_b
        .disconnect()
        .expect("adopt-disconnect runs ssh -O cancel per forward");

    // Master A's daemon must still be alive — adopt-disconnect
    // is partial cleanup, not termination.
    assert!(
        pid_alive(pid_a),
        "spawn-master daemon pid {pid_a} must still be alive after \
         adopt-disconnect of a sibling handle (locked point (b): \
         partial cleanup, not termination)"
    );

    // Cleanup: tear down master A explicitly. (Drop would also do
    // it, but explicit disconnect makes the test deterministic.)
    drop(master_a);
    let deadline = Instant::now() + Duration::from_secs(2);
    while pid_alive(pid_a) {
        if Instant::now() >= deadline {
            panic!("master A daemon {pid_a} did not die within 2s of A drop");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
