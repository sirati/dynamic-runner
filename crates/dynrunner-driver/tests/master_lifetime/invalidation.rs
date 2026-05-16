//! Invalidation contract for SshMaster: after the watcher observes
//! external death, `master_pid()` must return Some(last_known_pid),
//! `disconnect()` must return Ok(()), and Drop must be a no-op
//! (locked design points h.1, h.2, h.3).

use std::time::{Duration, Instant};

use dynrunner_driver::ssh_master::SshMaster;

use crate::helpers::{AuthorizedKey, make_config, serialise, sshd_reachable};

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

/// Locked design points (h.1), (h.2), (h.3): after watcher-observed
/// invalidation, `master_pid()` returns Some(last_known_pid),
/// `disconnect()` returns Ok(()), and Drop is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalidation_semantics() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — invalidation contract unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let mut master = SshMaster::spawn(make_config(&authorized)).expect("spawn");
    let pid = master.master_pid().expect("pid");
    assert!(!master.is_invalidated());

    // Kill the daemon externally — simulates the master dying under
    // us. The watcher polls at 1s cadence so we wait up to 3s for
    // it to set the invalidated flag.
    assert_eq!(unsafe { kill(pid as i32, 9) }, 0, "external SIGKILL");
    let deadline = Instant::now() + Duration::from_secs(3);
    while !master.is_invalidated() {
        if Instant::now() >= deadline {
            panic!("watcher did not set invalidated flag within 3s of external master death");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // (h.1): master_pid() returns last_known_pid (Some), not None.
    assert_eq!(
        master.master_pid(),
        Some(pid),
        "post-invalidation master_pid() must return last_known_pid (Some), not None — \
         the semantic is 'was alive, not is alive'"
    );

    // (h.2): disconnect() post-invalidation succeeds as no-op.
    master
        .disconnect()
        .expect("post-invalidation disconnect must succeed as no-op");

    // (h.3): Drop post-invalidation is a no-op. We can't directly
    // observe "Drop did nothing" from outside, but the absence of a
    // panic / hang / kill-of-our-own-pid is the contract. Drop
    // happens at end of scope.
    drop(master);
}
