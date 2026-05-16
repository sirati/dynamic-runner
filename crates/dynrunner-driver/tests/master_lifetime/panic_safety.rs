//! Panic-in-Drop prohibition test (locked design point j). Inject a
//! fake kill-ladder so Drop sees `UnkillableMaster`; run the scope
//! under `catch_unwind`; the master must NOT panic on its way out.

use std::time::{Duration, Instant};

use dynrunner_driver::ssh_master::SshMaster;
use dynrunner_driver::ssh_target::SshTarget;

use crate::helpers::{AuthorizedKey, make_config, pid_alive, serialise, sshd_reachable};

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

/// Locked design point (j): panic-in-Drop PROHIBITION. Inject a
/// fake kill-ladder via the `cfg(test)` hook so Drop sees an
/// `UnkillableMaster` outcome, run the master-drop scope under
/// `catch_unwind`, and assert no panic propagates. Drop must
/// `tracing::error!` and return.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_does_not_panic_on_unkillable_master() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — panic-in-Drop pin unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    // Spawn for real, then install the test-only kill hook before
    // dropping. The hook returns an UnkillableMaster outcome —
    // simulating "even SIGKILL didn't take" without actually
    // sending signals.
    let mut master = SshMaster::spawn(make_config(&authorized)).expect("spawn");
    let pid = master.master_pid().expect("pid");

    use dynrunner_driver::error::{KillLadder, SshMasterError};
    let target_clone = master.target().clone();
    master.install_test_kill_hook(move |hook_pid: u32, _: &SshTarget| {
        Err(SshMasterError::UnkillableMaster {
            target: target_clone.clone(),
            last_known_pid: hook_pid,
            kill_ladder_reached: KillLadder::SigkillButPidStillExists,
        })
    });

    // Run the drop under `catch_unwind`. Drop MUST NOT panic.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        drop(master);
    }));
    assert!(
        result.is_ok(),
        "Drop panicked on UnkillableMaster — locked point (j) \
         prohibits panic-in-Drop. The fake hook simulated an \
         unkillable master; Drop must log via tracing::error! and \
         return, never panic."
    );

    // Real cleanup: the production daemon is still alive (the test
    // hook bypassed actually killing it). Send SIGKILL ourselves.
    let _ = unsafe { kill(pid as i32, 9) };
    let deadline = Instant::now() + Duration::from_secs(2);
    while pid_alive(pid) {
        if Instant::now() >= deadline {
            // Best-effort cleanup; don't fail the test on residue.
            eprintln!("[warn] failed to clean up spawned daemon pid {pid}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
