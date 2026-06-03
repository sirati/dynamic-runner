//! Tests covering SshMaster's drop semantics and the daemon-PID
//! contract: T3/T4/T5 from the locked-design pinning + the
//! `master_pid_is_daemon_not_launcher` cross-check.

use std::time::{Duration, Instant};

use dynrunner_driver::ssh_master::SshMaster;

use crate::helpers::{
    AuthorizedKey, make_config, pid_alive, re_probe_master_pid, serialise, sshd_reachable,
};

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t3_drop_cleans_master() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — bug-(g) fix unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let master = SshMaster::spawn(make_config(&authorized))
        .expect("spawn against local sshd via temporary key");

    let pid = master
        .master_pid()
        .expect("framework-spawned master must report a PID");
    assert!(
        pid_alive(pid),
        "master must be alive immediately after spawn"
    );

    drop(master);

    let deadline = Instant::now() + Duration::from_secs(1);
    while pid_alive(pid) {
        if Instant::now() >= deadline {
            panic!("master pid {pid} still alive 1s after SshMaster drop — bug (g) regression");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[tracing_test::traced_test]
async fn t4_master_died_observer_emits_log() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — observer log unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let master = SshMaster::spawn(make_config(&authorized)).expect("spawn");
    let pid = master.master_pid().expect("master pid");
    let rc = unsafe { kill(pid as i32, 9) };
    assert_eq!(rc, 0, "kill(SIGKILL) on master pid {pid} failed");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let raw = {
            let buf = tracing_test::internal::global_buf().lock().unwrap();
            String::from_utf8_lossy(&buf).into_owned()
        };
        if raw.contains("SSH master exited unexpectedly") {
            // Cleanup: master is already dead, but the watcher may
            // still be in its final tick. Drop synchronously runs
            // the no-op-on-invalidation path now.
            drop(master);
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "tracing::error! \"SSH master exited unexpectedly\" never fired \
                 within 5s of master SIGKILL — observer regression\n\
                 captured logs:\n{raw}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t5_drop_kills_daemon_master() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — bug-(g)-redux unverified");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[skip] could not provision authorized_keys: {e}");
            return;
        }
    };

    let master = SshMaster::spawn(make_config(&authorized)).expect("spawn against local sshd");
    let daemon_pid = master
        .master_pid()
        .expect("master_pid() must report the daemon PID after spawn");
    assert!(
        pid_alive(daemon_pid),
        "daemon pid {daemon_pid} must be alive immediately after spawn"
    );

    drop(master);

    let deadline = Instant::now() + Duration::from_secs(1);
    while pid_alive(daemon_pid) {
        if Instant::now() >= deadline {
            panic!(
                "daemon pid {daemon_pid} still alive 1s after master drop — \
                 bug-(g)-redux regression: Drop is hitting the launcher \
                 zombie instead of the ControlPersist daemon"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Pin that `master_pid()` returns the daemon PID — the long-lived
/// `ControlPersist` process — by independently re-deriving it from a
/// fresh `ssh -O check` invocation. The two PIDs must agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn master_pid_is_daemon_not_launcher() {
    let _serial = serialise().await;
    if !sshd_reachable() {
        eprintln!("[skip] sshd not reachable on localhost:22 — daemon-PID contract unverified");
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
    let reported_pid = master
        .master_pid()
        .expect("master_pid() must report the daemon PID after spawn");

    let cp = master.control_path().to_path_buf();
    let target_str = master.target().as_str().to_owned();

    let independent_pid = re_probe_master_pid(&authorized, &cp, &target_str);

    assert_eq!(
        reported_pid, independent_pid,
        "master_pid() ({reported_pid}) must match the daemon PID reported \
         by an independent `ssh -O check` ({independent_pid})."
    );

    master.disconnect().expect("disconnect");
}
