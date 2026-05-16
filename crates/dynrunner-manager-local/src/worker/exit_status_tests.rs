//! Unit tests for `WorkerExitStatus` display + `try_reap_subprocess`
//! reap semantics. Loaded by `mod.rs` only under `#[cfg(test)]`.

use super::exit_status::{try_reap_subprocess, WorkerExitStatus};


// Display tests: pin the exact log-line text downstream operators
// grep for. Changes to these formats are breaking changes to the
// operator workflow, not just internal refactors.

#[test]
fn display_exited_zero() {
    let s = WorkerExitStatus {
        code: Some(0),
        signal: None,
        signal_name: None,
        core_dumped: false,
    };
    assert_eq!(s.to_string(), "exited with code 0");
    assert!(!s.was_killed());
}

#[test]
fn display_exited_nonzero() {
    let s = WorkerExitStatus {
        code: Some(137),
        signal: None,
        signal_name: None,
        core_dumped: false,
    };
    // Note: a worker exited with 137 typically means "killed by
    // SIGKILL but reported via a shell wrapper that converted the
    // signal to exit code 128+sig". The framework only sees the
    // shell-reported exit code in that case, not the signal. This
    // is a known-and-accepted blind spot: if a worker is launched
    // under a shell, the shell layer hides signal info.
    assert_eq!(s.to_string(), "exited with code 137");
}

#[test]
fn display_signaled_named() {
    let s = WorkerExitStatus {
        code: None,
        signal: Some(9),
        signal_name: Some("KILL"),
        core_dumped: false,
    };
    assert_eq!(s.to_string(), "killed by SIGKILL (9)");
    assert!(s.was_killed());
}

#[test]
fn display_signaled_unnamed_falls_back_to_question_mark() {
    let s = WorkerExitStatus {
        code: None,
        signal: Some(77),
        signal_name: None,
        core_dumped: false,
    };
    // Numeric signal still surfaces — the operator can look it up.
    // "SIG?" makes the fallback explicit rather than silent.
    assert_eq!(s.to_string(), "killed by SIG? (77)");
}

#[test]
fn display_signaled_with_core_dumped() {
    let s = WorkerExitStatus {
        code: None,
        signal: Some(11),
        signal_name: Some("SEGV"),
        core_dumped: true,
    };
    assert_eq!(s.to_string(), "killed by SIGSEGV (11), core dumped");
}

#[test]
fn try_reap_none_pid_returns_none() {
    // The "no PID tracked" branch — e.g. in-process channel
    // worker, factory returned None — must be a clean None,
    // not a panic.
    assert!(try_reap_subprocess(None).is_none());
}

// Live-subprocess reap tests. Spawn a real /bin/true / /bin/sleep,
// observe the kernel's exit-status reporting via try_reap_subprocess.
// These tests exercise the actual `waitpid` syscall path on unix
// (the path operators rely on in production).

#[cfg(unix)]
#[test]
fn try_reap_picks_up_clean_exit() {
    use std::process::Command;
    // `/bin/true` exits with code 0 immediately. Spawn, drop the
    // Child handle (std::process::Child::drop does not reap on
    // unix — the zombie persists), then reap via our path.
    let child = Command::new("true")
        .spawn()
        .expect("spawn `true`");
    let pid = child.id();
    drop(child);
    // Brief wait for the kernel to mark the child as exited.
    // The reap retry loop also rides out this race, but giving
    // it a head start makes the test deterministic on slow CI.
    std::thread::sleep(std::time::Duration::from_millis(25));
    let status = try_reap_subprocess(Some(pid)).expect("reap should succeed");
    assert_eq!(status.code, Some(0));
    assert_eq!(status.signal, None);
    assert!(!status.was_killed());
    assert!(!status.core_dumped);
}

#[cfg(unix)]
#[test]
fn try_reap_picks_up_sigkill() {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    use std::process::Command;
    // Spawn `/bin/sleep 30` and SIGKILL it. The reap must
    // return code=None, signal=Some(9), signal_name=Some("KILL").
    let child = Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("spawn `sleep 30`");
    let pid = child.id();
    kill(Pid::from_raw(pid as i32), Signal::SIGKILL).expect("send SIGKILL");
    drop(child);
    std::thread::sleep(std::time::Duration::from_millis(25));
    let status = try_reap_subprocess(Some(pid)).expect("reap should succeed");
    assert_eq!(status.code, None);
    assert_eq!(status.signal, Some(9));
    assert_eq!(status.signal_name, Some("KILL"));
    assert!(status.was_killed());
    // SIGKILL does not core-dump by default — assert the
    // formatter does not get this wrong.
    assert!(!status.core_dumped);
    assert_eq!(status.to_string(), "killed by SIGKILL (9)");
}

#[cfg(unix)]
#[test]
fn try_reap_picks_up_sigterm_with_name() {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    use std::process::Command;
    // Pin SIGTERM specifically because the watchdog's graceful
    // path sends SIGTERM, and operators should be able to
    // discriminate "killed by SIGTERM from watchdog" from
    // "killed by SIGKILL from cgroup-OOM".
    let child = Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("spawn `sleep 30`");
    let pid = child.id();
    kill(Pid::from_raw(pid as i32), Signal::SIGTERM).expect("send SIGTERM");
    drop(child);
    std::thread::sleep(std::time::Duration::from_millis(25));
    let status = try_reap_subprocess(Some(pid)).expect("reap should succeed");
    assert_eq!(status.signal, Some(15));
    assert_eq!(status.signal_name, Some("TERM"));
}
