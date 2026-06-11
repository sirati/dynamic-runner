//! Link a spawned long-lived child's lifetime to THIS process's
//! lifetime, so the child cannot outlive its parent on any exit path.
//!
//! # Concern
//!
//! ONE concern: a child that must die when its parent process dies —
//! including when the parent is taken down by an uncatchable signal
//! (SIGKILL) or by a SIGTERM the parent does not handle, where no
//! Rust destructor (`Drop`, `kill_on_drop`) ever runs. The kernel's
//! parent-death-signal (`PR_SET_PDEATHSIG`) is the only mechanism that
//! survives those paths: the kernel itself delivers a death signal to
//! the child the instant the parent thread that spawned it exits, with
//! no cooperation from the (already-dead) parent.
//!
//! This is a generic process-spawn primitive — it knows nothing of
//! ssh, tunnels, or roles. It is the death-linkage half of a layered
//! reaping contract; the orderly halves (`kill_on_drop(true)` for a
//! dropped handle, an explicit registry-shutdown teardown for the
//! graceful path) are the caller's to set, and stay where they are.
//!
//! # Why a child can outlive its parent without this
//!
//! `tokio::process::Command::kill_on_drop(true)` only fires when the
//! `Child` handle's `Drop` runs. A parent killed by signal never runs
//! `Drop` (nor any teardown), so a long-lived `ssh -N` child it spawned
//! is reparented to `init` and lingers indefinitely (#425: a
//! signalled late-joiner orphaned its `ssh -L` legs for ~75 minutes).
//!
//! # The set-then-recheck race close
//!
//! `pre_exec` runs in the forked child between `fork(2)` and
//! `execve(2)`. If the parent dies in the window AFTER `fork` but
//! BEFORE this child calls `prctl`, the death signal is already missed
//! — `PR_SET_PDEATHSIG` only arms future deaths. The canonical close
//! is to re-read the parent pid immediately after arming: if it is no
//! longer the spawner (reparented — typically to `init`, ppid `1`),
//! the parent is already gone, so the child exits at once instead of
//! becoming the very orphan this guards against.

use tokio::process::Command;

/// Arm `PR_SET_PDEATHSIG(SIGTERM)` on the child of `cmd` via a
/// `pre_exec` hook, so the kernel signals the child when this process
/// dies by ANY means — including SIGKILL/unhandled-SIGTERM of the
/// parent, where no `Drop`/`kill_on_drop`/teardown ever runs.
///
/// Call alongside `cmd.kill_on_drop(true)` (orderly drop) just before
/// `spawn()`; the two are complementary layers, not alternatives.
///
/// The death signal is `SIGTERM` (not `SIGKILL`) so the child — an
/// `ssh` process holding a forward — gets the same clean shutdown it
/// receives on the orderly teardown ladder, releasing its forward
/// bindings rather than being severed mid-syscall.
pub(crate) fn link_child_death_to_parent(cmd: &mut Command) {
    // SAFETY: the closure runs in the forked child after `fork(2)` and
    // before `execve(2)`, where only async-signal-safe operations are
    // permitted (no allocator/mutex activity that could deadlock). Every
    // call it makes is a single bare syscall — `prctl(2)`, `getppid(2)`,
    // `_exit(2)` — with no heap and no locks; it touches only the stack
    // and those syscalls. `_exit(2)` (NOT `exit(3)`) is used for the
    // race-close path precisely because the latter would run atexit
    // handlers + stdio flushes that are not async-signal-safe here.
    unsafe {
        cmd.pre_exec(|| {
            set_pdeathsig_and_recheck_parent();
            Ok(())
        });
    }
}

/// The `pre_exec` body. Arms the parent-death signal, then closes the
/// fork/arm race by re-reading the parent pid: a parent that already
/// died before the arm has handed this child to `init` (ppid 1), so we
/// `_exit` immediately rather than linger as the orphan PDEATHSIG was
/// meant to prevent.
///
/// A failed `prctl` is swallowed (the child still execs `ssh`): the
/// orderly `kill_on_drop` + registry-teardown layers remain, so a
/// missing PDEATHSIG degrades to the pre-#425 behaviour for that one
/// child rather than aborting the whole tunnel establishment. `errno`
/// inspection here would not be async-signal-safe anyway.
fn set_pdeathsig_and_recheck_parent() {
    use nix::sys::prctl::set_pdeathsig;
    use nix::sys::signal::Signal;
    use nix::unistd::getppid;

    let _ = set_pdeathsig(Signal::SIGTERM);

    // Race close: if the spawner already exited between `fork` and the
    // arm above, this child is now reparented (ppid == 1, `init`). The
    // PDEATHSIG never fired for that death, so terminate ourselves with
    // the async-signal-safe `_exit(2)` (never `Drop`/atexit-running).
    if getppid().as_raw() == 1 {
        // SAFETY: `_exit(2)` is async-signal-safe and never returns.
        unsafe { libc::_exit(0) };
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end PDEATHSIG reaping, exercised across a real
    //! process-death edge. `set_pdeathsig_and_recheck_parent` is NOT
    //! unit-tested in-process: arming PDEATHSIG mutates the calling
    //! process's process-global disposition, which would leak into the
    //! rest of the test binary. Instead the test re-execs THIS test
    //! binary in a helper mode (env-gated) that spawns a long-lived
    //! `sleep` grandchild through the real `link_child_death_to_parent`,
    //! writes the grandchild pid to a handshake file, then blocks
    //! holding it. The driver SIGKILLs the helper (the uncatchable path —
    //! no `Drop`, no `kill_on_drop`, no teardown) and asserts the kernel
    //! reaped the grandchild. Without layer (a) the grandchild survives →
    //! RED.
    //!
    //! The pid handshake is a FILE (not the helper's stdout): libtest's
    //! `--nocapture` stdout is buffered by the harness and only flushed
    //! when a test body returns — but the helper deliberately never
    //! returns, so a stdout read would block forever. A `std::fs::write`
    //! + poll is fully decoupled from the harness's I/O.

    use std::process::{Command as StdCommand, Stdio};
    use std::time::{Duration, Instant};

    use tokio::process::Command;

    /// Env gate selecting helper mode; its value is the pidfile path.
    const HELPER_PIDFILE_ENV: &str = "DYNRUNNER_PDEATHSIG_HELPER_PIDFILE";

    /// Helper mode: spawn a long-lived `sleep` through the real
    /// death-linkage primitive, write its pid to the handshake file, then
    /// block forever holding the child handle. The driver SIGKILLs us;
    /// PDEATHSIG must then take the grandchild down.
    ///
    /// Runs as a normal `#[test]` so it is a callable entry point in the
    /// re-execed binary, but no-ops unless the env gate is set — so the
    /// ordinary `cargo test` run does nothing here.
    #[test]
    fn pdeathsig_helper_mode_entrypoint() {
        let Some(pidfile) = std::env::var_os(HELPER_PIDFILE_ENV) else {
            return; // not the helper invocation; ordinary test run.
        };
        // A current-thread runtime is enough to spawn the child; the
        // helper does no other async work.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("helper runtime");
        rt.block_on(async {
            let mut cmd = Command::new("sleep");
            cmd.arg("600");
            cmd.kill_on_drop(true);
            super::link_child_death_to_parent(&mut cmd);
            let child = cmd.spawn().expect("helper: spawn sleep grandchild");
            let pid = child.id().expect("grandchild has a pid");
            // Publish the pid to the handshake file (harness-independent).
            std::fs::write(&pidfile, pid.to_string()).expect("helper: write pidfile");
            // Block forever holding the child handle. The driver SIGKILLs
            // us; we must NEVER run `Drop`/`kill_on_drop` — that is the
            // whole point. Park the thread.
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        });
    }

    /// `kill(pid, 0)` liveness probe: `Ok` ⇒ the pid exists (and we may
    /// signal it); `ESRCH` ⇒ it is gone.
    fn pid_alive(pid: i32) -> bool {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        !matches!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH))
    }

    /// Driver: launch the helper, read the grandchild pid via the
    /// handshake file, SIGKILL the helper (the no-`Drop` path), and
    /// assert PDEATHSIG reaped the grandchild within a grace window. This
    /// is the #425 regression pin.
    #[test]
    fn pdeathsig_reaps_grandchild_when_parent_is_sigkilled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let pidfile = tmp.path().join("grandchild.pid");

        let exe = std::env::current_exe().expect("test binary path");
        let mut helper = StdCommand::new(&exe)
            // Target the helper entrypoint by name, single-threaded, so
            // the re-execed binary runs exactly that one test body.
            .args([
                "--exact",
                "child_reaping::tests::pdeathsig_helper_mode_entrypoint",
                "--test-threads",
                "1",
            ])
            .env(HELPER_PIDFILE_ENV, &pidfile)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn helper");

        let grandchild_pid = read_grandchild_pid(&pidfile);

        // Sanity: the grandchild is alive while the helper holds it.
        assert!(
            pid_alive(grandchild_pid),
            "grandchild {grandchild_pid} should be alive before the helper dies"
        );

        // The uncatchable death: SIGKILL the helper. No `Drop`, no
        // `kill_on_drop`, no teardown runs — ONLY PDEATHSIG can save the
        // grandchild now.
        helper.kill().expect("SIGKILL helper");
        helper.wait().expect("reap helper");

        // PDEATHSIG fires on the helper's death; the grandchild must be
        // gone within the grace window. The grandchild is reparented to
        // init, which reaps it, so we probe for liveness disappearing.
        let deadline = Instant::now() + Duration::from_secs(10);
        while pid_alive(grandchild_pid) {
            if Instant::now() >= deadline {
                // Cleanup the leaked grandchild so the test host isn't
                // left with a stray `sleep`.
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(grandchild_pid),
                    nix::sys::signal::Signal::SIGKILL,
                );
                panic!(
                    "grandchild {grandchild_pid} survived the parent's SIGKILL — \
                     PDEATHSIG did not reap it (#425 regression)"
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Poll the handshake file until the helper has written a parseable
    /// pid, or the deadline expires.
    fn read_grandchild_pid(pidfile: &std::path::Path) -> i32 {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                Instant::now() < deadline,
                "helper never wrote a grandchild pid to {}",
                pidfile.display()
            );
            if let Ok(s) = std::fs::read_to_string(pidfile)
                && let Ok(pid) = s.trim().parse::<i32>()
            {
                return pid;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
