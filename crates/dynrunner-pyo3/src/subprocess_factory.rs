use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use dynrunner_core::WorkerId;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_transport_socket::named_socket::NamedSocketManagerEnd;
use dynrunner_transport_socket::socketpair::create_socketpair;

use crate::config::connection::ConnectionMode;
use crate::config::log_paths::LogPathConfig;
use crate::config::worker_spec::{RenderedCommand, WorkerSpec, WorkerVars};
use crate::transport::EitherManagerEnd;

/// Grace period between SIGTERM and SIGKILL during teardown. Matches
/// the timeout that the previous Python-side `PodmanExecWorkerFactory`
/// used (`proc.wait(timeout=5)` after `proc.terminate()`); kept uniform
/// across all worker subprocess kinds so containerised workers and
/// direct Python workers share one teardown ladder.
const TERMINATE_GRACE: Duration = Duration::from_secs(5);
/// Poll interval while waiting for SIGTERM to take effect. Cheap
/// `waitpid(WNOHANG)` calls are bounded by `TERMINATE_GRACE` anyway.
const TERMINATE_POLL: Duration = Duration::from_millis(50);

/// Tear down a vector of tracked worker children with the
/// SIGTERM → grace → SIGKILL ladder. Idempotent: slots that already
/// contained `None`, or children that have already exited, are no-ops.
///
/// This is the single source of truth for worker-subprocess teardown
/// — `SubprocessWorkerFactory::cleanup_all` calls it, and the
/// distributed-primary path that collects per-secondary children into a
/// shared vec calls it too. Direct `Child::kill()` (SIGKILL with no
/// grace) is hostile to podman-launched workers because podman traps
/// SIGTERM to clean up the container; SIGKILL would orphan the
/// conmon-supervised container.
pub(crate) fn terminate_children(children: &mut [Option<std::process::Child>]) {
    for slot in children.iter_mut() {
        if let Some(mut child) = slot.take() {
            terminate_child(&mut child);
        }
    }
}

/// SIGTERM → up to `TERMINATE_GRACE` poll → SIGKILL → reap one child.
/// Errors are logged at debug/warn but never propagated: teardown is a
/// best-effort lattice, and the only sane fallback if SIGKILL itself
/// fails is to leak the handle (the kernel will reap it).
fn terminate_child(child: &mut std::process::Child) {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let pid = Pid::from_raw(child.id() as i32);

    // SIGTERM. ESRCH means the child has already exited; not an error.
    match kill(pid, Signal::SIGTERM) {
        Ok(()) | Err(Errno::ESRCH) => {}
        Err(e) => {
            tracing::debug!(pid = pid.as_raw(), error = %e, "SIGTERM to worker failed");
        }
    }

    // Poll `try_wait` until the child is reaped or the grace window
    // expires. `try_wait` is non-blocking; on success it consumes the
    // child's exit status and frees the kernel slot.
    let deadline = Instant::now() + TERMINATE_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(error = %e, "try_wait on worker failed");
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(TERMINATE_POLL);
    }

    // Grace expired — escalate to SIGKILL and blocking-wait. SIGKILL
    // is unignorable, so the blocking wait is bounded.
    let _ = child.kill();
    let _ = child.wait();
}

/// One of the two transport-specific values that have to flow into the worker
/// argv: a socketpair file descriptor or a named-socket path.
enum FdOrSocket<'a> {
    Fd(i32),
    Socket(&'a Path),
}

/// Subprocess worker factory: spawns Python workers via socketpair or named socket.
pub(crate) struct SubprocessWorkerFactory {
    pub(crate) python_executable: PathBuf,
    pub(crate) source_dir: PathBuf,
    pub(crate) output_dir: PathBuf,
    pub(crate) log_dir: PathBuf,
    pub(crate) log_paths: LogPathConfig,
    pub(crate) worker_module: String,
    pub(crate) worker_cmd_args: Vec<String>,
    pub(crate) skip_existing: bool,
    pub(crate) connection_mode: ConnectionMode,
    pub(crate) manual_start_worker: bool,
    /// If `Some`, Python supplies the full argv/env/cwd template and the
    /// fields above are only used to render placeholders. If `None`, the
    /// factory falls back to the legacy hardcoded argv shape.
    pub(crate) worker_spec: Option<WorkerSpec>,
    pub(crate) child_processes: Vec<Option<std::process::Child>>,
}

impl SubprocessWorkerFactory {
    /// Build the legacy hardcoded argv when no explicit `WorkerSpec` was
    /// provided. The first element is the executable.
    fn legacy_argv(&self, worker_id: WorkerId, fd_or_socket: FdOrSocket<'_>) -> Vec<String> {
        let worker_log = self.log_paths.worker_log(&self.log_dir, worker_id);
        let mut argv: Vec<String> = vec![
            self.python_executable.to_string_lossy().into_owned(),
            "-m".into(),
            self.worker_module.clone(),
        ];
        match fd_or_socket {
            FdOrSocket::Fd(fd) => {
                argv.push("--dynamic_queue".into());
                argv.push(fd.to_string());
            }
            FdOrSocket::Socket(p) => {
                argv.push("--socket-path".into());
                argv.push(p.to_string_lossy().into_owned());
            }
        }
        argv.push("--source".into());
        argv.push(self.source_dir.to_string_lossy().into_owned());
        argv.push("--output".into());
        argv.push(self.output_dir.to_string_lossy().into_owned());
        argv.push("--log-file".into());
        argv.push(worker_log.to_string_lossy().into_owned());
        if self.skip_existing {
            argv.push("--skip_existing".into());
        }
        for arg in &self.worker_cmd_args {
            argv.push(arg.clone());
        }
        argv
    }

    /// Build the rendered argv + env + cwd for a worker, picking the explicit
    /// `WorkerSpec` template when present and falling back to the legacy argv
    /// otherwise.
    fn render_command(
        &self,
        worker_id: WorkerId,
        fd_or_socket: FdOrSocket<'_>,
    ) -> RenderedCommand {
        let worker_log = self.log_paths.worker_log(&self.log_dir, worker_id);
        if let Some(spec) = &self.worker_spec {
            let (fd, sock) = match fd_or_socket {
                FdOrSocket::Fd(fd) => (Some(fd), None),
                FdOrSocket::Socket(p) => (None, Some(p)),
            };
            spec.render(&WorkerVars {
                comm_fd: fd,
                socket_path: sock,
                worker_id,
                log_file: &worker_log,
            })
        } else {
            RenderedCommand {
                argv: self.legacy_argv(worker_id, fd_or_socket),
                env: std::collections::HashMap::new(),
                cwd: None,
            }
        }
    }

    /// Build a `std::process::Command` from a rendered template. Stdio is
    /// silenced; callers add transport-specific extras (e.g. socketpair
    /// `pre_exec` hooks) afterwards.
    ///
    /// Worker as its own process-group leader: `process_group(0)` asks the
    /// kernel to create a fresh process group with `pgid == child_pid` at
    /// exec time. Every descendant the worker forks inherits that pgid
    /// (unless it explicitly creates its own). This is the contract the
    /// manager-local layer's `sigterm_process_tree` /
    /// `sigkill_process_tree` rely on: a single `kill(-pgid, ...)`
    /// reaches the worker AND every child it spawned, which is the
    /// load-bearing primitive for the panik (emergency-stop) shutdown
    /// path. Without this, a worker that forked helper subprocesses
    /// would leave them alive after a tree-kill, blocking container
    /// teardown and orphaning compute that the operator already
    /// declared unwanted.
    fn command_from_rendered(rendered: &RenderedCommand) -> std::process::Command {
        use std::os::unix::process::CommandExt;
        let mut cmd = std::process::Command::new(&rendered.argv[0]);
        cmd.args(&rendered.argv[1..]);
        for (k, v) in &rendered.env {
            cmd.env(k, v);
        }
        if let Some(cwd) = &rendered.cwd {
            cmd.current_dir(cwd);
        }
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0);
        cmd
    }

    /// Tear down every tracked worker subprocess with the SIGTERM →
    /// grace → SIGKILL ladder. Idempotent; safe to call after the
    /// manager run loop has exited regardless of why it exited.
    pub(crate) fn cleanup_all(&mut self) {
        terminate_children(&mut self.child_processes);
    }

    /// Store a freshly-spawned child in the per-worker slot.
    fn track_child(&mut self, worker_id: WorkerId, child: std::process::Child) -> u32 {
        let pid = child.id();
        let idx = worker_id as usize;
        if self.child_processes.len() <= idx {
            self.child_processes.resize_with(idx + 1, || None);
        }
        self.child_processes[idx] = Some(child);
        pid
    }

    /// Spawn using socketpair mode: create a socketpair, pass child FD.
    fn spawn_socketpair(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let (manager_end, child_fd) =
            create_socketpair().map_err(|e| format!("failed to create socketpair: {e}"))?;

        let rendered = self.render_command(worker_id, FdOrSocket::Fd(child_fd));
        let mut cmd = Self::command_from_rendered(&rendered);

        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(move || {
                // The child_fd is already open; nothing to do.
                Ok(())
            });
        }

        let child = cmd
            .spawn()
            .map_err(|e| format!("failed to exec worker {worker_id}: {e}"))?;

        // Close child fd on parent side (duped into child).
        drop(unsafe { std::os::fd::OwnedFd::from_raw_fd(child_fd) });

        let pid = self.track_child(worker_id, child);
        Ok((EitherManagerEnd::Socketpair(manager_end), Some(pid)))
    }

    /// Spawn using named socket mode: bind socket, then optionally spawn subprocess.
    fn spawn_named(
        &mut self,
        worker_id: WorkerId,
        socket_dir: &Path,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let socket_path = self.log_paths.socket_path(socket_dir, worker_id);
        let manager_end = NamedSocketManagerEnd::bind(&socket_path)
            .map_err(|e| format!("failed to bind named socket: {e}"))?;

        let rendered = self.render_command(worker_id, FdOrSocket::Socket(&socket_path));

        if self.manual_start_worker {
            tracing::info!(
                worker_id,
                "\n[Worker {worker_id}] Please run the following command in another terminal:\n  {}\n[Worker {worker_id}] Manager will detect when worker connects via socket: {}",
                rendered.argv.join(" "),
                socket_path.display()
            );

            let endpoint = EitherManagerEnd::Named {
                inner: manager_end,
                accepted: false,
            };
            // No child process — worker started manually
            return Ok((endpoint, None));
        }

        let mut cmd = Self::command_from_rendered(&rendered);
        let child = cmd
            .spawn()
            .map_err(|e| format!("failed to exec worker {worker_id}: {e}"))?;
        let pid = self.track_child(worker_id, child);

        let endpoint = EitherManagerEnd::Named {
            inner: manager_end,
            accepted: false,
        };
        Ok((endpoint, Some(pid)))
    }
}

impl WorkerFactory<EitherManagerEnd> for SubprocessWorkerFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        match &self.connection_mode {
            ConnectionMode::Socketpair => self.spawn_socketpair(worker_id),
            ConnectionMode::Named { socket_dir } => {
                let socket_dir = socket_dir.clone();
                self.spawn_named(worker_id, &socket_dir)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `terminate_children` should be a no-op on an empty slice and on
    /// slots that contain `None`. Idempotence is essential because the
    /// distributed-primary path may aggregate already-drained child
    /// vectors from multiple secondaries.
    #[test]
    fn terminate_children_handles_empty_and_none_slots() {
        let mut empty: Vec<Option<std::process::Child>> = Vec::new();
        terminate_children(&mut empty);

        let mut nones: Vec<Option<std::process::Child>> = vec![None, None, None];
        terminate_children(&mut nones);
        assert!(nones.iter().all(Option::is_none));
    }

    /// A child that exits on SIGTERM must be reaped within the grace
    /// window — `terminate_children` should never escalate to SIGKILL
    /// when SIGTERM suffices.
    ///
    /// Uses `/bin/sh -c 'trap "exit 0" TERM; sleep 30'` so the child
    /// blocks until SIGTERM, then exits cleanly.
    #[test]
    fn terminate_children_reaps_sigterm_responsive_child() {
        let mut cmd = std::process::Command::new("/bin/sh");
        cmd.arg("-c").arg("trap 'exit 0' TERM; sleep 30");
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let child = cmd.spawn().expect("spawn /bin/sh sleep");

        let mut children = vec![Some(child)];
        let start = Instant::now();
        terminate_children(&mut children);
        let elapsed = start.elapsed();

        // Reaped, so the slot is empty.
        assert!(children[0].is_none());
        // SIGTERM-responsive child must NOT block out the full grace
        // window — that would mean we ignored its early exit.
        assert!(
            elapsed < TERMINATE_GRACE,
            "SIGTERM-responsive child took {elapsed:?}, grace={TERMINATE_GRACE:?}"
        );
    }

    /// A child that ignores SIGTERM must be escalated to SIGKILL once
    /// the grace window expires. Bounds the total wait so the test
    /// fails fast if the escalation ladder breaks.
    ///
    /// Uses Python rather than `/bin/sh` because POSIX shells reset
    /// `trap ''` on `exec` (and may last-command-exec into `sleep`),
    /// making a shell-level SIGTERM-ignore harder to maintain across
    /// system shells. Python's `signal.signal(signal.SIGTERM,
    /// signal.SIG_IGN)` survives reliably and is identical to the
    /// real-world "buggy worker" scenario the SIGKILL fallback exists
    /// to handle.
    #[test]
    fn terminate_children_escalates_to_sigkill_when_sigterm_ignored() {
        let mut cmd = std::process::Command::new("python3");
        cmd.arg("-c").arg(
            "import signal, time\n\
             signal.signal(signal.SIGTERM, signal.SIG_IGN)\n\
             while True:\n    time.sleep(1)\n",
        );
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let child = cmd.spawn().expect("spawn python loop");
        // Give the Python interpreter a moment to install the handler.
        std::thread::sleep(Duration::from_millis(200));

        let mut children = vec![Some(child)];
        let start = Instant::now();
        terminate_children(&mut children);
        let elapsed = start.elapsed();

        assert!(children[0].is_none());
        // Must have waited at least the grace window before escalating;
        // upper bound guards against the SIGKILL fallback going AWOL.
        assert!(
            elapsed >= TERMINATE_GRACE,
            "escalation happened before grace expired: {elapsed:?}"
        );
        assert!(
            elapsed < TERMINATE_GRACE + Duration::from_secs(2),
            "SIGKILL fallback took too long: {elapsed:?}"
        );
    }
}
