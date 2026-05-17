use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use dynrunner_core::{TypeId, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_transport_socket::named_socket::NamedSocketManagerEnd;
use dynrunner_transport_socket::socketpair::create_socketpair;

use crate::config::connection::ConnectionMode;
use crate::config::log_paths::LogPathConfig;
use crate::config::worker_spec::{RenderedCommand, WorkerSpec, WorkerVars};
use crate::task_def::{TypeRegistry, TypeRuntime};
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

/// Tear down a vector of tracked worker children AND every
/// descendant they spawned, using the SIGTERM → grace → SIGKILL
/// ladder against each child's process GROUP.
///
/// Distinct from [`terminate_children`]: that path signals each
/// worker's PID directly, which only reaches the worker process
/// itself. The negative-PGID idiom used here (`kill(-pgid, ...)`)
/// reaches every descendant sharing the pgid — the contract the
/// factory's `process_group(0)` spawn flag set up. This is the
/// primitive the panik (emergency-stop) path uses on the
/// LocalManager flow (where there's no `WorkerPool` to fan-out
/// through): a single sweep that takes down workers plus their
/// helper subprocesses (subprocess pools, container exec children,
/// etc.) before the manager process exits 137.
///
/// Wall-clock teardown is bounded by `grace` (one sleep across the
/// whole vec, not `grace * num_children`). Idempotent.
pub(crate) fn terminate_children_with_process_group(
    children: &mut [Option<std::process::Child>],
    grace: Duration,
) {
    use nix::errno::Errno;
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;

    if children.iter().all(Option::is_none) {
        return;
    }
    // Pass 1: SIGTERM each pgid in one fan-out.
    for slot in children.iter() {
        if let Some(child) = slot.as_ref() {
            let pgid = Pid::from_raw(-(child.id() as i32));
            match kill(pgid, Signal::SIGTERM) {
                Ok(()) | Err(Errno::ESRCH) => {}
                Err(e) => tracing::debug!(
                    pgid = pgid.as_raw(),
                    error = %e,
                    "SIGTERM to worker pgid failed"
                ),
            }
        }
    }
    // Single sleep across the whole batch — wall-clock teardown is
    // bounded by `grace`, not `grace * num_children`.
    std::thread::sleep(grace);
    // Pass 2: SIGKILL any pgid still alive + reap the leader Child.
    for slot in children.iter_mut() {
        if let Some(mut child) = slot.take() {
            let pgid = Pid::from_raw(-(child.id() as i32));
            // Probe with signal 0 — if the group is gone we skip
            // the SIGKILL.
            if !matches!(kill(pgid, None), Err(Errno::ESRCH)) {
                let _ = kill(pgid, Signal::SIGKILL);
            }
            // Reap the leader. The descendants the SIGKILL just
            // hit are inherited by init (PID 1) and reaped there;
            // the framework only owns the leader's `Child`.
            let _ = child.wait();
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
///
/// Per-type dispatch: each spawn resolves a [`TypeRuntime`] off of
/// [`types`] (the full `TypeRegistry` from the loaded `TaskDefinition`)
/// to pick the `worker_module` and `cmd_args` that match the task's
/// `TypeId`. `spawn_worker` (initial pool init) uses `types.first()`
/// to preserve the pre-fix single-type behaviour; `spawn_worker_for_type`
/// (the per-type respawn entry the pool fires on TypeId mismatch)
/// looks the requested `TypeId` up in the registry. Empty registries
/// surface a clear error.
pub(crate) struct SubprocessWorkerFactory {
    pub(crate) python_executable: PathBuf,
    pub(crate) source_dir: PathBuf,
    pub(crate) output_dir: PathBuf,
    pub(crate) log_dir: PathBuf,
    pub(crate) log_paths: LogPathConfig,
    /// All `TaskTypeSpec` runtimes extracted from
    /// `TaskDefinition.get_phases()`. The single source of truth the
    /// factory consults for per-spawn argv. Cloned in once at
    /// construction so the factory does not borrow `TaskDefinition`
    /// state for the run lifetime.
    pub(crate) types: TypeRegistry,
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
    /// Resolve the `TypeRuntime` for `type_id`, or surface a clear
    /// error if the registry has no entry for it. The lookup error
    /// indicates a `TaskDefinition.get_phases()` / `TaskInfo.type_id`
    /// mismatch — every `type_id` the manager dispatches must have
    /// come from the same `get_phases()` call that populated the
    /// registry.
    fn type_runtime_for(&self, type_id: &TypeId) -> Result<&TypeRuntime, String> {
        self.types.get(type_id).ok_or_else(|| {
            format!(
                "no TypeRuntime registered for TypeId '{type_id}'; \
                 TaskDefinition.get_phases() did not declare it"
            )
        })
    }

    /// Resolve a fallback `TypeRuntime` for initial-spawn paths that
    /// have not yet observed a task's `type_id`. Returns the
    /// `types.first()` entry — matching the pre-fix single-type
    /// behaviour where every worker spawned with the first declared
    /// type's argv. Errors when the registry is empty (a
    /// programmer-error: every `TaskDefinition` declaring at least one
    /// type was already validated at `LoadedTaskDefinition::from_python`
    /// time; an empty registry can only happen in the observer
    /// placeholder path where this fallback is unreachable anyway).
    fn first_type_runtime(&self) -> Result<&TypeRuntime, String> {
        self.types
            .first()
            .ok_or_else(|| "TypeRegistry is empty; cannot spawn worker".to_string())
    }

    /// Build the legacy hardcoded argv when no explicit `WorkerSpec` was
    /// provided. `runtime` decides the `worker_module` + per-type
    /// `cmd_args`; everything else is factory-global. The first
    /// element is the executable.
    fn legacy_argv(
        &self,
        worker_id: WorkerId,
        runtime: &TypeRuntime,
        fd_or_socket: FdOrSocket<'_>,
    ) -> Vec<String> {
        let worker_log = self.log_paths.worker_log(&self.log_dir, worker_id);
        let mut argv: Vec<String> = vec![
            self.python_executable.to_string_lossy().into_owned(),
            "-m".into(),
            runtime.worker_module.clone(),
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
        for arg in &runtime.cmd_args {
            argv.push(arg.clone());
        }
        argv
    }

    /// Build the rendered argv + env + cwd for a worker, picking the explicit
    /// `WorkerSpec` template when present and falling back to the legacy argv
    /// otherwise. `runtime` carries the per-type `worker_module` and
    /// `cmd_args` that drive the resulting argv.
    fn render_command(
        &self,
        worker_id: WorkerId,
        runtime: &TypeRuntime,
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
                argv: self.legacy_argv(worker_id, runtime, fd_or_socket),
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

    /// Tear down every tracked worker subprocess AND its child
    /// process tree with the negative-pgid SIGTERM → grace → SIGKILL
    /// ladder. Used by the panik (operator-emergency-stop) path on
    /// the LocalManager flow where there's no `WorkerPool` to fan-out
    /// through. Idempotent; safe to call before exit(137) so the
    /// manager process and every worker pgid go down in one bounded
    /// sweep.
    pub(crate) fn cleanup_all_process_trees(
        &mut self,
        grace: Duration,
    ) {
        terminate_children_with_process_group(&mut self.child_processes, grace);
    }

    /// Store a freshly-spawned child in the per-worker slot.
    ///
    /// If the slot already holds a `Child` (per-type respawn — the
    /// pool's `ensure_worker_for_type` SIGKILLed the prior worker
    /// before this call), the prior occupant is taken out and given
    /// the SIGTERM → grace → SIGKILL terminate ladder before the new
    /// `Child` lands. This reaps the prior PID synchronously so the
    /// kernel does not leak a zombie until the manager exits + init
    /// reaps it. `std::process::Child::drop` is a no-op on Unix —
    /// no implicit `wait()` — so without this the previous restart
    /// path quietly leaked zombies on every slot replacement.
    fn track_child(&mut self, worker_id: WorkerId, child: std::process::Child) -> u32 {
        let pid = child.id();
        let idx = worker_id as usize;
        if self.child_processes.len() <= idx {
            self.child_processes.resize_with(idx + 1, || None);
        }
        // Single-slot version of `terminate_children` — the prior
        // occupant (if any) is reaped before its handle is dropped.
        let mut prior = [self.child_processes[idx].take()];
        terminate_children(&mut prior);
        self.child_processes[idx] = Some(child);
        pid
    }

    /// Spawn using socketpair mode: create a socketpair, pass child FD.
    fn spawn_socketpair(
        &mut self,
        worker_id: WorkerId,
        runtime: &TypeRuntime,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let (manager_end, child_fd) =
            create_socketpair().map_err(|e| format!("failed to create socketpair: {e}"))?;

        let rendered = self.render_command(worker_id, runtime, FdOrSocket::Fd(child_fd));
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
        runtime: &TypeRuntime,
        socket_dir: &Path,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let socket_path = self.log_paths.socket_path(socket_dir, worker_id);
        let manager_end = NamedSocketManagerEnd::bind(&socket_path)
            .map_err(|e| format!("failed to bind named socket: {e}"))?;

        let rendered = self.render_command(worker_id, runtime, FdOrSocket::Socket(&socket_path));

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

impl SubprocessWorkerFactory {
    /// Internal entry: spawn with an explicit `TypeRuntime` reference.
    /// Both `spawn_worker` (first-type fallback for initial pool init)
    /// and `spawn_worker_for_type` (per-type respawn for type-shift)
    /// funnel through here so the connection-mode dispatch lives in
    /// exactly one place.
    fn spawn_with_runtime(
        &mut self,
        worker_id: WorkerId,
        runtime: &TypeRuntime,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        match &self.connection_mode {
            ConnectionMode::Socketpair => self.spawn_socketpair(worker_id, runtime),
            ConnectionMode::Named { socket_dir } => {
                let socket_dir = socket_dir.clone();
                self.spawn_named(worker_id, runtime, &socket_dir)
            }
        }
    }
}

impl WorkerFactory<EitherManagerEnd> for SubprocessWorkerFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        // Clone the first-type runtime so the immutable borrow against
        // `self.types` is released before `spawn_with_runtime` takes
        // `&mut self`. The clone is cheap — `TypeRuntime` holds Arc-
        // backed strings + a `Vec<String>` of cmd_args; only the
        // cmd_args vec actually copies, and at the once-per-restart
        // cadence this is dominated by the cost of forking Python.
        let runtime = self.first_type_runtime()?.clone();
        self.spawn_with_runtime(worker_id, &runtime)
    }

    fn spawn_worker_for_type(
        &mut self,
        worker_id: WorkerId,
        type_id: &TypeId,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let runtime = self.type_runtime_for(type_id)?.clone();
        self.spawn_with_runtime(worker_id, &runtime)
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
