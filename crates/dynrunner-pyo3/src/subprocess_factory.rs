use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use dynrunner_core::{TypeId, WorkerId};
use dynrunner_manager_local::{NestedCgroupHandle, WorkerFactory};
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
    /// Path to `<workers-cgroup>/cgroup.procs` injected by
    /// [`WorkerFactory::set_workers_cgroup`] when the worker pool's
    /// nested-cgroup setup succeeded. `None` covers both "pool didn't
    /// nest" (operator passed no `--mem-manager-reserved`) and "host
    /// doesn't support cgroup-v2 delegation" (graceful fallback from
    /// the `dynrunner_manager_local::cgroup` orchestrator). When
    /// `Some`, [`Self::command_from_rendered`] adds a `pre_exec`
    /// closure that writes the child's pid into this file
    /// post-`fork(2)` / pre-`execve(2)` so the worker subprocess
    /// lands in the nested workers cgroup BEFORE its binary runs.
    pub(crate) workers_cgroup_procs: Option<std::path::PathBuf>,
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
    ///
    /// Worker attached to nested cgroup on `fork(2)` /
    /// pre-`execve(2)`: when [`Self::workers_cgroup_procs`] is set,
    /// a `pre_exec` closure writes the child's pid to the workers/
    /// subgroup's `cgroup.procs`. The kernel migrates the child into
    /// that cgroup at the moment of the write, so the worker binary
    /// runs with the tightened `memory.max` from its first
    /// instruction. The `pre_exec` closure is `Send + 'static`, does
    /// not capture any owned heap state (only a `PathBuf` clone of
    /// the procs path), and only performs a single `std::fs::write`
    /// syscall — fork-safe under the documented `pre_exec` rules
    /// (no allocator activity that could deadlock against a parent
    /// holding the heap mutex at fork time). When the path is `None`
    /// no `pre_exec` is set and behaviour is unchanged.
    fn command_from_rendered(&self, rendered: &RenderedCommand) -> std::process::Command {
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

        if let Some(procs) = self.workers_cgroup_procs.clone() {
            // `pre_exec` runs in the forked child after `fork(2)` but
            // before `execve(2)`. `std::process::id()` returns the
            // CHILD'S pid in that context. The single `std::fs::write`
            // call opens, writes, and closes `cgroup.procs` — atomic
            // from the kernel's perspective and the only side-effect
            // is migrating the child into the workers cgroup.
            //
            // SAFETY: `pre_exec` is unsafe because the closure runs
            // in the forked child where most async-signal-unsafe
            // library calls (allocator-touching, mutex-acquiring)
            // would deadlock. The closure here writes the pid through
            // a stack-allocated `itoa::Buffer`: no heap activity, no
            // mutex acquisition. `std::fs::write` invokes
            // `open` → `write` → `close` syscalls only; all
            // async-signal-safe. The captured `PathBuf` was cloned
            // by the parent BEFORE the fork — the child only reads
            // it, never reallocates.
            // Stack-formatted pid to avoid any allocator activity in
            // the forked child between `fork(2)` and `execve(2)`. A
            // u32 in decimal is at most 10 digits; the buffer is
            // sized to 16 for headroom. Generating digits in reverse
            // into a temporary then copying forward gives the
            // canonical decimal representation. `std::fs::write`
            // itself opens-writes-closes via direct syscalls (no
            // heap activity for byte slices), satisfying the
            // pre-exec async-signal-safety contract.
            unsafe {
                cmd.pre_exec(move || {
                    let pid = std::process::id();
                    let mut tmp = [0u8; 16];
                    let mut tmp_len = 0usize;
                    let mut n = pid;
                    loop {
                        tmp[tmp_len] = b'0' + (n % 10) as u8;
                        tmp_len += 1;
                        n /= 10;
                        if n == 0 {
                            break;
                        }
                    }
                    let mut buf = [0u8; 16];
                    let mut len = 0usize;
                    while tmp_len > 0 {
                        tmp_len -= 1;
                        buf[len] = tmp[tmp_len];
                        len += 1;
                    }
                    std::fs::write(&procs, &buf[..len])
                });
            }
        }

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
        let mut cmd = self.command_from_rendered(&rendered);

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

        let mut cmd = self.command_from_rendered(&rendered);
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
    /// Stash the workers/ cgroup path so every subsequent spawn
    /// installs a `pre_exec` closure attaching the child to that
    /// cgroup. Called once per pool lifetime by
    /// [`dynrunner_manager_local::pool::WorkerPool::initialize`]. A
    /// `None` argument (graceful fallback or operator opt-out) is
    /// stored as `None`, leaving spawns at the legacy flat-cgroup
    /// behaviour.
    fn set_workers_cgroup(&mut self, handle: Option<NestedCgroupHandle>) {
        self.workers_cgroup_procs =
            handle.map(|h| h.workers_path().join("cgroup.procs"));
    }

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

    /// Build a two-type `TypeRegistry` fixture for the per-type
    /// dispatch tests below. `worker_module` is the only field these
    /// tests inspect; the rest match the defaults
    /// `LoadedTaskDefinition::from_python` would emit.
    fn make_two_type_registry() -> TypeRegistry {
        use std::collections::HashMap;
        let mut types = Vec::new();
        let mut index_by_id: HashMap<TypeId, usize> = HashMap::new();
        for (i, (id, module)) in [
            ("tokenize", "asm_tokenizer.worker_tokenize"),
            ("unify_vocab", "asm_tokenizer.worker_unify_vocab"),
        ]
        .iter()
        .enumerate()
        {
            let type_id = TypeId::from(*id);
            index_by_id.insert(type_id.clone(), i);
            types.push(TypeRuntime {
                type_id,
                worker_module: (*module).to_string(),
                cmd_args: Vec::new(),
                timeout: None,
                reserved_memory_per_worker: 0,
            });
        }
        TypeRegistry { types, index_by_id }
    }

    /// Build a `SubprocessWorkerFactory` backed by the two-type
    /// registry, with a `manual_start_worker=true` named-socket
    /// connection mode. The manual-start flag short-circuits the
    /// child-spawn step so the test exercises the registry lookup +
    /// argv-render path WITHOUT actually executing Python — important
    /// because the dynrunner-pyo3 lib test target cannot link against
    /// CPython (see `task_def.rs`'s phase-5a-followup note). Tests
    /// that want to assert "which TypeRuntime did the factory pick
    /// for this spawn?" wrap the call through `render_command` or
    /// the public trait method and inspect the resulting argv.
    fn make_factory_with_two_types() -> SubprocessWorkerFactory {
        SubprocessWorkerFactory {
            python_executable: PathBuf::from("/usr/bin/python3"),
            source_dir: PathBuf::from("/tmp/src"),
            output_dir: PathBuf::from("/tmp/out"),
            log_dir: PathBuf::from("/tmp/log"),
            log_paths: Default::default(),
            types: make_two_type_registry(),
            skip_existing: false,
            connection_mode: ConnectionMode::Named {
                socket_dir: PathBuf::from("/tmp/sockets"),
            },
            manual_start_worker: true,
            worker_spec: None,
            child_processes: Vec::new(),
            workers_cgroup_procs: None,
        }
    }

    /// Regression pin: `type_runtime_for` returns the registered
    /// `TypeRuntime` for declared `TypeId`s and a structured error
    /// for unknown ones. This is the lookup `spawn_worker_for_type`
    /// funnels through; without it, an unknown `TypeId` would silently
    /// fall back to `first()` and load the wrong module — the exact
    /// bug this commit set out to fix.
    #[test]
    fn type_runtime_for_resolves_declared_types() {
        let factory = make_factory_with_two_types();
        let tokenize = TypeId::from("tokenize");
        let unify = TypeId::from("unify_vocab");
        assert_eq!(
            factory.type_runtime_for(&tokenize).unwrap().worker_module,
            "asm_tokenizer.worker_tokenize"
        );
        assert_eq!(
            factory.type_runtime_for(&unify).unwrap().worker_module,
            "asm_tokenizer.worker_unify_vocab"
        );
    }

    #[test]
    fn type_runtime_for_unknown_type_id_errors_with_clear_message() {
        let factory = make_factory_with_two_types();
        let unknown = TypeId::from("memmap");
        let err = factory.type_runtime_for(&unknown).unwrap_err();
        assert!(
            err.contains("no TypeRuntime registered") && err.contains("memmap"),
            "error message should name the missing TypeId; got: {err}"
        );
    }

    /// Regression pin: `first_type_runtime` returns the first-declared
    /// type (preserving the pre-fix single-type `types.first()`
    /// fallback) and surfaces an empty-registry error rather than
    /// panicking. The empty-registry case is hit by the observer
    /// placeholder factory and any other unreachable-spawn site.
    #[test]
    fn first_type_runtime_uses_first_declared_type() {
        let factory = make_factory_with_two_types();
        assert_eq!(
            factory.first_type_runtime().unwrap().worker_module,
            "asm_tokenizer.worker_tokenize"
        );
    }

    #[test]
    fn first_type_runtime_on_empty_registry_errors() {
        let factory = SubprocessWorkerFactory {
            python_executable: PathBuf::from("/usr/bin/python3"),
            source_dir: PathBuf::new(),
            output_dir: PathBuf::new(),
            log_dir: PathBuf::new(),
            log_paths: Default::default(),
            types: TypeRegistry::default(),
            skip_existing: false,
            connection_mode: ConnectionMode::Named {
                socket_dir: PathBuf::new(),
            },
            manual_start_worker: true,
            worker_spec: None,
            child_processes: Vec::new(),
            workers_cgroup_procs: None,
        };
        let err = factory.first_type_runtime().unwrap_err();
        assert!(
            err.contains("empty"),
            "empty-registry error should say so; got: {err}"
        );
    }

    /// End-to-end pin on the per-type argv-render path: render twice,
    /// once with each type's `TypeRuntime`, and confirm the resulting
    /// argv carries the matching `-m <worker_module>` segment. This is
    /// the wire-level proof that a type-shift respawn produces
    /// observably different command lines — i.e. the worker
    /// subprocess actually loads the correct Python module for the
    /// task's `type_id`.
    #[test]
    fn render_command_emits_per_type_worker_module() {
        let factory = make_factory_with_two_types();
        let tokenize = factory
            .type_runtime_for(&TypeId::from("tokenize"))
            .unwrap()
            .clone();
        let unify = factory
            .type_runtime_for(&TypeId::from("unify_vocab"))
            .unwrap()
            .clone();

        let tmp = std::path::PathBuf::from("/tmp/sock");
        let rendered_a = factory.render_command(0, &tokenize, FdOrSocket::Socket(&tmp));
        let rendered_b = factory.render_command(0, &unify, FdOrSocket::Socket(&tmp));

        // The `-m <module>` pair sits right after the executable.
        assert_eq!(rendered_a.argv[1], "-m");
        assert_eq!(rendered_a.argv[2], "asm_tokenizer.worker_tokenize");
        assert_eq!(rendered_b.argv[1], "-m");
        assert_eq!(rendered_b.argv[2], "asm_tokenizer.worker_unify_vocab");
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

    /// `WorkerFactory::set_workers_cgroup` stashes the workers/
    /// cgroup procs path on the factory when given a handle, and
    /// clears it when given `None`. This is the boundary the worker
    /// pool's cgroup-setup hands the path across so every subsequent
    /// `command_from_rendered` call can install a `pre_exec` closure
    /// (or skip installing one in the `None` case).
    ///
    /// We can't easily introspect `std::process::Command`'s
    /// `pre_exec` field — it's an opaque sealed-box closure — so the
    /// test asserts on the factory field directly. The `pre_exec`
    /// install itself is exercised end-to-end in the SLURM smoke /
    /// e2e tests where a real worker subprocess attaches to a real
    /// nested cgroup; here we only pin the wiring.
    #[test]
    fn set_workers_cgroup_stashes_procs_path_or_clears() {
        use dynrunner_manager_local::WorkerFactory;
        let mut factory = make_factory_with_two_types();
        // Default: no nesting.
        assert!(factory.workers_cgroup_procs.is_none());

        // Build a synthetic handle pointing at a tempdir path.
        let root = tempfile::tempdir().unwrap();
        let workers = root.path().join("workers");
        std::fs::create_dir_all(&workers).unwrap();
        let handle = NestedCgroupHandle::from_workers_path_for_test(workers.clone());

        factory.set_workers_cgroup(Some(handle));
        assert_eq!(
            factory.workers_cgroup_procs.as_deref(),
            Some(workers.join("cgroup.procs").as_path())
        );

        // Setting back to None clears the path so subsequent spawns
        // fall back to the flat layout.
        factory.set_workers_cgroup(None);
        assert!(factory.workers_cgroup_procs.is_none());
    }
}
