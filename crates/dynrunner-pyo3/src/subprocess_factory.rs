use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use dynrunner_core::{TypeId, WorkerId};
use dynrunner_manager_local::{SubcgroupHandle, WorkerFactory};
use dynrunner_transport_socket::named_socket::NamedSocketManagerEnd;
use dynrunner_transport_socket::socketpair::create_socketpair;

use crate::config::connection::ConnectionMode;
use crate::config::log_paths::LogPathConfig;
use crate::config::worker_spec::{RenderedCommand, WorkerSpec, WorkerVars};
use crate::task_def::{SharedTypeRegistry, TypeRuntime};
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

/// Resolve a worker's `(stdout, stderr)` stdio destinations from an
/// optional capture-file path.
///
/// `Some(path)` → both streams append to that one per-worker file (two
/// independent OS handles via `File::try_clone`, so each stream has its own
/// file offset cursor and neither truncates the other). `None`, or any I/O
/// error opening/cloning the file → both fall back to `/dev/null`.
///
/// Single concern: turn "where should this worker's stdio go?" into the two
/// `Stdio` values `Command` consumes. Best-effort by contract — a failure to
/// open the log file silences the stream rather than aborting the spawn, so a
/// missing-mount or permission glitch on the log path can never stop a worker
/// from starting (observability must not gate liveness).
fn stdio_capture_streams(path: Option<&Path>) -> (std::process::Stdio, std::process::Stdio) {
    let null = || std::process::Stdio::null();
    let Some(path) = path else {
        return (null(), null());
    };
    // Append so a per-type respawn extends the same `worker_<id>.log` the
    // prior (now-killed) worker wrote — the pre-respawn output is preserved
    // and the respawn's crash output is appended after it.
    let opened = std::fs::OpenOptions::new().create(true).append(true).open(path);
    match opened {
        Ok(file) => match file.try_clone() {
            // Two handles to the same file: one drives stdout, the other
            // stderr. Both inherit the append flag, so the kernel serialises
            // each `write` to end-of-file and the two streams interleave
            // without clobbering.
            Ok(second) => (std::process::Stdio::from(file), std::process::Stdio::from(second)),
            Err(_) => (std::process::Stdio::from(file), null()),
        },
        Err(_) => (null(), null()),
    }
}

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
    use nix::sys::signal::{Signal, kill};
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
    /// factory consults for per-spawn argv.
    ///
    /// A SHARED cell ([`SharedTypeRegistry`]) so the secondary's run-config
    /// finalize closure can swap in a registry rebuilt from the delivered
    /// `forwarded_argv` (the boot-CLI placeholder cmd_args → the finalized
    /// command) and have EVERY subsequent spawn — initial pool + per-type
    /// respawn — read the swapped value. The non-secondary dispatch paths
    /// seed it once and never swap.
    pub(crate) types: SharedTypeRegistry,
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
    fn type_runtime_for(&self, type_id: &TypeId) -> Result<TypeRuntime, String> {
        self.types
            .lock()
            .expect("worker TypeRegistry mutex poisoned")
            .get(type_id)
            .cloned()
            .ok_or_else(|| {
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
    fn first_type_runtime(&self) -> Result<TypeRuntime, String> {
        self.types
            .lock()
            .expect("worker TypeRegistry mutex poisoned")
            .first()
            .cloned()
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
                // Capture OS-stdio to the SAME `worker_log` the legacy argv
                // passes as `--log-file`, mirroring the WorkerSpec path.
                stdio_capture: Some(worker_log),
            }
        }
    }

    /// Build a `std::process::Command` from a rendered template. `stdin` is
    /// always silenced (the comm channel is a socket / inherited fd, never
    /// stdin); callers add transport-specific extras (e.g. socketpair
    /// `pre_exec` hooks) afterwards.
    ///
    /// Worker stdout + stderr capture: when `rendered.stdio_capture` is
    /// `Some(path)`, both are redirected (append) to that per-worker file —
    /// the SAME `worker_<id>.log` the worker logs into via `--log-file` /
    /// `{LOG_FILE}`. This preserves anything the worker writes OUTSIDE Python
    /// logging (an interpreter traceback, a native fault, a bare `print`, an
    /// `exit(1)` diagnostic). Without it those bytes go to `/dev/null` and a
    /// worker that crashes before it ever logs — e.g. a per-type respawn that
    /// exits 1 on startup — leaves NO trace anywhere. Every spawn (initial
    /// pool + respawn) funnels through here, so the capture is uniform with no
    /// respawn-specific branch. If the file cannot be opened the spawn falls
    /// back to silencing that stream (best-effort observability must never
    /// block a worker from starting).
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
    /// Worker attached to its per-worker sub-cgroup on `fork(2)` /
    /// pre-`execve(2)`: when `subcgroup_procs` is `Some`, a `pre_exec`
    /// closure writes the child's pid to that leaf's `cgroup.procs`.
    /// The kernel migrates the child into the per-worker leaf at the
    /// moment of the write, so the worker binary runs in the nested
    /// observability cgroup from its first instruction. The `pre_exec`
    /// closure is `Send + 'static`, captures only the owned `PathBuf`
    /// the parent computed BEFORE `fork(2)`, and only performs a
    /// single `std::fs::write` syscall — fork-safe under the documented
    /// `pre_exec` rules (no allocator activity that could deadlock
    /// against a parent holding the heap mutex at fork time). When
    /// `None` no `pre_exec` is set and behaviour is unchanged (legacy
    /// flat-cgroup or in-process channel test factories).
    fn command_from_rendered(
        &self,
        rendered: &RenderedCommand,
        subcgroup_procs: Option<PathBuf>,
    ) -> std::process::Command {
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
            .process_group(0);
        // Route the worker's OS-stdout + stderr to its per-worker log file
        // (append) so a crash that bypasses Python logging is still captured;
        // fall back to silence when no capture path is set or the open fails.
        let (stdout, stderr) = stdio_capture_streams(rendered.stdio_capture.as_deref());
        cmd.stdout(stdout).stderr(stderr);

        if let Some(procs) = subcgroup_procs {
            // `pre_exec` runs in the forked child after `fork(2)` but
            // before `execve(2)`. `std::process::id()` returns the
            // CHILD'S pid in that context. The single `std::fs::write`
            // call opens, writes, and closes the per-worker leaf's
            // `cgroup.procs` — atomic from the kernel's perspective
            // and the only side-effect is migrating the child into
            // that leaf.
            //
            // SAFETY: `pre_exec` is unsafe because the closure runs
            // in the forked child where most async-signal-unsafe
            // library calls (allocator-touching, mutex-acquiring)
            // would deadlock. The closure here formats the pid into a
            // stack-allocated `[u8; 16]` buffer (no heap activity, no
            // mutex acquisition) and hands the resulting byte slice to
            // `std::fs::write`, which invokes `open` → `write` →
            // `close` syscalls only — all async-signal-safe. The
            // captured `PathBuf` was cloned by the parent BEFORE the
            // fork — the child only reads it, never reallocates.
            //
            // Stack-formatted pid: a u32 in decimal is at most 10
            // digits; the buffer is sized to 16 for headroom.
            // Generating digits in reverse into a temporary then
            // copying forward gives the canonical decimal
            // representation.
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
    pub(crate) fn cleanup_all_process_trees(&mut self, grace: Duration) {
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
        subcgroup_procs: Option<PathBuf>,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let (manager_end, child_fd) =
            create_socketpair().map_err(|e| format!("failed to create socketpair: {e}"))?;

        let rendered = self.render_command(worker_id, runtime, FdOrSocket::Fd(child_fd));
        let mut cmd = self.command_from_rendered(&rendered, subcgroup_procs);

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
        subcgroup_procs: Option<PathBuf>,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let requested_path = self.log_paths.socket_path(socket_dir, worker_id);
        let manager_end = NamedSocketManagerEnd::bind(&requested_path)
            .map_err(|e| format!("failed to bind named socket: {e}"))?;
        // `bind` owns the on-disk filename and hands back a per-bind-
        // unique sibling of the requested path (respawn-unlink fix), so
        // the worker's argv MUST carry the path the endpoint actually
        // bound — not the requested template path the worker could not
        // connect to.
        let socket_path = manager_end.socket_path().to_owned();

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

        let mut cmd = self.command_from_rendered(&rendered, subcgroup_procs);
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
    ///
    /// `subcgroup_procs` is the pre-joined `<worker-leaf>/cgroup.procs`
    /// path the trait caller derived from the per-spawn
    /// `Option<&SubcgroupHandle>`; passed by value so the
    /// connection-mode arms can move it into the per-`pre_exec`
    /// closure they install.
    fn spawn_with_runtime(
        &mut self,
        worker_id: WorkerId,
        runtime: &TypeRuntime,
        subcgroup_procs: Option<PathBuf>,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        match &self.connection_mode {
            ConnectionMode::Socketpair => {
                self.spawn_socketpair(worker_id, runtime, subcgroup_procs)
            }
            ConnectionMode::Named { socket_dir } => {
                let socket_dir = socket_dir.clone();
                self.spawn_named(worker_id, runtime, &socket_dir, subcgroup_procs)
            }
        }
    }
}

impl WorkerFactory<EitherManagerEnd> for SubprocessWorkerFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
        subcgroup: Option<&SubcgroupHandle>,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        // Read (clone out) the first-type runtime so the `SharedTypeRegistry`
        // lock is released before `spawn_with_runtime` takes `&mut self`. The
        // clone is cheap — `TypeRuntime` holds Arc-backed strings + a
        // `Vec<String>` of cmd_args; only the cmd_args vec actually copies,
        // and at the once-per-restart cadence this is dominated by the cost
        // of forking Python. Reads the SHARED cell, so a finalize-time swap
        // (the run-config deferral) is honoured by every spawn.
        let runtime = self.first_type_runtime()?;
        let subcgroup_procs = subcgroup.map(|h| h.procs_path());
        self.spawn_with_runtime(worker_id, &runtime, subcgroup_procs)
    }

    fn spawn_worker_for_type(
        &mut self,
        worker_id: WorkerId,
        type_id: &TypeId,
        subcgroup: Option<&SubcgroupHandle>,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let runtime = self.type_runtime_for(type_id)?;
        let subcgroup_procs = subcgroup.map(|h| h.procs_path());
        self.spawn_with_runtime(worker_id, &runtime, subcgroup_procs)
    }

    /// Tear down the tracked worker subprocesses via the shared
    /// SIGTERM→grace→SIGKILL primitive. `Node::run`'s secondary arm invokes
    /// this at end of run (gated off the panik path — see the trait doc).
    /// Without it the `std::process::Child` handles would drop without
    /// killing, leaking the podman worker subprocesses (a `Child` drop is a
    /// no-op on the OS process). The ladder is a brief blocking teardown at
    /// end-of-run (no pump activity remains for this node), matching the
    /// pre-Node pyo3 wrapper's post-loop `cleanup_all()` call exactly.
    async fn cleanup(&mut self) {
        self.cleanup_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_def::TypeRegistry;

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
            types: std::sync::Arc::new(std::sync::Mutex::new(make_two_type_registry())),
            skip_existing: false,
            connection_mode: ConnectionMode::Named {
                socket_dir: PathBuf::from("/tmp/sockets"),
            },
            manual_start_worker: true,
            worker_spec: None,
            child_processes: Vec::new(),
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
            types: std::sync::Arc::new(std::sync::Mutex::new(TypeRegistry::default())),
            skip_existing: false,
            connection_mode: ConnectionMode::Named {
                socket_dir: PathBuf::new(),
            },
            manual_start_worker: true,
            worker_spec: None,
            child_processes: Vec::new(),
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

    /// When `command_from_rendered` is handed a `Some(procs)` path,
    /// the installed `pre_exec` closure writes the forked child's pid
    /// into that file post-`fork(2)` / pre-`execve(2)`. We can't
    /// introspect `std::process::Command`'s `pre_exec` field directly
    /// (it's an opaque sealed-box), so the test exercises the wiring
    /// end-to-end against a tempdir-rooted fake `cgroup.procs` and
    /// spawns `/bin/true` so the child exits immediately. After
    /// `wait()` returns we read the file and assert it contains the
    /// child's pid in decimal.
    ///
    /// Note: a real cgroup-v2 `cgroup.procs` is a kernel pseudo-file
    /// with write-append-pid semantics; a tmpfs path is plain
    /// write-truncate. The pre_exec closure uses `std::fs::write` in
    /// either case (one open-write-close), so the tmpfs file ends up
    /// holding exactly the pid bytes — which is what the kernel would
    /// have observed as the appended line. The test asserts that
    /// observable byte content.
    #[test]
    fn command_from_rendered_writes_child_pid_to_subcgroup_procs() {
        let factory = make_factory_with_two_types();
        let tmp = tempfile::tempdir().unwrap();
        let procs_path = tmp.path().join("cgroup.procs");

        let rendered = RenderedCommand {
            argv: vec!["true".to_string()],
            env: std::collections::HashMap::new(),
            cwd: None,
            stdio_capture: None,
        };
        let mut cmd = factory.command_from_rendered(&rendered, Some(procs_path.clone()));
        let mut child = cmd.spawn().expect("spawn true");
        let pid = child.id();
        let status = child.wait().expect("wait true");
        assert!(status.success(), "true exited non-success: {status:?}");

        let written = std::fs::read_to_string(&procs_path)
            .expect("pre_exec should have written cgroup.procs");
        assert_eq!(written, pid.to_string());
    }

    /// When `command_from_rendered` is handed `None`, no `pre_exec`
    /// cgroup closure is installed and no cgroup-related file is
    /// created. Spawns `true` (no env/cwd plumbing) and asserts
    /// the tempdir is empty post-spawn.
    #[test]
    fn command_from_rendered_without_subcgroup_writes_nothing() {
        let factory = make_factory_with_two_types();
        let tmp = tempfile::tempdir().unwrap();

        let rendered = RenderedCommand {
            argv: vec!["true".to_string()],
            env: std::collections::HashMap::new(),
            cwd: None,
            stdio_capture: None,
        };
        let mut cmd = factory.command_from_rendered(&rendered, None);
        let mut child = cmd.spawn().expect("spawn true");
        child.wait().expect("wait true");

        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .expect("read tempdir")
            .filter_map(Result::ok)
            .collect();
        assert!(
            entries.is_empty(),
            "tempdir should be untouched when no subcgroup is supplied; got: {entries:?}"
        );
    }

    /// Diagnostic-gap pin: `render_command` MUST populate
    /// `RenderedCommand::stdio_capture` with the SAME per-worker log file it
    /// passes as `--log-file`, so EVERY spawn that funnels through
    /// `command_from_rendered` — initial pool AND per-type respawn — routes
    /// its OS-stdio to a capturable file instead of `/dev/null`.
    ///
    /// Revert-check: drop the `stdio_capture: Some(worker_log)` assignment in
    /// the legacy `render_command` arm (back to the pre-fix shape) and this
    /// asserts `None`, failing.
    #[test]
    fn render_command_sets_stdio_capture_to_worker_log() {
        let factory = make_factory_with_two_types();
        let tokenize = factory
            .type_runtime_for(&TypeId::from("tokenize"))
            .unwrap()
            .clone();
        let tmp = std::path::PathBuf::from("/tmp/sock");
        let rendered = factory.render_command(0, &tokenize, FdOrSocket::Socket(&tmp));

        // The capture path is exactly the file the factory hands the worker as
        // its `--log-file` (LogPathConfig default: `<log_dir>/worker_<id>.log`).
        let expected = factory.log_paths.worker_log(&factory.log_dir, 0);
        assert_eq!(
            rendered.stdio_capture.as_deref(),
            Some(expected.as_path()),
            "render_command must capture worker stdio to its --log-file so a \
             crash-on-startup respawn is diagnosable",
        );
    }

    /// End-to-end pin that `command_from_rendered` actually REDIRECTS the
    /// spawned worker's OS-stdout AND stderr to the `stdio_capture` file.
    /// Spawns `/bin/sh -c 'echo OUT; echo ERR >&2'` and asserts BOTH lines
    /// land in the file — the exact wiring that makes a respawned worker's
    /// otherwise-`/dev/null` crash output recoverable from `worker_<id>.log`.
    ///
    /// Revert-check: restore the `command_from_rendered` stdout/stderr to
    /// `Stdio::null()` and the file ends up empty, failing both asserts.
    #[test]
    fn command_from_rendered_routes_stdout_and_stderr_to_capture_file() {
        let factory = make_factory_with_two_types();
        let tmp = tempfile::tempdir().unwrap();
        let log_path = tmp.path().join("worker_0.log");

        let rendered = RenderedCommand {
            argv: vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo OUT; echo ERR >&2".to_string(),
            ],
            env: std::collections::HashMap::new(),
            cwd: None,
            stdio_capture: Some(log_path.clone()),
        };
        let mut cmd = factory.command_from_rendered(&rendered, None);
        let status = cmd.spawn().expect("spawn sh").wait().expect("wait sh");
        assert!(status.success(), "sh exited non-success: {status:?}");

        let captured =
            std::fs::read_to_string(&log_path).expect("capture file should exist post-spawn");
        assert!(
            captured.contains("OUT"),
            "worker stdout must be captured to the log file; got: {captured:?}",
        );
        assert!(
            captured.contains("ERR"),
            "worker stderr must be captured to the log file; got: {captured:?}",
        );
    }

    /// `stdio_capture_streams(None)` silences both streams to `/dev/null` —
    /// the explicit-opt-out path the cgroup-wiring tests rely on (a worker
    /// constructed with `stdio_capture: None` must not create any file).
    #[test]
    fn stdio_capture_streams_none_silences_both() {
        // Smoke that the helper returns without panicking and creates no file
        // when handed `None`; the observable "no file created" is asserted by
        // `command_from_rendered_without_subcgroup_writes_nothing` above (it
        // uses `stdio_capture: None` and asserts the tempdir stays empty).
        let _ = stdio_capture_streams(None);
    }
}
