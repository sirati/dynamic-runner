use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};

use db_comm_api_base::WorkerId;
use db_local_manager::WorkerFactory;
use db_transport_socket::named_socket::NamedSocketManagerEnd;
use db_transport_socket::socketpair::create_socketpair;

use crate::config::connection::ConnectionMode;
use crate::config::log_paths::LogPathConfig;
use crate::config::worker_spec::{RenderedCommand, WorkerSpec, WorkerVars};
use crate::transport::EitherManagerEnd;

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
    fn command_from_rendered(rendered: &RenderedCommand) -> std::process::Command {
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
            .stderr(std::process::Stdio::null());
        cmd
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
        socket_dir: &PathBuf,
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
