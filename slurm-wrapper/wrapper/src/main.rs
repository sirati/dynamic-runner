//! `dynrunner-slurm-wrapper` — the SLURM secondary wrapper binary.
//!
//! This binary replaces the bash heredoc that
//! `crates/dynrunner-slurm/src/wrapper_script/generate.rs` used to
//! render. Invoked by a tiny stub script as the sbatch entrypoint
//! (`exec <wrapper-bin> --config <cfg.json>`), it deserializes a
//! [`WrapperConfig`] and performs the full secondary lifecycle:
//!
//!   1. derive scratch-dir layout + create dirs        (`dirs`)
//!   2. resolve podman/rm absolute paths               (`bin_resolve`)
//!   3. install signalfd-based signal provenance       (`signals`)
//!   4. spawn the out-of-cgroup shutdown manager        (`shutdown_spawn`)
//!   5. pre-flight orphan-container sweep               (`preflight`)
//!   6. detect the container memory cap                 (`memcap`)
//!   7. resolve peer IPs + allocate ports + peer-info   (`network`)
//!   8. start the FIFO command relay                    (`relay`)
//!   9. copy + load the image                           (`image`)
//!  10. build argv + run the container to completion    (`podman_run`)
//!  11. teardown: forward shutdown nudge + drain relay  (`teardown`)
//!
//! Each step is a single-concern module with a clean boundary; `main`
//! is the only place that knows the lifecycle order AND the concurrency
//! between the container child, the relay, and the signal monitor.
//!
//! Signal-mask ordering (C1): `fn main()` is SYNC and blocks the monitored
//! signal set via [`signals::block_signals`] BEFORE building the tokio
//! runtime — otherwise a signal delivered to a worker/blocking-pool thread
//! that has not yet inherited the block triggers the default disposition
//! and the signalfd never sees it. The runtime is built manually after the
//! block and `block_on`s [`run`].

mod bin_resolve;
mod dirs;
mod image;
mod memcap;
mod network;
mod podman_run;
mod preflight;
mod relay;
mod shutdown_spawn;
mod signals;
mod teardown;

use dynrunner_slurm_wrapper_config::{ConnectionMode, WrapperConfig};
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use crate::dirs::Layout;
use crate::network::PeerIps;

/// Stable log prefix so operators can grep wrapper lines out of mixed
/// SLURM job stderr (mirrors the shutdown-manager's `[shutdown-mgr]`).
const LOG_TARGET: &str = "slurm-wrapper";

/// Bounded grace for the container to exit after a forwarded SIGTERM on the
/// terminating-signal path before teardown proceeds regardless (mirrors the
/// bash `stop -t 10` graceful window).
const SIGNAL_GRACE: Duration = Duration::from_secs(10);

/// SYNC entrypoint. Blocks the monitored signal set BEFORE the tokio runtime
/// exists (C1), then builds the runtime and `block_on`s the async lifecycle.
fn main() -> ExitCode {
    init_logging();

    // C1: block the monitored set process-wide BEFORE any thread (tokio
    // workers / blocking pool) is spawned, so the signalfd is the sole
    // consumer of every monitored delivery.
    if let Err(e) = signals::block_signals() {
        tracing::error!(target: LOG_TARGET, "failed to block signal set: {e}");
        return ExitCode::from(2);
    }

    let config_path = match parse_config_path(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "argv error: {e}");
            return ExitCode::from(2);
        }
    };

    let cfg = match load_config(&config_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                target: LOG_TARGET,
                "failed to load config {}: {e}",
                config_path.display()
            );
            return ExitCode::from(2);
        }
    };

    tracing::info!(
        target: LOG_TARGET,
        secondary_id = %cfg.secondary_id,
        suffix = %cfg.rand_suffix,
        "wrapper starting"
    );

    // Build the multi-thread runtime AFTER the block so every worker /
    // blocking-pool thread inherits the blocked mask.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "failed to build tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };

    runtime.block_on(run(cfg))
}

/// Run the secondary lifecycle, composing the single-concern modules in the
/// documented order. Teardown (`forward_shutdown_nudge` + relay drain) runs
/// on EVERY exit path — normal container exit, image-load error, and
/// terminating signal — and is the last thing before returning the code.
async fn run(cfg: WrapperConfig) -> ExitCode {
    // --- 1. layout + dirs (generate.rs:328-346) ---
    let layout = Layout::derive(&cfg);
    if let Err(e) = layout.create_dirs() {
        tracing::error!(target: LOG_TARGET, "failed to create scratch dirs: {e}");
        return ExitCode::from(1);
    }
    banner_job_start(&layout);

    // --- 2. resolve bins (generate.rs:364-387) ---
    let bins = bin_resolve::resolve();

    // --- 3. start signal provenance monitor (set already blocked in main) ---
    let mut monitor = match signals::start_monitor() {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "failed to start signal monitor: {e}");
            return ExitCode::from(1);
        }
    };

    // --- 4. spawn out-of-cgroup shutdown manager (generate.rs:214-296) ---
    let wrapper_pid = std::process::id();
    let mode = shutdown_spawn::spawn(&cfg, &layout, &bins, wrapper_pid);

    // --- 5. pre-flight orphan sweep (generate.rs:452-489) ---
    preflight::run(&bins.podman);

    // --- 6. memory cap (generate.rs:540-569) ---
    let mem_cap = memcap::detect_memory_cap();

    // --- 7. network: ports, peer-info, peer IPs, secondary_url ---
    let net = match resolve_network(&cfg).await {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "network setup failed: {e}");
            // No relay/container yet; only the shutdown manager exists.
            teardown::forward_shutdown_nudge(&mode);
            return ExitCode::from(1);
        }
    };

    // --- 8. start FIFO command relay (generate.rs:645-693) ---
    let relay = match relay::spawn(&layout) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "failed to start command relay: {e}");
            teardown::forward_shutdown_nudge(&mode);
            return ExitCode::from(1);
        }
    };

    // --- 9. copy + load image (generate.rs:695-711) ---
    if let Err(marker) = image::copy_and_load(&cfg, &layout, &bins) {
        tracing::error!(target: LOG_TARGET, "{marker}");
        // Image-load failure: teardown ALWAYS (relay was started above).
        teardown_all(&mode, relay).await;
        return ExitCode::from(1);
    }

    // --- 10. build argv + run the container concurrently with the monitor ---
    let argv = podman_run::build_run_argv(
        &cfg,
        &layout,
        &bins,
        mem_cap,
        &net.peer_ips,
        net.quic_port,
        &net.secondary_url,
    );
    banner_container_start(&cfg, &layout, &net);

    let exit_code = match spawn_container(&bins.podman, &argv, &layout) {
        Ok(child) => run_to_completion(child, &mut monitor).await,
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "failed to spawn container: {e}");
            // Treat a spawn failure like the bash set-e abort: exit 1.
            1
        }
    };

    // --- 11. teardown ALWAYS, then exit with the container's code ---
    teardown_all(&mode, relay).await;
    monitor.shutdown();
    banner_job_completed();
    ExitCode::from(exit_code as u8)
}

/// Resolved network inputs for the container run.
struct Network {
    peer_ips: PeerIps,
    quic_port: u16,
    secondary_url: String,
}

/// Step 7, mode-aware (generate.rs:583-642, :775-791). Allocates the QUIC
/// port always; for Reverse also allocates the tunnel port and writes the
/// v2 peer-info file. Standard mode writes NO peer-info file. The peer IPs
/// feed the `PRIMARY_NODE_IPV4/6` container env.
async fn resolve_network(cfg: &WrapperConfig) -> std::io::Result<Network> {
    let peer_ips = network::detect_peer_ips();
    let quic_port = network::alloc_free_port()?;

    let secondary_url = match &cfg.connection {
        ConnectionMode::Reverse { connection_info_dir } => {
            let tunnel_port = network::alloc_free_port()?;
            tracing::info!(target: LOG_TARGET, "Using tunnel port: {tunnel_port}");
            tracing::info!(target: LOG_TARGET, "Using QUIC port: {quic_port}");
            let hostname = hostname_fqdn();
            network::write_connection_info(
                std::path::Path::new(connection_info_dir),
                &cfg.secondary_id,
                &hostname,
                tunnel_port,
                quic_port,
                &peer_ips,
                cfg.is_observer,
            )?;
            tracing::info!(
                target: LOG_TARGET,
                "Connection info written to: {connection_info_dir}/{}.info",
                cfg.secondary_id
            );
            secondary_url(&cfg.connection, tunnel_port)
        }
        ConnectionMode::Standard { .. } => {
            tracing::info!(target: LOG_TARGET, "Using QUIC port: {quic_port}");
            secondary_url(&cfg.connection, 0)
        }
    };

    Ok(Network {
        peer_ips,
        quic_port,
        secondary_url,
    })
}

/// Derive the `--secondary` URL per mode (generate.rs:775-791). Reverse uses
/// the locally-allocated `tunnel_port`; Standard uses the configured gateway
/// host:port (the `tunnel_port` arg is ignored). PURE — testable.
fn secondary_url(connection: &ConnectionMode, tunnel_port: u16) -> String {
    match connection {
        ConnectionMode::Reverse { .. } => format!("tcp://localhost:{tunnel_port}"),
        ConnectionMode::Standard {
            gateway_host,
            gateway_port,
        } => format!("tcp://{gateway_host}:{gateway_port}"),
    }
}

/// `hostname -f` equivalent for the peer-info line-1 host (generate.rs:605).
fn hostname_fqdn() -> String {
    std::process::Command::new("hostname")
        .arg("-f")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

/// Spawn the container as a tokio child with the per-child env override
/// (C3: `XDG_RUNTIME_DIR=<layout.podman_run>` ONLY here, not process-global)
/// and the C2 child mask reset (`pre_exec`). Returns the live child so the
/// caller can race its exit against the signal monitor.
fn spawn_container(
    podman: &str,
    argv: &[String],
    layout: &Layout,
) -> std::io::Result<tokio::process::Child> {
    let mut command = tokio::process::Command::new(podman);
    command
        .args(argv)
        // C3: per-child XDG only — podman's rootless storage cookie lives in
        // $XDG_RUNTIME_DIR; setting it globally would corrupt the
        // shutdown-manager's systemd-user bus probe (it reads the real
        // /run/user/<uid>). The container child gets podman_run; the wrapper
        // process keeps its inherited value.
        .env("XDG_RUNTIME_DIR", &layout.podman_run);
    // C2: reset the inherited blocked mask so conmon and the container PID 1
    // receive SIGTERM for graceful stop.
    // SAFETY: child_pre_exec runs only an async-signal-safe sigprocmask.
    unsafe {
        command.pre_exec(signals::child_pre_exec());
    }
    command.spawn()
}

/// Race the container child against the signal monitor (TEARDOWN-ALWAYS).
/// Returns the exit code to propagate: the container's code on normal exit,
/// or `128 + signo` on signal-termination. Logs the explicit wait-status
/// (the forensic deliverable: WIFSIGNALED/WTERMSIG vs exit code).
async fn run_to_completion(
    mut child: tokio::process::Child,
    monitor: &mut signals::SignalMonitor,
) -> i32 {
    tokio::select! {
        // Container finished on its own.
        status = child.wait() => {
            exit_code_from_status(status)
        }
        // A terminating signal arrived first: log the cause, forward SIGTERM
        // to the container (idempotent if SLURM already group-signalled it),
        // await its exit with a bounded grace, then return 128+signo.
        term = monitor.recv_terminating() => {
            tracing::warn!(
                target: LOG_TARGET,
                signo = term.signo,
                signame = %term.signame,
                sender_pid = term.sender_pid,
                si_code = %term.si_code,
                comm = %term.comm,
                cmdline = %term.cmdline,
                "shutdown cause: terminating signal received; forwarding SIGTERM to container"
            );
            forward_sigterm(&child);
            // Bounded grace: let the container stop, but never wedge teardown.
            let _ = tokio::time::timeout(SIGNAL_GRACE, child.wait()).await;
            // Best-effort reap to avoid a zombie if the grace elapsed.
            let _ = child.try_wait();
            128 + term.signo as i32
        }
    }
}

/// Forward SIGTERM to the container child by pid (best-effort).
fn forward_sigterm(child: &tokio::process::Child) {
    if let Some(pid) = child.id() {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
}

/// Map a container wait-status to an exit code, logging the explicit
/// wait-status (forensic deliverable). On signal-termination, log
/// WIFSIGNALED + WTERMSIG and return `128 + signo` (bash's `$?` convention);
/// otherwise log and return the exit code.
fn exit_code_from_status(status: std::io::Result<std::process::ExitStatus>) -> i32 {
    match status {
        Ok(s) => {
            if let Some(code) = s.code() {
                tracing::info!(target: LOG_TARGET, "Container exited with code: {code}");
                code
            } else if let Some(sig) = s.signal() {
                tracing::warn!(
                    target: LOG_TARGET,
                    wtermsig = sig,
                    "Container terminated by signal (WIFSIGNALED); exit code 128+signo"
                );
                128 + sig
            } else {
                tracing::warn!(target: LOG_TARGET, "Container exited with unknown status");
                1
            }
        }
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "failed to wait on container: {e}");
            1
        }
    }
}

/// Teardown: forward the SIGCONT nudge to the shutdown manager, then drain
/// the relay (generate.rs cleanup trap, :306-310 + :404-407). Consumes the
/// relay handle.
async fn teardown_all(mode: &shutdown_spawn::ShutdownMode, relay: relay::RelayHandle) {
    teardown::forward_shutdown_nudge(mode);
    relay.shutdown().await;
}

// ---- human-readable banners (faithful-enough to the bash echoes) ----

fn banner_job_start(layout: &Layout) {
    tracing::info!(target: LOG_TARGET, "==================================================");
    tracing::info!(target: LOG_TARGET, "SLURM Secondary Job Starting");
    tracing::info!(target: LOG_TARGET, "==================================================");
    tracing::info!(target: LOG_TARGET, "Scratch dir: {}", layout.rndtmp.display());
    tracing::info!(target: LOG_TARGET, "Podman storage: {}", layout.podman_storage.display());
    tracing::info!(target: LOG_TARGET, "Podman run root: {}", layout.podman_run.display());
}

fn banner_container_start(cfg: &WrapperConfig, layout: &Layout, net: &Network) {
    tracing::info!(target: LOG_TARGET, "Starting Docker container...");
    tracing::info!(target: LOG_TARGET, "  Volumes:");
    tracing::info!(target: LOG_TARGET, "    {} -> /app/src-tmp", layout.src_tmp.display());
    tracing::info!(target: LOG_TARGET, "    {} -> /app/out-tmp", layout.out_tmp.display());
    tracing::info!(target: LOG_TARGET, "    {} -> /app/log-tmp", layout.log_tmp.display());
    tracing::info!(target: LOG_TARGET, "    {} -> /app/src-network (ro)", cfg.srcbins_network);
    tracing::info!(target: LOG_TARGET, "    {} -> /app/out-network", cfg.output_network);
    tracing::info!(target: LOG_TARGET, "    {} -> /app/log-network", cfg.log_network);
    if let Some(dir) = &cfg.dynrunner_network_dir {
        tracing::info!(target: LOG_TARGET, "    {dir} -> /app/dynrunner-network");
    }
    tracing::info!(target: LOG_TARGET, "    {} -> /app/sockets", layout.socket_dir.display());
    tracing::info!(target: LOG_TARGET, "  Secondary ID: {}", cfg.secondary_id);
    match &cfg.connection {
        ConnectionMode::Reverse { .. } => {
            tracing::info!(
                target: LOG_TARGET,
                "  Mode: SSH ProxyJump (primary tunnels to secondary via gateway)"
            );
        }
        ConnectionMode::Standard {
            gateway_host,
            gateway_port,
        } => {
            tracing::info!(target: LOG_TARGET, "  Gateway: {gateway_host}:{gateway_port}");
            tracing::info!(
                target: LOG_TARGET,
                "  Mode: Standard (secondary connects to primary via gateway)"
            );
        }
    }
    let _ = net;
}

fn banner_job_completed() {
    tracing::info!(target: LOG_TARGET, "==================================================");
    tracing::info!(target: LOG_TARGET, "Job completed");
    tracing::info!(target: LOG_TARGET, "==================================================");
}

/// `--config <path>` is the only argument. Mirrors the stub script's
/// `exec <wrapper-bin> --config <cfg.json>`.
fn parse_config_path<I: Iterator<Item = String>>(mut args: I) -> Result<PathBuf, String> {
    match args.next().as_deref() {
        Some("--config") => match args.next() {
            Some(path) => Ok(PathBuf::from(path)),
            None => Err("--config requires a path argument".to_string()),
        },
        Some(other) => Err(format!("unexpected argument: {other} (expected --config <path>)")),
        None => Err("missing --config <path>".to_string()),
    }
}

/// Read + deserialize the [`WrapperConfig`] JSON.
fn load_config(path: &PathBuf) -> Result<WrapperConfig, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

/// stderr logging via `tracing`, honouring `RUST_LOG` (default `info`).
fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_config_flag() {
        let args = vec!["--config".to_string(), "/tmp/cfg.json".to_string()];
        assert_eq!(
            parse_config_path(args.into_iter()).unwrap(),
            PathBuf::from("/tmp/cfg.json")
        );
    }

    #[test]
    fn rejects_missing_flag() {
        assert!(parse_config_path(Vec::<String>::new().into_iter()).is_err());
        assert!(parse_config_path(vec!["--config".to_string()].into_iter()).is_err());
        assert!(parse_config_path(vec!["bogus".to_string()].into_iter()).is_err());
    }

    #[test]
    fn secondary_url_reverse_uses_tunnel_port() {
        let mode = ConnectionMode::Reverse {
            connection_info_dir: "/net/conn".to_string(),
        };
        assert_eq!(secondary_url(&mode, 12345), "tcp://localhost:12345");
    }

    #[test]
    fn secondary_url_standard_uses_gateway() {
        let mode = ConnectionMode::Standard {
            gateway_host: "gw.cluster".to_string(),
            gateway_port: 4433,
        };
        // tunnel_port is ignored for Standard.
        assert_eq!(secondary_url(&mode, 999), "tcp://gw.cluster:4433");
    }

    #[test]
    fn exit_code_normal_exit() {
        // A real exited status: spawn `true` and wait synchronously.
        let status = std::process::Command::new("true").status().unwrap();
        assert_eq!(exit_code_from_status(Ok(status)), 0);
        let status = std::process::Command::new("false").status().unwrap();
        assert_eq!(exit_code_from_status(Ok(status)), 1);
    }

    #[test]
    fn exit_code_signal_termination_is_128_plus_signo() {
        use std::os::unix::process::ExitStatusExt;
        // Construct a signal-terminated ExitStatus (SIGKILL = 9).
        let status = std::process::ExitStatus::from_raw(9); // raw wait status: signo in low 7 bits
        // from_raw takes the raw wait() status word; 9 => terminated by signal 9.
        assert_eq!(status.signal(), Some(9));
        assert_eq!(exit_code_from_status(Ok(status)), 128 + 9);
    }

    #[test]
    fn exit_code_wait_error_is_one() {
        let err = std::io::Error::new(std::io::ErrorKind::Other, "boom");
        assert_eq!(exit_code_from_status(Err(err)), 1);
    }

    // ---- lifecycle orchestration: spawn_container + run_to_completion ----
    //
    // These exercise the genuinely-new concurrency (the select! that races
    // the container child against the signal monitor and routes whichever
    // fires into the same exit-code mapping) WITHOUT real podman: a fake
    // "podman" bash script stands in.
    //
    // The signal path is driven by INJECTING a synthetic TerminatingSignal
    // through `SignalMonitor::for_test`, NOT by raising a real
    // process-directed signal: once cargo's test harness is multithreaded,
    // `block_signals`/`sigprocmask` only reliably block the CALLING thread,
    // so a process-directed SIGTERM can land on an unblocked harness thread
    // and trip the default disposition (it would SIGTERM the whole test
    // binary). Injection tests the select!/forward/exit-code routing
    // deterministically; the real signalfd delivery (the C1 invariant) is a
    // single-threaded `fn main()` property covered by Phase 5 on the cluster.
    //
    // What only Phase 5 can cover: actual podman/conmon SIGTERM forwarding,
    // the systemd-run cgroup escape, the live peer-info handshake, and the
    // real signalfd receiving a process-directed signal after the C1
    // sync-block-then-runtime restructure.

    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;

    /// Write an executable fake-podman script into `dir` and return its path.
    fn write_fake_podman(dir: &std::path::Path, body: &str) -> String {
        let path = dir.join("fake-podman");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "#!/usr/bin/env bash\n{body}").unwrap();
        drop(f);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn test_layout(root: &std::path::Path) -> Layout {
        let podman_run = root.join("run");
        std::fs::create_dir_all(&podman_run).unwrap();
        Layout {
            rndtmp: root.to_path_buf(),
            container_name: "asm-test-0".to_string(),
            src_tmp: root.join("src"),
            out_tmp: root.join("out"),
            log_tmp: root.join("log"),
            podman_storage: root.join("storage"),
            podman_run,
            socket_dir: root.join("sockets"),
            cmd_socket: root.join("sockets/cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-test".to_string(),
            shutdown_log_path: root.join("shutdown-manager.log"),
            shutdown_pid_file: root.join("shutdown-manager.pid"),
            local_image: root.join("image.tar"),
        }
    }

    /// Happy path: fake podman exits 0 → code 0; the per-child
    /// XDG_RUNTIME_DIR is the layout's podman_run (C3), proven by the fake
    /// echoing it into a file the test reads back.
    #[tokio::test]
    async fn lifecycle_happy_path_exit_zero_and_per_child_xdg() {
        let tmp = tempfile::tempdir().unwrap();
        let probe = tmp.path().join("xdg.txt");
        let podman = write_fake_podman(
            tmp.path(),
            &format!("printf '%s' \"$XDG_RUNTIME_DIR\" > {}; exit 0", probe.display()),
        );
        let layout = test_layout(tmp.path());
        let (mut monitor, _inject) = signals::SignalMonitor::for_test();
        let child = spawn_container(&podman, &[], &layout).unwrap();
        let code = tokio::time::timeout(
            Duration::from_secs(15),
            run_to_completion(child, &mut monitor),
        )
        .await
        .expect("happy path resolves");
        assert_eq!(code, 0, "fake podman exit 0 -> code 0");
        let xdg = std::fs::read_to_string(&probe).unwrap();
        assert_eq!(
            xdg,
            layout.podman_run.to_string_lossy(),
            "C3: container child must see XDG_RUNTIME_DIR=<layout.podman_run>"
        );
    }

    /// Non-zero container exit propagates verbatim.
    #[tokio::test]
    async fn lifecycle_nonzero_exit_propagates() {
        let tmp = tempfile::tempdir().unwrap();
        let podman = write_fake_podman(tmp.path(), "exit 37");
        let layout = test_layout(tmp.path());
        let (mut monitor, _inject) = signals::SignalMonitor::for_test();
        let child = spawn_container(&podman, &[], &layout).unwrap();
        let code = tokio::time::timeout(
            Duration::from_secs(15),
            run_to_completion(child, &mut monitor),
        )
        .await
        .expect("nonzero path resolves");
        assert_eq!(code, 37);
    }

    /// Signal → teardown routing: a long-running fake podman (`exec sleep`) is
    /// still running when a synthetic SIGTERM is INJECTED into the monitor.
    /// run_to_completion must pick the signal branch of the select!, forward
    /// SIGTERM to the live container (a real signalable `sleep`), await its
    /// exit within the grace, and return 128+SIGTERM. This asserts the
    /// teardown-always routing and the 128+signo mapping deterministically.
    #[tokio::test]
    async fn lifecycle_injected_signal_forwards_and_maps_exit_code() {
        let tmp = tempfile::tempdir().unwrap();
        let podman = write_fake_podman(tmp.path(), "exec sleep 30");
        let layout = test_layout(tmp.path());
        let (mut monitor, inject) = signals::SignalMonitor::for_test();
        let child = spawn_container(&podman, &[], &layout).unwrap();
        let container_pid = child.id().expect("child has a pid");

        let sigterm = nix::sys::signal::Signal::SIGTERM as i32;
        inject
            .send(signals::TerminatingSignal {
                signo: sigterm as u32,
                signame: "SIGTERM".to_string(),
                sender_pid: 0,
                sender_uid: 0,
                si_code: "SI_USER".to_string(),
                comm: "<test>".to_string(),
                cmdline: "<test>".to_string(),
            })
            .await
            .unwrap();

        let code = tokio::time::timeout(
            Duration::from_secs(15),
            run_to_completion(child, &mut monitor),
        )
        .await
        .expect("run_to_completion must resolve via the injected signal path");
        assert_eq!(code, 128 + sigterm, "signal path returns 128+signo (SIGTERM)");

        // The container (sleep) must have been SIGTERM'd — it is no longer a
        // live process (reaped via the wait inside run_to_completion). Sending
        // signal 0 to its pid should now fail with ESRCH.
        let alive = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(container_pid as i32),
            None,
        )
        .is_ok();
        assert!(!alive, "container should have been terminated by the forwarded SIGTERM");
    }
}
