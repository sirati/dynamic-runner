//! `dynrunner-slurm-wrapper` — the SLURM secondary wrapper binary.
//!
//! This binary replaces the bash heredoc that
//! `crates/dynrunner-slurm/src/wrapper_script/generate.rs` used to
//! render. Invoked by a tiny stub script as the sbatch entrypoint
//! (`exec <wrapper-bin> --name-prefix ... --connection ...`), it parses a
//! [`WrapperConfig`] from its CLI flags and performs the full secondary
//! lifecycle:
//!
//!   1. derive scratch-dir layout + create dirs        (`dirs`)
//!   2. resolve podman/rm absolute paths               (`bin_resolve`)
//!   3. install signalfd-based signal provenance       (`signals`)
//!   4. spawn the out-of-cgroup shutdown manager        (`shutdown_spawn`)
//!      (then: CHECK + HONOR the submitter-set login-session
//!      decoupling — logind linger (`linger`))
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
mod cgroup;
mod dirs;
mod image;
mod linger;
mod memcap;
mod network;
mod podman_run;
mod preflight;
mod relay;
mod scratch_lock;
mod shutdown_spawn;
mod signals;
mod teardown;

use dynrunner_slurm_wrapper_config::{ConnectionMode, WrapperConfig};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitCode;
use std::time::Duration;

use crate::dirs::Layout;
use crate::network::PeerIps;

/// Stable log prefix so operators can grep wrapper lines out of mixed
/// SLURM job stderr (mirrors the shutdown-manager's `[shutdown-mgr]`).
const LOG_TARGET: &str = "slurm-wrapper";

/// SYNC entrypoint. Blocks the monitored signal set BEFORE the tokio runtime
/// exists (C1), then builds the runtime and `block_on`s the async lifecycle.
fn main() -> ExitCode {
    // C1: block the monitored set process-wide BEFORE any thread (tokio
    // workers / blocking pool) is spawned, so the signalfd is the sole
    // consumer of every monitored delivery. This precedes the runtime and
    // the logging init; the two fatal startup errors below predate the
    // tracing subscriber, so they go straight to stderr via `eprintln!`.
    if let Err(e) = signals::block_signals() {
        eprintln!("{LOG_TARGET}: failed to block signal set: {e}");
        return ExitCode::from(2);
    }

    let cfg = match dynrunner_slurm_wrapper_config::parse_args(std::env::args()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{LOG_TARGET}: argv error: {e}");
            return ExitCode::from(2);
        }
    };

    // Derive the scratch/log layout (PURE) so logging can be teed into the
    // persistent per-secondary `wrapper.log` BEFORE any lifecycle line is
    // emitted. `run` re-derives the same layout from `cfg` (cheap, pure).
    let layout = Layout::derive(&cfg);
    init_logging(&layout.wrapper_log_path);

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

    let code = runtime.block_on(run(cfg));

    // The signal-provenance monitor is a `spawn_blocking` loop parked in the
    // blocking `SignalFd::read_signal()` syscall, which has NO cancellation
    // point: `JoinHandle::abort()` cannot interrupt an in-flight blocking
    // closure (see signals.rs). Dropping a multi-thread runtime JOINS its
    // outstanding blocking-pool threads, so a plain `drop(runtime)` here would
    // wedge FOREVER and the container exit code would never reach SLURM.
    // `shutdown_background()` detaches the runtime without joining the blocking
    // pool — the parked thread dies at process exit. This mirrors bash's
    // `exit $CONTAINER_EXIT_CODE`: return the container's code immediately.
    runtime.shutdown_background();
    code
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
    // Mark THIS scratch root LIVE for the whole run: the exclusive
    // `wrapper.lock` is what a concurrent sibling job's pre-flight
    // sweep probes to tell a live root from an orphan (see
    // `scratch_lock`). Held by this scope until `run` returns; the
    // kernel releases it on ANY process exit, so a killed wrapper
    // leaves a probe-dead root the next sweep cleans. Best-effort:
    // failing to mark liveness must never gate the launch (it merely
    // leaves this job as exposed as a pre-fix one — logged loudly).
    let _scratch_lock = match scratch_lock::acquire(&layout.rndtmp) {
        Ok(guard) => Some(guard),
        Err(e) => {
            tracing::warn!(
                target: LOG_TARGET,
                "could not acquire scratch-root liveness lock ({e}); a \
                 concurrent sibling job's pre-flight sweep may treat this \
                 job's containers as orphans"
            );
            None
        }
    };
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

    // --- 4.5 HONOR the submitter-set login-session decoupling (linger) ---
    // The SUBMITTER's setup enables logind linger over its ssh to this node
    // at tunnel-build time (that ssh carries a pam_systemd session, which the
    // enable needs; this sbatch/slurmstepd context does NOT, so the wrapper
    // cannot enable it here). The wrapper's role is to CHECK + HONOR that
    // state and WARN if it reads as not set — a safety net surfacing a
    // setup-enable that silently failed. Linger is a resilience property,
    // NEVER a launch gate: the container always proceeds.
    match linger::check_linger() {
        linger::LingerCheck::Enabled { user } => tracing::info!(
            target: LOG_TARGET,
            user,
            "linger is enabled for the run user; workers are decoupled from the submitter login session"
        ),
        linger::LingerCheck::NotEnabled { user } => tracing::warn!(
            target: LOG_TARGET,
            user,
            "linger is NOT enabled for the run user — workers are NOT decoupled from the \
             submitter -R login session; a session drop may fan-kill this secondary. The \
             submitter setup is expected to enable it at tunnel-build time; surface this if it \
             persists (e.g. polkit-restricted node: pre-set `loginctl enable-linger` for the run \
             user or delegate it). Proceeding with container launch (linger is best-effort, not \
             a launch gate)."
        ),
        linger::LingerCheck::UserUnresolved => tracing::warn!(
            target: LOG_TARGET,
            "cannot CHECK linger: the run user's name did not resolve — no passwd entry \
             (this static-musl binary reads only /etc/passwd, never LDAP/sssd) and no \
             SLURM_JOB_USER/USER/LOGNAME in the environment. The linger marker may well be \
             present; this says nothing about the actual linger state. Proceeding with \
             container launch (linger is best-effort, not a launch gate)."
        ),
    }

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

    // --- 10. resolve job-cgroup containment (design §4 a1+a2) ---
    // The wrapper is already a member of the slurmstepd per-job cgroup;
    // discover it once. (a1) `--cgroup-parent` is attempted ONLY when a
    // write-probe proves cgroup-v2 delegation is present (podman can mkdir
    // a child cgroup beneath it); otherwise the flags are omitted and the
    // post-launch (a2) `cgroup.procs` adopt + the in-band reap carry the
    // load. Both are belt-and-suspenders: the orphan is reaped regardless
    // of delegation.
    let job_cgroup = cgroup::current_job_cgroup_real();
    let cgroup_parent: Option<String> = job_cgroup.as_ref().and_then(|cg| {
        if cgroup::cgroup_parent_probe_real(cg) {
            tracing::info!(
                target: LOG_TARGET,
                cgroup = %cg.0,
                "job cgroup is delegated; launching container under --cgroup-parent (a1)"
            );
            Some(cg.as_parent_arg().to_string())
        } else {
            tracing::info!(
                target: LOG_TARGET,
                cgroup = ?job_cgroup.as_ref().map(|c| &c.0),
                "job cgroup not delegated (or absent); omitting --cgroup-parent, \
                 relying on cgroup.procs adopt (a2) + in-band reap"
            );
            None
        }
    });

    // --- 11. build argv + run the container concurrently with the monitor ---
    let argv = podman_run::build_run_argv(
        &cfg,
        &layout,
        &bins,
        mem_cap,
        &net.peer_ips,
        net.quic_port,
        &net.secondary_url,
        cgroup_parent.as_deref(),
    );
    banner_container_start(&cfg, &layout, &net);

    let exit_code = match spawn_container(&bins.podman, &argv, &layout) {
        Ok(child) => {
            // (a2) Adopt conmon into the wrapper's own (job) cgroup as the
            // delegation-independent backstop: resolve conmon's host PID
            // and write it to our own `cgroup.procs`, pulling the
            // double-forked conmon back inside SLURM's authoritative
            // sweep. Best-effort and forensic-logged; the in-band reap is
            // the hard guarantee if this cannot run.
            //
            // The adopt POLLS `podman inspect` for conmon's PID for up to
            // ~2s of `std::thread::sleep` — a SYNCHRONOUS, blocking probe.
            // Running it inline here would block this tokio worker (and
            // therefore delay the `run_to_completion` select! below) for up
            // to 2s, so a SIGTERM arriving in that window would not be
            // raced for up to 2s. Instead it runs on a `spawn_blocking`
            // thread CONCURRENTLY with `run_to_completion`: the signal path
            // is live from the instant the container is spawned, and the
            // adopt's blocking polls never sit on a runtime worker. The
            // handle is awaited AFTER the race resolves (the adopt has
            // almost always finished by then; awaiting only reaps the
            // blocking task cleanly).
            let adopt_podman = bins.podman.clone();
            let adopt_layout = layout.clone();
            let adopt_cgroup = job_cgroup.clone();
            let adopt = tokio::task::spawn_blocking(move || {
                adopt_conmon_into_job_cgroup(&adopt_podman, &adopt_layout, adopt_cgroup.as_ref());
            });
            let code = run_to_completion(child, &mut monitor, &bins.podman, &layout).await;
            let _ = adopt.await;
            code
        }
        Err(e) => {
            tracing::error!(target: LOG_TARGET, "failed to spawn container: {e}");
            // Treat a spawn failure like the bash set-e abort: exit 1.
            1
        }
    };

    // --- 11. teardown ALWAYS, then exit with the container's code ---
    // NB: the signal monitor is NOT joined/aborted here. Its `spawn_blocking`
    // loop is parked in a blocking `read_signal()` syscall with no
    // cancellation point, so `JoinHandle::abort()` is a no-op against it; the
    // runtime is detached via `shutdown_background()` in `main` and the parked
    // thread dies at process exit.
    let _ = monitor;
    teardown_all(&mode, relay).await;
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
        ConnectionMode::Reverse {
            connection_info_dir,
        } => {
            let tunnel_port = network::alloc_free_port()?;
            tracing::info!(target: LOG_TARGET, "Using tunnel port: {tunnel_port}");
            tracing::info!(target: LOG_TARGET, "Using QUIC port: {quic_port}");
            let hostname = hostname_fqdn();
            let record = network::write_connection_info(
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
            tracing::info!(
                target: LOG_TARGET,
                "Connection info record: {}",
                network::record_log_line(&record)
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
///
/// On a terminating signal it runs the BOUNDED SYNCHRONOUS in-band reap
/// (`teardown::reap_container_inband`) — graceful `podman stop`, then
/// identity-checked SIGTERM→grace→SIGKILL of conmon AND the workload,
/// then `podman rm -f` — finishing inside `KillWait` BEFORE returning so
/// the orphan never survives the cgroup sweep. The reap is sync/blocking
/// (it sleeps its graces), so it runs on `spawn_blocking`.
async fn run_to_completion(
    mut child: tokio::process::Child,
    monitor: &mut signals::SignalMonitor,
    podman: &str,
    layout: &Layout,
) -> i32 {
    tokio::select! {
        // Container finished on its own.
        status = child.wait() => {
            exit_code_from_status(status)
        }
        // A terminating signal arrived first: log the cause (full
        // provenance from the signalfd), run the bounded in-band reap,
        // then return 128+signo.
        term = monitor.recv_terminating() => {
            tracing::warn!(
                target: LOG_TARGET,
                signo = term.signo,
                signame = %term.signame,
                sender_pid = term.sender_pid,
                si_code = %term.si_code,
                comm = %term.comm,
                cmdline = %term.cmdline,
                "shutdown cause: terminating signal received; beginning bounded in-band reap"
            );
            // Run the sync reap off the async runtime. Owned clones cross
            // the 'static spawn_blocking boundary; the wrapper is exiting
            // so the small clone cost is irrelevant.
            let podman_owned = podman.to_string();
            let layout_owned = layout.clone();
            let _ = tokio::task::spawn_blocking(move || {
                teardown::reap_container_inband(&podman_owned, &layout_owned)
            })
            .await;
            // The foreground `podman run` client child is reaped by the
            // in-band stop/rm; best-effort wait to avoid a zombie.
            let _ = child.try_wait();
            128 + term.signo as i32
        }
    }
}

/// Resolve conmon's host PID via `podman inspect {{.State.ConmonPid}}` and
/// adopt it into the wrapper's own (job) cgroup — the (a2) backstop.
/// Best-effort + forensic-logged: a missing cgroup, an unresolvable
/// conmon, or a failed adopt is not fatal (the in-band reap is the
/// guarantee). No-op when the job cgroup could not be discovered (v1 host).
fn adopt_conmon_into_job_cgroup(
    podman: &str,
    layout: &Layout,
    job_cgroup: Option<&cgroup::CgroupPath>,
) {
    let Some(cg) = job_cgroup else {
        return;
    };
    // Poll briefly for conmon's PID — `podman inspect` may report 0/absent
    // for a moment right after `podman run` starts while conmon comes up.
    let mut conmon_pid = None;
    for _ in 0..20 {
        if let Some(pid) = inspect_conmon_pid(podman, layout) {
            conmon_pid = Some(pid);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    match conmon_pid {
        Some(pid) => {
            cgroup::adopt_into_self_cgroup(cg, pid);
        }
        None => tracing::warn!(
            target: LOG_TARGET,
            "could not resolve conmon PID for cgroup adopt; relying on in-band reap"
        ),
    }
}

/// `podman inspect --format {{.State.ConmonPid}}` → conmon host PID, or
/// `None` when the record is gone / the field is 0 / unparsable. Uses the
/// per-secondary storage prefix so it sees the just-launched container.
fn inspect_conmon_pid(podman: &str, layout: &Layout) -> Option<u32> {
    let out = std::process::Command::new(podman)
        .arg("--root")
        .arg(&layout.podman_storage)
        .arg("--runroot")
        .arg(&layout.podman_run)
        .arg("--cgroup-manager=cgroupfs")
        .arg("inspect")
        .arg("--format")
        .arg("{{.State.ConmonPid}}")
        .arg(&layout.container_name)
        .env("XDG_RUNTIME_DIR", &layout.podman_run)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let pid = String::from_utf8_lossy(&out.stdout)
        .trim()
        .lines()
        .next()?
        .trim()
        .parse::<u32>()
        .ok()?;
    match pid {
        0 => None,
        _ => Some(pid),
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

/// `tracing` logging, honouring `RUST_LOG` (default `info`). Output is TEED
/// to two sinks: the SLURM job stderr (always) AND the persistent
/// per-secondary `wrapper.log` (`<shutdown_log_dir>/wrapper.log`), so the
/// operator finds the wrapper's narrative alongside the shutdown-manager log
/// and the container log for that secondary, surviving the scratch-tree
/// teardown.
///
/// The file sink is best-effort: its parent dir is created and the file
/// opened in append mode; on any failure we degrade to stderr-only with a
/// single warning, NEVER aborting (losing the file copy is strictly less bad
/// than losing the run). No new crate is pulled in — `tracing_subscriber`'s
/// `MakeWriterExt::and` tees stderr with an `Arc<File>` writer directly.
fn init_logging(wrapper_log_path: &std::path::Path) {
    use std::sync::Arc;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt::writer::MakeWriterExt;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Best-effort open of the persistent file sink. The parent dir is the
    // per-secondary network dir; `create_dirs` (in `run`) also ensures it,
    // but logging inits FIRST, so the logging concern creates its own sink's
    // parent here rather than depending on a later step's side effect.
    let file = wrapper_log_path
        .parent()
        .map(std::fs::create_dir_all)
        .transpose()
        .and_then(|_| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(wrapper_log_path)
        });

    match file {
        Ok(f) => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(std::io::stderr.and(Arc::new(f)))
                .try_init();
        }
        Err(e) => {
            // Degrade to stderr-only; warn AFTER init so the warning itself
            // is captured by the subscriber we just installed.
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(std::io::stderr)
                .try_init();
            tracing::warn!(
                target: LOG_TARGET,
                path = %wrapper_log_path.display(),
                "could not open wrapper.log file sink ({e}); logging to stderr only"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wrapper now parses its `WrapperConfig` straight from CLI flags.
    /// A full standard-mode flag set parses into the expected struct (the
    /// exhaustive encode↔decode round-trip lives in the config crate; here
    /// we only confirm `main`'s entrypoint wiring through `parse_args`).
    #[test]
    fn parses_config_from_flags() {
        let argv = [
            "dynrunner-slurm-wrapper",
            "--name-prefix",
            "asm",
            "--rand-suffix",
            "2f1d4e89",
            "--secondary-id",
            "sec-0",
            "--image-path",
            "/staged/img.tar",
            "--image-tar-basename",
            "img.tar",
            "--image-digest",
            "a1b2c3d4e5f6",
            "--image-name",
            "img",
            "--image-tag",
            "latest",
            "--load-command",
            "true",
            "--container-command",
            "cmd",
            "--cores-spec",
            "-2",
            "--max-memory-spec",
            "-2G",
            "--secondary-module",
            "pkg.secondary",
            "--srcbins-network",
            "/net/srcbins",
            "--output-network",
            "/net/out",
            "--log-network",
            "/net/log",
            "--connection",
            "standard",
            "--gateway-host",
            "gw",
            "--gateway-port",
            "4433",
            "--is-observer",
            "false",
        ];
        let cfg = dynrunner_slurm_wrapper_config::parse_args(argv).unwrap();
        assert_eq!(cfg.secondary_id, "sec-0");
        assert_eq!(cfg.secondary_module, "pkg.secondary");
        assert_eq!(cfg.image_digest, "a1b2c3d4e5f6");
        assert_eq!(
            cfg.connection,
            ConnectionMode::Standard {
                gateway_host: "gw".to_string(),
                gateway_port: 4433,
            }
        );
    }

    /// Argv errors (missing required flag, unknown flag, no args) surface
    /// as `Err` so `main` can exit nonzero.
    #[test]
    fn rejects_bad_argv() {
        assert!(dynrunner_slurm_wrapper_config::parse_args(["dynrunner-slurm-wrapper"]).is_err());
        assert!(dynrunner_slurm_wrapper_config::parse_args([
            "dynrunner-slurm-wrapper",
            "--name-prefix",
            "asm"
        ])
        .is_err());
        assert!(
            dynrunner_slurm_wrapper_config::parse_args(["dynrunner-slurm-wrapper", "--bogus"])
                .is_err()
        );
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
        let err = std::io::Error::other("boom");
        assert_eq!(exit_code_from_status(Err(err)), 1);
    }

    /// Regression for the exit-hang: the wrapper's signal monitor is a
    /// `spawn_blocking` loop parked in a blocking `SignalFd::read_signal()`
    /// syscall that has NO cancellation point. Dropping a multi-thread tokio
    /// runtime JOINS its outstanding blocking-pool threads, so a plain
    /// `drop(runtime)` would wedge FOREVER on such a parked thread and the
    /// container exit code would never reach SLURM. `main` instead tears the
    /// runtime down with `shutdown_background()`, which detaches WITHOUT
    /// joining the blocking pool.
    ///
    /// This reproduces the hazard generically — WITHOUT a real signalfd and
    /// WITHOUT touching the process-wide signal mask (which would leak into
    /// sibling tests; that is exactly why the real-signalfd test is deferred
    /// to Phase 5). A `spawn_blocking` closure parks on a channel `recv()`
    /// that never receives, standing in for the parked `read_signal()`. We
    /// confirm runtime liveness, then exercise the SAME teardown `main` uses
    /// and assert it returns within a tight bound. A plain `drop(runtime)`
    /// here would instead block until the test runner SIGKILLs the binary,
    /// so this test bites the original bug.
    #[test]
    fn runtime_teardown_does_not_join_parked_blocking_task() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build multi-thread runtime");

        // A blocking-pool closure parked indefinitely (never-fed channel),
        // mirroring the monitor's parked `read_signal()`. No signalfd, no
        // signal-mask mutation — pure generic hazard.
        let (_never_tx, never_rx) = std::sync::mpsc::channel::<()>();
        runtime.spawn_blocking(move || {
            // Blocks forever: the sender is held by this thread's frame and
            // never sends, so recv() parks until the process exits.
            let _ = never_rx.recv();
        });

        // Confirm the runtime is live before teardown.
        let live = runtime.block_on(async { 21 * 2 });
        assert_eq!(live, 42, "runtime must run a trivial future");

        // Exercise the EXACT teardown main uses, timed on the spawning thread.
        let start = std::time::Instant::now();
        runtime.shutdown_background();
        let elapsed = start.elapsed();

        // `shutdown_background()` returns essentially immediately; a generous
        // bound proves it did NOT join the parked blocking thread. (A plain
        // `drop(runtime)` would hang here indefinitely.)
        assert!(
            elapsed < Duration::from_secs(2),
            "runtime teardown must not join the parked blocking task (took {elapsed:?})"
        );
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
            shutdown_log_dir: root.join("log-network/sec-0"),
            shutdown_log_path: root.join("log-network/sec-0/shutdown-manager.log"),
            wrapper_log_path: root.join("log-network/sec-0/wrapper.log"),
            shutdown_pid_file: root.join("shutdown-manager.pid"),
            local_image: root.join("image.tar"),
            image_cache_root: root.join("imgcache"),
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
            &format!(
                "printf '%s' \"$XDG_RUNTIME_DIR\" > {}; exit 0",
                probe.display()
            ),
        );
        let layout = test_layout(tmp.path());
        let (mut monitor, _inject) = signals::SignalMonitor::for_test();
        let child = spawn_container(&podman, &[], &layout).unwrap();
        let code = tokio::time::timeout(
            Duration::from_secs(15),
            run_to_completion(child, &mut monitor, &podman, &layout),
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
            run_to_completion(child, &mut monitor, &podman, &layout),
        )
        .await
        .expect("nonzero path resolves");
        assert_eq!(code, 37);
    }

    /// Signal → in-band-reap routing: a long-running fake podman
    /// (`run` → `exec sleep`) is still running when a synthetic SIGTERM is
    /// INJECTED into the monitor. `run_to_completion` must pick the signal
    /// branch of the select!, run the BOUNDED IN-BAND REAP
    /// (`teardown::reap_container_inband` — which shells `podman
    /// stop`/`rm`), and return 128+SIGTERM. The fake podman is
    /// subcommand-aware: `run` execs `sleep`; `inspect` reports no record
    /// (exit 1) so the reap captures no PIDs (NotApplicable); `stop`/`rm`
    /// record a marker file and exit 0. We assert the routing + exit code
    /// AND that the in-band reap actually invoked stop+rm. The reap's
    /// kill-by-PID behaviour is unit-tested in `teardown`/`dynrunner-reap`.
    #[tokio::test]
    async fn lifecycle_injected_signal_runs_inband_reap_and_maps_exit_code() {
        let tmp = tempfile::tempdir().unwrap();
        let marker = tmp.path().join("podman-calls.log");
        // Subcommand-aware fake podman. Args include podman GLOBAL flags
        // before the subcommand, so scan all args for the first known verb.
        let body = format!(
            "verb=\"\"\n\
             for a in \"$@\"; do case \"$a\" in run|inspect|stop|rm) verb=\"$a\"; break;; esac; done\n\
             case \"$verb\" in\n\
             run) exec sleep 30;;\n\
             inspect) exit 1;;\n\
             stop) echo stop >> {m}; exit 0;;\n\
             rm) echo rm >> {m}; exit 0;;\n\
             *) exit 0;;\n\
             esac",
            m = marker.display()
        );
        let podman = write_fake_podman(tmp.path(), &body);
        let layout = test_layout(tmp.path());
        let (mut monitor, inject) = signals::SignalMonitor::for_test();
        let child = spawn_container(&podman, &[], &layout).unwrap();

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
            Duration::from_secs(20),
            run_to_completion(child, &mut monitor, &podman, &layout),
        )
        .await
        .expect("run_to_completion must resolve via the injected signal path");
        assert_eq!(
            code,
            128 + sigterm,
            "signal path returns 128+signo (SIGTERM)"
        );

        // The in-band reap must have shelled `podman stop` then `podman rm`.
        let calls = std::fs::read_to_string(&marker).unwrap_or_default();
        assert!(
            calls.contains("stop"),
            "in-band reap must invoke `podman stop`; calls: {:?}",
            calls
        );
        assert!(
            calls.contains("rm"),
            "in-band reap must invoke `podman rm`; calls: {:?}",
            calls
        );
    }
}
