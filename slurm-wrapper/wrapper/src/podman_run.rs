//! Single concern: build the `podman run` argv (byte-for-byte the same
//! effective argv as generate.rs:826-843) and run the container to
//! completion. Adds `--log-level=debug` + conmon debug logging (new
//! capability). Phase 1 (1H) fills bodies.

use crate::bin_resolve::ResolvedBins;
use crate::dirs::{
    reaper_panik_container_path, Layout, LOG_TMP_CONTAINER_PATH, WORK_TMP_CONTAINER_PATH,
};
use crate::network::PeerIps;
use dynrunner_slurm_wrapper_config::WrapperConfig;

/// Container-internal bind-mount path for the staged-source drive.
/// Used for BOTH the `-v <src>:/app/src-network:ro` volume spec and the
/// `--src-network=/app/src-network` framework flag, mirroring
/// `WRAPPER_SRC_NETWORK_CONTAINER_PATH` in `generate.rs` so the two stay
/// in lockstep.
const SRC_NETWORK_CONTAINER_PATH: &str = "/app/src-network";

/// Assemble the full argv vector for the `podman ... run ...` invocation.
/// `mem_cap_bytes == None` omits the `--memory` flags; `secondary_url` is
/// mode-derived (`tcp://localhost:<tunnel_port>` or
/// `tcp://<gateway>:<port>`).
///
/// Returns ONLY the arguments — the caller sets the program to
/// `bins.podman`. The vec therefore starts at the first podman GLOBAL
/// flag (`--root`).
///
/// Byte-for-byte port of the `podman run` block at `generate.rs:826-843`
/// (after bash word-splitting/quoting), PLUS the one intentional forensic
/// addition `--log-level=debug` (placed last among the podman global
/// flags, before `run`; it makes podman run conmon at debug too).
///
/// ASSUMPTION: `cfg.container_command` holds no embedded shell quotes or
/// globs — the bare `dynamic_runner._secondary_bootstrap` shim module the
/// image entrypoint (`python -m`) runs. The legacy bash splices it
/// unquoted into the heredoc, so word-splitting on ASCII whitespace is the
/// faithful equivalent. If a consumer is found to pass quoted/space-bearing
/// container-command args, this split would diverge from bash and the
/// contract must move `container_command` to a pre-split `Vec<String>`.
// The argv is built by interleaved conditional `push`es with per-section
// generate.rs provenance comments; a `vec![]` literal would erase that
// structure (and is impossible across the conditionals). The eight inputs
// are each a distinct, independent container-launch parameter — bundling
// them into a struct would only move the same arity behind a name. Both
// are deliberate, not accidental complexity.
#[allow(clippy::vec_init_then_push, clippy::too_many_arguments)]
pub fn build_run_argv(
    cfg: &WrapperConfig,
    layout: &Layout,
    _bins: &ResolvedBins,
    mem_cap_bytes: Option<u64>,
    peer_ips: &PeerIps,
    quic_port: u16,
    secondary_url: &str,
    host_hostname: &str,
    cgroup_parent: Option<&str>,
) -> Vec<String> {
    let mut argv: Vec<String> = Vec::new();

    // ---- podman GLOBAL flags (before `run`) — generate.rs:826 ----
    argv.push("--root".to_string());
    argv.push(layout.podman_storage.display().to_string());
    argv.push("--runroot".to_string());
    argv.push(layout.podman_run.display().to_string());
    argv.push("--cgroup-manager=cgroupfs".to_string());
    // Intentional forensic addition: debug podman + conmon logging.
    argv.push("--log-level=debug".to_string());

    // ---- run + fixed flags — generate.rs:826-831 ----
    argv.push("run".to_string());
    argv.push("--rm".to_string());
    // Containment (design §4 a1): when the wrapper's job cgroup is
    // delegated (the caller's `cgroup_parent` probe passed), create the
    // container's cgroup BENEATH the slurmstepd job cgroup so conmon + the
    // container land inside SLURM's authoritative `proctrack/cgroup` sweep
    // and are reaped at `KillWait` with no dynrunner watchdog needed.
    // `--cgroups=enabled` ensures podman actually creates the cgroup (not
    // `no-conmon`/`disabled`). Omitted when the probe failed (no
    // delegation): then the post-launch cgroup.procs adopt (a2) + the
    // in-band reap (b) carry the load, so `None` here yields the
    // byte-for-byte pre-existing argv.
    if let Some(parent) = cgroup_parent {
        argv.push(format!("--cgroup-parent={parent}"));
        argv.push("--cgroups=enabled".to_string());
    }
    argv.push("--name".to_string());
    argv.push(layout.container_name.clone());
    argv.push("--pull=never".to_string());
    argv.push("--network".to_string());
    argv.push("host".to_string());
    // In-container identity = the node. `--network host` shares the net
    // namespace but podman still gives the container a PRIVATE UTS
    // namespace whose default hostname is not the node's — the runner
    // then reports "unknown" in its welcome/logs. Pass the wrapper
    // host's external FQDN (the SAME source the Reverse-mode peer-info
    // record publishes) so node-keyed forensics line up.
    argv.push("--hostname".to_string());
    argv.push(host_hostname.to_string());
    // Podman's rootless default is pids_limit=2048 (from containers.conf).
    // Under SLURM, fork-heavy or thread-heavy workloads (JVM, parallel
    // compilers, autotools) exhaust that cap → clone() EAGAIN. Pass 0
    // (unlimited) explicitly so the silent builtin default never fires;
    // the host protections are the RAM cap and nice level.
    argv.push("--pids-limit=0".to_string());
    argv.push("--ulimit".to_string());
    argv.push("nproc=32768:32768".to_string());

    // ---- MEM_FLAGS — generate.rs:563-567, :832 ----
    // bash `${MEM_FLAGS}` expands (word-split) to two tokens when set,
    // to nothing when empty.
    if let Some(n) = mem_cap_bytes {
        argv.push(format!("--memory={n}"));
        argv.push("--memory-swap=-1".to_string());
    }

    // ---- ENV — generate.rs:833-834 + dynrunner_env_block (:62-70) ----
    // The bash passes the possibly-empty value verbatim
    // (`-e PRIMARY_NODE_IPV4="$PRIMARY_NODE_IPV4"`).
    argv.push("-e".to_string());
    argv.push(format!(
        "PRIMARY_NODE_IPV4={}",
        peer_ips.ipv4.clone().unwrap_or_default()
    ));
    argv.push("-e".to_string());
    argv.push(format!(
        "PRIMARY_NODE_IPV6={}",
        peer_ips.ipv6.clone().unwrap_or_default()
    ));
    if cfg.dynrunner_network_dir.is_some() {
        argv.push("-e".to_string());
        argv.push("DYNRUNNER_NETWORK=/app/dynrunner-network".to_string());
    }
    // Point the in-container framework at the bind-mounted scratch work
    // dir so the settled-CRDT spill lands on node-local scratch (the
    // framework reads `settled_spill::WORK_DIR_ENV`). Always present —
    // the work mount below is unconditional, mirroring out-tmp/log-tmp.
    argv.push("-e".to_string());
    argv.push(format!("DYNRUNNER_WORK_DIR={WORK_TMP_CONTAINER_PATH}"));

    // ---- VOLUMES — generate.rs:835-841 + dynrunner_volume_block ----
    argv.push("-v".to_string());
    argv.push(format!("{}:/app/src-tmp", layout.src_tmp.display()));
    argv.push("-v".to_string());
    argv.push(format!("{}:/app/out-tmp", layout.out_tmp.display()));
    argv.push("-v".to_string());
    argv.push(format!(
        "{}:{}",
        layout.log_tmp.display(),
        LOG_TMP_CONTAINER_PATH
    ));
    // Per-node scratch WORK mount (settled-CRDT spill), sibling shape to
    // out-tmp/log-tmp; exported as DYNRUNNER_WORK_DIR above.
    argv.push("-v".to_string());
    argv.push(format!(
        "{}:{}",
        layout.work_tmp.display(),
        WORK_TMP_CONTAINER_PATH
    ));
    argv.push("-v".to_string());
    argv.push(format!(
        "{}:{}:ro",
        cfg.srcbins_network, SRC_NETWORK_CONTAINER_PATH
    ));
    argv.push("-v".to_string());
    argv.push(format!("{}:/app/out-network", cfg.output_network));
    argv.push("-v".to_string());
    argv.push(format!("{}:/app/log-network", cfg.log_network));
    if let Some(dir) = &cfg.dynrunner_network_dir {
        argv.push("-v".to_string());
        argv.push(format!("{dir}:/app/dynrunner-network"));
    }
    argv.push("-v".to_string());
    argv.push(format!("{}:/app/sockets", layout.socket_dir.display()));

    // ---- EXTRA run args — generate.rs:79-83, :842 ----
    // Passed through verbatim (execve, not bash — no shell quoting).
    for arg in &cfg.extra_run_args {
        argv.push(arg.clone());
    }

    // ---- IMAGE REF — generate.rs:734, :842 ----
    argv.push(format!("{}:{}", cfg.image_name, cfg.image_tag));

    // ---- CONTAINER COMMAND — generate.rs:736, :843 ----
    // bash splices unquoted → word-split on ASCII whitespace.
    for tok in cfg.container_command.split_whitespace() {
        argv.push(tok.to_string());
    }

    // ---- FRAMEWORK FLAGS — generate.rs:843 ----
    // Mind SPACE vs EQUALS exactly as the bash.
    argv.push("--secondary".to_string());
    argv.push(secondary_url.to_string());
    argv.push("--secondary-id".to_string());
    argv.push(cfg.secondary_id.clone());
    // The container entrypoint runs the framework bootstrap shim
    // (`container_command` = `dynamic_runner._secondary_bootstrap`);
    // `--secondary-module` names the consumer's REAL secondary
    // entrypoint, which the shim `runpy`s after fetching the run config
    // over the peer mesh. Replaces the old `forwarded_argv` argv splice
    // (the dispatcher's task-specific argv now travels over the mesh).
    argv.push("--secondary-module".to_string());
    argv.push(cfg.secondary_module.clone());
    argv.push("--secondary-quic-port".to_string());
    argv.push(quic_port.to_string());
    argv.push(format!("--cores={}", cfg.cores_spec));
    argv.push(format!("--max-memory={}", cfg.max_memory_spec));
    argv.push(format!("--src-network={SRC_NETWORK_CONTAINER_PATH}"));
    argv.push("--log-dir=/app/log-network".to_string());
    // Persist this container's framework runner log on the gateway-shared
    // `--log-dir` mount, keyed per-node by `secondary_id` so the relocated/
    // co-located primary and each secondary write to distinct, host-readable
    // files (logging.rs composes the per-role filenames under this dir).
    // Forwarded as a framework `--full-log-dir` CLI arg — parsed by the
    // secondary's argparse and threaded into the explicit `init_logging`
    // param — not an env injection.
    argv.push(format!(
        "--full-log-dir=/app/log-network/{}",
        cfg.secondary_id
    ));
    // Framework-owned reaper-panik sentinel. The host-side
    // shutdown-manager writes this exact file (its host path, via
    // `--panik-file` on the reaper) as a graceful last resort when its
    // direct PID-reap cannot confirm the workload dead; this `--panik-file`
    // makes the secondary's in-container watcher poll the SAME file
    // across the `log_tmp` bind mount, so it sees the sentinel appear and
    // runs its own graceful shutdown. Prepended to any operator
    // `--panik-file` paths the dispatcher forwards below — the
    // watcher races all of them (first match wins).
    argv.push("--panik-file".to_string());
    argv.push(reaper_panik_container_path());
    // mem-manager-reserved — generate.rs:748-751 (omitted when None).
    if let Some(b) = cfg.mem_manager_reserved_bytes {
        argv.push(format!("--mem-manager-reserved={b}"));
    }

    argv
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_slurm_wrapper_config::ConnectionMode;
    use std::path::PathBuf;

    fn bins() -> ResolvedBins {
        ResolvedBins {
            podman: "/usr/bin/podman".to_string(),
            rm: "/usr/bin/rm".to_string(),
        }
    }

    fn layout() -> Layout {
        Layout {
            rndtmp: PathBuf::from("/tmp/asm-2f1d4e89"),
            container_name: "asm-2f1d4e89-sec-0".to_string(),
            src_tmp: PathBuf::from("/tmp/asm-2f1d4e89/src"),
            out_tmp: PathBuf::from("/tmp/asm-2f1d4e89/out"),
            log_tmp: PathBuf::from("/tmp/asm-2f1d4e89/log"),
            work_tmp: PathBuf::from("/tmp/asm-2f1d4e89/work"),
            podman_storage: PathBuf::from("/tmp/asm-2f1d4e89/storage"),
            podman_run: PathBuf::from("/tmp/asm-2f1d4e89/run"),
            socket_dir: PathBuf::from("/tmp/asm-2f1d4e89/sockets"),
            cmd_socket: PathBuf::from("/tmp/asm-2f1d4e89/sockets/cmd.sock"),
            shutdown_unit_name: "dynrunner-shutdown-2f1d4e89".to_string(),
            shutdown_log_dir: PathBuf::from("/net/log/sec-0"),
            shutdown_log_path: PathBuf::from("/net/log/sec-0/shutdown-manager.log"),
            wrapper_log_path: PathBuf::from("/net/log/sec-0/wrapper.log"),
            shutdown_pid_file: PathBuf::from("/tmp/asm-2f1d4e89/shutdown-manager.pid"),
            local_image: PathBuf::from("/tmp/asm-2f1d4e89/asm-tokenizer.tar"),
            image_cache_root: PathBuf::from("/tmp/asm-imgcache"),
        }
    }

    /// Maximal config: every optional present, non-empty lists.
    fn maximal_cfg(connection: ConnectionMode) -> WrapperConfig {
        WrapperConfig {
            name_prefix: "asm".to_string(),
            rand_suffix: "2f1d4e89".to_string(),
            secondary_id: "sec-0".to_string(),
            image_path: "/home/u/staged/asm-tokenizer.tar".to_string(),
            image_tar_basename: "asm-tokenizer.tar".to_string(),
            image_digest: "a1b2c3d4e5f6".to_string(),
            image_name: "asm-tokenizer".to_string(),
            image_tag: "latest".to_string(),
            load_command: "podman load -i \"$LOCAL_IMAGE\"".to_string(),
            container_command: "dynamic_runner._secondary_bootstrap".to_string(),
            cores_spec: "-2".to_string(),
            max_memory_spec: "-2G".to_string(),
            mem_manager_reserved_bytes: Some(524_288_000),
            secondary_module: "asm_tokenizer.secondary".to_string(),
            extra_run_args: vec!["--ulimit".to_string(), "nofile=8192:8192".to_string()],
            srcbins_network: "/net/srcbins".to_string(),
            output_network: "/net/out".to_string(),
            log_network: "/net/log".to_string(),
            dynrunner_network_dir: Some("/net/dynrunner".to_string()),
            connection,
            is_observer: false,
            shutdown_manager_bin_path: Some(PathBuf::from("/opt/dynrunner-slurm-shutdown")),
        }
    }

    /// Minimal config: every optional absent, empty lists.
    fn minimal_cfg(connection: ConnectionMode) -> WrapperConfig {
        let mut cfg = maximal_cfg(connection);
        cfg.mem_manager_reserved_bytes = None;
        cfg.extra_run_args.clear();
        cfg.dynrunner_network_dir = None;
        cfg.shutdown_manager_bin_path = None;
        cfg
    }

    fn both_ips() -> PeerIps {
        PeerIps {
            ipv4: Some("10.0.0.5".to_string()),
            ipv6: Some("fe80::1".to_string()),
        }
    }

    /// (a) Maximal case, asserted token-for-token.
    #[test]
    fn maximal_standard() {
        let cfg = maximal_cfg(ConnectionMode::Standard {
            gateway_host: "gw.cluster".to_string(),
            gateway_port: 4433,
        });
        let secondary_url = "tcp://gw.cluster:4433";
        let argv = build_run_argv(
            &cfg,
            &layout(),
            &bins(),
            Some(8_589_934_592),
            &both_ips(),
            7777,
            secondary_url,
            "node01.cluster",
            None,
        );

        let expected: Vec<String> = vec![
            "--root",
            "/tmp/asm-2f1d4e89/storage",
            "--runroot",
            "/tmp/asm-2f1d4e89/run",
            "--cgroup-manager=cgroupfs",
            "--log-level=debug",
            "run",
            "--rm",
            "--name",
            "asm-2f1d4e89-sec-0",
            "--pull=never",
            "--network",
            "host",
            "--hostname",
            "node01.cluster",
            "--pids-limit=0",
            "--ulimit",
            "nproc=32768:32768",
            "--memory=8589934592",
            "--memory-swap=-1",
            "-e",
            "PRIMARY_NODE_IPV4=10.0.0.5",
            "-e",
            "PRIMARY_NODE_IPV6=fe80::1",
            "-e",
            "DYNRUNNER_NETWORK=/app/dynrunner-network",
            "-e",
            "DYNRUNNER_WORK_DIR=/app/work-tmp",
            "-v",
            "/tmp/asm-2f1d4e89/src:/app/src-tmp",
            "-v",
            "/tmp/asm-2f1d4e89/out:/app/out-tmp",
            "-v",
            "/tmp/asm-2f1d4e89/log:/app/log-tmp",
            "-v",
            "/tmp/asm-2f1d4e89/work:/app/work-tmp",
            "-v",
            "/net/srcbins:/app/src-network:ro",
            "-v",
            "/net/out:/app/out-network",
            "-v",
            "/net/log:/app/log-network",
            "-v",
            "/net/dynrunner:/app/dynrunner-network",
            "-v",
            "/tmp/asm-2f1d4e89/sockets:/app/sockets",
            "--ulimit",
            "nofile=8192:8192",
            "asm-tokenizer:latest",
            "dynamic_runner._secondary_bootstrap",
            "--secondary",
            "tcp://gw.cluster:4433",
            "--secondary-id",
            "sec-0",
            "--secondary-module",
            "asm_tokenizer.secondary",
            "--secondary-quic-port",
            "7777",
            "--cores=-2",
            "--max-memory=-2G",
            "--src-network=/app/src-network",
            "--log-dir=/app/log-network",
            "--full-log-dir=/app/log-network/sec-0",
            "--panik-file",
            "/app/log-tmp/.dynrunner-reaper.panik",
            "--mem-manager-reserved=524288000",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        assert_eq!(argv, expected);
        assert!(argv.contains(&"--log-level=debug".to_string()));
        // Explicit pids-limit=0: must be present so podman's silent 2048 builtin
        // default never fires (fork-heavy workloads hit clone() EAGAIN at 2048).
        assert!(
            argv.contains(&"--pids-limit=0".to_string()),
            "--pids-limit=0 must be present in argv"
        );
        // In-container identity: `--hostname <host FQDN>` must be present so
        // the runner doesn't report the container UTS default ("unknown").
        let hn = argv
            .iter()
            .position(|a| a == "--hostname")
            .expect("--hostname present");
        assert_eq!(argv[hn + 1], "node01.cluster");
        // The dispatcher's task-specific argv now travels over the peer
        // mesh (cold-start fetch), NOT on the launch command line: no
        // `--forwarded-arg` flag and none of its values appear.
        assert!(
            !argv.iter().any(|a| a == "--forwarded-arg"),
            "container argv must not carry a --forwarded-arg flag"
        );
        assert!(
            !argv.iter().any(|a| a == "--platform" || a == "x86"),
            "forwarded task-argv tokens must not reach the container argv"
        );
        // The consumer's real module is named via --secondary-module for
        // the bootstrap shim, never spliced as the entrypoint itself.
        let sm = argv
            .iter()
            .position(|a| a == "--secondary-module")
            .expect("--secondary-module present");
        assert_eq!(argv[sm + 1], "asm_tokenizer.secondary");
    }

    /// (a1) With a delegated job cgroup the caller passes `Some(parent)`:
    /// `--cgroup-parent=<parent>` + `--cgroups=enabled` are inserted right
    /// after `run --rm` (before `--name`), and NOTHING else changes vs the
    /// `None` argv. This pins the containment-flag placement and proves
    /// the flags are gated on the parent being present.
    #[test]
    fn cgroup_parent_inserts_containment_flags_after_run_rm() {
        let cfg = maximal_cfg(ConnectionMode::Standard {
            gateway_host: "gw.cluster".to_string(),
            gateway_port: 4433,
        });
        let parent = "/system.slice/slurmstepd.scope/job_153731/step_batch";
        let with_parent = build_run_argv(
            &cfg,
            &layout(),
            &bins(),
            Some(8_589_934_592),
            &both_ips(),
            7777,
            "tcp://gw.cluster:4433",
            "node01.cluster",
            Some(parent),
        );
        let without_parent = build_run_argv(
            &cfg,
            &layout(),
            &bins(),
            Some(8_589_934_592),
            &both_ips(),
            7777,
            "tcp://gw.cluster:4433",
            "node01.cluster",
            None,
        );

        // The two argvs differ ONLY by the two containment tokens inserted
        // immediately after `run --rm`.
        let rm_idx = with_parent
            .iter()
            .position(|a| a == "--rm")
            .expect("--rm present");
        assert_eq!(
            with_parent[rm_idx + 1],
            format!("--cgroup-parent={parent}"),
            "--cgroup-parent must follow --rm"
        );
        assert_eq!(
            with_parent[rm_idx + 2],
            "--cgroups=enabled",
            "--cgroups=enabled must follow --cgroup-parent"
        );
        // Removing the two inserted tokens reproduces the None argv exactly.
        let mut stripped = with_parent.clone();
        stripped.remove(rm_idx + 2);
        stripped.remove(rm_idx + 1);
        assert_eq!(
            stripped, without_parent,
            "Some(parent) must add EXACTLY the two containment tokens and nothing else"
        );
    }

    /// (b) Minimal case: no mem cap, no dynrunner, no mem-manager-reserved,
    /// empty extra/forwarded, both peer IPs None — empty env values, no
    /// `--memory`/dynrunner tokens.
    #[test]
    fn minimal_standard() {
        let cfg = minimal_cfg(ConnectionMode::Standard {
            gateway_host: "gw".to_string(),
            gateway_port: 1,
        });
        let secondary_url = "tcp://gw:1";
        let argv = build_run_argv(
            &cfg,
            &layout(),
            &bins(),
            None,
            &PeerIps::default(),
            5555,
            secondary_url,
            "node02.cluster",
            None,
        );

        let expected: Vec<String> = vec![
            "--root",
            "/tmp/asm-2f1d4e89/storage",
            "--runroot",
            "/tmp/asm-2f1d4e89/run",
            "--cgroup-manager=cgroupfs",
            "--log-level=debug",
            "run",
            "--rm",
            "--name",
            "asm-2f1d4e89-sec-0",
            "--pull=never",
            "--network",
            "host",
            "--hostname",
            "node02.cluster",
            "--pids-limit=0",
            "--ulimit",
            "nproc=32768:32768",
            "-e",
            "PRIMARY_NODE_IPV4=",
            "-e",
            "PRIMARY_NODE_IPV6=",
            "-e",
            "DYNRUNNER_WORK_DIR=/app/work-tmp",
            "-v",
            "/tmp/asm-2f1d4e89/src:/app/src-tmp",
            "-v",
            "/tmp/asm-2f1d4e89/out:/app/out-tmp",
            "-v",
            "/tmp/asm-2f1d4e89/log:/app/log-tmp",
            "-v",
            "/tmp/asm-2f1d4e89/work:/app/work-tmp",
            "-v",
            "/net/srcbins:/app/src-network:ro",
            "-v",
            "/net/out:/app/out-network",
            "-v",
            "/net/log:/app/log-network",
            "-v",
            "/tmp/asm-2f1d4e89/sockets:/app/sockets",
            "asm-tokenizer:latest",
            "dynamic_runner._secondary_bootstrap",
            "--secondary",
            "tcp://gw:1",
            "--secondary-id",
            "sec-0",
            "--secondary-module",
            "asm_tokenizer.secondary",
            "--secondary-quic-port",
            "5555",
            "--cores=-2",
            "--max-memory=-2G",
            "--src-network=/app/src-network",
            "--log-dir=/app/log-network",
            "--full-log-dir=/app/log-network/sec-0",
            "--panik-file",
            "/app/log-tmp/.dynrunner-reaper.panik",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        assert_eq!(argv, expected);
        assert!(argv.contains(&"--log-level=debug".to_string()));
        // Explicit pids-limit=0: must be present so podman's silent 2048 builtin
        // default never fires (fork-heavy workloads hit clone() EAGAIN at 2048).
        assert!(
            argv.contains(&"--pids-limit=0".to_string()),
            "--pids-limit=0 must be present in argv"
        );
        // Empty env values present verbatim.
        assert!(argv.contains(&"PRIMARY_NODE_IPV4=".to_string()));
        assert!(argv.contains(&"PRIMARY_NODE_IPV6=".to_string()));
        // No memory / dynrunner tokens.
        assert!(!argv.iter().any(|a| a.starts_with("--memory")));
        assert!(!argv.iter().any(|a| a.contains("dynrunner-network")));
        assert!(!argv.iter().any(|a| a.starts_with("DYNRUNNER_NETWORK")));
        assert!(!argv.iter().any(|a| a.starts_with("--mem-manager-reserved")));
    }

    /// (c) Reverse-mode secondary_url asserted in place.
    #[test]
    fn reverse_mode_secondary_url() {
        let cfg = maximal_cfg(ConnectionMode::Reverse {
            connection_info_dir: "/net/conn".to_string(),
        });
        let secondary_url = "tcp://localhost:12345";
        let argv = build_run_argv(
            &cfg,
            &layout(),
            &bins(),
            Some(4_294_967_296),
            &both_ips(),
            9001,
            secondary_url,
            "node01.cluster",
            None,
        );

        // Find the `--secondary` flag and assert its value token follows.
        let idx = argv
            .iter()
            .position(|a| a == "--secondary")
            .expect("--secondary present");
        assert_eq!(argv[idx + 1], "tcp://localhost:12345");
        assert!(argv.contains(&"--log-level=debug".to_string()));
    }

    /// Importance-stdio mode is SUBMITTER-LOCAL: it must never reach a
    /// secondary container, neither as a `-e DYNRUNNER_IMPORTANT_STDIO_ONLY`
    /// env nor as an `--important-stdio-only` CLI token. `build_run_argv`
    /// no longer splices ANY dispatcher task-argv onto the launch command
    /// line (it now travels over the peer mesh) and injects a fixed env
    /// set that does not include the importance vars. This pins the
    /// guarantee at the spawn-argv level so a future env addition can't
    /// silently flip a secondary into important-only mode.
    #[test]
    fn secondary_argv_never_carries_importance_stdio() {
        let cfg = maximal_cfg(ConnectionMode::Standard {
            gateway_host: "gw".to_string(),
            gateway_port: 4433,
        });
        let argv = build_run_argv(
            &cfg,
            &layout(),
            &bins(),
            Some(8_589_934_592),
            &both_ips(),
            7777,
            "tcp://gw:4433",
            "node01.cluster",
            None,
        );

        assert!(
            !argv
                .iter()
                .any(|a| a.contains("DYNRUNNER_IMPORTANT_STDIO_ONLY")),
            "container env must not carry DYNRUNNER_IMPORTANT_STDIO_ONLY"
        );
        assert!(
            !argv.iter().any(|a| a.contains("DYNRUNNER_FULL_LOG_FILE")),
            "container env must not carry DYNRUNNER_FULL_LOG_FILE"
        );
        assert!(
            !argv.iter().any(|a| a == "--important-stdio-only"),
            "container command must not carry the --important-stdio-only flag"
        );
    }
}
