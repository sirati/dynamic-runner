//! Public configuration surface for the wrapper-script generator:
//! [`WrapperScriptConfig`] (the data the secondary-mode generator
//! reads), [`ConnectionMode`] (Direct vs Reverse vs Local-tcp), and
//! the [`WRAPPER_SRC_NETWORK_CONTAINER_PATH`] mount-point constant.
//! See the module-level docs in [`super`] for the generator's
//! design rationale.

use std::path::Path;

use crate::config::SlurmConfig;

/// Configuration for generating a SLURM wrapper script.
pub struct WrapperScriptConfig<'a> {
    pub slurm_config: &'a SlurmConfig,
    /// Consumer-supplied short identifier for their program/deployment
    /// (e.g. `"asm"`). Prefixes BOTH the scratch dir
    /// `/tmp/<name_prefix>-<suffix>` and the container name
    /// `<name_prefix>-<suffix>-<secondary_id>` (derived inside the
    /// wrapper binary), replacing the legacy hardcoded `asm` literal —
    /// dynrunner is a framework and must not bake in any one consumer's
    /// program name. The renderer threads this verbatim into the
    /// binary's [`WrapperConfig::name_prefix`]. There is NO default: the
    /// caller (Python dispatch) must source it from the consumer's
    /// deployment spec.
    pub name_prefix: &'a str,
    /// Compute-node path of the `dynrunner-slurm-wrapper` musl-static
    /// binary. The renderer emits a TINY stub wrapper script that
    /// `exec`s this binary with the [`WrapperConfig::to_args`] vector
    /// (each element bash-quoted); the Rust wrapper binary then performs
    /// the full secondary lifecycle. This is the ONLY render path: the
    /// legacy inline bash heredoc (old `setsid` fallback, no
    /// `--cgroup-parent`/adopt, no in-band reap) was deleted at root —
    /// every secondary now runs the binary. The `#SBATCH`/entrypoint
    /// mechanics are owned by `submit_job`, unchanged.
    pub wrapper_bin_path: &'a Path,
    /// Absolute (already tilde-expanded) path to the docker-archive
    /// tar on the gateway.
    pub image_path: &'a str,
    /// Identifier of the secondary that will run inside the container.
    pub secondary_id: &'a str,
    /// Container image name (e.g. `asm-tokenizer`).
    pub image_name: &'a str,
    /// Container image tag (e.g. `latest`).
    pub image_tag: &'a str,
    /// Basename of the docker-archive tar on the compute node's local
    /// /tmp copy. Mirrors `TaskDeploymentSpec.image_tar_basename`
    /// (typically `<image_name>.tar`).
    pub image_tar_basename: &'a str,
    /// SHA-256 (hex) of the image tarball
    /// (`PodmanImageMetadata.image_hash`). Threaded into the wrapper
    /// binary's [`WrapperConfig::image_digest`] as the content key for
    /// the node-local image cache (the binary's `image.rs` reuses a
    /// digest-keyed node-local copy instead of re-reading the shared-FS
    /// tarball per secondary). Empty string disables the cache.
    pub image_digest: &'a str,
    /// Bash snippet that loads the image into podman storage. The
    /// caller pre-substitutes `$LOCAL_IMAGE`, `$PODMAN_STORAGE`,
    /// `$PODMAN_RUN`; the generator emits this verbatim inside the
    /// `if ! { ... }` failure-marker block.
    pub load_command: &'a str,
    /// In-container entrypoint and its args after `--secondary` URL,
    /// `--secondary-id`, `--secondary-module`, `--secondary-quic-port`,
    /// and `--cores` are appended. On the SLURM dispatch path this is
    /// the framework bootstrap shim (`dynamic_runner._secondary_bootstrap`):
    /// the image entrypoint (`python -m`) runs it, it fetches the run
    /// config over the peer mesh, then `runpy`s the consumer's real
    /// [`secondary_module`](Self::secondary_module).
    pub container_command: &'a str,
    /// Consumer's real secondary entrypoint module (the consumer's
    /// `TaskDeploymentSpec.secondary_module`). Threaded into the wrapper
    /// binary's [`WrapperConfig::secondary_module`] and emitted onto the
    /// container argv as `--secondary-module <module>` for the bootstrap
    /// shim to `runpy` after its mesh fetch. Replaces the old
    /// `forwarded_argv` launch-line splice — the dispatcher's
    /// task-specific argv now travels over the peer mesh, not the
    /// container command line.
    pub secondary_module: &'a str,
    /// CLI `--cores` spec (verbatim string: `"0"`, `"N"`, `"+N"`,
    /// `"-N"`) forwarded to the secondary subprocess inside the
    /// container. Each secondary parses this locally against its
    /// own container's detected CPU count via `parse_cores`,
    /// preserving the per-machine semantic. The framework's
    /// `PySecondaryConfig.__new__` auto-detect (which reads the
    /// host's `available_parallelism` from inside a cgroup-CPU-
    /// quota'd container and returns the host CPU count, not the
    /// SLURM cgroup's quota) is then suppressed because the
    /// secondary's argparse parses `--cores` and explicitly
    /// populates `num_workers`. Symmetric with the
    /// `--multi-computer local` fix in `spawn_secondary.py`
    /// (commit 38a0c30 / task #26).
    pub cores_spec: &'a str,
    /// CLI `--max-memory` spec (verbatim string: `"16G"`, `"4G"`,
    /// `"-2G"`, `"+1G"`, …) forwarded to the secondary subprocess
    /// inside the container. Each secondary parses this locally via
    /// `parse_memory` against its OWN host's `/proc/meminfo:MemTotal`
    /// (or cgroup-v2 memory.max if a cap applies), preserving the
    /// per-machine semantic in heterogeneous SLURM clusters. Without
    /// forwarding, the secondary's argparse default (`"-2G"` =
    /// host_memory - 2 GiB) and PySecondaryConfig's auto-detect
    /// (`detect_total_memory_bytes`) read the host's full RAM from
    /// inside the cgroup-memory-quota'd container (asm-dataset-nix
    /// observed `budget_mb=92030` for a worker in a 4 GiB-capped
    /// container at 3c5f105) — workers then think they have 90+ GiB
    /// each and over-allocate. Symmetric with `cores_spec`. The
    /// `--multi-computer local` path INTENTIONALLY does NOT forward
    /// memory (single-host shared RAM = double-counting); this
    /// SLURM-only forward is correct because SLURM secondaries are
    /// each on a different host with their own RAM budget.
    pub max_memory_spec: &'a str,
    /// Connection-mode-specific config (gateway/standard vs reverse).
    pub connection: ConnectionMode<'a>,
    /// Optional override for the run-log directory used as the
    /// `/app/log-network` mount source. Falls back to
    /// `slurm_config.log_path()` when None.
    pub run_log_dir: Option<&'a str>,
    /// Optional bind-mount source for the framework's filesystem
    /// control-plane (mounted at `/app/dynrunner-network` and exposed
    /// via `DYNRUNNER_NETWORK` env in the container). When None the
    /// volume and env are omitted entirely. Mirrors
    /// `TaskDeploymentSpec.dynrunner_network_dir`.
    pub dynrunner_network_dir: Option<&'a str>,
    /// Bind-mount source for the cluster-wide src-bins network mount
    /// (typically `slurm_config.get_srcbins_mount_source()` from the
    /// Python side; pre-tilde-expanded). When None the generator
    /// defaults to `slurm_config.src_bins_path()` for back-compat.
    pub srcbins_mount_source: Option<&'a str>,
    /// Bind-mount source for the cluster-wide output mount. When
    /// None defaults to `slurm_config.output_path()`.
    pub output_dir: Option<&'a str>,
    /// Consumer-supplied additional flags to interpolate into the
    /// `podman run` invocation BEFORE the `{image_name}:{image_tag}`
    /// argument and AFTER the framework's own flags. Each entry is
    /// bash-quoted by the generator (callers MUST NOT pre-quote).
    /// Mirrors `TaskDeploymentSpec.extra_run_args`.
    pub extra_run_args: &'a [String],
    /// Whether the secondary launched by this wrapper script is an
    /// observer (Task #36 / Step 7 of the transport-unification
    /// refactor): no workers, non-promotable. The flag is written
    /// into the v2 peer-info file (`is_observer=true|false`) so a
    /// late-joining peer reading the connection-info directory can
    /// see that this peer cannot host the primary role. Defaults to
    /// `false` at the callers; observer-mode is opted into explicitly.
    pub is_observer: bool,
    /// Absolute path to the `dynrunner-slurm-shutdown` binary on the
    /// compute-node filesystem. When `Some`, the rendered wrapper
    /// spawns the shutdown manager via `systemd-run --user --unit`
    /// (service mode) right after scratch-dir creation; the wrapper's
    /// signal trap forwards SIGTERM/SIGCONT/etc. to the unit via
    /// `systemctl --user kill`. When `None`, the wrapper emits no
    /// shutdown-manager spawn block and the cleanup trap is a
    /// minimal CMD_RELAY-only teardown (legacy behavior, NO /tmp
    /// cleanup on SLURM-induced termination — caller's responsibility
    /// to ensure they wanted that).
    ///
    /// Replaces the pre-2026-05 inline `setsid -f bash -c '...'`
    /// watchdog, which signalled pid 1 of the container (= bash, no
    /// signal forwarding) and lived inside the slurmd cgroup (so it
    /// died alongside the wrapper on cgroup teardown — defeating the
    /// "survives wrapper exit" purpose). The out-of-cgroup
    /// shutdown-manager process started via `systemd-run --user`
    /// inherits the user's `user@<uid>.service` cgroup instead, so
    /// SLURM cgroup-v2 teardown of the job's pidtree does not reap
    /// it. Cluster prerequisite: `loginctl enable-linger` for the
    /// SLURM user (LMU Krater has this set).
    pub shutdown_manager_bin_path: Option<&'a Path>,
    /// Bytes to reserve for the secondary process itself when the
    /// nested workers cgroup is enabled. `Some(n)` renders an
    /// extra `--mem-manager-reserved=<n>` flag onto the
    /// secondary's container-command argv; the secondary's argparse
    /// stores it on `args.mem_manager_reserved`, the dispatcher
    /// hands it to `SecondaryConfig(mem_manager_reserved_bytes=...)`,
    /// and the `WorkerPool::initialize` cgroup-setup picks it up.
    /// `None` omits the flag and the secondary falls through to its
    /// argparse default (`"500M"` parsed via `parse_memory`). See
    /// `dynrunner_manager_distributed::SecondaryConfig::mem_manager_reserved_bytes`
    /// for the contract.
    pub mem_manager_reserved_bytes: Option<u64>,
}

/// How the secondary connects to the primary.
pub enum ConnectionMode<'a> {
    /// Secondary connects to primary via gateway host:port.
    Standard {
        gateway_host: &'a str,
        gateway_port: u16,
    },
    /// Primary tunnels to secondary via ProxyJump; secondary writes
    /// connection info into `connection_info_dir` for the primary
    /// to pick up.
    Reverse { connection_info_dir: &'a str },
}

/// In-container bind-mount path for the cluster-wide source-binaries
/// network drive. The wrapper renders TWO references to this path:
///
///   1. The `-v "{srcbins_network}:{path}:ro"` bind-mount line, so the
///      gateway-side staged corpus is visible to the secondary process.
///   2. The `--src-network {path}` flag on the secondary's container
///      command, so the secondary's argparse stores it as
///      `args.src_network` and `_dispatch_secondary` forwards it
///      verbatim into `SecondaryConfig(src_network=...)` without
///      relying on `PySecondaryConfig.__new__`'s path-exists auto-
///      detect.
///
/// Centralising the literal here keeps the two references in lockstep:
/// any future change to the bind-mount path (`/app/src-network` →
/// `/srv/staged-bins`, etc.) updates both sites atomically.
///
/// Cross-crate note: `crates/dynrunner-pyo3/src/config/primary_secondary.rs`
/// has an independent constant `WRAPPER_SRC_NETWORK` with the same
/// literal value. That copy serves the auto-detect path
/// (`Path::exists(...)` returns `true` inside the wrapper container);
/// keeping them as two separate constants avoids a circular crate
/// dependency (dynrunner-pyo3 already depends on dynrunner-slurm, so
/// the auto-detect could import this constant, but the inverse is
/// fine and the literal duplication is a single line). The explicit
/// `--src-network` flag added by this generator makes the auto-detect
/// redundant for SLURM secondaries; the auto-detect now serves only
/// as a defence-in-depth fallback for callers that omit the flag
/// (e.g. operators invoking `python -m <module> --secondary <url>`
/// manually outside the SLURM wrapper).
pub const WRAPPER_SRC_NETWORK_CONTAINER_PATH: &str = "/app/src-network";
