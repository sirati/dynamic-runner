use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_manager_distributed::{
    PrimaryConfig as RustPrimaryConfig, SecondaryConfig as RustSecondaryConfig,
};

use super::distributed::DistributedConfig;
use super::resources::PyResourceMap;

/// Per-primary tuning. Combine with `DistributedConfig` for the shared
/// connect/peer/keepalive knobs.
#[pyclass(name = "PrimaryConfig", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyPrimaryConfig {
    #[pyo3(get, set)]
    pub(crate) node_id: String,
    #[pyo3(get, set)]
    pub(crate) num_secondaries: u32,
    #[pyo3(get, set)]
    pub(crate) distributed_config: DistributedConfig,
}

#[pymethods]
impl PyPrimaryConfig {
    #[new]
    #[pyo3(signature = (
        num_secondaries,
        node_id = "primary".to_string(),
        distributed_config = None,
    ))]
    fn new(
        num_secondaries: u32,
        node_id: String,
        distributed_config: Option<DistributedConfig>,
    ) -> Self {
        Self {
            node_id,
            num_secondaries,
            distributed_config: distributed_config.unwrap_or_default(),
        }
    }
}

impl PyPrimaryConfig {
    /// Build the Rust-side config. Currently unused — the
    /// `PyPrimaryCoordinator` constructor pulls each field
    /// individually rather than through this wrapper. Kept as a
    /// documented API for callers that prefer a one-step conversion.
    #[allow(dead_code)]
    pub(crate) fn to_rust(&self) -> RustPrimaryConfig {
        RustPrimaryConfig {
            node_id: self.node_id.clone(),
            num_secondaries: self.num_secondaries,
            connect_timeout: self.distributed_config.connect_timeout(),
            peer_timeout: self.distributed_config.peer_timeout(),
            keepalive_interval: self.distributed_config.keepalive_interval(),
            keepalive_miss_threshold: self.distributed_config.keepalive_miss_threshold(),
            // Pre-staged mode is plumbed through PyPrimaryCoordinator's
            // own constructor (the SLURM-pipeline path); this config
            // shim defaults to off.
            source_pre_staged_root: None,
            // File-based items is the historical default; consumers
            // that opt out do so via PyPrimaryCoordinator (which
            // reads `TaskDefinition.uses_file_based_items`).
            uses_file_based_items: true,
            required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: self.distributed_config.retry_max_passes(),
            oom_retry_max_passes: self.distributed_config.oom_retry_max_passes(),
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(60),
            mass_death_grace: self.distributed_config.mass_death_grace(),
            mass_death_min_count: self.distributed_config.mass_death_min_count(),
            // The PyO3 shim doesn't surface the staging
            // walk's source root (PyPrimaryCoordinator's
            // own constructor takes that kwarg directly
            // for the SLURM and network-primary paths).
            source_dir: None,
            // PyPrimaryCoordinator surfaces this on its own
            // `__init__` kwarg (and via the
            // `set_unfulfillable_reinject_max_per_task`
            // setter). The plain `PyPrimaryConfig` shim,
            // which only the in-process distributed
            // pipeline routes through, defaults to
            // unbounded; consumers that need a cap go via
            // `PyPrimaryCoordinator`.
            unfulfillable_reinject_max_per_task: None,
            setup_promote_deadline: self.distributed_config.setup_promote_deadline(),
        }
    }
}

/// Per-secondary tuning.
#[pyclass(name = "SecondaryConfig", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PySecondaryConfig {
    #[pyo3(get, set)]
    pub(crate) secondary_id: String,
    #[pyo3(get, set)]
    pub(crate) num_workers: u32,
    #[pyo3(get, set)]
    pub(crate) max_resources: PyResourceMap,
    #[pyo3(get, set)]
    pub(crate) hostname: String,
    /// Shared-drive directory the primary stages binaries into, as
    /// seen from the secondary's filesystem view. `None` outside the
    /// SLURM wrapper container — there's no shared drive in
    /// in-process distributed mode and the secondary just resolves
    /// task paths against the primary's filesystem view directly.
    #[pyo3(get, set)]
    pub(crate) src_network: Option<PathBuf>,
    /// Per-secondary local scratch dir where StageFile copies land.
    /// Always populated after `__new__` (via auto-resolution to
    /// `/app/src-tmp` inside the wrapper container, or a unique
    /// `<TMPDIR>/secondary-<id>-<pid>-src` outside it).
    #[pyo3(get, set)]
    pub(crate) src_tmp: PathBuf,
    /// Per-secondary directory where workers write outputs. Always
    /// populated after `__new__` (auto-resolves to
    /// `/app/out-network` inside the wrapper container — durable
    /// bind to gateway's `<slurm_root>/out` — or a unique
    /// `<TMPDIR>/secondary-<id>-<pid>-out` outside it).
    #[pyo3(get, set)]
    pub(crate) output_dir: PathBuf,
    #[pyo3(get, set)]
    pub(crate) distributed_config: DistributedConfig,
    /// Bytes withheld from the workers cgroup so the secondary
    /// process itself has a protected slice of the container's
    /// `memory.max`. `None` skips the nested workers cgroup
    /// entirely (legacy flat layout); `Some(n)` materialises the
    /// subgroup and tightens its cap by `n`. Surfaced to Python so
    /// the CLI `--mem-manager-reserved` flag plumbs end-to-end.
    /// See `dynrunner_manager_distributed::SecondaryConfig::mem_manager_reserved_bytes`
    /// for the full contract.
    #[pyo3(get, set)]
    pub(crate) mem_manager_reserved_bytes: Option<u64>,
    /// Python-side `--memprofile` opt-in. The Rust-side coordinator
    /// (`PySecondaryCoordinator`) resolves the actual output
    /// directory at run start by combining this flag with
    /// `RustSecondaryCoordinator.output_dir` (operator-supplied,
    /// always set) and the SLURM wrapper's `/app/out-network`
    /// bind-mount probe (legacy backstop). The flag's behaviour
    /// lives entirely in Rust; Python only flips this bool.
    #[pyo3(get, set)]
    pub(crate) memprofile_enabled: bool,
}

#[pymethods]
impl PySecondaryConfig {
    /// Construct a `SecondaryConfig`. Every optional field
    /// auto-resolves to a sensible default when omitted, so the
    /// only required argument is `secondary_id`. Auto-resolution
    /// rules:
    ///
    /// - `num_workers`: number of logical CPUs visible to the
    ///   current process via `std::thread::available_parallelism()`.
    ///   Respects cgroup CPU limits, which is what we want under
    ///   SLURM `--cpus-per-task` — over-spawning vs the host's
    ///   physical core count is what the previous psutil-based
    ///   path was silently doing.
    ///
    /// - `max_resources`: `{"memory": <MemTotal from /proc/meminfo>}`.
    ///   Linux-only probe; returns 0 elsewhere, which makes the
    ///   scheduler treat the node as having no memory budget and
    ///   surfaces the misdetection as immediate scheduling failures.
    ///
    /// - `src_network`: `/app/src-network` if that path exists
    ///   (the SLURM wrapper bind-mounts the gateway's
    ///   shared-binaries drive there), else `None`. The
    ///   in-process distributed manager doesn't have a shared
    ///   drive — the secondary just resolves task paths against
    ///   the primary's filesystem view directly.
    ///
    /// - `src_tmp`: `/app/src-tmp` inside the wrapper, else a
    ///   unique `<TMPDIR>/secondary-<secondary_id>-<pid>-src`.
    ///
    /// - `output_dir`: `/app/out-network` inside the wrapper
    ///   (durable bind to gateway's `<slurm_root>/out` — final
    ///   outputs survive the wrapper's per-job
    ///   `/tmp/asm-XXXX` trap-cleanup), else a unique
    ///   `<TMPDIR>/secondary-<secondary_id>-<pid>-out`.
    ///
    /// All directory paths created by the resolver are mkdir'd
    /// (`create_dir_all`) so the worker doesn't have to.
    ///
    /// The whole resolution lives in Rust because every Python
    /// step it would otherwise live in is "compute a value, hand
    /// it straight back to Rust" — no Python-exclusive content.
    #[new]
    #[pyo3(signature = (
        secondary_id,
        num_workers = None,
        max_resources = None,
        hostname = "localhost".to_string(),
        src_network = None,
        src_tmp = None,
        output_dir = None,
        distributed_config = None,
        mem_manager_reserved_bytes = None,
        memprofile_enabled = false,
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        secondary_id: String,
        num_workers: Option<u32>,
        max_resources: Option<PyResourceMap>,
        hostname: String,
        src_network: Option<PathBuf>,
        src_tmp: Option<PathBuf>,
        output_dir: Option<PathBuf>,
        distributed_config: Option<DistributedConfig>,
        mem_manager_reserved_bytes: Option<u64>,
        memprofile_enabled: bool,
    ) -> PyResult<Self> {
        let num_workers = num_workers.unwrap_or_else(detect_num_workers);
        let max_resources = max_resources
            .unwrap_or_else(|| PyResourceMap::from_single("memory", detect_total_memory_bytes()));

        let in_wrapper_container = std::path::Path::new(WRAPPER_SRC_NETWORK).exists();
        let src_network = src_network.or_else(|| {
            if in_wrapper_container {
                Some(PathBuf::from(WRAPPER_SRC_NETWORK))
            } else {
                None
            }
        });
        let src_tmp = src_tmp.unwrap_or_else(|| {
            default_secondary_dir(&secondary_id, in_wrapper_container, WRAPPER_SRC_TMP, "src")
        });
        let output_dir = output_dir.unwrap_or_else(|| {
            default_secondary_dir(
                &secondary_id,
                in_wrapper_container,
                WRAPPER_OUT_NETWORK,
                "out",
            )
        });

        std::fs::create_dir_all(&src_tmp).map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "SecondaryConfig: failed to create src_tmp {}: {e}",
                src_tmp.display()
            ))
        })?;
        std::fs::create_dir_all(&output_dir).map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "SecondaryConfig: failed to create output_dir {}: {e}",
                output_dir.display()
            ))
        })?;

        Ok(Self {
            secondary_id,
            num_workers,
            max_resources,
            hostname,
            src_network,
            src_tmp,
            output_dir,
            distributed_config: distributed_config.unwrap_or_default(),
            mem_manager_reserved_bytes,
            memprofile_enabled,
        })
    }
}

/// Bind-mount path the SLURM wrapper attaches the gateway's
/// shared-binaries directory to inside the container. Its presence
/// is the runtime signal "I'm running under our wrapper, the
/// `/app/...` layout is set up".
const WRAPPER_SRC_NETWORK: &str = "/app/src-network";
/// Bind-mount path the SLURM wrapper attaches the per-job
/// scratch directory to inside the container.
const WRAPPER_SRC_TMP: &str = "/app/src-tmp";
/// Bind-mount path the SLURM wrapper attaches the gateway's
/// durable output directory to inside the container.
const WRAPPER_OUT_NETWORK: &str = "/app/out-network";

/// Resolve a per-secondary scratch directory: the wrapper's bind
/// mount when running under it, else a unique tempdir keyed by
/// (secondary_id, pid) so concurrent local-mode secondaries on the
/// same machine don't collide.
fn default_secondary_dir(
    secondary_id: &str,
    in_wrapper_container: bool,
    wrapper_path: &str,
    suffix: &str,
) -> PathBuf {
    if in_wrapper_container {
        PathBuf::from(wrapper_path)
    } else {
        std::env::temp_dir().join(format!(
            "secondary-{secondary_id}-{}-{suffix}",
            std::process::id()
        ))
    }
}

// Resource detection helpers live in `crate::system_resources` —
// shared with the PyO3-exposed `parse_cores` / `parse_memory` /
// `pick_free_port` so the framework has one source of truth for
// "what does this machine look like".
use crate::system_resources::{
    detect_logical_cpu_count as detect_num_workers, detect_total_memory_bytes,
};

impl PySecondaryConfig {
    /// Build the Rust-side config. Currently unused — the
    /// `PySecondaryCoordinator` constructor pulls each field
    /// individually rather than through this wrapper. Kept as a
    /// documented API for callers that prefer a one-step conversion.
    #[allow(dead_code)]
    pub(crate) fn to_rust(&self) -> RustSecondaryConfig {
        RustSecondaryConfig {
            secondary_id: self.secondary_id.clone(),
            num_workers: self.num_workers,
            max_resources: self.max_resources.to_rust(),
            hostname: self.hostname.clone(),
            keepalive_interval: self.distributed_config.keepalive_interval(),
            src_network: self.src_network.clone(),
            // RustSecondaryConfig.src_tmp is still `Option<PathBuf>`
            // for back-compat with the in-process distributed
            // manager that constructs it directly without going
            // through the PyO3 wrapper. The PyO3 layer always has a
            // resolved path post-`__new__`, so we always send Some.
            src_tmp: Some(self.src_tmp.clone()),
            peer_timeout: self.distributed_config.peer_timeout(),
            keepalive_miss_threshold: self.distributed_config.keepalive_miss_threshold(),
            retry_max_passes: self.distributed_config.retry_max_passes(),
            oom_retry_max_passes: self.distributed_config.oom_retry_max_passes(),
            primary_link_failure_threshold: self
                .distributed_config
                .primary_link_failure_threshold(),
            primary_link_failure_window: self.distributed_config.primary_link_failure_window(),
            unconfigured_deadline: self.distributed_config.unconfigured_deadline(),
            is_observer: false,
            // Conservative default for this (unused) one-step builder: the
            // live secondary-construction site sets `can_be_primary` from
            // its actual mesh presence (`mesh_send_handle.is_some()`); this
            // wrapper has no transport, so the safe value is `false`.
            can_be_primary: false,
            resource_check_interval: self.distributed_config.resource_check_interval(),
            log_oom_watcher: self.distributed_config.log_oom_watcher(),
            // Hardcoded to the SecondaryConfig::default() value (2 s).
            // The grace gate is an internal heuristic on the
            // promoted-primary natural-quiesce branch (see
            // `SecondaryConfig::promoted_primary_quiesce_grace` for
            // the rationale). Not exposed through PyDistributedConfig
            // yet — operators who need to tune it would do so via the
            // Rust crate directly; surfacing through the Python CLI
            // is queued behind operator demand.
            promoted_primary_quiesce_grace: std::time::Duration::from_secs(2),
            // Plumbed through the PyO3 constructor as a kwarg on
            // `PySecondaryCoordinator` (defaulting to None / unbounded).
            // `PySecondaryConfig` does not currently surface this knob
            // — the live secondary-construction sites set it directly.
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: self.mem_manager_reserved_bytes,
            // `PySecondaryConfig::to_rust` is the documented-but-
            // unused conversion path; the live secondary-
            // construction site
            // (`PySecondaryCoordinator::run` in
            // `managers/secondary/run.rs`) resolves the memprofile
            // directory at run start from `memprofile_enabled` plus
            // the wrapper-bind-mount check, and feeds it into
            // `SecondaryConfig.output_dir` directly. This wrapper
            // path stays opt-out (None) so PyO3 callers that go
            // through `to_rust` don't accidentally get a
            // half-resolved sampler path.
            output_dir: None,
            // Same rationale as `output_dir`: the live
            // construction site derives the memuse log path from
            // `self.output_dir`; this opt-out wrapper stays
            // silent so callers that go through it don't pick
            // up an unintended log target.
            memuse_log_path: None,
        }
    }
}
