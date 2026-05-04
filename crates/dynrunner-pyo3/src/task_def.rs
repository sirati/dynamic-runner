//! Common Python `TaskDefinition` adapter.
//!
//! Every Python-facing manager (`RustLocalManager`, `RustDistributedManager`,
//! `RustSecondaryCoordinator`) needs the same set of fields off of the
//! `task_definition` Python object. This module bundles the extraction so a
//! single source of truth governs the `TaskDefinition` ABI seen from Rust.
//!
//! The runner extracts topology (phases × types) once at run start by
//! calling `task_definition.get_phases()`. Each `PhaseSpec` contributes
//! its `depends_on` list to `phase_deps`, and each contained
//! `TaskTypeSpec` contributes one `TypeRuntime` entry — a per-type bundle
//! of worker module, cmd args, timeout, and reserved memory. The
//! resulting `TypeRegistry` is the single source of truth for "what does
//! the worker for this `TypeId` look like" downstream.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use pyo3::prelude::*;

use dynrunner_core::{PhaseId, TypeId};

use crate::config::log_paths::LogPathConfig;
use crate::estimator::PyMemoryEstimatorBridge;

/// Per-type worker runtime resolved from one `TaskTypeSpec` entry.
///
/// `cmd_args` is the result of calling
/// `task_definition.build_worker_command_args(type_id, ...)` once at run
/// start; the framework appends it to whatever transport-specific argv
/// the subprocess factory builds.
///
/// `type_id`, `timeout`, and `reserved_memory_per_worker` are recorded
/// now but consumed by Phase-5A follow-ups (per-type subprocess
/// dispatch, the per-type watchdog, and the resource pre-reservation
/// scheduler hint). The `#[allow(dead_code)]` markers keep clippy
/// silent until those follow-ups land.
#[derive(Clone, Debug)]
pub(crate) struct TypeRuntime {
    #[allow(dead_code)]
    pub(crate) type_id: TypeId,
    pub(crate) worker_module: String,
    pub(crate) cmd_args: Vec<String>,
    #[allow(dead_code)]
    pub(crate) timeout: Option<Duration>,
    #[allow(dead_code)]
    pub(crate) reserved_memory_per_worker: u64,
}

/// Map `TypeId → TypeRuntime` plus insertion-ordered storage.
///
/// Insertion order matches the order phases × types appear in
/// `get_phases()`. `index_by_id` gives O(1) lookup; the `Vec` is kept so
/// callers that need a deterministic iteration order (logs, the
/// "first-type fallback" the subprocess factory uses today) get one.
#[derive(Clone, Debug, Default)]
pub(crate) struct TypeRegistry {
    pub(crate) types: Vec<TypeRuntime>,
    pub(crate) index_by_id: HashMap<TypeId, usize>,
}

impl TypeRegistry {
    /// Look up a `TypeRuntime` by its `TypeId`. Returns `None` if the
    /// caller asks about a type that wasn't declared in `get_phases()`.
    #[allow(dead_code)]
    pub(crate) fn get(&self, type_id: &TypeId) -> Option<&TypeRuntime> {
        self.index_by_id.get(type_id).map(|i| &self.types[*i])
    }

    /// First registered `TypeRuntime`, or `None` if the registry is
    /// empty. Callers that haven't yet been refactored to per-type
    /// dispatch use this to keep working in the single-type case;
    /// see the TODO in the subprocess factory wiring.
    pub(crate) fn first(&self) -> Option<&TypeRuntime> {
        self.types.first()
    }
}

/// One row from `get_phases()` before per-type cmd_args are built.
///
/// Splitting this out lets `LoadedTopology` extract everything that
/// doesn't require the worker-spawn parameters (paths, task_args,
/// skip_existing). Only the local/distributed managers — which actually
/// spawn worker subprocesses — need the cmd_args; the network primary
/// only dispatches to remote secondaries and stops at the topology.
struct TypeSpecRaw {
    type_id: TypeId,
    worker_module: String,
    timeout: Option<Duration>,
    reserved_memory_per_worker: u64,
}

/// Topology-only extraction from `task_definition.get_phases()`: the
/// per-type estimator bridge plus the phase dependency graph.
///
/// Used by callers that don't spawn worker subprocesses locally (the
/// network primary). The local/distributed managers wrap this into a
/// `LoadedTaskDefinition` that additionally builds per-type
/// `cmd_args` and resolves run paths.
pub(crate) struct LoadedTopology {
    pub(crate) estimator: PyMemoryEstimatorBridge,
    pub(crate) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Per-type concurrency caps from `TaskTypeSpec.max_concurrent`.
    /// Absent type → unconstrained. Propagated into
    /// `PrimaryConfig.max_concurrent_per_type`.
    pub(crate) max_concurrent_per_type: HashMap<TypeId, u32>,
    raw_types: Vec<TypeSpecRaw>,
}

impl LoadedTopology {
    pub(crate) fn from_python(task_definition: &Bound<'_, PyAny>) -> PyResult<Self> {
        let phases_obj = task_definition.call_method0("get_phases")?;
        let phases_iter: Vec<Bound<'_, PyAny>> = phases_obj.extract()?;

        let mut raw_types: Vec<TypeSpecRaw> = Vec::new();
        let mut seen_type_ids: HashSet<TypeId> = HashSet::new();
        let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
        let mut estimator_specs: Vec<(TypeId, String)> = Vec::new();
        let mut max_concurrent_per_type: HashMap<TypeId, u32> = HashMap::new();

        for phase_spec in &phases_iter {
            let phase_id_str: String = phase_spec.getattr("phase_id")?.extract()?;
            let phase_id = PhaseId::from(phase_id_str);
            let depends_on: Vec<String> = phase_spec.getattr("depends_on")?.extract()?;
            phase_deps.insert(
                phase_id.clone(),
                depends_on.into_iter().map(PhaseId::from).collect(),
            );

            let types_tuple: Vec<Bound<'_, PyAny>> =
                phase_spec.getattr("types")?.extract()?;
            for tts in &types_tuple {
                let type_id_str: String = tts.getattr("type_id")?.extract()?;
                let type_id = TypeId::from(type_id_str);
                let worker_module: String = tts.getattr("worker_module")?.extract()?;
                let estimator_attr: String = tts.getattr("estimator_attr")?.extract()?;
                let timeout_obj = tts.getattr("timeout_seconds")?;
                let timeout = if timeout_obj.is_none() {
                    None
                } else {
                    let secs: f64 = timeout_obj.extract()?;
                    Some(Duration::from_secs_f64(secs))
                };
                let reserved_memory_per_worker: u64 =
                    tts.getattr("reserved_memory_per_worker")?.extract()?;

                // Optional per-type concurrency cap. `None` (or
                // missing attr for old task definitions) → no cap on
                // this type.
                if let Ok(mc) = tts.getattr("max_concurrent") {
                    if !mc.is_none() {
                        let cap: u32 = mc.extract()?;
                        max_concurrent_per_type.insert(type_id.clone(), cap);
                    }
                }

                if !seen_type_ids.insert(type_id.clone()) {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "duplicate TypeId in get_phases(): {}",
                        type_id
                    )));
                }
                estimator_specs.push((type_id.clone(), estimator_attr));
                raw_types.push(TypeSpecRaw {
                    type_id,
                    worker_module,
                    timeout,
                    reserved_memory_per_worker,
                });
            }
        }

        let estimator =
            PyMemoryEstimatorBridge::from_python(task_definition, &estimator_specs)?;

        Ok(Self {
            estimator,
            phase_deps,
            max_concurrent_per_type,
            raw_types,
        })
    }
}

/// Resolved fields pulled out of a Python `task_definition` instance, plus the
/// per-run paths the runner derives from it.
pub(crate) struct LoadedTaskDefinition {
    pub(crate) estimator: PyMemoryEstimatorBridge,
    pub(crate) types: TypeRegistry,
    pub(crate) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    pub(crate) source_path: PathBuf,
    pub(crate) output_path: PathBuf,
    pub(crate) log_dir: PathBuf,
    pub(crate) log_paths: LogPathConfig,
    pub(crate) python_executable: PathBuf,
}

impl LoadedTaskDefinition {
    pub(crate) fn from_python(
        py: Python<'_>,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        source_dir: &str,
        output_dir: &str,
        skip_existing: bool,
        log_paths: Option<LogPathConfig>,
    ) -> PyResult<Self> {
        let topology = LoadedTopology::from_python(task_definition)?;

        let source_path = PathBuf::from(source_dir);
        let output_path = PathBuf::from(output_dir);
        let source_str = source_path.to_str().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "source_dir is not valid UTF-8: {:?}",
                source_path
            ))
        })?;
        let output_str = output_path.to_str().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "output_dir is not valid UTF-8: {:?}",
                output_path
            ))
        })?;

        // Build per-type cmd_args by calling
        // `task_definition.build_worker_command_args(type_id, ...)` once
        // per declared type. This is the only step that requires the
        // run-time paths / task_args, hence the split out of
        // `LoadedTopology`.
        let mut types: Vec<TypeRuntime> = Vec::with_capacity(topology.raw_types.len());
        let mut index_by_id: HashMap<TypeId, usize> =
            HashMap::with_capacity(topology.raw_types.len());
        for raw in topology.raw_types {
            let cmd_args: Vec<String> = task_definition
                .call_method1(
                    "build_worker_command_args",
                    (
                        raw.type_id.as_str(),
                        task_args,
                        source_str,
                        output_str,
                        skip_existing,
                    ),
                )?
                .extract()?;
            index_by_id.insert(raw.type_id.clone(), types.len());
            types.push(TypeRuntime {
                type_id: raw.type_id,
                worker_module: raw.worker_module,
                cmd_args,
                timeout: raw.timeout,
                reserved_memory_per_worker: raw.reserved_memory_per_worker,
            });
        }

        let log_paths = log_paths.unwrap_or_default();
        let log_dir = log_paths.resolve_log_dir(py, &output_path)?;
        std::fs::create_dir_all(&log_dir).ok();

        let sys = py.import("sys")?;
        let python_executable: String = sys.getattr("executable")?.extract()?;

        Ok(Self {
            estimator: topology.estimator,
            types: TypeRegistry { types, index_by_id },
            phase_deps: topology.phase_deps,
            source_path,
            output_path,
            log_dir,
            log_paths,
            python_executable: PathBuf::from(python_executable),
        })
    }
}

// NOTE(phase-5a-followup): a focused unit test for
// `LoadedTaskDefinition::from_python` (two phases × three types,
// asserting the registry + phase_deps shape) is desirable but blocked
// on a workspace-level PyO3 setup issue: the `extension-module` feature
// is enabled unconditionally on the workspace `pyo3` dep, which makes
// `cargo test -p dynrunner-pyo3` fail to link against CPython symbols.
// The fix lives in `Cargo.toml` (split the feature so test profile
// links against `pyo3-build-config`'s `python3` lib) and is filed as
// follow-up to keep this Phase 5A change scoped to topology
// extraction. The Python pytest suite under `python/dynamic_runner/`
// already exercises this path end-to-end via the `RustLocalManager`
// boot sequence.
