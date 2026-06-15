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

/// A `TypeRegistry` shared between the worker factory (which reads the
/// per-type `cmd_args` at every spawn) and the run-config finalize closure
/// (which swaps in a freshly-rebuilt registry once the post-welcome
/// `RunConfig` push has delivered the consumer's forwarded argv).
///
/// `Arc<Mutex<_>>` so the swap is observed by every subsequent spawn —
/// initial pool init AND per-type respawn — through the one cell. The lock
/// is taken per spawn only to clone the matching `TypeRuntime` out; at the
/// once-per-(re)spawn cadence (dominated by the cost of forking Python) the
/// lock is free. The non-secondary dispatch paths (local / distributed
/// managers) parse args eagerly at construction and never swap, so they
/// simply seed the cell once and the lock is uncontended.
pub(crate) type SharedTypeRegistry = std::sync::Arc<std::sync::Mutex<TypeRegistry>>;

/// Wrap a `TypeRegistry` into the [`SharedTypeRegistry`] cell.
///
/// Single concern: own the `Arc<Mutex<_>>` construction so the every
/// factory-seeding call site (local / distributed / secondary managers) does
/// not re-spell the wrapper internals — the cell's representation stays a
/// private detail of this alias.
pub(crate) fn shared_registry(reg: TypeRegistry) -> SharedTypeRegistry {
    std::sync::Arc::new(std::sync::Mutex::new(reg))
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
    /// Phases the consumer declared `PhaseSpec.may_be_empty` — the
    /// empty-drain proceed-or-fail opt-out. Registered on the primary
    /// (`register_phase_may_be_empty`) and replicated via
    /// `ClusterMutation::PhaseMayBeEmptySet`. Empty on the common run.
    pub(crate) phase_may_be_empty: Vec<PhaseId>,
    /// Phases the consumer declared `PhaseSpec.barrier=False` — the
    /// pipelined-edge opt-in that authorises the scheduler to dispatch
    /// tasks from these phases without first waiting for whole-of-
    /// upstream drain. Registered on the primary
    /// (`register_phase_no_barrier`) and replicated via
    /// `ClusterMutation::PhaseNoBarrierSet`; consumed by the pool's
    /// `set_no_barrier_phases` initialiser (a no-barrier phase starts
    /// `Active` instead of `Blocked`) and by the runtime-spawn barrier
    /// interlock in `apply_spawn_tasks` (a `Blocked` barrier=True phase
    /// rejects a runtime spawn). Empty on the common strict-barrier run
    /// (every phase barrier=True, the default).
    pub(crate) phase_no_barrier: Vec<PhaseId>,
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
        let mut phase_may_be_empty: Vec<PhaseId> = Vec::new();
        let mut phase_no_barrier: Vec<PhaseId> = Vec::new();
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
            // Optional per-phase empty-drain opt-out. Missing attr (older
            // task definitions) or `False` → not opted out; `True` records
            // the phase as one that may legitimately drain with zero items.
            if let Ok(mbe) = phase_spec.getattr("may_be_empty")
                && mbe.extract::<bool>().unwrap_or(false)
            {
                phase_may_be_empty.push(phase_id.clone());
            }
            // Optional per-phase barrier flag (`PhaseSpec.barrier`).
            // Defaults to `true` for old task definitions missing the attr
            // — strict barriers preserve the historical behaviour every
            // existing consumer relies on. Only `barrier=false` is the
            // explicit pipelined-edge opt-in, which records the phase in
            // `phase_no_barrier`; the scheduler then starts it `Active`
            // (per the pool's `set_no_barrier_phases` rule) and the
            // runtime-spawn interlock accepts spawns into it while its
            // upstream is still draining.
            let barrier = phase_spec
                .getattr("barrier")
                .ok()
                .and_then(|v| v.extract::<bool>().ok())
                .unwrap_or(true);
            if !barrier {
                phase_no_barrier.push(phase_id.clone());
            }

            let types_tuple: Vec<Bound<'_, PyAny>> = phase_spec.getattr("types")?.extract()?;
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
                if let Ok(mc) = tts.getattr("max_concurrent")
                    && !mc.is_none()
                {
                    let cap: u32 = mc.extract()?;
                    max_concurrent_per_type.insert(type_id.clone(), cap);
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

        let estimator = PyMemoryEstimatorBridge::from_python(task_definition, &estimator_specs)?;

        Ok(Self {
            estimator,
            phase_deps,
            phase_may_be_empty,
            phase_no_barrier,
            max_concurrent_per_type,
            raw_types,
        })
    }
}

/// Resolved fields pulled out of a Python `task_definition` instance, plus the
/// per-run paths the runner derives from it.
///
/// Per-secondary log-dir resolution is *not* baked in here: the
/// in-process distributed manager runs N secondaries from one
/// `LoadedTaskDefinition`, each with its own `secondary_id`, and a
/// single eager `log_dir` would force them to share a directory and
/// collide their `worker_*.log` filenames. Each manager calls
/// `log_paths.resolve_log_dir(py, &log_path, &secondary_id)` itself
/// once it knows which secondary the directory belongs to.
///
/// `log_path` is the per-run log-mount root: the framework feeds it
/// (not `output_path`) to `LogPathConfig::resolve_log_dir` so logs
/// land under the dedicated log-mount tree on SLURM deployments
/// (`/app/log-network`) rather than spilling into the output-mount
/// tree (`/app/out-network`). It defaults to `output_path` when the
/// caller did not supply an explicit log-mount root — preserving the
/// pre-split behaviour for single-host deployments where output and
/// log roots are intentionally the same directory.
pub(crate) struct LoadedTaskDefinition {
    pub(crate) estimator: PyMemoryEstimatorBridge,
    pub(crate) types: TypeRegistry,
    pub(crate) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Phases declared `PhaseSpec.may_be_empty` — carried from
    /// `LoadedTopology` to the manager constructors that register it on the
    /// primary (the empty-drain proceed-or-fail opt-out).
    pub(crate) phase_may_be_empty: Vec<PhaseId>,
    /// Phases declared `PhaseSpec.barrier=False` — carried from
    /// `LoadedTopology` to the manager constructors that register it on
    /// the primary (the pipelined-edge opt-in). Empty on the common
    /// strict-barrier run.
    pub(crate) phase_no_barrier: Vec<PhaseId>,
    pub(crate) source_path: PathBuf,
    pub(crate) output_path: PathBuf,
    pub(crate) log_path: PathBuf,
    pub(crate) log_paths: LogPathConfig,
    pub(crate) python_executable: PathBuf,
    /// `TaskDefinition.uses_file_based_items` (FR-2). False means
    /// `TaskInfo.path` is an opaque identifier, not a real filesystem
    /// path — workers won't open it and the framework skips
    /// hash-based staging. Defaults to True for old task definitions
    /// missing the attribute.
    pub(crate) uses_file_based_items: bool,
    /// Per-type concurrency caps from `TaskTypeSpec.max_concurrent`
    /// (FR-1). Carried over from `LoadedTopology` rather than re-parsed
    /// at every call site that needs to construct a `PrimaryConfig`.
    pub(crate) max_concurrent_per_type: HashMap<TypeId, u32>,
}

impl LoadedTaskDefinition {
    // Internal extractor — adding `log_dir` pushed this past clippy's
    // 7-arg comfort threshold. The shape is dictated by what every
    // manager constructor needs from the Python `task_definition` +
    // per-run paths; a struct of these would just push the same set
    // of params one level up. Collapsing into a builder is a separate
    // API refactor (same allow as the manager constructors).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_python(
        py: Python<'_>,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        source_dir: &str,
        output_dir: &str,
        log_dir: Option<&str>,
        skip_existing: bool,
        log_paths: Option<LogPathConfig>,
    ) -> PyResult<Self> {
        // Note: `py` is still required for `sys.executable` lookup
        // below, even though log-dir resolution moved out to the
        // managers (each manager owns its own `secondary_id`).
        let topology = LoadedTopology::from_python(task_definition)?;
        let uses_file_based_items: bool = task_definition
            .getattr("uses_file_based_items")
            .ok()
            .and_then(|v| v.extract().ok())
            .unwrap_or(true);

        let source_path = PathBuf::from(source_dir);
        let output_path = PathBuf::from(output_dir);
        // Single-source-of-truth fallback: when the caller did not
        // supply a dedicated log-mount root (single-host deployments,
        // legacy callers), the log root degenerates to the output
        // root — preserving the pre-split behaviour. The three
        // managers below read `task.log_path` unconditionally; the
        // None-handling lives here so each call site stays a single
        // boolean-free expression.
        let log_path = log_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| output_path.clone());
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

        let sys = py.import("sys")?;
        let python_executable: String = sys.getattr("executable")?.extract()?;

        Ok(Self {
            estimator: topology.estimator,
            types: TypeRegistry { types, index_by_id },
            phase_deps: topology.phase_deps,
            phase_may_be_empty: topology.phase_may_be_empty,
            phase_no_barrier: topology.phase_no_barrier,
            source_path,
            output_path,
            log_path,
            log_paths,
            python_executable: PathBuf::from(python_executable),
            uses_file_based_items,
            max_concurrent_per_type: topology.max_concurrent_per_type,
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
//
// The `test-with-python` feature (introduced later for `estimator.rs`'s
// gated test module) IS now wired up workspace-wide, so the barrier
// extraction test below uses it directly. The broader `from_python`
// surface still lives under the older NOTE above.

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! `PhaseSpec.barrier` extraction contract for the pyo3
    //! `LoadedTopology` adapter. The native scheduler USES the
    //! extracted `phase_no_barrier` set to decide each phase's initial
    //! state (`Active` vs `Blocked`) and to gate the runtime-spawn
    //! interlock; if the extractor silently drops the field the whole
    //! barrier=False semantic regresses to a no-op (the bug #540 fixes).
    //! These tests pin the extractor to the wire shape Python sends.
    //!
    //! Tests require an embedded CPython interpreter; gated behind the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python task_def -- --test-threads=1`
    use super::*;
    use pyo3::types::PyModule;

    /// Per-call atomic counter so each module gets a unique name (the
    /// parallel `cargo test` harness resolves duplicate names through
    /// `sys.modules`); the `--test-threads=1` flag is recommended but
    /// the nonce is belt-and-braces.
    static MODULE_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    /// Build a minimal Python `TaskDefinition`-shaped stub with two
    /// phases and a single type, optionally setting `barrier=False` on
    /// the named phase. `may_be_empty` is left at its default
    /// (`False`) and `max_concurrent` at `None` so this test exercises
    /// only the barrier field.
    fn build_task_def<'py>(
        py: Python<'py>,
        barrier_false_phase: Option<&str>,
    ) -> Bound<'py, PyAny> {
        let nonce = MODULE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let module_name = format!("mock_task_def_{nonce}");
        let file_name = format!("{module_name}.py");
        // The stub defines a `TaskTypeSpec`-shaped object and a
        // `PhaseSpec`-shaped object with the exact field names the
        // pyo3 extractor reads via `getattr`. Mirrors the duck-typed
        // contract: no class hierarchy required, just the fields. The
        // `barrier` attribute is set per phase from the closure arg.
        let phase_a_barrier = if barrier_false_phase == Some("A") {
            "False"
        } else {
            "True"
        };
        let phase_b_barrier = if barrier_false_phase == Some("B") {
            "False"
        } else {
            "True"
        };
        let source = format!(
            r#"
class TypeSpec:
    def __init__(self, type_id):
        self.type_id = type_id
        self.worker_module = "stub.worker"
        self.estimator_attr = "estimate_memory"
        self.timeout_seconds = None
        self.reserved_memory_per_worker = 0
        self.max_concurrent = None


class PhaseSpec:
    def __init__(self, phase_id, depends_on, types, barrier):
        self.phase_id = phase_id
        self.depends_on = depends_on
        self.types = types
        self.barrier = barrier
        self.may_be_empty = False


class Task:
    def get_phases(self):
        return (
            PhaseSpec("A", (), (TypeSpec("ta"),), barrier={phase_a_barrier}),
            PhaseSpec("B", ("A",), (TypeSpec("tb"),), barrier={phase_b_barrier}),
        )

    def estimate_memory(self, task):
        return 0
"#
        );
        let module = PyModule::from_code(
            py,
            std::ffi::CString::new(source).unwrap().as_c_str(),
            std::ffi::CString::new(file_name).unwrap().as_c_str(),
            std::ffi::CString::new(module_name).unwrap().as_c_str(),
        )
        .expect("compile mock task-definition module");
        module.getattr("Task").unwrap().call0().unwrap()
    }

    /// Build a stub task-definition that OMITS the `barrier` attribute
    /// entirely (older task definitions / Python callers that predate
    /// the field). The extractor must default the missing attribute to
    /// `barrier=True` — every phase strict-barrier — preserving the
    /// historical behaviour.
    fn build_task_def_no_barrier_attr(py: Python<'_>) -> Bound<'_, PyAny> {
        let nonce = MODULE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let module_name = format!("mock_task_def_no_barrier_{nonce}");
        let file_name = format!("{module_name}.py");
        let source = r#"
class TypeSpec:
    def __init__(self, type_id):
        self.type_id = type_id
        self.worker_module = "stub.worker"
        self.estimator_attr = "estimate_memory"
        self.timeout_seconds = None
        self.reserved_memory_per_worker = 0
        self.max_concurrent = None


class PhaseSpec:
    def __init__(self, phase_id, depends_on, types):
        self.phase_id = phase_id
        self.depends_on = depends_on
        self.types = types
        self.may_be_empty = False
        # NB: no `barrier` attr — simulates an older task definition.


class Task:
    def get_phases(self):
        return (PhaseSpec("A", (), (TypeSpec("ta"),)),)

    def estimate_memory(self, task):
        return 0
"#
        .to_string();
        let module = PyModule::from_code(
            py,
            std::ffi::CString::new(source).unwrap().as_c_str(),
            std::ffi::CString::new(file_name).unwrap().as_c_str(),
            std::ffi::CString::new(module_name).unwrap().as_c_str(),
        )
        .expect("compile mock task-definition module");
        module.getattr("Task").unwrap().call0().unwrap()
    }

    #[test]
    fn barrier_true_default_yields_empty_no_barrier_set() {
        Python::attach(|py| {
            let task_def = build_task_def(py, None);
            let topo = LoadedTopology::from_python(&task_def).expect("topology loaded");
            assert!(
                topo.phase_no_barrier.is_empty(),
                "with every phase barrier=True, phase_no_barrier must be empty (got {:?})",
                topo.phase_no_barrier
            );
        });
    }

    #[test]
    fn barrier_false_on_one_phase_lands_in_no_barrier_set() {
        Python::attach(|py| {
            let task_def = build_task_def(py, Some("B"));
            let topo = LoadedTopology::from_python(&task_def).expect("topology loaded");
            assert_eq!(
                topo.phase_no_barrier,
                vec![PhaseId::from("B")],
                "phase B was declared barrier=False; phase_no_barrier must contain exactly it"
            );
        });
    }

    #[test]
    fn missing_barrier_attr_defaults_true() {
        // Old task definitions predating the `barrier` attribute must
        // continue to behave strict-barrier (the historical default).
        // The extractor's `getattr().ok().and_then(extract)` fallback
        // is what makes this wire-safe — pin it.
        Python::attach(|py| {
            let task_def = build_task_def_no_barrier_attr(py);
            let topo = LoadedTopology::from_python(&task_def).expect("topology loaded");
            assert!(
                topo.phase_no_barrier.is_empty(),
                "a task definition missing the `barrier` attribute must default to strict \
                 barriers (got {:?})",
                topo.phase_no_barrier
            );
        });
    }
}
