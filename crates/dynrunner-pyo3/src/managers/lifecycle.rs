//! Python `TaskDefinition` lifecycle-hook bridge.
//!
//! The manager core (`LocalManager`, `PrimaryCoordinator`) accepts
//! `FnMut` closures for `on_phase_start` / `on_phase_end`; the runner's
//! top-level run wrapper additionally invokes `on_run_start` /
//! `on_run_end` synchronously around the manager run. Every Python
//! manager pyclass needs the same pair of GIL-reacquiring closures, so
//! single-source them here.
//!
//! Error policy:
//! - `on_phase_start` / `on_phase_end` exceptions log and continue.
//!   Phase boundaries are not the place to surface fatal errors;
//!   exceptions out of the consumer's hook are a consumer bug, not a
//!   reason to abort an in-flight pool drain.
//! - `on_run_start` exceptions abort the run (see
//!   `run::run_local`/`run_primary`/`run_distributed`): the consumer's
//!   setup hasn't completed; dispatching items would race with
//!   half-built resources.
//! - `on_run_end` exceptions log and continue (the run is over; nothing
//!   to recover).

use pyo3::prelude::*;
use pyo3::types::PyDict;

use dynrunner_core::PhaseId;

/// Boxed `on_phase_start` callback shape callers wire into the
/// manager run loop.
pub(crate) type OnPhaseStart = Box<dyn FnMut(&PhaseId) + Send>;

/// Boxed `on_phase_end` callback shape callers wire into the
/// manager run loop. Receives `(phase_id, completed_count,
/// failed_count, &phase_outputs)` at every Active → Drained
/// transition, where `phase_outputs` is the just-completed phase's
/// PUBLISHED task outputs keyed by `task_id`
/// (`{ task_id: TaskOutputs }`). Uniform across the local manager and
/// the distributed primary so this single bridge wires both.
pub(crate) type OnPhaseEnd = Box<
    dyn FnMut(&PhaseId, u32, u32, &std::collections::BTreeMap<String, dynrunner_core::TaskOutputs>)
        + Send,
>;

/// Build an `on_phase_start` closure that re-acquires the GIL and calls
/// `task_definition.on_phase_start(phase_id)`.
///
/// The returned closure is `'static + Send` so it can be passed to the
/// manager's `process_binaries` / `run` whose closure types require both
/// (the manager runs the closure on its own LocalSet under
/// `py.detach`, off the GIL thread).
pub(crate) fn make_on_phase_start(
    task_definition: Py<PyAny>,
) -> impl FnMut(&PhaseId) + Send + 'static {
    move |phase_id: &PhaseId| {
        Python::attach(|py| {
            if let Err(e) = task_definition
                .bind(py)
                .call_method1("on_phase_start", (phase_id.as_str(),))
            {
                tracing::warn!(
                    error = %e,
                    phase = %phase_id,
                    "TaskDefinition.on_phase_start raised; continuing"
                );
            }
        });
    }
}

/// Build an `on_phase_end` closure that re-acquires the GIL and calls
/// `task_definition.on_phase_end(phase_id, completed, failed,
/// phase_outputs=<dict>)`.
///
/// `phase_outputs` carries the just-completed phase's PUBLISHED task
/// outputs as a back-compat KWARG (mirroring `on_run_start`'s
/// `primary_handle` kwarg-with-fallback): the value handed to Python is
/// the dict `{ task_id: { output_key: {"kind": "inline"|"file", "value":
/// str} } }` — the SAME shape a worker's `predecessor_outputs` carries —
/// so a consumer reads a finished task's published bytes WITHOUT a
/// filesystem path. The map serialises once (serde-JSON, the
/// wire-canonical `TaskOutputs` shape) and `json.loads`-decodes GIL-side
/// into the dict.
///
/// Back-compat: a legacy `on_phase_end(self, phase_id, completed,
/// failed)` signature (no `phase_outputs` parameter) raises CPython's
/// arg-binding `TypeError: ... unexpected keyword argument
/// 'phase_outputs'` BEFORE the body runs; the substring guard detects
/// that exact shape and retries positional-only — no double-execution of
/// side-effect-bearing user code. Other `TypeError`s (from inside the
/// body) propagate to the catch-all warn unchanged.
pub(crate) fn make_on_phase_end(
    task_definition: Py<PyAny>,
) -> impl FnMut(&PhaseId, u32, u32, &std::collections::BTreeMap<String, dynrunner_core::TaskOutputs>)
+ Send
+ 'static {
    move |phase_id: &PhaseId,
          completed: u32,
          failed: u32,
          phase_outputs: &std::collections::BTreeMap<String, dynrunner_core::TaskOutputs>| {
        Python::attach(|py| {
            let task = task_definition.bind(py);
            let positional = (phase_id.as_str(), completed, failed);
            // Serialise the phase outputs to the wire-canonical JSON and
            // decode into a Python object GIL-side, so the consumer reads
            // the same `{task_id: {key: {"kind","value"}}}` dict shape a
            // worker's `predecessor_outputs` carries. An encode/decode
            // failure degrades to `phase_outputs=None` (the call still
            // runs; the consumer sees no outputs) rather than dropping the
            // phase-end notification.
            let phase_outputs_obj: Bound<'_, PyAny> = match serde_json::to_string(phase_outputs)
                .ok()
                .and_then(|json| json_loads(py, &json).ok())
            {
                Some(obj) => obj,
                None => {
                    tracing::warn!(
                        phase = %phase_id,
                        "on_phase_end: phase_outputs JSON encode/decode failed; \
                         passing phase_outputs=None"
                    );
                    py.None().into_bound(py)
                }
            };
            let kwargs = PyDict::new(py);
            if let Err(e) = kwargs.set_item("phase_outputs", phase_outputs_obj) {
                tracing::warn!(
                    error = %e,
                    phase = %phase_id,
                    "on_phase_end: failed to build phase_outputs kwarg; \
                     calling positional-only"
                );
                if let Err(e) = task.call_method1("on_phase_end", positional) {
                    tracing::warn!(
                        error = %e,
                        phase = %phase_id,
                        "TaskDefinition.on_phase_end raised; continuing"
                    );
                }
                return;
            }
            match task.call_method("on_phase_end", positional, Some(&kwargs)) {
                Ok(_) => {}
                // Legacy signature without the kwarg: the arg-binder
                // rejected the call BEFORE the body ran, so retrying
                // positional-only is safe (no double-execution).
                Err(e)
                    if e.is_instance_of::<pyo3::exceptions::PyTypeError>(py)
                        && e.value(py).to_string().contains("phase_outputs") =>
                {
                    if let Err(e2) = task.call_method1("on_phase_end", positional) {
                        tracing::warn!(
                            error = %e2,
                            phase = %phase_id,
                            "TaskDefinition.on_phase_end raised; continuing"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        phase = %phase_id,
                        completed,
                        failed,
                        "TaskDefinition.on_phase_end raised; continuing"
                    );
                }
            }
        });
    }
}

/// Decode a JSON string into a Python object via the stdlib `json.loads`,
/// GIL-side. Single small helper so `make_on_phase_end` doesn't restate
/// the import/lookup; the `TaskOutputs` wire JSON (adjacently-tagged
/// `{"kind","value"}`) decodes to the exact nested-dict shape the
/// consumer reads (same as a worker's `predecessor_outputs`).
fn json_loads<'py>(py: Python<'py>, json: &str) -> PyResult<Bound<'py, PyAny>> {
    let json_mod = py.import("json")?;
    json_mod.call_method1("loads", (json,))
}

/// Fire `task_definition.on_run_start(source_dir, output_dir, args[,
/// primary_handle])` synchronously under the GIL. Any exception raised
/// by the Python callback propagates: the run hasn't started yet, so
/// the consumer's setup failure is fatal.
///
/// `primary_handle` is the optional in-flight runtime handle the
/// modern task signature consumes. Caller policy decides who supplies
/// it:
/// - Primary-side dispatchers (`run_primary`, `run_distributed`, the
///   SLURM `drive_rust_primary`) pass `Some(coord.handle())` so the
///   task can drive `spawn_tasks(...)` from inside `on_run_start`.
/// - Secondary-side dispatchers (`run_secondary`) pass `None`; the
///   secondary holds no handle and the kwarg's omission keeps the
///   call shape positional-compatible.
///
/// # Backward compatibility
///
/// When `primary_handle` is `Some`, the call is invoked with the
/// `primary_handle=` kwarg. Legacy tasks whose `on_run_start`
/// signature does NOT accept the kwarg raise `TypeError: ... got an
/// unexpected keyword argument 'primary_handle'` from CPython's
/// arg-binding layer. That specific shape is detected by inspecting
/// the exception message for `"primary_handle"`; on match, we retry
/// without the kwarg so legacy tasks keep working. Other `TypeError`s
/// (raised from inside the task body) propagate unchanged — the
/// substring guard is the load-bearing filter that prevents
/// double-invocation of side-effect-bearing user code.
///
/// When `primary_handle` is `None`, the call goes through the
/// positional-only path with no compatibility dance.
pub(crate) fn fire_on_run_start(
    task_definition: &Bound<'_, PyAny>,
    source_dir: &str,
    output_dir: &str,
    task_args: &Bound<'_, PyAny>,
    primary_handle: Option<Py<PyAny>>,
) -> PyResult<()> {
    let py = task_definition.py();
    let positional = (source_dir, output_dir, task_args.clone());
    let Some(handle) = primary_handle else {
        return task_definition
            .call_method1("on_run_start", positional)
            .map(|_| ());
    };
    let kwargs = PyDict::new(py);
    kwargs.set_item("primary_handle", handle)?;
    match task_definition.call_method("on_run_start", positional.clone(), Some(&kwargs)) {
        Ok(_) => Ok(()),
        Err(e)
            if e.is_instance_of::<pyo3::exceptions::PyTypeError>(py)
                && e.value(py).to_string().contains("primary_handle") =>
        {
            // Legacy task signature without the kwarg. The arg-binding
            // layer rejected the call *before* the task body ran, so
            // retrying without the kwarg is safe (no double-execution of
            // user-visible side effects).
            task_definition
                .call_method1("on_run_start", positional)
                .map(|_| ())
        }
        Err(e) => Err(e),
    }
}

/// Fire `task_definition.on_run_end(success)` synchronously under the
/// GIL. Exceptions are logged and swallowed — the run has already
/// terminated; there is no recovery, and propagating would mask the
/// real outcome (success or the manager's own error).
pub(crate) fn fire_on_run_end(task_definition: &Bound<'_, PyAny>, success: bool) {
    if let Err(e) = task_definition.call_method1("on_run_end", (success,)) {
        tracing::warn!(
            error = %e,
            success,
            "TaskDefinition.on_run_end raised; ignoring (run already complete)"
        );
    }
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Pins the `fire_on_run_start` kwarg contract:
    //!   1. Modern signatures receive `primary_handle` as a kwarg.
    //!   2. Legacy signatures fall back to the positional-only shape
    //!      without re-running the task body.
    //!   3. A `TypeError` raised from inside the task body — whose
    //!      message does NOT mention `primary_handle` — propagates
    //!      unchanged (the substring guard is the discriminator).
    //!
    //! Tests require an embedded CPython interpreter; gated behind the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python lifecycle`
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static MODULE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Compile a one-off `mock_task_<n>` Python module exposing a
    /// recording `Task` class with the given `on_run_start` body.
    /// Returns the instance + the module globals dict so tests can
    /// inspect any recorded state afterwards.
    fn make_task(on_run_start_src: &str) -> (Py<PyAny>, Py<PyAny>) {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_lifecycle_task_{nonce}");
        let file_name = format!("{module_name}.py");
        let body = format!(
            "calls = []\n\
             class Task:\n    {}\n",
            on_run_start_src.replace('\n', "\n    "),
        );
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(body).unwrap().as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .expect("compile mock task module");
            let cls = module.getattr("Task").unwrap();
            let instance = cls.call0().unwrap().unbind();
            let globals = module.dict().unbind().into_any();
            (instance, globals)
        })
    }

    /// Modern task signature: `on_run_start(self, source_dir,
    /// output_dir, args, primary_handle=None)` records that the kwarg
    /// reached it. A `Py<PyAny>` sentinel substitutes for a real
    /// `PrimaryHandle` here — the bridge only forwards opaque
    /// `Py<PyAny>` so the test does not need a live handle.
    #[test]
    fn fire_on_run_start_passes_primary_handle_to_modern_signature() {
        let (task_obj, globals) = make_task(
            "def on_run_start(self, source_dir, output_dir, args, primary_handle=None):\n        \
                 calls.append((source_dir, output_dir, args, primary_handle))",
        );
        Python::attach(|py| {
            let task = task_obj.bind(py);
            // Use a Python int as the opaque handle sentinel.
            let sentinel: Py<PyAny> = 42i64.into_pyobject(py).unwrap().into_any().unbind();
            let args = py.None();
            fire_on_run_start(task, "/src", "/out", args.bind(py), Some(sentinel))
                .expect("modern signature accepts primary_handle kwarg");
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(list.len(), 1);
            let entry = list.get_item(0).unwrap();
            let tuple = entry.cast::<pyo3::types::PyTuple>().unwrap();
            let captured_handle: i64 = tuple.get_item(3).unwrap().extract().unwrap();
            assert_eq!(captured_handle, 42, "kwarg value reached the task body");
        });
    }

    /// Legacy task signature: no `primary_handle` parameter. CPython's
    /// arg-binder raises `TypeError: ... got an unexpected keyword
    /// argument 'primary_handle'` *before* the body runs; the bridge
    /// must detect that, retry positional-only, and the body must run
    /// exactly once.
    #[test]
    fn fire_on_run_start_falls_back_to_positional_for_legacy_signature() {
        let (task_obj, globals) = make_task(
            "def on_run_start(self, source_dir, output_dir, args):\n        \
                 calls.append((source_dir, output_dir, args))",
        );
        Python::attach(|py| {
            let task = task_obj.bind(py);
            let sentinel: Py<PyAny> = 7i64.into_pyobject(py).unwrap().into_any().unbind();
            let args = py.None();
            fire_on_run_start(task, "/src", "/out", args.bind(py), Some(sentinel))
                .expect("legacy signature falls back without surfacing the TypeError");
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(
                list.len(),
                1,
                "the body runs exactly once after the kwarg-binding TypeError",
            );
        });
    }

    /// A `TypeError` raised from INSIDE the task body whose message
    /// does not mention `primary_handle` must propagate unchanged —
    /// the substring guard is what distinguishes the kwarg-binding
    /// failure (safe to retry) from a real user-code error (must
    /// surface to abort the run).
    #[test]
    fn fire_on_run_start_propagates_unrelated_type_error() {
        let (task_obj, _globals) = make_task(
            "def on_run_start(self, source_dir, output_dir, args, primary_handle=None):\n        \
                 raise TypeError('something else broke')",
        );
        Python::attach(|py| {
            let task = task_obj.bind(py);
            let sentinel: Py<PyAny> = 1i64.into_pyobject(py).unwrap().into_any().unbind();
            let args = py.None();
            let err = fire_on_run_start(task, "/src", "/out", args.bind(py), Some(sentinel))
                .expect_err("unrelated TypeError must propagate");
            assert!(err.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
            assert!(err.value(py).to_string().contains("something else broke"));
        });
    }

    /// `None` for `primary_handle` skips the kwarg dance entirely
    /// and calls the legacy positional-only shape. Used by
    /// secondary-side dispatchers that have no handle to supply.
    #[test]
    fn fire_on_run_start_omits_kwarg_when_handle_is_none() {
        let (task_obj, globals) = make_task(
            "def on_run_start(self, source_dir, output_dir, args):\n        \
                 calls.append((source_dir, output_dir, args))",
        );
        Python::attach(|py| {
            let task = task_obj.bind(py);
            let args = py.None();
            fire_on_run_start(task, "/src", "/out", args.bind(py), None)
                .expect("positional-only call against legacy signature");
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(list.len(), 1);
        });
    }

    /// Modern `on_phase_end(self, phase_id, completed, failed, *,
    /// phase_outputs=None)` receives the phase outputs as the kwarg,
    /// decoded to the `{task_id: {key: {"kind","value"}}}` dict shape
    /// from the wire-canonical `TaskOutputs` JSON.
    #[test]
    fn make_on_phase_end_passes_phase_outputs_to_modern_signature() {
        let (task_obj, globals) = make_task(
            "def on_phase_end(self, phase_id, completed, failed, *, phase_outputs=None):\n        \
                 calls.append((phase_id, completed, failed, phase_outputs))",
        );
        let mut outputs: std::collections::BTreeMap<String, dynrunner_core::TaskOutputs> =
            std::collections::BTreeMap::new();
        let mut m: std::collections::BTreeMap<String, dynrunner_core::ResultValue> =
            std::collections::BTreeMap::new();
        m.insert(
            "dependency_graph_pkl".to_string(),
            dynrunner_core::ResultValue::Inline("BASE64PICKLE".to_string()),
        );
        outputs.insert("dependency_graph".to_string(), dynrunner_core::TaskOutputs(m));

        let mut cb = make_on_phase_end(task_obj);
        cb(&PhaseId::from("dependency_graph"), 1, 0, &outputs);

        Python::attach(|py| {
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(list.len(), 1, "on_phase_end called exactly once");
            let entry = list.get_item(0).unwrap();
            let tuple = entry.cast::<pyo3::types::PyTuple>().unwrap();
            // phase_outputs reached the body as a dict; the consumer's
            // access pattern resolves the published pickle string.
            let phase_outputs = tuple.get_item(3).unwrap();
            let value: String = phase_outputs
                .get_item("dependency_graph")
                .unwrap()
                .get_item("dependency_graph_pkl")
                .unwrap()
                .get_item("value")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(value, "BASE64PICKLE", "consumer access pattern resolves");
        });
    }

    /// Legacy `on_phase_end(self, phase_id, completed, failed)` (no
    /// `phase_outputs` parameter) raises the kwarg-binding `TypeError`
    /// BEFORE the body runs; the bridge detects it, retries
    /// positional-only, and the body runs EXACTLY ONCE.
    #[test]
    fn make_on_phase_end_falls_back_to_positional_for_legacy_signature() {
        let (task_obj, globals) = make_task(
            "def on_phase_end(self, phase_id, completed, failed):\n        \
                 calls.append((phase_id, completed, failed))",
        );
        let empty: std::collections::BTreeMap<String, dynrunner_core::TaskOutputs> =
            std::collections::BTreeMap::new();
        let mut cb = make_on_phase_end(task_obj);
        cb(&PhaseId::from("p0"), 2, 1, &empty);

        Python::attach(|py| {
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(
                list.len(),
                1,
                "the body runs exactly once after the kwarg-binding TypeError",
            );
        });
    }
}
