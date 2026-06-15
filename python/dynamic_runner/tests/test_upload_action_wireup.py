"""The #336 P1 / #493 option-A upload-action Python-side wire-up.

Single concern: pin the Python boundary that lets a consumer's
``upload_action`` callable flow through the framework's Python kwarg
surface (``RustDistributedManager``, ``RustPrimaryCoordinator``, and the
``_rs.run_distributed`` / ``_rs.run_primary`` shims), into the in-process
primary's setup executor where it is invoked for every upload setup task
derived from a ``TaskInfo.files=`` declaration.

Pre-fix the wire-up was missing on the in-process distributed path:
``PyDistributedManager::new`` had no ``upload_action`` kwarg at all, so
every ``--multi-computer local|single-process|remote-podman`` consumer
(asm-dataset-nix is here) could not register an uploader; any setup task
derived from a ``files=`` declaration then failed with a wiring-error
terminal. The Rust executor + bridge + ``PyPrimaryCoordinator`` already
had the plumbing — this test pins the four newly-added Python-surface
seams (one constructor kwarg + two pyo3 shim kwargs + one task-protocol
attribute).

The Rust pyclass test ``upload_action_kwarg_is_stored_on_manager``
(crates/dynrunner-pyo3/src/managers/distributed/tests.rs) covers the
``RustDistributedManager`` constructor's field-storage contract under
the GIL. This file covers the COMPLEMENTARY Python-visible surface: a
consumer importing the package can name + pass the kwarg AND attach
``task.upload_action`` and have run.py forward it. The dedup /
setup-task derivation / retry classification stay fully in Rust (the
``setup_staging.rs`` + ``setup_exec.rs`` + ``upload_action_bridge.rs``
test suites cover those end-to-end).
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest


pytest.importorskip(
    "dynamic_runner",
    reason=(
        "dynamic_runner not installed; run `maturin develop --release` "
        "in this worktree first."
    ),
)


def _stub_uploader_callable():
    """A bare ``(source, dest) -> None`` callable matching the
    :data:`dynamic_runner.task_protocol.UploadAction` shape.
    """
    return lambda source, dest: None


# ── Python pyclass / pyfn kwarg-surface ──────────────────────────────


def test_distributed_manager_accepts_upload_action_kwarg() -> None:
    """``RustDistributedManager.__init__`` accepts the ``upload_action``
    kwarg. Pre-fix the constructor had no such param at all — its
    signature now mirrors ``RustPrimaryCoordinator`` (the SLURM-path
    primary), so the in-process distributed manager's setup executor
    can install an uploader on its in-process primary BEFORE ``run()``
    enters. The constructor accepts AND stores it (the Rust pyclass
    test pins the storage contract; here we pin the Python-visible
    surface that ``_rs.run_distributed`` calls through).
    """
    import dynamic_runner as _rs

    mgr = _rs.RustDistributedManager(
        # Positional contract (see crates/dynrunner-pyo3/src/managers/distributed/new.rs).
        1,                             # num_secondaries
        1,                             # num_workers_per_secondary
        64 * 1024 * 1024,              # ram_per_secondary
        "/tmp/src",                    # source_dir
        "/tmp/out",                    # output_dir
        _TaskDefStub(),                # task_definition
        SimpleNamespace(),             # task_args
        upload_action=_stub_uploader_callable(),
    )
    # The constructor accepted the kwarg without raising — that is the
    # whole contract this test pins. The field-storage contract is
    # pinned by the Rust pyclass test (same file's sibling).
    assert mgr is not None


def test_primary_coordinator_accepts_upload_action_kwarg() -> None:
    """Symmetric guard on the SLURM-path primary. ``RustPrimaryCoordinator``
    has carried the kwarg since #336 P1 shipped — this test guards
    against a future refactor accidentally dropping it (the brief calls
    out the field's wiring as load-bearing).
    """
    import dynamic_runner as _rs

    coord = _rs.RustPrimaryCoordinator(
        1,                             # num_secondaries
        _TaskDefStub(),                # task_definition
        _noop_spawn,                   # spawn_secondary
        upload_action=_stub_uploader_callable(),
    )
    assert coord is not None


def test_run_distributed_pyfn_signature_carries_upload_action() -> None:
    """The ``_rs.run_distributed`` pyo3 helper declares an
    ``upload_action`` kwarg, so a consumer can pass
    ``upload_action=task.upload_action`` through the public Python
    entry point. Pre-fix the helper lacked the kwarg even though the
    underlying manager could store it; the kwarg surface was the gap
    blocking every ``--multi-computer local|single-process|remote-podman``
    consumer from a mode-1 files= run.

    We verify by introspecting the pyfn signature — a pure surface
    check that does not require standing up an in-process mesh
    (``num_secondaries=0`` is forbidden under mesh-always, and a real
    mesh run is too heavy for a wireup pin).
    """
    import dynamic_runner as _rs

    sig = inspect.signature(_rs.run_distributed)
    assert "upload_action" in sig.parameters, (
        "run_distributed pyo3 signature must declare upload_action= "
        "so a consumer's callable flows through to RustDistributedManager"
    )


def test_run_primary_pyfn_signature_carries_upload_action() -> None:
    """Symmetric on the network-primary path. ``_rs.run_primary`` was
    pre-fix missing the ``upload_action`` kwarg on its pyo3 signature
    even though ``RustPrimaryCoordinator`` accepted it — only direct
    SLURM-pipeline construction reached it. This pin guards against a
    regression: the helper must declare the kwarg so the in-process
    network primary path (``--multi-computer local`` /
    ``--multi-computer slurm`` direct ``run_primary`` callers) gets
    the same registration path.
    """
    import dynamic_runner as _rs

    sig = inspect.signature(_rs.run_primary)
    assert "upload_action" in sig.parameters, (
        "run_primary pyo3 signature must declare upload_action= so the "
        "callable flows through to RustPrimaryCoordinator"
    )


# ── Task-protocol attribute surface ──────────────────────────────────


def test_task_protocol_documents_upload_action_alias() -> None:
    """The task-protocol module exports an ``UploadAction`` typealias.

    A consumer reading ``task_protocol`` to discover the surface should
    see the callable shape documented and importable, mirroring
    ``TaskCompletedListener`` / ``CustomMessageHandler`` /
    ``WorkerMessageListener``. This pin guards against the alias being
    accidentally removed (it is what type-checkers see, even though the
    runtime uses ``getattr(task, "upload_action", None)`` duck typing).
    """
    from dynamic_runner import task_protocol as _tp

    assert hasattr(_tp, "UploadAction"), (
        "task_protocol must export UploadAction; consumers rely on the "
        "typealias for type-checking their upload callable"
    )


def test_run_py_forwards_task_upload_action_to_distributed_dispatch() -> None:
    """The Python ``run._dispatch_single_process`` / ``_dispatch_local``
    helpers must forward ``getattr(task, "upload_action", None)`` into
    ``_rs.run_distributed`` / ``_rs.run_primary``. Pre-fix the four
    Python forwards did not pass the kwarg through, so a consumer's
    ``task.upload_action`` attribute was silently dropped at the
    framework boundary even after the pyfn declared the kwarg.

    A pure source-grep is the right test here: the call shape lives
    inside a multi-screen kwarg block (around 100 lines per dispatch
    helper), end-to-end exercise would need a full mesh stand-up, and
    the load-bearing contract IS just "this getattr is in the kwarg
    block". One assertion per dispatch site keeps the regression
    surface intact.
    """
    # `dynamic_runner.run` re-exports the ``run()`` entry-point
    # callable, shadowing the submodule of the same name. Import the
    # submodule explicitly via ``importlib`` so ``getsource`` sees the
    # whole file (not just the re-exported function).
    import importlib

    _run_py = importlib.import_module("dynamic_runner.run")
    src = inspect.getsource(_run_py)
    distributed_dispatch = src[
        src.index("def _dispatch_single_process") :
        src.index("def _dispatch_multi_computer_local")
    ]
    local_dispatch = src[
        src.index("def _dispatch_multi_computer_local") :
        src.index("def _dispatch_slurm")
    ]
    forward = 'upload_action=getattr(task, "upload_action", None)'
    assert forward in distributed_dispatch, (
        "run.py::_dispatch_single_process must forward task.upload_action "
        "into _rs.run_distributed"
    )
    assert forward in local_dispatch, (
        "run.py::_dispatch_multi_computer_local must forward task.upload_action "
        "into _rs.run_primary"
    )


# ── helpers ────────────────────────────────────────────────────────────


def _noop_spawn(*args, **kwargs):
    """A no-op ``spawn_secondary`` callback for the constructor smoke
    tests; the surface tests never call ``run()`` so the callback is
    never invoked.
    """
    raise AssertionError(
        "spawn_secondary must not be invoked in surface-only tests"
    )


class _TaskDefStub:
    """Minimal TaskDefinition for the constructor smoke tests.

    Implements only the duck-typed surface the constructors consult.
    Lifecycle hooks are absent — these tests never enter ``run()``.
    """

    uses_file_based_items = True

    def get_phases(self):
        from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec

        return (
            PhaseSpec(
                phase_id="only-phase",
                types=(
                    TaskTypeSpec(
                        type_id="default",
                        worker_module="dynamic_runner.tests._failover_stub_worker",
                    ),
                ),
            ),
        )

    def discover_items(self, source_dir, args):
        return []

    def estimate_memory(self, item) -> int:
        return 1024 * 1024

    def add_task_arguments(self, parser) -> None:
        pass

    def build_worker_command_args(
        self, type_id, args, source_dir, output_dir, skip_existing
    ):
        return []

    def get_output_filename_pattern(self, type_id, item) -> str:
        return f"{item.path}.done"
