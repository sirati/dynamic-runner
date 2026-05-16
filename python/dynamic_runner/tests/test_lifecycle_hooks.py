"""Phase 5B: TaskDefinition lifecycle hook integration tests.

Records calls to a stub TaskDefinition's lifecycle hooks and asserts
they fire in the expected order around a `run_local` call:

    on_run_start(source, output, args)
        ├── on_phase_start(<phase_id>)
        │   ... item dispatch ...
        ├── on_phase_end(<phase_id>, completed, failed)
        ... (per phase) ...
    on_run_end(success)

The hooks must:

* fire exactly once per run-boundary (`on_run_start`, `on_run_end`),
* fire once per *user-visible* phase
  (`on_phase_start`, `on_phase_end`),
* receive the same `phase_id` string the framework saw on the items,
* receive `success=True` from `on_run_end` when the manager run
  returned cleanly, and a sane `(completed, failed)` pair from
  `on_phase_end` totaling the items in that phase.

Per Phase 5B: exceptions raised by the per-phase hooks log and
continue. `on_run_start` failures abort the run. `on_run_end`
failures log and are swallowed. The "exceptions log and continue"
behaviour is exercised here only to the extent the framework keeps
running — the actual log lines are tracing-side and not asserted.

Note: this test depends on `maturin develop` having rebuilt the
wheel in the active Python environment. It is `pytest.importorskip`-
gated on the dynamic_runner extension; the framework milestone (when
the wheel is auto-rebuilt in CI) will exercise it end-to-end.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import pytest


pytest.importorskip(
    "dynamic_runner",
    reason=(
        "dynamic_runner not installed; run `maturin develop --release` "
        "in this worktree first."
    ),
)


@dataclass
class _StubBinaryIdentifier:
    binary_name: str

    def identifier_key(self) -> str:
        return self.binary_name


@dataclass
class _StubTaskInfo:
    path: str
    size: int
    identifier: _StubBinaryIdentifier
    phase_id: str = ""
    type_id: str = ""
    affinity_id: str | None = None
    payload: dict = field(default_factory=dict)


class _RecordingTask:
    """Minimal TaskDefinition that records every lifecycle call.

    The recording is sequenced — each call appends a tuple
    ``(method_name, *args)`` to ``self.calls``. Tests can assert both
    the multiset of calls and the relative order.
    """

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    # ── Topology ────────────────────────────────────────────────────
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

    # ── Item discovery / per-type plumbing ──────────────────────────
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

    # ── Lifecycle hooks (recorded) ──────────────────────────────────
    def on_run_start(self, source_dir, output_dir, args) -> None:
        self.calls.append(("on_run_start", str(source_dir), str(output_dir)))

    def on_run_end(self, success: bool) -> None:
        self.calls.append(("on_run_end", success))

    def on_phase_start(self, phase_id) -> None:
        self.calls.append(("on_phase_start", phase_id))

    def on_phase_end(self, phase_id, completed: int, failed: int) -> None:
        self.calls.append(("on_phase_end", phase_id, completed, failed))


def _ordered_calls(task: _RecordingTask) -> list[str]:
    """Return just the ordered list of method names. Useful for
    asserting that `on_run_start < on_phase_start < on_phase_end <
    on_run_end` without coupling the test to concrete IDs.
    """
    return [c[0] for c in task.calls]


def test_on_run_hooks_fire_once_per_run(tmp_path: Path) -> None:
    """`on_run_start` and `on_run_end` each fire exactly once around
    `run_local`, even with an empty binary list.
    """
    import dynamic_runner as _rs

    task = _RecordingTask()
    # Native config takes a typed ResourceMap, not a scalar `max_memory=`.
    # See `dynamic_runner.run._dispatch_local` for the canonical shape.
    cfg = _rs.LocalManagerConfig(
        num_workers=1,
        max_resources=_rs.ResourceMap({"memory": 64 * 1024 * 1024}),
    )
    args = SimpleNamespace()

    _rs.run_local(
        cfg,
        task,
        args,
        str(tmp_path / "src"),
        str(tmp_path / "out"),
        [],
    )

    names = _ordered_calls(task)
    assert names.count("on_run_start") == 1, names
    assert names.count("on_run_end") == 1, names
    # Order: start before end.
    assert names.index("on_run_start") < names.index("on_run_end"), names


def test_on_phase_hooks_fire_once_per_phase(tmp_path: Path) -> None:
    """A run with items in a single phase fires `on_phase_start` once
    and `on_phase_end` once for that phase, between the run hooks.
    """
    import dynamic_runner as _rs

    task = _RecordingTask()
    # Native config takes a typed ResourceMap, not a scalar `max_memory=`.
    # See `dynamic_runner.run._dispatch_local` for the canonical shape.
    cfg = _rs.LocalManagerConfig(
        num_workers=1,
        max_resources=_rs.ResourceMap({"memory": 64 * 1024 * 1024}),
    )
    args = SimpleNamespace()

    binaries = [
        _StubTaskInfo(
            path=f"item_{i}",
            size=100,
            identifier=_StubBinaryIdentifier(binary_name=f"item_{i}"),
            phase_id="only-phase",
            type_id="default",
        )
        for i in range(2)
    ]

    _rs.run_local(
        cfg,
        task,
        args,
        str(tmp_path / "src"),
        str(tmp_path / "out"),
        binaries,
    )

    names = _ordered_calls(task)
    assert names.count("on_phase_start") == 1, names
    assert names.count("on_phase_end") == 1, names
    # Order: run_start < phase_start < phase_end < run_end.
    rs = names.index("on_run_start")
    ps = names.index("on_phase_start")
    pe = names.index("on_phase_end")
    re = names.index("on_run_end")
    assert rs < ps < pe < re, names

    # phase_id matches the phase the items declared.
    phase_start = next(c for c in task.calls if c[0] == "on_phase_start")
    phase_end = next(c for c in task.calls if c[0] == "on_phase_end")
    assert phase_start[1] == "only-phase", phase_start
    assert phase_end[1] == "only-phase", phase_end


def test_on_phase_hook_exception_does_not_abort_run(tmp_path: Path) -> None:
    """If `on_phase_start` raises, the run still proceeds: per Phase
    5B, phase-boundary callback exceptions log and continue.
    """
    import dynamic_runner as _rs

    class _BadTask(_RecordingTask):
        def on_phase_start(self, phase_id) -> None:  # type: ignore[override]
            super().on_phase_start(phase_id)
            raise RuntimeError("boom")

    task = _BadTask()
    # Native config takes a typed ResourceMap, not a scalar `max_memory=`.
    # See `dynamic_runner.run._dispatch_local` for the canonical shape.
    cfg = _rs.LocalManagerConfig(
        num_workers=1,
        max_resources=_rs.ResourceMap({"memory": 64 * 1024 * 1024}),
    )
    args = SimpleNamespace()

    # Should not raise.
    _rs.run_local(
        cfg,
        task,
        args,
        str(tmp_path / "src"),
        str(tmp_path / "out"),
        [],
    )

    names = _ordered_calls(task)
    # `on_run_end` still fired despite the in-phase exception.
    assert "on_run_end" in names, names


def test_on_run_start_exception_aborts_run(tmp_path: Path) -> None:
    """Per Phase 5B: `on_run_start` failures propagate — the consumer
    hasn't completed setup, so the manager must not dispatch.
    """
    import dynamic_runner as _rs

    class _BadStartTask(_RecordingTask):
        def on_run_start(self, source_dir, output_dir, args) -> None:  # type: ignore[override]
            super().on_run_start(source_dir, output_dir, args)
            raise RuntimeError("setup failed")

    task = _BadStartTask()
    # Native config takes a typed ResourceMap, not a scalar `max_memory=`.
    # See `dynamic_runner.run._dispatch_local` for the canonical shape.
    cfg = _rs.LocalManagerConfig(
        num_workers=1,
        max_resources=_rs.ResourceMap({"memory": 64 * 1024 * 1024}),
    )
    args = SimpleNamespace()

    with pytest.raises(RuntimeError, match="setup failed"):
        _rs.run_local(
            cfg,
            task,
            args,
            str(tmp_path / "src"),
            str(tmp_path / "out"),
            [],
        )

    names = _ordered_calls(task)
    # No phase hooks fired (run aborted before dispatch).
    assert "on_phase_start" not in names, names
    assert "on_phase_end" not in names, names


def test_run_secondary_fires_on_run_start_before_connect(tmp_path: Path) -> None:
    """`run_secondary` must invoke `on_run_start` synchronously before
    attempting to reach the primary. Pre-fix the secondary code path
    silently skipped the run-level hooks, so a consumer's setup never
    ran on SLURM/network secondaries — only the primary's in-process
    `run_local` / `run_distributed` paths fired them.

    Drives the assertion with a deliberately unresolvable primary URL +
    short `connect_timeout_secs`: `on_run_start` runs under the GIL in
    `run_secondary` before `coord.run()` enters the Rust async runtime,
    so the recorded call must land regardless of whether the connect
    later succeeds or the resolve/connect path bails.
    """
    import dynamic_runner as _rs

    task = _RecordingTask()
    cfg = _rs.SecondaryConfig(
        secondary_id="test-secondary-0",
        num_workers=1,
        max_resources=_rs.ResourceMap({"memory": 64 * 1024 * 1024}),
        distributed_config=_rs.DistributedConfig(connect_timeout_secs=0.1),
    )
    args = SimpleNamespace()

    _rs.run_secondary(
        cfg,
        # 127.0.0.1:1 resolves but nothing is listening there, so the
        # WSS connect fails ECONNREFUSED on the first attempt and the
        # 0.1s `connect_timeout_secs` budget bails the retry loop.
        # `coord.run()` returns without doing anything operational.
        "tcp://127.0.0.1:1",
        task,
        args,
        str(tmp_path / "src"),
        str(tmp_path / "out"),
    )

    names = _ordered_calls(task)
    assert names.count("on_run_start") == 1, names
    assert names.count("on_run_end") == 1, names
    assert names.index("on_run_start") < names.index("on_run_end"), names


def test_run_distributed_passes_primary_handle_to_on_run_start(tmp_path: Path) -> None:
    """`run_distributed` must pre-mint a `PrimaryHandle` and pass it to
    the task's `on_run_start` as the `primary_handle=` kwarg — matching
    the contract `run_primary` already honours.

    The in-process `RustDistributedManager` builds its command-channel
    pair at `__init__` (mirroring `RustPrimaryCoordinator`), exposes a
    pre-run `handle()` factory, and the `run_distributed` pyfunction
    calls that factory BEFORE blocking on `mgr.run(...)` so the handle
    is live by the time the consumer's hook runs.

    This test pins the wiring: a modern `on_run_start(..., primary_handle=None)`
    receives a non-`None` `PrimaryHandle` instance, and the call signature
    is the only thing required to make the pipeline use it.
    """
    import dynamic_runner as _rs

    class _HandleCapturingTask(_RecordingTask):
        def __init__(self) -> None:
            super().__init__()
            self.captured_handle = None

        def on_run_start(
            self,
            source_dir,
            output_dir,
            args,
            primary_handle=None,
        ) -> None:  # type: ignore[override]
            self.calls.append(
                ("on_run_start", str(source_dir), str(output_dir))
            )
            self.captured_handle = primary_handle

    task = _HandleCapturingTask()
    primary_cfg = _rs.PrimaryConfig(num_secondaries=1)
    secondary_template = _rs.SecondaryConfig(
        secondary_id="<template>",
        num_workers=1,
        max_resources=_rs.ResourceMap({"memory": 64 * 1024 * 1024}),
    )
    args = SimpleNamespace()

    _rs.run_distributed(
        primary_cfg,
        secondary_template,
        task,
        args,
        str(tmp_path / "src"),
        str(tmp_path / "out"),
        [],
    )

    # `on_run_start` fired, and it received a live `PrimaryHandle`.
    names = _ordered_calls(task)
    assert names.count("on_run_start") == 1, names
    assert task.captured_handle is not None, (
        "run_distributed must pass a non-None primary_handle kwarg"
    )
    # Type-name check is enough — the `PrimaryHandle` class is private
    # to the Rust extension, so importing it isn't worth the coupling.
    assert type(task.captured_handle).__name__ == "PrimaryHandle", (
        f"expected PrimaryHandle, got {type(task.captured_handle).__name__}"
    )
