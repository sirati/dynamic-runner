"""Single-process (``--multi-computer single-process``) composite-run
regression tests.

Consumer-observed breakage (asm-tokenizer ``FullPipelineTask`` shape —
phases 2/3 lazily injected from ``on_phase_end`` via
``primary_handle.spawn_tasks``):

1. The mesh-always cutover made the in-process setup peer relocate the
   primary onto an in-process secondary, but the ONE Python
   ``PrimaryHandle`` command channel stayed on the setup peer (the
   promote recipe got ``command_channel: None``). The receiver dies
   with the demoted coordinator, so every ``spawn_tasks`` from
   ``on_phase_end`` raises ``PrimaryHandle: command channel closed``,
   the raise latch escalates to a cluster-wide ``RunAborted``, and the
   downstream phase bodies NEVER execute.
2. The in-process boundary mapped EVERY observed ``RunTerminal::Aborted``
   to ``DuplicateTaskIdPrePhase``, so the operator saw "duplicate task
   identity in the initial batch" for runs with zero duplicates.

These tests drive the REAL dispatch path end-to-end (real in-process
primary+secondaries, real subprocess workers) and assert EFFECTS
(per-phase output files the worker bodies write), not just status
counters.

Hermetic: skipped when the native module is not importable (build with
``maturin develop`` first).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("dynamic_runner._native")

from dynamic_runner import run  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parent.parent

_NUM_P1 = 6


def _make_dirs(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "src"
    output = tmp_path / "out"
    source.mkdir()
    output.mkdir()
    for i in range(_NUM_P1):
        (source / f"input-{i}.txt").write_text(f"data{i}\n")
    return source, output


def _argv(source: Path, output: Path) -> list[str]:
    return [
        "--source", str(source),
        "--output", str(output),
        "--multi-computer", "single-process",
        "--jobs", "2",
        "--cores", "2",
        "--max-memory", "2G",
        "--num-tasks", str(_NUM_P1),
    ]


@pytest.fixture(autouse=True)
def _worker_module_importable(monkeypatch):
    """The spawned worker subprocesses resolve ``tests.spc_consumer.worker``
    via ``python -m``; make the repo root importable for them regardless
    of the pytest invocation cwd."""
    monkeypatch.chdir(_REPO_ROOT)
    monkeypatch.setenv("PYTHONPATH", str(_REPO_ROOT))


def test_single_process_composite_lazy_chain_runs_downstream_bodies(tmp_path):
    """Face 1+2: the full three-phase lazy chain must run EVERY task body.

    The worker writes ``<task>.out`` under ``--output`` per executed
    body — the effect assertion. Pre-fix this run dies with
    ``PrimaryHandle: command channel closed`` out of ``on_phase_end``
    (escalated to a cluster-wide abort), and the unify/memmap bodies
    never execute.
    """
    from tests.spc_consumer.task import CompositeTask

    source, output = _make_dirs(tmp_path)
    run(task=CompositeTask(), argv=_argv(source, output))

    p1_outputs = sorted(p.name for p in output.glob("input-*.txt.out"))
    assert len(p1_outputs) == _NUM_P1, (
        f"every phase-1 body must run (effects on disk); got {p1_outputs}"
    )
    assert (output / "unify-agg.out").exists(), (
        "the lazily-spawned unify body must actually run "
        "(spawn_tasks from on_phase_end reaches the live primary)"
    )
    assert (output / "memmap-agg.out").exists(), (
        "the lazily-spawned memmap body must actually run "
        "(second chained on_phase_end spawn)"
    )


def test_single_process_abort_reason_is_not_mislabelled_as_duplicate(tmp_path):
    """Face 3: an aborted run must surface ITS OWN reason, never the
    fabricated "duplicate task identity in the initial batch" lecture.

    Forces a deliberate consumer-hook failure; the cluster-wide abort
    this escalates to must carry the hook's reason verbatim. Pre-fix
    the in-process boundary re-typed every observed Aborted terminal as
    ``DuplicateTaskIdPrePhase``, telling the operator to fix duplicate
    task ids that do not exist.
    """
    from tests.spc_consumer.task import CompositeTask

    marker = "synthetic-consumer-hook-raise-for-abort-label-test"

    def raising_hook(phase_id: str) -> None:
        raise RuntimeError(marker)

    source, output = _make_dirs(tmp_path)
    with pytest.raises(RuntimeError) as excinfo:
        run(task=CompositeTask(on_phase_end_hook=raising_hook),
            argv=_argv(source, output))

    message = str(excinfo.value)
    assert marker in message, (
        f"the abort must carry the actual reason; got: {message}"
    )
    assert "duplicate task identity" not in message, (
        "an abort whose reason is NOT a duplicate-task-id must not be "
        f"re-typed as one; got: {message}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-x", "-q"]))
