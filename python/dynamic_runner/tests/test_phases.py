"""Phase / type / affinity end-to-end regression test.

Drives a synthetic ``TaskDefinition`` through ``run_local`` /
``run_distributed`` and post-mortems a shared dispatch log to assert
the four core invariants of the phases-types-affinity redesign:

1. **Phase dependencies** — items in a phase only dispatch after every
   item of every parent phase has finished.
2. **Affinity soft-pin** — items sharing an ``affinity_id`` cluster on
   a small worker set, not the whole pool.
3. **Free-pool item among affinity classes** — a ``None``-affinity item
   dispatches without starvation while pinned classes are running.
4. **Worker death mid-dispatch** — a worker that crashes before
   replying ``done`` has its in-flight item re-queued and eventually
   completed by another worker.

The worker subprocess (``_phases_worker.py``) records one JSON line per
dispatch into ``--phases-log <path>``; this test parses that log to
check the invariants.

Notes
-----
* The path string is the only payload the manager sends to the worker
  (the wire protocol carries the relative_path only). We therefore
  encode ``(phase, type, affinity, index)`` into the path so the
  worker can record which item it actually saw.
* Cross-phase activation is exercised through the distributed-runner
  path because that surfaces both the LocalManager's mid-run drain
  flush and the promoted primary's phase-aware dispatch
  (`PendingPool::take_first_match` honours `phase_state`).
* ``maturin develop --release`` must have run in this venv. The test
  is ``importorskip``-gated so collection still works otherwise.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable

import pytest

pytest.importorskip(
    "dynamic_runner",
    reason=(
        "dynamic_runner not installed; run `maturin develop --release` "
        "in this worktree first."
    ),
)

import dynamic_runner as _rs  # noqa: E402
from dynamic_runner._shared import BinaryIdentifier, TaskInfo  # noqa: E402
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec  # noqa: E402
from dynamic_runner.tests._phases_worker import NONE_AFFINITY, PATH_SEP  # noqa: E402


# ─── synthetic task definition ─────────────────────────────────────────


@dataclass
class _ItemSpec:
    """One item to discover, before encoding into a TaskInfo.

    ``type`` defaults to ``f'{phase}-default'`` to match the per-phase
    type-id chosen by `_phase_with_type` (TypeIds must be unique across
    the topology, so we cannot reuse a single ``"default"`` everywhere).
    Override it explicitly if a test wants two types in one phase.
    """

    phase: str
    type: str | None = None
    affinity: str | None = None

    def resolved_type(self) -> str:
        return self.type if self.type is not None else _type_for_phase(self.phase)


WORKER_MODULE = "dynamic_runner.tests._phases_worker"


@dataclass
class _PhasedTask:
    """Minimal TaskDefinition for the phase regression test.

    ``items`` is the full list discovered (possibly across phases).
    ``phases`` is the topology — each PhaseSpec carries the same single
    ``default`` type pointing at our worker module.
    ``log_path`` is forwarded to the worker via
    ``build_worker_command_args``; the worker appends one JSON line per
    dispatch to it.
    ``kill_phase`` (test #4 only) wires through to the worker so its
    first item of that phase exits before replying ``done``.
    """

    items: list[_ItemSpec]
    phases: tuple[PhaseSpec, ...]
    log_path: Path
    kill_phase: str | None = None
    kill_marker: Path | None = None

    # ── topology ───────────────────────────────────────────────────
    def get_phases(self) -> tuple[PhaseSpec, ...]:
        return self.phases

    # ── item discovery ─────────────────────────────────────────────
    def discover_items(self, source_dir: Path, args) -> Iterable[TaskInfo]:
        # Items are pre-built — the framework only iterates them once.
        return list(self._encode_items(source_dir))

    def _encode_items(self, source_dir: Path) -> Iterable[TaskInfo]:
        # Encode (phase, type, affinity-or-NONE, index) into the path
        # so the worker can recover it from the wire-level relative_path.
        for idx, spec in enumerate(self.items):
            aff = spec.affinity if spec.affinity is not None else NONE_AFFINITY
            type_id = spec.resolved_type()
            stem = PATH_SEP.join([spec.phase, type_id, aff, str(idx)])
            yield TaskInfo(
                path=Path(source_dir) / stem,
                size=1024,
                identifier=BinaryIdentifier(
                    binary_name=stem,
                    platform="x86_64",
                    compiler="gcc",
                    version="1",
                    opt_level="O0",
                ),
                phase_id=spec.phase,
                type_id=type_id,
                affinity_id=spec.affinity,
                payload={},
                # ``task_id`` became REQUIRED (non-empty) at the
                # Python->Rust boundary in commit c0a05719
                # ("core(task): task_id is now required"). ``stem``
                # already encodes (phase, type, affinity, idx)
                # uniquely across the items list, so it is a stable,
                # collision-free id without an extra synthesis step.
                task_id=stem,
            )

    # ── per-type plumbing ──────────────────────────────────────────
    def estimate_memory(self, item: TaskInfo) -> int:
        return 16 * 1024 * 1024  # 16 MiB; small enough N workers fit in 256 MiB

    def add_task_arguments(self, parser) -> None:  # pragma: no cover - unused
        pass

    def build_worker_command_args(
        self, type_id, args, source_dir, output_dir, skip_existing
    ) -> list[str]:
        argv = ["--phases-log", str(self.log_path)]
        if self.kill_phase is not None:
            argv += ["--phases-kill-phase", self.kill_phase]
            if self.kill_marker is not None:
                argv += ["--phases-kill-marker", str(self.kill_marker)]
        return argv

    def get_output_filename_pattern(self, type_id, item: TaskInfo) -> str:
        return f"{Path(item.path).name}.done"

    # ── lifecycle hooks (no-ops) ───────────────────────────────────
    def on_run_start(self, source_dir, output_dir, args) -> None:
        pass

    def on_run_end(self, success: bool) -> None:
        pass

    def on_phase_start(self, phase_id) -> None:
        pass

    def on_phase_end(self, phase_id, completed: int, failed: int) -> None:
        pass


def _phase_with_type(phase_id: str, depends_on: tuple[str, ...] = ()) -> PhaseSpec:
    """One phase with one type. TypeIds must be unique across the
    whole topology (the framework rejects duplicates in
    ``get_phases``), so each phase gets its own ``<phase>-default``
    type id. Items must declare matching ``type_id`` to land in the
    right bucket — encoders set that automatically below.
    """
    return PhaseSpec(
        phase_id=phase_id,
        types=(
            TaskTypeSpec(
                type_id=f"{phase_id}-default",
                worker_module=WORKER_MODULE,
            ),
        ),
        depends_on=depends_on,
    )


def _make_phase_chain(*phase_ids: str) -> tuple[PhaseSpec, ...]:
    """A→B→C→D-style dependency chain. Each phase has its own type."""
    out: list[PhaseSpec] = []
    prev: tuple[str, ...] = ()
    for pid in phase_ids:
        out.append(_phase_with_type(pid, depends_on=prev))
        prev = (pid,)
    return tuple(out)


def _single_phase(phase_id: str) -> tuple[PhaseSpec, ...]:
    return (_phase_with_type(phase_id),)


def _type_for_phase(phase_id: str) -> str:
    """Mirror the ``f'{phase}-default'`` choice in `_phase_with_type`."""
    return f"{phase_id}-default"


def _read_log(log_path: Path) -> list[dict]:
    if not log_path.exists():
        return []
    rows: list[dict] = []
    for line in log_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _ensure_dirs(tmp_path: Path) -> tuple[Path, Path]:
    src = tmp_path / "src"
    out = tmp_path / "out"
    src.mkdir(exist_ok=True)
    out.mkdir(exist_ok=True)
    return src, out


def _run_local(
    task: _PhasedTask,
    *,
    tmp_path: Path,
    num_workers: int = 4,
    max_memory: int = 256 * 1024 * 1024,
    cfg_kwargs: dict | None = None,
) -> dict:
    cfg = _rs.LocalManagerConfig(
        num_workers=num_workers,
        max_resources=_rs.ResourceMap({"memory": max_memory}),
        **(cfg_kwargs or {}),
    )
    src, out = _ensure_dirs(tmp_path)
    return _rs.run_local(
        cfg,
        task,
        SimpleNamespace(),
        str(src),
        str(out),
        list(task._encode_items(src)),
    )


def _run_distributed(
    task: _PhasedTask,
    *,
    tmp_path: Path,
    num_secondaries: int = 1,
    workers_per_secondary: int = 2,
    max_memory_per_secondary: int = 256 * 1024 * 1024,
) -> dict:
    """In-process distributed primary + N secondaries. Used for tests
    that exercise multi-phase dependency barriers across the full
    primary → promoted primary → secondary dispatch chain."""
    primary_cfg = _rs.PrimaryConfig(num_secondaries=num_secondaries)
    secondary_template = _rs.SecondaryConfig(
        secondary_id="<template>",
        num_workers=workers_per_secondary,
        max_resources=_rs.ResourceMap({"memory": max_memory_per_secondary}),
    )
    src, out = _ensure_dirs(tmp_path)
    return _rs.run_distributed(
        primary_cfg,
        secondary_template,
        task,
        SimpleNamespace(),
        str(src),
        str(out),
        list(task._encode_items(src)),
    )


# ─── scenario 1: 4-phase pipeline with deps ──────────────────────────


@pytest.mark.xfail(
    run=False,
    reason=(
        "#558: multi-phase A→B→C→D distributed pipeline deadlocks at the "
        "primary→promoted-primary→secondary dispatch chain. Tracked separately; "
        "run=False so the deadlock does not hang the suite."
    ),
)
def test_phase_dependencies_respected(tmp_path: Path) -> None:
    """Items in a child phase only dispatch after every item of every
    parent phase has finished.

    With phases A→B→C→D and N items each, every B dispatch should
    follow every A dispatch (and likewise B/C, C/D). The worker
    records ``time.time()`` (wall-clock — workers are separate
    processes so `time.monotonic` is not comparable across them), and
    the test compares per-phase max/min timestamps.

    The unconditional asserts (every item dispatched, every phase
    visited) hold today — only the strict per-item ordering does
    not. Until the framework gap above is closed, the strict-order
    check raises and the test is marked xfail.
    """
    log_path = tmp_path / "dispatch.log"
    items = (
        [_ItemSpec(phase="A") for _ in range(3)]
        + [_ItemSpec(phase="B") for _ in range(2)]
        + [_ItemSpec(phase="C") for _ in range(2)]
        + [_ItemSpec(phase="D") for _ in range(1)]
    )
    task = _PhasedTask(
        items=items,
        phases=_make_phase_chain("A", "B", "C", "D"),
        log_path=log_path,
    )
    _run_distributed(task, tmp_path=tmp_path, workers_per_secondary=2)

    rows = _read_log(log_path)
    # Every item recorded at least once. The distributed primary may
    # double-dispatch a small number of items via its retry/requeue
    # passes — that's fine, the per-item invariants still hold.
    assert len(rows) >= len(items), (
        f"expected >= {len(items)} dispatches, got {len(rows)}"
    )
    seen_indices = {r["index"] for r in rows}
    expected_indices = set(range(len(items)))
    assert expected_indices <= seen_indices, (
        f"missing item indices in log: {expected_indices - seen_indices}"
    )

    by_phase: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        by_phase[r["phase"]].append(r["ts"])
    for p in ("A", "B", "C", "D"):
        assert by_phase[p], f"no dispatches recorded for phase {p}"

    # Strict ordering — the assertion this test is named for. xfail
    # bites here today; will pass once the framework gap closes.
    assert max(by_phase["A"]) <= min(by_phase["B"]), (
        f"phase B dispatched before A drained: A.max={max(by_phase['A'])}, "
        f"B.min={min(by_phase['B'])}"
    )
    assert max(by_phase["B"]) <= min(by_phase["C"]), (
        f"phase C dispatched before B drained: B.max={max(by_phase['B'])}, "
        f"C.min={min(by_phase['C'])}"
    )
    assert max(by_phase["C"]) <= min(by_phase["D"]), (
        f"phase D dispatched before C drained: C.max={max(by_phase['C'])}, "
        f"D.min={min(by_phase['D'])}"
    )


# ─── scenario 2: affinity soft-pin ───────────────────────────────────


def test_affinity_soft_pin(tmp_path: Path) -> None:
    """Items sharing an ``affinity_id`` cluster on a small worker set.

    With 2 workers and 2 affinity classes (X, Y), each class should
    end up pinned to a single worker — the first one that took it.
    The framework's soft-pin priority (see
    `PendingPool::view_for_worker`) is:

        1. worker's currently-pinned bucket
        2. unpinned typed (non-free-pool) bucket in an Active phase
        3. free-pool bucket
        4. any other bucket (co-pin)

    So under steady-state with N items × 2 classes × 2 workers, X
    and Y each end up on exactly one worker — the ones that claimed
    them first. The bigger 4-workers / 2-classes case is left out
    because the BTreeMap-ordered fall-through co-pins the surplus
    workers onto class X (the alphabetically smaller key), which
    doesn't carry the soft-pin signal cleanly.
    """
    log_path = tmp_path / "dispatch.log"
    items = (
        [_ItemSpec(phase="P", affinity="X") for _ in range(6)]
        + [_ItemSpec(phase="P", affinity="Y") for _ in range(6)]
    )
    task = _PhasedTask(
        items=items,
        phases=_single_phase("P"),
        log_path=log_path,
    )
    _run_local(task, tmp_path=tmp_path, num_workers=2)

    rows = _read_log(log_path)
    assert len(rows) == len(items), f"expected {len(items)} dispatches, got {len(rows)}"

    workers_per_aff: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        if r["affinity"] is not None:
            workers_per_aff[r["affinity"]].add(r["worker_id"])

    for aff in ("X", "Y"):
        n_workers = len(workers_per_aff[aff])
        assert n_workers == 1, (
            f"affinity {aff} scattered across {n_workers} workers "
            f"(expected exactly 1 with 2 workers / 2 classes); "
            f"workers={sorted(workers_per_aff[aff])}"
        )

    # The two classes pinned to different workers (i.e. they ran in
    # parallel rather than serialising on the same worker).
    assert workers_per_aff["X"].isdisjoint(workers_per_aff["Y"]), (
        f"both affinity classes pinned to the same worker(s): "
        f"X={workers_per_aff['X']}, Y={workers_per_aff['Y']}"
    )


# ─── scenario 3: free-pool item among affinity classes ────────────────


def test_free_pool_item_does_not_starve(tmp_path: Path) -> None:
    """A ``None``-affinity item dispatches without starvation while
    pinned classes are running.

    The pool's pop algorithm prefers the worker's affine bucket first,
    then *unpinned typed buckets*, then the *free-pool bucket*. So a
    sole free-pool item must still land on some worker before the
    phase drains.
    """
    log_path = tmp_path / "dispatch.log"
    items = (
        [_ItemSpec(phase="P", affinity="X") for _ in range(6)]
        + [_ItemSpec(phase="P", affinity="Y") for _ in range(6)]
        + [_ItemSpec(phase="P", affinity=None)]  # the free-pool item
    )
    task = _PhasedTask(
        items=items,
        phases=_single_phase("P"),
        log_path=log_path,
    )
    _run_local(task, tmp_path=tmp_path, num_workers=3)

    rows = _read_log(log_path)
    assert len(rows) == len(items), f"expected {len(items)} dispatches, got {len(rows)}"
    free_rows = [r for r in rows if r["affinity"] is None]
    assert len(free_rows) == 1, f"free-pool item missing or duplicated: {free_rows}"


# ─── scenario 4: worker death mid-phase ───────────────────────────────


def test_worker_death_requeues_item(tmp_path: Path) -> None:
    """A worker that crashes mid-dispatch has its in-flight item
    re-queued and eventually completed.

    The worker's ``_phases_worker._run_protocol`` exits with
    ``os._exit(137)`` before replying ``done`` for the first item it
    sees of ``--phases-kill-phase``. The manager's
    worker-death/disconnect path requeues the in-flight item; the
    respawned slot (or another worker) replays it. The first attempt
    leaves a record in the log with no ``done`` reply; the requeued
    attempt produces a second record (possibly on a different pid).

    Single-phase intentionally — the cross-phase barrier path has a
    separate `xfail` test. Here we verify only that **every distinct
    item index** is recorded as completed at least once, so the
    re-queue path works end-to-end.

    The full task list completes (including the killed item) only if
    the manager actually replayed the requeued item. ``num_workers=2``
    so the surviving worker keeps draining the rest of the queue
    while the slot for the killed worker respawns.
    """
    log_path = tmp_path / "dispatch.log"
    kill_marker = tmp_path / "kill_marker"
    items = [_ItemSpec(phase="K") for _ in range(6)]
    phases = (_phase_with_type("K"),)
    task = _PhasedTask(
        items=items,
        phases=phases,
        log_path=log_path,
        kill_phase="K",
        kill_marker=kill_marker,
    )

    # The default policy already restarts workers each task, and the
    # worker-death/disconnect path respawns the dead slot regardless.
    # retry_max_attempts gives the requeued item a few chances if the
    # timing trips multiple kills.
    cfg_kwargs = {
        "retry_max_attempts": 4,
    }
    _run_local(
        task,
        tmp_path=tmp_path,
        num_workers=2,
        cfg_kwargs=cfg_kwargs,
    )

    rows = _read_log(log_path)
    seen_indices = {r["index"] for r in rows}
    expected_indices = set(range(len(items)))
    assert expected_indices <= seen_indices, (
        f"missing indices in log: {expected_indices - seen_indices}; "
        f"rows={rows}"
    )

    # The killed item appears at least twice in the log (one record
    # for the crash, one for the replay). Identify it as: the unique
    # index whose first record has a `pid` not seen on any of its
    # later records — equivalently, the index with >1 distinct pids.
    pids_per_index: dict[int, set[int]] = defaultdict(set)
    for r in rows:
        pids_per_index[r["index"]].add(r["pid"])
    requeued = [idx for idx, pids in pids_per_index.items() if len(pids) > 1]
    assert len(requeued) >= 1, (
        f"no item shows multi-pid replay; expected the killed item to "
        f"be re-dispatched. pids per index: {dict(pids_per_index)}"
    )
