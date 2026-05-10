"""Unit tests for the matrix-mode parser + failure-summary formatter.

Single concern: the format/parse boundary that sits between
``run_e2e.py``'s argparse layer and its matrix loop. The full
matrix iteration is exercised by ``run_e2e.py --workers 1,4``
against the slurm-test-env; these tests cover the helpers in
isolation so regressions surface without a live cluster.

Why under ``tests/e2e/`` rather than ``python/dynamic_runner/tests/``:
this code is e2e-driver infrastructure, not framework code. Mirrors
the placement of ``test_worker_leak_gate.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


# Tests/e2e/ is the directory; bring repo root onto sys.path so the
# absolute import works whether pytest is invoked from the repo root
# or from this dir. Mirrors the dance ``run_e2e.py`` itself does.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.e2e.run_e2e import (  # noqa: E402
    DEFAULT_WORKERS_MATRIX,
    _build_argparser,
    _format_failure_label,
    _parse_workers_list,
)


# ── _parse_workers_list (argparse type) ─────────────────────────────


def test_workers_list_parser_single_value() -> None:
    """``--workers 4`` → ``[4]``: backward-compat with the pre-matrix
    invocation. Existing CI that passes ``--workers 4`` must keep
    working with no observable change."""
    assert _parse_workers_list("4") == [4]


def test_workers_list_parser_csv() -> None:
    """``--workers 1,4`` → ``[1, 4]``: the canonical matrix form
    operators type by hand. Order is preserved (operator-supplied)."""
    assert _parse_workers_list("1,4") == [1, 4]


def test_workers_list_parser_default_when_omitted() -> None:
    """When ``--workers`` is omitted, argparse uses the default
    matrix ``[1, 4]``. Tested against the live argparser to catch
    a desync between the module-level constant and the
    ``add_argument(default=...)`` wiring."""
    parser = _build_argparser()
    ns = parser.parse_args(["--scenario", "all"])
    assert ns.workers == list(DEFAULT_WORKERS_MATRIX)
    assert ns.workers == [1, 4]


def test_workers_list_parser_rejects_negative_or_zero() -> None:
    """Zero/negative are operator typos, not legal cluster sizes.
    Exit-2 (argparse) is the right channel: the operator sees the
    error before any dispatch starts."""
    parser = _build_argparser()
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--scenario", "all", "--workers", "-1"])
    assert excinfo.value.code == 2

    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--scenario", "all", "--workers", "0"])
    assert excinfo.value.code == 2


def test_workers_list_parser_rejects_garbage() -> None:
    """Non-integer tokens reject with exit-2 — same channel as
    negatives. ``--workers a,b`` is an operator mistake (perhaps
    confusing the flag with a name list); fail loud at parse time."""
    parser = _build_argparser()
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--scenario", "all", "--workers", "a,b"])
    assert excinfo.value.code == 2


def test_workers_list_parser_rejects_empty_entry() -> None:
    """``"1,,4"`` is a typo, not ``[1, 4]``. Empty entries reject
    so the operator sees the bad input rather than getting silent
    coalescing."""
    parser = _build_argparser()
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--scenario", "all", "--workers", "1,,4"])
    assert excinfo.value.code == 2


def test_workers_list_parser_extensible() -> None:
    """Three+ values work — the matrix is open-ended. Operators
    iterating on cross-job-count regressions need ``--workers
    2,4,8`` to map a regression curve, not just ``1,4``."""
    assert _parse_workers_list("2,4,8") == [2, 4, 8]


# ── _format_failure_label (failure-summary line) ───────────────────


def test_failure_label_format() -> None:
    """``"name@wN"`` is the failure-summary format. Greppable, short,
    distinct from any path/file syntax. The matrix iteration emits
    this for every failed (scenario, worker-count) pair."""
    assert _format_failure_label("phase-deps", 1) == "phase-deps@w1"
    assert (
        _format_failure_label("parallel-4-workers", 4)
        == "parallel-4-workers@w4"
    )


def test_failure_label_format_summary_join() -> None:
    """The driver joins (name, N) pairs into the failure summary
    via ``", ".join(_format_failure_label(...))``. Verify the
    composite reads the way the spec asks for."""
    pairs = [("phase-deps", 1), ("parallel-4-workers", 4)]
    rendered = ", ".join(_format_failure_label(n, w) for n, w in pairs)
    assert rendered == "phase-deps@w1, parallel-4-workers@w4"
