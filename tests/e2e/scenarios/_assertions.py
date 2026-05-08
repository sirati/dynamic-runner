"""Reusable output assertions.

Single concern: the "did the publish destination receive what we
expected" check, parameterised over filename patterns. Scenarios with
bespoke layouts (publish-atomic checking for partials) compose their
own assertions; the rest delegate here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


def assert_files_present(
    publish_dst: Path,
    expected_names: Iterable[str],
) -> tuple[bool, list[str]]:
    """Assert each named file exists under ``publish_dst`` and is non-empty.

    Returns ``(ok, missing)``: ``ok=True`` when everything passed;
    ``missing`` is a list of human-readable strings for failure
    diagnostics. Both an absent file and a zero-byte file are
    treated as failure — empty publish output usually indicates an
    aborted task that nevertheless ran ``publish``.
    """
    missing: list[str] = []
    for name in expected_names:
        p = publish_dst / name
        if not p.exists():
            missing.append(f"{p} (missing)")
        elif p.stat().st_size == 0:
            missing.append(f"{p} (empty)")
    return (not missing, missing)


def expected_canonical_outputs(num_tasks: int) -> list[str]:
    """Default produce/consume filename pattern.

    Mirrors :func:`tests.e2e.test_consumer.task._produce_output_filename`
    and ``_consume_output_filename``; centralised so a rename in the
    consumer requires touching exactly one place in the test suite.
    """
    out: list[str] = []
    for i in range(num_tasks):
        out.append(f"produce-{i}.out")
        out.append(f"consume-{i}.out")
    return out


def assert_no_partial_files(
    publish_dst: Path,
    *,
    partial_suffixes: tuple[str, ...] = (".tmp", ".part", ".partial"),
) -> tuple[bool, list[str]]:
    """Assert no partial-write files leaked into the publish destination.

    Atomic publish should rename ``staging/foo`` → ``dst/foo`` in one
    operation. If the framework ever wrote ``dst/foo.tmp`` and then
    failed before the rename, the leftover would surface here.
    """
    leaked: list[str] = []
    for path in publish_dst.iterdir():
        for suffix in partial_suffixes:
            if path.name.endswith(suffix):
                leaked.append(str(path))
                break
    return (not leaked, [f"{p} (partial leak)" for p in leaked])


__all__ = [
    "assert_files_present",
    "assert_no_partial_files",
    "expected_canonical_outputs",
]
