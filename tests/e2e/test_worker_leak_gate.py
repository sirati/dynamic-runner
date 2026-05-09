"""Unit tests for the worker-node leak gate plumbing.

Single concern: parser + report-formatter coverage that doesn't
require a live cluster. The full gate (srun-into-each-worker) is
exercised by ``run_e2e.py --scenario phase-deps`` against the
slurm-test-env; these tests cover the format/parse boundary that
sits between the gate's transport layer and its decision layer.

Why under ``tests/e2e/`` rather than ``python/dynamic_runner/tests/``:
this code is e2e-driver infrastructure, not framework code. Per the
handoff task spec, e2e-related unit tests stay alongside the e2e
driver they serve.
"""

from __future__ import annotations

import sys
from pathlib import Path


# Tests/e2e/ is the directory; bring repo root onto sys.path so the
# absolute import works whether pytest is invoked from the repo root
# or from this dir. Mirrors the dance ``run_e2e.py`` itself does.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.e2e.run_e2e import (  # noqa: E402
    _build_probe_shell,
    _format_worker_leak_report,
    _parse_probe_output,
    _WorkerLeak,
    _WRAPPER_TOKEN_PREFIX,
    _WRAPPER_TOKEN_REGEX,
)
from tests.e2e.scenarios._base import DispatchEnv  # noqa: E402


def _env() -> DispatchEnv:
    """Minimal DispatchEnv for the probe-shell renderer."""
    return DispatchEnv(
        instance_id="e2e",
        ssh_port=2222,
        slurm_root_folder="/home/e2e-user/dynrunner-e2e",
        workers=2,
        mode="slurm",
        ssh_user="e2e-user",
    )


def test_probe_shell_renders_token_filters() -> None:
    """The rendered probe must carry the wrapper's token in three
    places: tempdir glob, podman name filter, pgrep regex. These
    are the discriminators between framework state and any other
    operator's processes — a missing one would either false-positive
    on shared hosts (no filter) or false-negative on real leaks
    (wrong pattern).
    """
    shell = _build_probe_shell(_env())
    # Tempdir glob.
    assert "/tmp/asm-*" in shell
    # Container name filter.
    assert f"--filter 'name={_WRAPPER_TOKEN_PREFIX}'" in shell
    # pgrep regex (ERE — `{8}` not `\{8\}`).
    assert _WRAPPER_TOKEN_REGEX in shell
    # User filter on pgrep (per task spec's PKILL CAVEAT — the leak
    # gate must never touch another operator's processes).
    assert "pgrep -u e2e-user" in shell


def test_probe_shell_uses_podman_format_string_unmolested() -> None:
    """str.format()-escaped `{{.ID}}` etc must reach the shell as
    literal `{{.ID}}` — that's podman's go-template syntax. A common
    mis-escape would render `{.ID}` and podman silently emits empty
    rows, which would mask container leaks.
    """
    shell = _build_probe_shell(_env())
    assert "--format '{{.ID}} {{.Names}} {{.Image}} {{.Status}}'" in shell


def test_parse_probe_clean() -> None:
    """Empty section bodies → is_empty()."""
    stdout = (
        "##TEMPDIRS##\n"
        "##CONTAINERS##\n"
        "##PROCESSES##\n"
        "##END##\n"
    )
    leak = _parse_probe_output("slurm-worker1", stdout)
    assert leak.is_empty()
    assert leak.tempdirs == []
    assert leak.containers == []
    assert leak.processes == []


def test_parse_probe_all_classes_leaked() -> None:
    """Container ID is the first token of each container line; pid
    is the first token of each pgrep line. Both shapes mirror the
    podman --format and pgrep -af outputs the probe shell pins.
    """
    stdout = (
        "##TEMPDIRS##\n"
        "/tmp/asm-deadbeef\n"
        "/tmp/asm-cafe1234\n"
        "##CONTAINERS##\n"
        "abc123def asm-deadbeef-sec-01 myimage:latest Up 5 minutes\n"
        "##PROCESSES##\n"
        "12345 setsid -f bash -c '...' watchdog 999 asm-deadbeef-sec-01\n"
        "##END##\n"
    )
    leak = _parse_probe_output("slurm-worker2", stdout)
    assert not leak.is_empty()
    assert leak.tempdirs == ["/tmp/asm-deadbeef", "/tmp/asm-cafe1234"]
    assert leak.container_ids == ["abc123def"]
    assert leak.process_pids == ["12345"]


def test_parse_probe_partial_leak_only_tempdir() -> None:
    """A tempdir-only leak still flags non-empty — covers the
    ``podman unshare rm -rf $RNDTMP`` regression class where the
    container exited cleanly but the cleanup trap's tree-rm got
    SIGKILLed before completing.
    """
    stdout = (
        "##TEMPDIRS##\n"
        "/tmp/asm-aaaaaaaa\n"
        "##CONTAINERS##\n"
        "##PROCESSES##\n"
        "##END##\n"
    )
    leak = _parse_probe_output("worker", stdout)
    assert not leak.is_empty()
    assert leak.tempdirs == ["/tmp/asm-aaaaaaaa"]
    assert leak.container_ids == []
    assert leak.process_pids == []


def test_parse_probe_ignores_unknown_section_markers() -> None:
    """A future addition of section markers must not break older
    parsers. Lines outside known sections are silently dropped.
    """
    stdout = (
        "##TEMPDIRS##\n"
        "/tmp/asm-deadbeef\n"
        "##FUTURE_NEW_SECTION##\n"
        "irrelevant content\n"
        "##CONTAINERS##\n"
        "##PROCESSES##\n"
        "##END##\n"
    )
    leak = _parse_probe_output("worker", stdout)
    assert leak.tempdirs == ["/tmp/asm-deadbeef"]


def test_format_report_includes_wrapper_pointer() -> None:
    """The FAIL message must point at the wrapper script so the
    operator has a starting place. Per the task spec: "Pointer to
    crates/dynrunner-pyo3/src/slurm/wrapper_script.rs for 'Known
    issues'" — the pyo3 file is a thin shim, so the pointer is to
    the canonical Rust generator under dynrunner-slurm.
    """
    leak = _WorkerLeak(
        hostname="slurm-worker1",
        tempdirs=["/tmp/asm-deadbeef"],
        containers=[],
        container_ids=[],
        processes=[],
        process_pids=[],
    )
    lines = _format_worker_leak_report("phase-deps", [leak])
    joined = "\n".join(lines)
    assert "FAIL: phase-deps" in joined
    assert "wrapper_script.rs" in joined
    assert "slurm-worker1" in joined
    assert "/tmp/asm-deadbeef" in joined


def test_format_report_groups_by_worker() -> None:
    """Multi-worker leaks group lines by hostname so the operator
    sees which compute nodes need attention. The container line
    surfaces ID + name + image + status verbatim from the probe.
    """
    leaks = [
        _WorkerLeak(
            hostname="slurm-worker1",
            tempdirs=["/tmp/asm-aaaaaaaa"],
            containers=[],
            container_ids=[],
            processes=[],
            process_pids=[],
        ),
        _WorkerLeak(
            hostname="slurm-worker2",
            tempdirs=[],
            containers=[
                "abc asm-bbbbbbbb-sec-01 testimg:latest Up 1 minute",
            ],
            container_ids=["abc"],
            processes=[],
            process_pids=[],
        ),
    ]
    lines = _format_worker_leak_report("phase-deps", leaks)
    joined = "\n".join(lines)
    assert "slurm-worker1" in joined
    assert "/tmp/asm-aaaaaaaa" in joined
    assert "slurm-worker2" in joined
    assert "asm-bbbbbbbb-sec-01" in joined
