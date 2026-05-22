"""Worker for the inherit-outputs e2e consumer.

Single concern: run one synthetic task in the A->B->C chain, publish
its inline "nonce" via :meth:`Task.publish_string`, and (for B and C)
assert the framework surfaced the right keys in
``Task.predecessor_outputs``. Failure modes raise
:exc:`NonRecoverableError` so the framework exits non-zero and the
scenario's exit-code gate flips.

Dispatch shape::

    A           — no predecessors; publishes nonce "nonce-a"
    B           — predecessor A;          publishes nonce "nonce-b";
                  asserts predecessor_outputs[A] carries "nonce-a"
    C           — predecessors B (direct) + A (inherit_outputs=True);
                  asserts predecessor_outputs[B] carries "nonce-b"
                  AND   predecessor_outputs[A] carries "nonce-a"

C's assertion is the load-bearing one: without
``inherit_outputs=True`` on the C->A edge, A's outputs would be absent
from ``predecessor_outputs`` (the framework only surfaces direct
predecessors' outputs by default). A green C asserts both ends of the
transitive-ancestry chain.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from dynamic_runner.worker import (
    NonRecoverableError,
    Task,
    WorkerOutput,
    publish as _publish,
    run as _run_worker,
    task_function,
)
from dynamic_runner.worker.publish import (
    DEFAULT_SRC_ROOT,
    ENV_SRC_ROOT,
)


_logger = logging.getLogger(__name__)

_NONCE_KEY = "nonce"
_NONCE_KIND = "inline"


def _staging_root() -> Path:
    """Where the worker writes intermediate output — read from the
    same env var :func:`dynamic_runner.worker.publish` reads, so a
    misconfigured deployment fails loud here rather than silently
    writing under the default and re-publishing from there.
    """
    return Path(os.environ.get(ENV_SRC_ROOT, DEFAULT_SRC_ROOT))


def _resolve_source_path(task: Task, source_dir: Path) -> Path:
    """Resolve the source-side input file for ``task``.

    Mirrors the same helper in :mod:`tests.e2e.test_consumer.worker`.
    Centralising would create a cross-package shared utility module —
    not worth the indirection for a 4-line helper. The duplication is
    pinned to the path-resolution concern, not the topology / per-task
    logic where divergence matters.
    """
    p = Path(task.open_path)
    if p.is_absolute():
        return p
    return source_dir / p


def _expected_nonce(task_id: str) -> str:
    """Deterministic per-task nonce string.

    Both the producing side (``publish_string`` at end-of-handle) and
    the asserting side (downstream task's read of
    ``predecessor_outputs``) derive the expected string from this
    single helper so a future rename touches exactly one place.
    """
    return f"nonce-{task_id}"


def _write_and_publish(task_id: str, contents: bytes) -> None:
    """Write ``contents`` to the staging root under the canonical
    ``{task_id}.out`` filename and publish it.

    The actual bytes are irrelevant for the assertion logic (the
    scenario only checks file presence + non-zero size at the
    destination root); the worker echoes the inputs for post-mortem
    readability.
    """
    staging = _staging_root()
    staging.mkdir(parents=True, exist_ok=True)
    out_path = staging / f"{task_id}.out"
    out_path.write_bytes(contents)
    _logger.info(
        "task %s: wrote %d bytes to %s, publishing",
        task_id,
        out_path.stat().st_size,
        out_path,
    )
    _publish(out_path)


def _assert_predecessor_nonce(
    task: Task, current_task_id: str, predecessor_task_id: str
) -> None:
    """Assert ``task.predecessor_outputs[predecessor_task_id]`` carries
    the canonical nonce produced by ``predecessor_task_id``.

    Single concern: the keyed-outputs read path. Fails loud with a
    detailed diagnostic so a regression in the publish_string ->
    result_data -> cluster cache -> dispatch -> predecessor_outputs_json
    -> Task.predecessor_outputs chain points to a specific layer.

    Reused for both the B->A direct-predecessor read AND the C->A
    transitive-predecessor read; the assertion shape is identical (the
    framework surfaces both kinds in the same dict), the difference
    lives in the declarer's ``task_depends_on`` edge.
    """
    outputs = task.predecessor_outputs.get(predecessor_task_id)
    if outputs is None:
        raise NonRecoverableError(
            f"task {current_task_id}: predecessor_outputs missing "
            f"{predecessor_task_id!r}; available keys: "
            f"{sorted(task.predecessor_outputs)}"
        )
    entry = outputs.get(_NONCE_KEY)
    if entry is None:
        raise NonRecoverableError(
            f"task {current_task_id}: predecessor_outputs"
            f"[{predecessor_task_id!r}] missing {_NONCE_KEY!r}; "
            f"available keys: {sorted(outputs)}"
        )
    actual_value = entry.get("value")
    actual_kind = entry.get("kind")
    expected_value = _expected_nonce(predecessor_task_id)
    if actual_value != expected_value or actual_kind != _NONCE_KIND:
        raise NonRecoverableError(
            f"task {current_task_id}: predecessor_outputs"
            f"[{predecessor_task_id!r}][{_NONCE_KEY!r}] mismatch — "
            f"expected (kind={_NONCE_KIND!r}, value={expected_value!r}),"
            f" got (kind={actual_kind!r}, value={actual_value!r})"
        )


def _run_task(task: Task, source_dir: Path) -> WorkerOutput:
    """Common per-task body: read the input, write+publish the output,
    publish the nonce inline, plus the per-task assertions.

    Per-task differences (which predecessors to assert on) live in
    :data:`_ASSERTIONS` keyed by ``task_id``. Keeping the dispatch
    table inline here avoids an if-cascade over task ids — the
    contract is "every task does the same publish + publish_string
    work; only the assertion list differs".
    """
    task_id = (task.payload or {}).get("task_id")
    if task_id is None:
        raise NonRecoverableError(
            f"inherit-consumer task has no 'task_id' in payload: "
            f"{task.payload!r}"
        )

    # Assert THIS task's predecessors carry the right outputs. Looked
    # up by task_id so a new chain-stage just appends an entry to the
    # table without touching control flow.
    expected_predecessors = _ASSERTIONS.get(task_id, ())
    for predecessor_id in expected_predecessors:
        _assert_predecessor_nonce(task, task_id, predecessor_id)

    src = _resolve_source_path(task, source_dir)
    try:
        content = src.read_bytes()
    except OSError as e:
        raise NonRecoverableError(
            f"task {task_id}: failed to read source {src}: {e}"
        ) from e

    _write_and_publish(task_id, b"inherit:" + content)

    # Publish the canonical inline nonce so downstream tasks can
    # assert through predecessor_outputs. Uniform across A/B/C —
    # the difference between "what B publishes" and "what C reads"
    # is the dep-graph topology, not the publishing API.
    task.publish_string(_NONCE_KEY, _expected_nonce(task_id))
    return WorkerOutput()


# Per-task assertion table.
#
# Single concern: declare which predecessors each task must see in its
# ``predecessor_outputs``. The framework's contract is that direct
# predecessors are always present; ``inherit_outputs=True`` edges add
# transitive ancestors. So:
#
# * A   — root; no predecessors; no assertion.
# * B   — direct predecessor A; assert A's nonce surfaces.
# * C   — direct predecessor B AND transitive (inherit) predecessor A;
#         assert BOTH B's and A's nonces surface. C's pair is the
#         load-bearing inherit-outputs gate: without the
#         ``TaskDep("a", inherit_outputs=True)`` edge in
#         ``task.py``, the A entry would be absent.
_ASSERTIONS: dict[str, tuple[str, ...]] = {
    "a": (),
    "b": ("a",),
    "c": ("b", "a"),
}


@task_function
def handle(task: Task) -> WorkerOutput:
    """Dispatch every task through the uniform :func:`_run_task` body.

    The per-task behavioural difference (assertion set) is driven by
    the task_id-keyed :data:`_ASSERTIONS` table, not a control-flow
    branch in this function.
    """
    source_dir = _SOURCE_DIR
    if source_dir is None:
        raise NonRecoverableError(
            "worker source_dir not configured — did the framework "
            "forget to pass --source?"
        )
    return _run_task(task, source_dir)


# Module-level handle to the configured ``--source`` directory. The
# framework-injected worker CLI carries it; pulled out in :func:`_on_args`
# so :func:`handle` reads it without re-parsing.
_SOURCE_DIR: Path | None = None


def _build_parser() -> argparse.ArgumentParser:
    """Worker CLI.

    The framework injects ``--source``, ``--output``, ``--log-file``,
    one of ``--dynamic_queue`` / ``--socket-path``, and
    ``--skip_existing`` into every spawned worker. We declare exactly
    those flags here plus the consumer-specific ``--num-tasks``
    (accept-and-ignore: forwarded by
    :meth:`InheritSyntheticTask.build_worker_command_args` because the
    shared dispatch builder always emits it; the worker does not use
    it because the inherit chain has a fixed 3-task topology).
    """
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dynamic_queue", type=int, metavar="SOCKET_FD")
    group.add_argument("--socket-path", type=str, metavar="SOCKET_PATH")
    parser.add_argument("--log-file", type=str, default=None)
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--skip_existing", action="store_true")
    parser.add_argument("--num-tasks", type=int, default=3)
    return parser


def _on_args(args: argparse.Namespace) -> None:
    global _SOURCE_DIR
    _SOURCE_DIR = Path(args.source)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [worker] %(levelname)s %(name)s: %(message)s",
    )
    _logger.info(
        "inherit-worker started: source=%s output=%s skip_existing=%s",
        args.source,
        args.output,
        args.skip_existing,
    )


def main() -> None:
    _run_worker(argparser=_build_parser(), on_args=_on_args)


if __name__ == "__main__":
    main()
