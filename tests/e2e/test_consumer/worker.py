"""Worker for the synthetic e2e consumer.

Single concern: produce one output file per dispatched task and deliver
it to the destination root via :func:`dynamic_runner.worker.publish`.

The worker is uniform across both phases — the per-phase behaviour is
selected by ``task.payload['kind']``:

* ``produce`` — read the source input file, write a small output to
  the staging root, publish it.
* ``consume`` — verify the producer's output already exists at the
  publish destination (proves the cross-phase / task-dep barrier
  worked), then write + publish its own output.

The worker does NOT branch on local-vs-slurm deployment. The publish
contract handles both: ``DYNRUNNER_PUBLISH_{SRC,DST}_ROOT`` env vars
let local tests redirect the staging tree to a tmpdir, while the
slurm wrapper sets them to ``/app/out-tmp`` and ``/app/out-network``
by default.
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
    DEFAULT_DST_ROOT,
    DEFAULT_SRC_ROOT,
    ENV_DST_ROOT,
    ENV_SRC_ROOT,
)


_logger = logging.getLogger(__name__)


def _staging_root() -> Path:
    """Where the worker writes intermediate output — one of the two
    paths :func:`dynamic_runner.worker.publish` reads from env.

    Reads the same env var the publish module does so a misconfigured
    deployment fails loud at the worker (clear error) rather than
    silently writing under the default ``/app/out-tmp`` and then
    publishing succeeding from there.
    """
    return Path(os.environ.get(ENV_SRC_ROOT, DEFAULT_SRC_ROOT))


def _destination_root() -> Path:
    """Where published files end up — used by ``consume`` tasks to
    verify the producer's output landed before doing their own work.
    """
    return Path(os.environ.get(ENV_DST_ROOT, DEFAULT_DST_ROOT))


def _resolve_source_path(task: Task, source_dir: Path) -> Path:
    """Resolve the source-side input file for ``task``.

    ``Task.open_path`` returns ``resolved_path`` when set (extraction-
    cache or pre-staged-source mode) and ``relative_path`` otherwise.
    The framework always emits relative paths in our discover_items
    (per the task_protocol contract), so the common case is
    ``source_dir / open_path``.
    """
    p = Path(task.open_path)
    if p.is_absolute():
        return p
    return source_dir / p


def _maybe_sleep() -> None:
    """Honor ``DYNRUNNER_E2E_TASK_SLEEP_S`` for distribution scenarios.

    The synthetic per-task work otherwise completes in <50ms, which is
    too short for the parallel-4-workers scenario: the first-online
    secondary grabs the entire queue before its peers finish their
    startup handshake. Setting this env to e.g. 0.5 makes each task
    take 500ms and forces multi-secondary distribution.
    """
    import time
    raw = os.environ.get("DYNRUNNER_E2E_TASK_SLEEP_S", "")
    if not raw:
        return
    try:
        secs = float(raw)
    except ValueError:
        return
    if secs > 0:
        time.sleep(secs)


def _expected_nonce(idx: int) -> str:
    """Deterministic per-produce-task nonce string.

    Both ``_produce`` (when it commits the value) and ``_consume`` (when
    it asserts the value) derive the expected string from this single
    helper so a future rename touches exactly one place.
    """
    return f"nonce-{idx}"


def _produce(task: Task, source_dir: Path) -> WorkerOutput:
    """Read the input, write an output, publish it."""
    payload = task.payload or {}
    idx = payload.get("idx")
    if idx is None:
        raise NonRecoverableError(
            f"produce task has no 'idx' in payload: {payload!r}"
        )
    _maybe_sleep()

    src = _resolve_source_path(task, source_dir)
    try:
        content = src.read_bytes()
    except OSError as e:
        raise NonRecoverableError(f"failed to read source {src}: {e}") from e

    staging = _staging_root()
    staging.mkdir(parents=True, exist_ok=True)
    out_name = f"produce-{idx}.out"
    out_path = staging / out_name
    # The actual byte content is irrelevant — the test only checks that
    # SOMETHING landed at the publish destination. Echoing the input
    # plus a tag still keeps the per-item content distinguishable for
    # post-mortem debugging.
    out_path.write_bytes(b"produce:" + content)
    _logger.info(
        "produce-%d: wrote %d bytes to %s, publishing", idx, out_path.stat().st_size, out_path
    )
    _publish(out_path)
    # Keyed-outputs exercise: emit a deterministic inline nonce so the
    # downstream consume task can assert the value-and-kind through the
    # framework's predecessor_outputs plumbing. Gated on the payload
    # flag (set by SyntheticTask.discover_items when --keyed-outputs)
    # so existing scenarios that use this consumer stay unchanged.
    if payload.get("keyed_outputs"):
        task.publish_string("nonce", _expected_nonce(idx))
    return WorkerOutput()


def _consume(task: Task, source_dir: Path) -> WorkerOutput:
    """Verify the producer's output published, then write + publish ours."""
    _maybe_sleep()
    payload = task.payload or {}
    idx = payload.get("idx")
    expected_producer_output = payload.get("expects_output")
    if idx is None or expected_producer_output is None:
        raise NonRecoverableError(
            f"consume task has malformed payload: {payload!r}"
        )

    dst_root = _destination_root()
    expected = dst_root / expected_producer_output
    if not expected.exists():
        # The framework's task_depends_on edge MUST gate dispatch so this
        # never fires. If it does, the e2e found a real bug in the
        # phase / task-dep machinery — fail loud and non-recoverable.
        raise NonRecoverableError(
            f"consume-{idx} ran but producer output {expected} is missing — "
            f"task-dependency barrier failed?"
        )
    producer_bytes = expected.read_bytes()

    # Keyed-outputs exercise: when the run was dispatched with the
    # keyed-outputs flag, the predecessor produce task committed an
    # inline "nonce" output. Verify the value AND the kind round-trip
    # cleanly. A mismatch indicates a regression somewhere on the
    # publish_string → result_data → cluster cache → dispatch →
    # predecessor_outputs_json → Task.predecessor_outputs chain.
    if payload.get("keyed_outputs"):
        _assert_predecessor_nonce(task, idx)

    staging = _staging_root()
    staging.mkdir(parents=True, exist_ok=True)
    out_name = f"consume-{idx}.out"
    out_path = staging / out_name
    out_path.write_bytes(b"consume:" + producer_bytes)
    _logger.info(
        "consume-%d: producer output ok (%d bytes), publishing %d bytes",
        idx,
        len(producer_bytes),
        out_path.stat().st_size,
    )
    _publish(out_path)
    return WorkerOutput()


def _assert_predecessor_nonce(task: Task, idx: int) -> None:
    """Verify ``task.predecessor_outputs`` carries produce-{idx}'s nonce.

    Single concern: the keyed-outputs read path. Fails loud with a
    detailed diagnostic so the operator can tell whether the predecessor
    key was missing, the inner key was missing, the kind was wrong, or
    the value drifted. Any of those discriminates a different layer of
    the publish_string → predecessor_outputs round-trip.
    """
    producer_id = f"produce-{idx}"
    producer_outputs = task.predecessor_outputs.get(producer_id)
    if producer_outputs is None:
        raise NonRecoverableError(
            f"consume-{idx}: predecessor_outputs missing "
            f"{producer_id!r} key; available keys: "
            f"{sorted(task.predecessor_outputs)}"
        )
    nonce_entry = producer_outputs.get("nonce")
    if nonce_entry is None:
        raise NonRecoverableError(
            f"consume-{idx}: predecessor_outputs[{producer_id!r}] "
            f"missing 'nonce' key; available keys: "
            f"{sorted(producer_outputs)}"
        )
    actual_value = nonce_entry.get("value")
    actual_kind = nonce_entry.get("kind")
    expected_value = _expected_nonce(idx)
    expected_kind = "inline"
    if actual_value != expected_value or actual_kind != expected_kind:
        raise NonRecoverableError(
            f"consume-{idx}: predecessor_outputs[{producer_id!r}]['nonce'] "
            f"mismatch — expected (kind={expected_kind!r}, "
            f"value={expected_value!r}), got (kind={actual_kind!r}, "
            f"value={actual_value!r})"
        )


@task_function
def handle(task: Task) -> WorkerOutput:
    """Dispatch on ``task.payload['kind']``.

    The per-phase paths are co-located in this file because they share
    the same single concern (run one synthetic unit of work + publish);
    splitting them across modules would just duplicate the publish
    plumbing.
    """
    payload = task.payload or {}
    kind = payload.get("kind")
    source_dir = _SOURCE_DIR
    if source_dir is None:
        raise NonRecoverableError(
            "worker source_dir not configured — did the framework "
            "forget to pass --source?"
        )
    if kind == "produce":
        return _produce(task, source_dir)
    if kind == "consume":
        return _consume(task, source_dir)
    raise NonRecoverableError(f"unknown task kind: {kind!r}")


# Module-level handle to the configured ``--source`` directory. The
# framework-injected worker CLI carries it; we pull it out in
# :func:`_on_args` so :func:`handle` can use it without re-parsing.
_SOURCE_DIR: Path | None = None


def _build_parser() -> argparse.ArgumentParser:
    """Worker CLI.

    The framework injects ``--source``, ``--output``, ``--log-file``,
    one of ``--dynamic_queue`` / ``--socket-path``, and
    ``--skip_existing`` into every spawned worker (see
    ``dynrunner-pyo3/src/subprocess_factory.rs::legacy_argv``). We
    declare exactly those flags here plus our consumer-specific
    ``--num-tasks`` (forwarded by ``SyntheticTask.build_worker_command_args``).
    """
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dynamic_queue", type=int, metavar="SOCKET_FD")
    group.add_argument("--socket-path", type=str, metavar="SOCKET_PATH")
    parser.add_argument("--log-file", type=str, default=None)
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--skip_existing", action="store_true")
    parser.add_argument("--num-tasks", type=int, default=2)
    # Accept-and-ignore: SyntheticTask.build_worker_command_args may
    # forward --keyed-outputs so the worker process's argparser
    # tolerates it. The actual per-task branch reads
    # ``task.payload['keyed_outputs']`` set on the dispatcher side.
    parser.add_argument("--keyed-outputs", action="store_true")
    return parser


def _on_args(args: argparse.Namespace) -> None:
    global _SOURCE_DIR
    _SOURCE_DIR = Path(args.source)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [worker] %(levelname)s %(name)s: %(message)s",
    )
    _logger.info(
        "worker started: source=%s output=%s num_tasks=%d skip_existing=%s",
        args.source,
        args.output,
        args.num_tasks,
        args.skip_existing,
    )


def main() -> None:
    _run_worker(argparser=_build_parser(), on_args=_on_args)


if __name__ == "__main__":
    main()
