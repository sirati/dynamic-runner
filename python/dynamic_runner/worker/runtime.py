"""Framework-provided worker runtime.

Single concern: own the read/run/respond cycle of a worker process, so
consumer code only has to write the per-task body and raise typed
errors. Pre-Phase-R every consumer hand-rolled the loop (open the
socket, send Ready, read commands, dispatch, format error responses,
classify exceptions). The hand-rolled shape was both repetitive and
load-bearing in subtle ways: forgetting to send `ErrorResponse` on a
mid-task exception lets the worker process crash and the framework's
disconnect path takes over with default classification, which is a
strictly weaker signal than the worker's own `try/except`.

API surface (everything callers see):

    from dynamic_runner.worker import (
        run, task_function, Task, WorkerOutput,
        RecoverableError, NonRecoverableError,
    )

    @task_function
    def handle(task: Task) -> WorkerOutput | None:
        do_stuff(task.relative_path, task.payload)
        return WorkerOutput(warnings=2)

    if __name__ == "__main__":
        run()  # picks up the @task_function-decorated handler

`run(handle)` may also be called explicitly; the registry only acts as
a default when no explicit handler is passed.

Exception → wire mapping (load-bearing — see plan §D6):

* ``MemoryError``                  → ErrorResponse(OUT_OF_MEMORY) + exit
* ``RecoverableError``             → ErrorResponse(RECOVERABLE)
* ``NonRecoverableError``          → ErrorResponse(NON_RECOVERABLE)
* ``subprocess.CalledProcessError``→ ErrorResponse(RECOVERABLE)
* any other ``Exception``          → WorkerExceptionResponse(traceback,
                                       error_type=RECOVERABLE)
* ``KeyboardInterrupt``/``SystemExit`` mid-task → ErrorResponse(
                                       RECOVERABLE) then exit
* ``SIGTERM`` / ``SIGHUP``         → installed handler raises
                                       SystemExit, falling into the
                                       branch above. (SIGHUP also
                                       covers the publish-rename
                                       signal-deferral delivery and
                                       ssh-session teardown.)

The "default to RECOVERABLE for unclassified exceptions" choice is
symmetric with the Rust-side disconnect default (manager-local Phase D
flip): an unhandled bug retried on a fresh task is the safer default
than a permanent failure, because the retry-pass exhaustion logic
catches the actually-permanent case after MAX_RETRY_ATTEMPTS.
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import socket as _socket
import subprocess
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

# Module-level logger for the worker runtime's own diagnostic
# observability. Lives at `dynamic_runner.worker.runtime` so
# operators can filter via standard Python logging config. Default
# behaviour is unconfigured-logger silence; the framework's CLI
# entry points install a handler that routes worker log records to
# the `--log-file` argument.
_LOG = logging.getLogger(__name__)

from ..comm import (
    CommunicationInterface,
    DoneResponse,
    ErrorResponse,
    ErrorType,
    KeepaliveResponse,
    NamedSocketInterface,
    PhaseUpdateResponse,
    ProcessBinaryCommand,
    ReadyResponse,
    StopCommand,
    UnixSocketInterface,
    WorkerExceptionResponse,
)


class RecoverableError(Exception):
    """Raised by a task handler to signal a recoverable failure.

    The framework retries the task per the primary's retry-pass
    budget; the worker process stays alive for the next task.
    """


class NonRecoverableError(Exception):
    """Raised by a task handler to signal a non-recoverable failure.

    The framework will not retry this task, and the worker process is
    restarted on the next assignment.
    """


@dataclass
class Task:
    """A single task delivered to the worker.

    ``relative_path`` is the worker-facing identifier passed verbatim
    by the framework — for file-based tasks it's a real path the
    worker opens; for ``uses_file_based_items=False`` tasks (FR-2)
    it's an opaque identifier the worker resolves however it wants.

    ``resolved_path`` is the secondary's locally-resolved on-disk
    location when the file lives outside the worker's configured
    source dir (extraction-cache hit / pre-staged shared mount).
    ``None`` means "open ``relative_path`` against the configured
    source dir" — the legacy behaviour. When set, the handler
    should open this path directly while still using
    ``relative_path`` as the identity / output-mirroring key.
    The convenience property ``open_path`` returns
    ``resolved_path`` when set, else ``relative_path``, so most
    handlers can stay path-agnostic.

    ``payload`` is the parsed JSON value attached to ``TaskInfo.payload``,
    or ``None`` if the task carries no payload. ``payload_str`` is
    the raw JSON string for handlers that need it verbatim (e.g. to
    pass it through to a subprocess).

    ``keepalive()`` and ``set_phase(name)`` emit live signaling
    responses to the manager. They gate the manager's
    ``stage_timeouts`` enforcement and stuck-worker reporting (see
    ``manager-local`` ``check_timeouts`` / ``report_stuck_workers``):
    without a phase update, no per-phase timeout fires; without
    keepalives, even a phase-emitting worker is killed at the
    timeout boundary. Calls are no-ops when the task was constructed
    outside the runtime loop (no ``_emit`` hook wired) so unit-test
    handlers can construct ``Task`` directly without side-effects.

    ``publish()`` / ``publish_all()`` atomically deliver staged files
    to their destination. See ``dynamic_runner.worker.publish`` for
    the full contract and env-var configuration. These methods are
    process-state, not task-state — they work whether or not
    ``_emit`` is wired, so unit-test handlers can call them directly.

    ``predecessor_outputs`` carries the keyed outputs of every direct
    (and, when ``TaskDep.inherit_outputs`` is set, transitive)
    predecessor in this task's dependency graph. Shape:
    ``{predecessor_task_id -> {output_key -> {"kind": "inline"|"file",
    "value": str}}}``. ``kind`` discriminates between an inline string
    value and a post-publish destination path (a file on the shared
    mount); ``value`` is the string in both cases, and the wire format
    intentionally preserves ``kind`` so consumers can branch without
    guessing whether a string happens to look like a path. The dict
    is empty for tasks with no deps or whose predecessors produced no
    outputs. The framework populates this at task construction from
    the ``ProcessBinaryCommand.predecessor_outputs_json`` field set by
    the dispatcher; direct construction in tests defaults to ``{}``.

    ``publish_string(key, value)`` records an inline string output
    under ``key``; ``publish(src, dst, key=k)`` records a file output
    at the post-publish destination ``dst``. Both accumulate into
    ``_outputs_accumulator``, which the runtime flushes into
    ``DoneResponse.result_data`` on task return. Consumers reach the
    accumulated values via the dependent task's
    ``predecessor_outputs`` dict.
    """

    relative_path: str
    payload: Any = None
    payload_str: Optional[str] = None
    resolved_path: Optional[str] = None
    predecessor_outputs: dict[str, dict[str, dict[str, str]]] = field(
        default_factory=dict
    )
    _emit: Optional[Callable[[Any], None]] = field(default=None, repr=False)
    # {output_key -> {"kind": "inline"|"file", "value": str}}. Built up
    # by publish_string and publish(key=...); flushed into
    # DoneResponse.result_data on task return. Lives on the Task so
    # the loop's post-handler code can read it; the underlying
    # `publish.publish` call in `dynamic_runner.worker.publish` stays
    # untouched (the accumulator side-effect lives at the Task wrapper).
    _outputs_accumulator: dict[str, dict[str, str]] = field(
        default_factory=dict, repr=False
    )

    @property
    def open_path(self) -> str:
        """Path the handler should open: ``resolved_path`` when set,
        else ``relative_path``. Use this in handlers that don't need
        to distinguish the two (the common case).
        """
        return self.resolved_path if self.resolved_path is not None else self.relative_path

    def keepalive(self) -> None:
        if self._emit is not None:
            self._emit(KeepaliveResponse())

    def set_phase(self, phase_name: str) -> None:
        if self._emit is not None:
            self._emit(PhaseUpdateResponse(phase_name=phase_name))

    def publish(self, src, dst=None, *, key: Optional[str] = None) -> None:
        from .publish import publish as _publish
        # `_publish` returns the resolved destination (either the
        # caller-supplied `dst` verbatim or `dst_root / (src - src_root)`
        # when omitted). The accumulator records that resolved path so a
        # downstream consumer reading `predecessor_outputs[...][key]`
        # sees the actual destination on the shared mount even when the
        # caller relied on the auto-derived dst. Destination resolution
        # is the single concern of `dynamic_runner.worker.publish`; the
        # Task wrapper only owns the keyed-outputs side-effect.
        resolved_dst = _publish(src, dst)
        if key is not None:
            self._outputs_accumulator[key] = {
                "kind": "file",
                "value": str(resolved_dst),
            }

    def publish_string(self, key: str, value: str) -> None:
        """Record an inline string output under ``key``.

        See :class:`Task` docstring for the merged-with-WorkerOutput
        wire shape. Consumers reach the value via the dependent task's
        ``predecessor_outputs[my_task_id][key]["value"]``.
        """
        self._outputs_accumulator[key] = {"kind": "inline", "value": value}

    def publish_all(self, *srcs) -> None:
        from .publish import publish_all as _publish_all
        _publish_all(*srcs)


@dataclass
class WorkerOutput:
    """Successful task result.

    ``warnings`` and ``filtered`` are consumer-facing convenience
    counters the runtime encodes as a JSON payload inside the wire's
    opaque ``result_data`` field. Both default to 0, which yields a
    bare ``done`` wire response (no payload). Any nonzero value
    triggers a ``done:<json>`` wire frame whose JSON shape is
    ``{"warnings": N, "filtered": M}``; the framework itself does
    not inspect those bytes — only the producing worker and any
    consuming primary that opts in to decoding them care.
    """

    warnings: int = 0
    filtered: int = 0


_DEFAULT_OUTPUT = WorkerOutput()


def _encode_done_payload(
    output: WorkerOutput,
    outputs_accumulator: dict[str, dict[str, str]],
) -> Optional[bytes]:
    """Convert a ``WorkerOutput`` plus a keyed-outputs accumulator
    into the opaque bytes the framework threads through
    ``DoneResponse.result_data``. The framework's wire contract is
    "anything richer than ``done`` vs ``error`` is consumer-defined
    opaque bytes" — see ``DoneResponse`` and the Rust ``codec.rs``.
    ``None`` here means "emit a bare ``done``"; a JSON object means
    "the consumer's primary may opt in to decoding the keys".

    Merge shape (post-keyed-outputs): emits a JSON object containing
    only the keys whose values are present. ``warnings`` and
    ``filtered`` are omitted when zero; ``outputs`` is omitted when
    the accumulator is empty. When all three are absent the function
    returns ``None`` so the wire bytes stay byte-identical to the
    pre-feature path for tasks that don't use any of these surfaces.
    The Rust-side decoder (``apply_tasks.rs``) reads ``outputs`` as
    ``TaskOutputs::default()`` when the key is absent, so a payload
    that only carries counters round-trips losslessly.
    """
    body: dict[str, Any] = {}
    if output.warnings:
        body["warnings"] = output.warnings
    if output.filtered:
        body["filtered"] = output.filtered
    if outputs_accumulator:
        body["outputs"] = outputs_accumulator
    if not body:
        # Preserves the byte-identical legacy wire shape: a task that
        # uses none of the WorkerOutput counters and never calls
        # publish_string / publish(key=...) emits the same bare-`done`
        # bytes it did pre-feature.
        return None
    return json.dumps(body).encode("utf-8")


_HandlerFn = Callable[[Task], Optional[WorkerOutput]]


@dataclass
class _Registry:
    """Module-level registry holding the most recently
    @task_function-decorated handler. Lookup is by ``__default__``
    only — multiple decorators in one process overwrite each other,
    matching the "one handler per worker module" convention.
    """

    default: Optional[_HandlerFn] = None
    overwritten: bool = False


_REGISTRY = _Registry()


def task_function(fn: _HandlerFn) -> _HandlerFn:
    """Mark a callable as the worker's task handler.

    Validates that ``fn`` is callable and registers it as the
    default handler that ``run()`` uses when called without an
    explicit ``handle`` argument. The decorator returns the function
    unchanged — there is no wrapping.

    Decorating a second function in the same process replaces the
    first as the default; the runtime emits a one-shot tracing-style
    note via ``_REGISTRY.overwritten`` so consumer code that does this
    by accident is debuggable.
    """
    if not callable(fn):
        raise TypeError(f"@task_function expects a callable, got {fn!r}")
    if _REGISTRY.default is not None and _REGISTRY.default is not fn:
        _REGISTRY.overwritten = True
    _REGISTRY.default = fn
    return fn


def _build_default_argparser() -> argparse.ArgumentParser:
    """The standard worker CLI: --dynamic_queue OR --socket-path,
    optional --log-file. Consumers needing extra args build their own
    parser, add these flags, and pass it to ``run(argparser=...)``.
    """
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dynamic_queue",
        type=int,
        metavar="SOCKET_FD",
        help="Receive tasks via socket file descriptor (anonymous socket).",
    )
    group.add_argument(
        "--socket-path",
        type=str,
        metavar="SOCKET_PATH",
        help="Receive tasks via named Unix socket at this path.",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to a per-worker log file (consumer-managed).",
    )
    return parser


def _open_comm(args: argparse.Namespace) -> CommunicationInterface:
    """Open the comm interface from parsed CLI args. Mirrors the
    consumer hand-rolled shape: --socket-path → NamedSocketInterface,
    --dynamic_queue (FD) → UnixSocketInterface over a socket built
    from the inherited file descriptor.
    """
    if getattr(args, "socket_path", None):
        return NamedSocketInterface(args.socket_path, is_server=False)
    fd = args.dynamic_queue
    sock = _socket.socket(fileno=fd)
    return UnixSocketInterface(sock)


def _parse_payload(payload_str: Optional[str]) -> Any:
    """Best-effort JSON-decode of the wire payload string. A payload
    that fails to decode is surfaced as the raw string so handlers
    that route opaque blobs (e.g. nix manifest paths) keep working.
    """
    if payload_str is None:
        return None
    try:
        return json.loads(payload_str)
    except (json.JSONDecodeError, ValueError):
        return payload_str


def _classify_exception(exc: BaseException) -> Optional[ErrorResponse]:
    """Map a known exception type to ``ErrorResponse``. Returns
    ``None`` for unknown exceptions; the caller routes those through
    ``WorkerExceptionResponse`` so the traceback survives the wire.
    """
    if isinstance(exc, MemoryError):
        return ErrorResponse(
            error_type=ErrorType.OUT_OF_MEMORY,
            error_message=str(exc) or "MemoryError",
        )
    if isinstance(exc, RecoverableError):
        return ErrorResponse(
            error_type=ErrorType.RECOVERABLE,
            error_message=str(exc) or "RecoverableError",
        )
    if isinstance(exc, NonRecoverableError):
        return ErrorResponse(
            error_type=ErrorType.NON_RECOVERABLE,
            error_message=str(exc) or "NonRecoverableError",
        )
    if isinstance(exc, subprocess.CalledProcessError):
        return ErrorResponse(
            error_type=ErrorType.RECOVERABLE,
            error_message=f"CalledProcessError: {exc}",
        )
    return None


# Catchable signals the worker translates into SystemExit so the
# loop's KeyboardInterrupt/SystemExit branches own every shutdown
# uniformly:
#
# * SIGTERM — the framework's graceful-stop signal.
# * SIGHUP  — ssh-session teardown (remote-podman dispatch: the
#   coordinator kills the per-secondary ssh, the session ends, and
#   the container's processes get SIGHUP — see
#   `packaging/remote_podman.py`). It also arrives via the
#   publish-rename signal-deferral: SIGHUP delivered during the
#   masked rename phase is held pending by the kernel and delivered
#   on unblock, where without a handler it would hit the default
#   terminate-without-cleanup. Installing the handler routes that
#   deferred SIGHUP through the same clean SystemExit path. Handling
#   the signal at the worker-process level does not stop the kernel
#   from delivering SIGHUP to the rest of the container/pod — this
#   only changes THIS process's disposition, so the wrapper's own
#   SIGHUP trap still fires.
#
# SIGINT is already SystemExit-equivalent in Python (KeyboardInterrupt),
# so no re-installation is needed for that signal.
_EXIT_SIGNALS = (signal.SIGTERM, signal.SIGHUP)


def _install_exit_signal_handlers() -> dict[int, Any]:
    """Translate the catchable exit signals into SystemExit so the
    loop's existing KeyboardInterrupt/SystemExit branches handle the
    shutdown uniformly. Returns a ``{signum: previous_handler}`` map
    so the caller can restore each on exit.

    Degrades gracefully per-signal: a signal that can't be installed
    (not on the main thread, subsystem unavailable) is simply skipped
    and absent from the returned map, so it follows the OS default
    (immediate kill) and the framework's disconnect-with-task path
    classifies it as Recoverable per Phase D.
    """

    def _raise_systemexit(signum, _frame):
        raise SystemExit(f"signal {signum}")

    previous: dict[int, Any] = {}
    for signum in _EXIT_SIGNALS:
        try:
            previous[signum] = signal.signal(signum, _raise_systemexit)
        except (ValueError, OSError):
            continue
    return previous


def _sweep_stale_publish_tmps() -> None:
    """Reap ``.publish-tmp`` leftovers a prior worker run left in the
    destination dir when it was hard-killed mid-publish. Run once at
    worker run-start, before any task is processed.

    The dest-root resolution and the own-host / dead-pid scoped sweep
    both live in ``dynamic_runner.worker.publish`` (and, underneath,
    the native crate) — this is a thin invocation that only owns the
    "log if anything was reaped" observability. A sweep failure must
    not block the worker from starting (a leftover temp is benign and
    the next run will retry), so any error is logged and swallowed.
    """
    from .publish import dst_root, sweep_stale_tmps

    try:
        reaped = sweep_stale_tmps(dst_root())
    except Exception as exc:  # noqa: BLE001 — best-effort startup hygiene
        _LOG.warning(
            "worker.runtime: stale .publish-tmp sweep failed at run-start "
            "(non-fatal): %s",
            exc,
        )
        return
    if reaped:
        _LOG.info(
            "worker.runtime: reaped %d stale .publish-tmp leftover(s) "
            "from a prior crashed run",
            reaped,
        )


@dataclass
class _RunCtx:
    """Loop-internal state: the comm channel, the user's handler,
    and a flag that the in-loop signal-aware exception handler can
    consult to decide whether emitting an error response is safe.
    """

    comm: CommunicationInterface
    handle: _HandlerFn
    task_in_flight: bool = False
    exit_after_response: bool = False
    last_send_failed: bool = False
    fatal_send_errors: list[str] = field(default_factory=list)
    # Whether at least one NonRecoverableError was raised by the
    # task handler during this run. Set when `_classify_exception`
    # produces an ErrorType.NON_RECOVERABLE response; consumed at
    # end-of-`run()` to decide the process exit code.
    #
    # The contract `run()` enforces: a worker that emitted a
    # NonRecoverableError MUST exit with a non-zero process exit
    # code so OS-level supervisors (slurm accounting, podman exit
    # watchers, parent-process waitpid via #41) can discriminate
    # "worker ran to clean completion" from "worker bailed via
    # non-recoverable error". The wire-protocol-level
    # ErrorResponse(NON_RECOVERABLE) reaches the manager
    # synchronously; the exit-code half of the contract reaches
    # whoever spawned the worker process.
    non_recoverable_emitted: bool = False


def _try_send(ctx: _RunCtx, response: Any) -> None:
    """Send a response, recording failures so the loop can decide to
    bail out cleanly when the channel is gone. Avoids raising from
    the send path — every classified-exception branch funnels
    through here, and we don't want the runtime to mask user errors
    with "broken pipe while reporting your error".
    """
    ok, err = ctx.comm.send_response(response)
    if not ok:
        ctx.last_send_failed = True
        if err is not None:
            ctx.fatal_send_errors.append(err)


def _process_one(ctx: _RunCtx, command: Any) -> bool:
    """Handle a single inbound command. Returns ``True`` to keep the
    loop running, ``False`` to break out (clean shutdown OR fatal
    state — the caller owns the post-loop close()).
    """
    if isinstance(command, StopCommand):
        return False

    if not isinstance(command, ProcessBinaryCommand):
        _try_send(
            ctx,
            ErrorResponse(
                error_type=ErrorType.NON_RECOVERABLE,
                error_message=f"unknown command shape: {type(command).__name__}",
            ),
        )
        return not ctx.last_send_failed

    task = Task(
        relative_path=command.relative_path,
        payload=_parse_payload(command.payload),
        payload_str=command.payload,
        resolved_path=command.resolved_path,
        # The PyO3 bridge guarantees `predecessor_outputs_json` is a
        # JSON object literal even when the dispatcher attached no
        # outputs (`"{}"` default in `PyProcessBinaryCommand::__new__`),
        # so `json.loads` always yields a dict — see the bridge's own
        # contract docstring at `commands.rs`.
        predecessor_outputs=json.loads(command.predecessor_outputs_json),
        _emit=lambda response, _ctx=ctx: _try_send(_ctx, response),
    )

    ctx.task_in_flight = True
    try:
        result = ctx.handle(task)
    except (KeyboardInterrupt, SystemExit) as exc:
        _try_send(
            ctx,
            ErrorResponse(
                error_type=ErrorType.RECOVERABLE,
                error_message=f"worker interrupted mid-task: {type(exc).__name__}",
            ),
        )
        return False
    except BaseException as exc:  # noqa: BLE001 — by design (see classify)
        classified = _classify_exception(exc)
        if classified is not None:
            _try_send(ctx, classified)
            # OOM is a process-level signal: the kernel may have
            # killed something we depend on, so exit and let the
            # framework restart the worker. Other classified errors
            # leave the worker alive for the next task.
            if classified.error_type == ErrorType.OUT_OF_MEMORY:
                return False
            # NonRecoverableError marks the worker as having
            # produced an error result for this task; the worker
            # stays alive for the next assignment (manager's
            # respawn contract handles the dead-worker case via
            # the broken-pipe path in the pool). Flag this on the
            # ctx so `run()`'s post-loop exit-code decision can
            # discriminate `clean shutdown` from `bailed via
            # non-recoverable error`. The wire-protocol-level
            # ErrorResponse already reached the manager; the
            # exit-code half of the contract reaches the parent
            # process.
            if classified.error_type == ErrorType.NON_RECOVERABLE:
                ctx.non_recoverable_emitted = True
                _LOG.info(
                    "worker.runtime: NonRecoverableError observed; "
                    "process exit code will be 1 at run() return "
                    "(message=%r last_send_failed=%s)",
                    classified.error_message,
                    ctx.last_send_failed,
                )
            return not ctx.last_send_failed
        # Unknown exception → ship the full traceback. Default to
        # RECOVERABLE per plan D6: an unhandled bug retried on a
        # fresh task is safer than a permanent fail; retry-pass
        # exhaustion catches the truly-permanent case.
        _try_send(
            ctx,
            WorkerExceptionResponse(
                exception_type=type(exc).__name__,
                exception_message=str(exc),
                traceback_str=traceback.format_exc(),
                error_type=ErrorType.RECOVERABLE,
            ),
        )
        return not ctx.last_send_failed
    else:
        output = result if result is not None else _DEFAULT_OUTPUT
        _try_send(
            ctx,
            DoneResponse(
                result_data=_encode_done_payload(
                    output, task._outputs_accumulator
                ),
            ),
        )
        return not ctx.last_send_failed
    finally:
        ctx.task_in_flight = False


def run(
    handle: Optional[_HandlerFn] = None,
    *,
    argparser: Optional[argparse.ArgumentParser] = None,
    on_args: Optional[Callable[[argparse.Namespace], None]] = None,
    comm: Optional[CommunicationInterface] = None,
    args: Optional[argparse.Namespace] = None,
) -> None:
    """Run the worker's read/run/respond loop.

    Arguments:
      handle: Task-handler callable. Falls back to the most
        recently ``@task_function``-decorated function if omitted.
      argparser: Custom ``argparse.ArgumentParser`` if the consumer
        needs extra CLI flags. Must accept ``--dynamic_queue`` /
        ``--socket-path``; the runtime opens the comm channel from
        whichever is set. Ignored if ``comm`` is passed.
      on_args: Hook invoked with the parsed namespace before the
        loop starts. Useful for setting up logging, validating
        consumer-specific flags, etc.
      comm: Override the comm channel directly — primarily for
        tests. Bypasses argparse + socket setup entirely.
      args: Pre-parsed ``argparse.Namespace`` — primarily for tests.
        If passed, ``argparser`` is ignored.

    The function returns when the framework signals shutdown
    (StopCommand, channel-close, SIGTERM, or a mid-task interrupt).
    The comm channel is always ``close()``-d before returning.
    """
    if handle is None:
        handle = _REGISTRY.default
    if handle is None:
        raise RuntimeError(
            "no @task_function-decorated handler registered, and no "
            "`handle` argument passed to run()"
        )

    if comm is None:
        if args is None:
            parser = argparser or _build_default_argparser()
            args = parser.parse_args()
        if on_args is not None:
            on_args(args)
        # Framework default: route this worker's logs to the per-worker
        # `--log-file` in the compact `{h:mm:ss} {LEVEL} W-<id>  {message}`
        # shape that matches the manager-side per-role files. A no-op when the
        # consumer already configured logging in `on_args` (it runs first and
        # wins) or when no `--log-file` was passed. Local import keeps the
        # logging concern out of the runtime module's import surface.
        from .logging_setup import setup_worker_logging

        setup_worker_logging(getattr(args, "log_file", None))
        comm = _open_comm(args)

    prev_handlers = _install_exit_signal_handlers()
    _sweep_stale_publish_tmps()
    ctx = _RunCtx(comm=comm, handle=handle)
    try:
        _try_send(ctx, ReadyResponse())
        if ctx.last_send_failed:
            return
        while True:
            try:
                command = comm.receive_command(blocking=True)
            except (KeyboardInterrupt, SystemExit):
                # No task in flight at this point — clean shutdown,
                # no error response needed (the framework's
                # disconnect path treats this as a normal worker
                # exit).
                break
            if command is None:
                break
            if not _process_one(ctx, command):
                break
            if ctx.last_send_failed:
                # Channel is gone. Continuing would just rack up
                # more send failures; let the framework's disconnect
                # path classify the worker.
                break
    finally:
        try:
            comm.close()
        except Exception:
            pass
        for signum, prev in prev_handlers.items():
            try:
                signal.signal(signum, prev)
            except (ValueError, OSError):
                pass

    # Exit-code contract: if the task handler raised
    # NonRecoverableError during this run, the worker process
    # exits with code 1 so OS-level supervisors (slurm accounting,
    # podman exit watchers, parent-process waitpid via #41) can
    # discriminate "worker ran to clean completion" from "worker
    # bailed via non-recoverable error". The wire-protocol-level
    # ErrorResponse(NON_RECOVERABLE) reached the manager earlier;
    # the process exit code reaches the parent process tree.
    #
    # SystemExit propagates past the finally block above without
    # disturbing comm.close() / signal-handler restoration — both
    # have already run.
    _LOG.info(
        "worker.runtime: run() loop exited (non_recoverable_emitted=%s "
        "last_send_failed=%s); about to choose exit code",
        ctx.non_recoverable_emitted,
        ctx.last_send_failed,
    )
    if ctx.non_recoverable_emitted:
        _LOG.info(
            "worker.runtime: raising SystemExit(1) per "
            "NonRecoverableError exit-code contract"
        )
        raise SystemExit(1)


__all__ = [
    "RecoverableError",
    "NonRecoverableError",
    "Task",
    "WorkerOutput",
    "task_function",
    "run",
]
