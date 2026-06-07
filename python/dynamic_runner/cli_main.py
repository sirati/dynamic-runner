"""The one-call CLI entry point built on the composable framework surface.

Single concern: assemble the framework parser + a consumer's optional flags
+ the task's own flags, parse an explicit argv, resolve the task (directly
or via a factory chosen from the parsed args), and hand off to
:func:`dynamic_runner.run.run`. This is the framework's OWN entrypoint
shape — `python -m dynamic_runner` and any consumer one-liner go through
here, so there is one argv→run implementation, not two.

The split between "consumer-selector flags" (parsed first, may pick the
task via a factory) and "task flags" (registered once the task is known) is
owned HERE, using :func:`dynamic_runner.cli.add_framework_arguments`'s
flag-knowledge — so a consumer never does `parse_known_args` surgery or a
`sys.argv` rewrite of its own.
"""

from __future__ import annotations

import argparse
import sys
from typing import Callable, Union

from .cli import add_framework_arguments, build_arg_parser, validate_parsed_args
from .deployment_spec import TaskDeploymentSpec
from .logging_setup import setup_logging
from ._forwarded_argv import filter_framework_argv
from .run import dispatch as _dispatch
from .run import make_reparse_finalizer as _make_reparse_finalizer
from .run import relax_required as _relax_required
from .task_protocol import TaskDefinition

TaskOrFactory = Union[
    TaskDefinition,
    Callable[[argparse.Namespace], TaskDefinition],
]


def cli_main(
    task_or_factory: TaskOrFactory,
    *,
    add_consumer_args: Callable[[argparse.ArgumentParser], None] | None = None,
    deployment: TaskDeploymentSpec | None = None,
    description: str = "Dynamic batch processing with memory-aware parallel execution",
    argv: list[str] | None = None,
) -> None:
    """Run a task from the command line in one call.

    Args:
        task_or_factory: A :class:`TaskDefinition`, OR a
            ``Callable[[Namespace], TaskDefinition]`` that picks the task
            from the parsed args (e.g. asm-tokenizer's ``--task`` dispatch
            choosing tokenize / unify-vocab / build-memmap / full-pipeline).
        add_consumer_args: Optional hook to register consumer-owned flags
            (e.g. the ``--task`` selector) onto the parser BEFORE the first
            parse, so a factory can read them.
        deployment: Task-package deployment metadata, forwarded to
            :func:`run`.
        description: argparse help description.
        argv: Explicit argv slice. Defaults to ``sys.argv[1:]`` — reading
            the process command line is the ONE legitimate place to do so
            (this is the program's entry point); everything downstream
            takes the explicit slice, never global ``sys.argv``.

    Flag-separation: consumer flags (registered via ``add_consumer_args``)
    and the task selector are parsed first; ONLY the framework+task argv is
    forwarded to secondaries. The framework derives the forward-set from its
    own registered flags, so consumer flags are excluded automatically.
    """
    raw = list(sys.argv[1:] if argv is None else argv)

    # Phase 1 — parse framework + consumer flags to resolve the task. A
    # factory needs the consumer's selector flags; a plain task does not,
    # but parsing the same way keeps one code path. `parse_known_args`
    # leaves task-specific flags in `task_argv` for phase 2.
    selector_parser = argparse.ArgumentParser(description=description, add_help=False)
    add_framework_arguments(selector_parser)
    if add_consumer_args is not None:
        add_consumer_args(selector_parser)
    selector_ns, _ = selector_parser.parse_known_args(raw)

    task = (
        task_or_factory(selector_ns)
        if callable(task_or_factory) and not isinstance(task_or_factory, TaskDefinition)
        else task_or_factory
    )

    # Phase 2 — register the chosen task's flags and parse the full argv.
    # Consumer flags are re-registered so the full parse accepts them (they
    # validate but are not forwarded). The framework+task argv to forward is
    # `raw` minus consumer flags — derived by re-running the framework+task
    # parser over `raw` with `parse_known_args` and keeping what it accepts.
    parser = build_arg_parser(description)
    task.add_task_arguments(parser)
    if add_consumer_args is not None:
        add_consumer_args(parser)
    if selector_ns.secondary:
        # SECONDARY boot: the regenerated secondary argv carries only the
        # framework-regenerated flags; the task-specific run-config arrives over
        # the mesh AFTER connect. Relax task-arg `required` so a required-arg
        # task (e.g. asm-tokenizer `build_memmap --unified-vocab`) boots without
        # `SystemExit(2)`; the STRICT parse + `validate_parsed_args` is deferred
        # to the finalize over `[*boot_argv, *forwarded_argv]` (the SUBMITTER
        # path keeps the strict full parse + validation — it has all args).
        _relax_required(parser)
    args = parser.parse_args(raw)
    if not selector_ns.secondary:
        validate_parsed_args(args, parser)

    # The forward-set is the framework+task argv with consumer flags
    # removed. A framework+task-only parser recognises exactly that subset;
    # `filter_framework_argv` then drops the regenerated/submitter-local
    # framework flags. Consumer flags never reach the secondary.
    framework_task_argv = _framework_task_argv(task, raw, description)
    args.forwarded_argv = filter_framework_argv(framework_task_argv)

    # The secondary's deferred run-config finalize re-parses
    # `[*boot_argv, *delivered_forwarded_argv]`. The boot argv is the
    # framework+task subset (consumer flags excluded — the secondary's parser
    # carries only framework+task flags), so the re-parse splice matches the
    # secondary's argparse exactly. The framework owns this parse, so the
    # reparse finalizer (not the identity) is correct.
    args._boot_argv = list(framework_task_argv)
    args._finalize_run_config = _make_reparse_finalizer(task, description, args)

    setup_logging(args)
    _dispatch(task, args, deployment)


def _framework_task_argv(
    task: TaskDefinition, raw: list[str], description: str
) -> list[str]:
    """The framework+task subset of ``raw`` — consumer flags removed.

    Built by parsing ``raw`` with a parser carrying ONLY the framework and
    task flags (no consumer flags); the recognised tokens are reconstructed
    in original order. This is how the framework excludes consumer flags
    from the secondary-forward set using its own flag knowledge, with no
    consumer strip-set.
    """
    ft_parser = argparse.ArgumentParser(description=description, add_help=False)
    add_framework_arguments(ft_parser)
    task.add_task_arguments(ft_parser)
    # Tokens the framework+task parser does NOT recognise are consumer
    # flags; `parse_known_args` returns them in `extras`. Remove those
    # tokens (and any value they consumed) from `raw` while preserving the
    # order of the rest — argparse's `extras` are the literal unrecognised
    # tokens, so a set-membership walk reconstructs the kept subset.
    _, extras = ft_parser.parse_known_args(raw)
    extras_remaining = list(extras)
    kept: list[str] = []
    for token in raw:
        if extras_remaining and extras_remaining[0] == token:
            extras_remaining.pop(0)
            continue
        kept.append(token)
    return kept
