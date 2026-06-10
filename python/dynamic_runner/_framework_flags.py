"""The framework's knowledge of its OWN command-line flags.

Single concern: own the answer to "which option strings does the framework
register, and how is each classified for secondary-forwarding?". Every
other module that needs to reason about framework-vs-consumer flags
(`_forwarded_argv`, `run`, `cli_main`) asks here rather than carrying its
own list — so a consumer never maintains a strip-set.

The framework registers its flags in one place
(:func:`dynamic_runner.cli.add_framework_arguments`). This module derives
the canonical option-string set by introspecting a throwaway parser that
function populates, so the set can never drift from what argparse actually
accepts. The two *behavioural* sub-classifications below are the only hand-
maintained data, and they are deliberately tiny:

  * :data:`FRAMEWORK_REGENERATED_FLAGS` — value-taking framework flags the
    SLURM spawn paths emit afresh per-job (gateway-derived URL, secondary
    index, host-detected cores/memory, container bind-mount paths,
    per-node full-log dir). Forwarding them too would duplicate the flag
    and confuse the secondary's argparse, so they are dropped on forward.
  * :data:`SUBMITTER_LOCAL_FLAGS` — value-less framework flags that steer
    the *submitter* process only and must never reach a secondary. Today
    the sole member is ``--important-stdio-only``.

Everything else the framework registers (and every task flag a consumer
adds via ``task.add_task_arguments``) is forwarded verbatim — that is the
load-bearing behaviour for setup-promote discovery on the secondary.
Consumer-CLI-only flags (asm-tokenizer ``--task``; asm-dataset subcommand
args) are never in the framework+task argv the forward filter operates on,
so they are excluded with zero per-consumer configuration.
"""

from __future__ import annotations

import argparse
from functools import lru_cache

from .logging_setup import IMPORTANT_STDIO_ONLY_FLAG


# Value-taking framework flags the SLURM spawn paths regenerate from
# per-job state. `--full-log-dir` joins this set: the spawn paths forward
# it as `--full-log-dir=/app/log-network/{secondary_id}` (replacing the
# pre-existing `DYNRUNNER_FULL_LOG_DIR` env injection), so the dispatcher's
# own `--full-log-dir` (if any) must NOT also ride through.
#
# `--mem-manager-reserved` is in the same category: the dispatcher's value
# flows through the SLURM pipeline and the wrapper RE-RENDERS it afresh onto
# every secondary's launch line as `--mem-manager-reserved=<bytes>`
# (`slurm-wrapper/wrapper/src/podman_run.rs`). Dropping it on forward keeps
# the re-derived forward-set byte-identical across nodes (a cold-start
# secondary re-runs `filter_framework_argv` over its full argv, which carries
# the wrapper-injected copy) and avoids handing the secondary's argparse the
# flag twice.
#
# `--output-dir` is per-spawn state too: the `--multi-computer local`
# spawner emits it afresh per secondary (the dispatcher's resolved
# `--output`, so same-host secondaries publish into the operator's output
# directory), and inside the SLURM wrapper container the flag is absent on
# purpose — `SecondaryConfig.__new__` auto-resolves the publish target to
# the user-visible `/app/out-network` bind-mount. Forwarding a node's own
# value to OTHER nodes would both duplicate the flag and leak one host's
# filesystem view across hosts.
#
# NOTE — `--panik-file` is deliberately NOT in this set: the
# wrapper injects a node-local reaper sentinel under that SAME flag, but the
# operator's cluster-wide `--panik-file` paths legitimately ride
# `forwarded_argv` (the only channel that reaches secondaries). A string-level
# filter cannot tell the two values apart, so stripping `--panik-file` would
# break the operator's cluster panik; the leftover wrapper-injected reaper
# path is harmless (append/idempotent, node-local).
FRAMEWORK_REGENERATED_FLAGS: frozenset[str] = frozenset(
    {
        "--secondary",
        "--secondary-id",
        "--secondary-quic-port",
        "--src-network",
        "--output-dir",
        "--cores",
        "--max-memory",
        "--mem-manager-reserved",
        "--log-dir",
        "--full-log-dir",
    }
)


# Value-less `store_true` framework flags that are SUBMITTER-LOCAL: they
# steer the submitter process only and must never propagate to a secondary.
# `--important-stdio-only` arms LLM-wake stdio mode on the submitter; the
# secondary still gets `--full-log-dir` for its per-role file logs.
SUBMITTER_LOCAL_FLAGS: frozenset[str] = frozenset(
    {
        IMPORTANT_STDIO_ONLY_FLAG,
    }
)


@lru_cache(maxsize=1)
def framework_option_strings() -> frozenset[str]:
    """Every option string the framework registers, derived by
    introspecting :func:`dynamic_runner.cli.add_framework_arguments`.

    Built from a throwaway parser so the set is exactly what argparse
    accepts — it cannot drift from the registration site. Cached: the
    registration is pure and the result is immutable.
    """
    # Local import avoids an import cycle: `cli` imports nothing from here,
    # but keeping the dependency one-directional (this module → cli) is
    # cleaner than a top-level import either way.
    from .cli import add_framework_arguments

    parser = argparse.ArgumentParser(add_help=False)
    add_framework_arguments(parser)
    options: set[str] = set()
    for action in parser._actions:
        options.update(action.option_strings)
    return frozenset(options)
