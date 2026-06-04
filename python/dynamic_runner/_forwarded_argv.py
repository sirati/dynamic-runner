"""Filter ``sys.argv[1:]`` for forwarding to a setup-promoted secondary.

Single concern: given the dispatcher's argv, return the subset the
secondary's argparse should re-parse on its own. The secondary uses
the same parser (``build_arg_parser + task.add_task_arguments``) so
anything that parsed on the dispatcher will re-parse identically on
the secondary — *except* two categories of flag this filter drops:

  * **framework-regenerated value flags** (``--secondary``,
    ``--secondary-id``, ``--secondary-quic-port``, ``--src-network``,
    ``--cores``, ``--max-memory``, ``--log-dir``) which the SLURM
    wrapper emits afresh per-job; forwarding them would duplicate the
    flag and confuse argparse, and

  * **submitter-local boolean flags** — flags that configure the
    *submitter's* behaviour only and must NOT change how a secondary
    runs. Today the sole member is ``--important-stdio-only``: the
    operator arms LLM-wake stdio mode on the submitter, but secondaries
    must keep their FULL logs for debugging (and post-relocation the
    operator's narrative comes from the observer reading the CRDT, not
    from secondaries' stdout). The literal is owned by the logging
    concern (:mod:`dynamic_runner.logging_setup`); this filter imports
    it so the two cannot drift.

The result is opaque to all downstream layers (SlurmPreparation,
SlurmJobManager, the Rust wrapper-script generator). Only this
module owns the filtering RULE; the classification of *which* flags
fall into each category is owned by
:mod:`dynamic_runner._framework_flags` (single source of truth, derived
from the framework's own flag registration), so the two cannot drift.
"""

from __future__ import annotations

from ._framework_flags import (
    FRAMEWORK_REGENERATED_FLAGS,
    SUBMITTER_LOCAL_FLAGS,
)


def filter_framework_argv(argv: list[str]) -> list[str]:
    """Drop framework-regenerated ``(flag, value)`` pairs from ``argv``.

    Each flag in :data:`FRAMEWORK_REGENERATED_FLAGS` takes a value, so
    it appears either as the two-token ``--flag VALUE`` form or the
    single-token ``--flag=VALUE`` form. Both shapes are recognised and
    elided; every other token is preserved verbatim.

    The input is assumed to be a well-formed argv slice (the result
    of ``sys.argv[1:]`` on a successfully-parsed invocation). No
    interpretation of unknown flags is attempted — they pass through
    unchanged for the secondary's argparse to handle.
    """
    out: list[str] = []
    i = 0
    n = len(argv)
    while i < n:
        token = argv[i]
        # Submitter-local boolean flags: value-less, drop the single
        # token. Checked first because these `store_true` flags carry no
        # value and must not trigger the value-pair drop below.
        if token in SUBMITTER_LOCAL_FLAGS:
            i += 1
            continue
        # `--flag=VALUE` form: single token, drop it whole.
        if "=" in token:
            head, _ = token.split("=", 1)
            if head in FRAMEWORK_REGENERATED_FLAGS:
                i += 1
                continue
        # `--flag VALUE` form: two tokens, drop both. Guarded by
        # bounds-check so a trailing bare flag (malformed argv) drops
        # only the flag itself rather than walking off the end.
        if token in FRAMEWORK_REGENERATED_FLAGS:
            i += 2 if i + 1 < n else 1
            continue
        out.append(token)
        i += 1
    return out
