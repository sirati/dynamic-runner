"""Bounded retry for transient gateway transfer faults.

Single concern: shield idempotent gateway file copies from one-off
transport hiccups (an scp stream reset, a dropped ssh master frame).
Production showed a multi-GB many-blob upload dying at the dispatch
boundary because ONE `transfer_file` raised `OSError` — while an
immediately identical redispatch uploaded clean. The fault class is
transient; the policy lives here, once, and call sites wrap their
copy in :func:`retry_transient`.

## Why `OSError`

The Rust PyO3 gateway maps every transport-level `GatewayError` to
`OSError` (see `crates/dynrunner-pyo3/src/gateway/ssh.rs`,
`map_gateway_err`) — with the single exception of `NotConnected`,
which becomes `RuntimeError`. That split is exactly the retry
boundary we want: I/O faults are worth re-attempting, calling a
method before `connect()` is a programming error and must surface
immediately.

## What may be wrapped

Only idempotent operations — re-running the callable after a partial
first attempt must converge to the same end state. A file copy to a
fixed target qualifies (the second attempt overwrites); a remote
`mv` does NOT (a lost-reply success leaves no source for attempt
two). Never wrap non-idempotent or interactive operations.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Backoff taken between consecutive attempts. Total attempts is
# len(_BACKOFF_SECONDS) + 1 — i.e. (1s, 3s) means 3 attempts.
_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 3.0)

# See module docstring — OSError is the PyO3 gateway's transport-fault
# class; RuntimeError (NotConnected) deliberately stays out.
_TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (OSError,)


def retry_transient(fn: Callable[[], T], what: str) -> T:
    """Run `fn`, retrying transient faults with a short bounded backoff.

    Each retry is announced with a WARN naming `what` and the attempt
    number, so operators see flakiness without it being fatal. When
    the final attempt fails, the ORIGINAL exception propagates
    unchanged — callers and logs depend on its identity, never a
    wrapper type.

    `fn` must be idempotent (see module docstring).
    """
    attempts = len(_BACKOFF_SECONDS) + 1
    for attempt, backoff in enumerate(_BACKOFF_SECONDS, start=1):
        try:
            return fn()
        except _TRANSIENT_EXCEPTIONS as exc:
            logger.warning(
                "%s failed (attempt %d/%d): %s — retrying in %.0fs",
                what,
                attempt,
                attempts,
                exc,
                backoff,
            )
            time.sleep(backoff)
    # Final attempt: a failure here propagates unchanged.
    return fn()
