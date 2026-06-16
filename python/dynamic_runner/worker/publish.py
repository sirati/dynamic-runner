"""Atomic stage→destination publish — Python-side glue.

Single concern: hand fully-explicit ``(src, dst)`` pairs to the
native publish primitives, supplying the ``src_root`` the native
layer needs for cross-FS/EXDEV staging and under-root validation.

The contract:

* The deployment layer mounts a *staging* directory at
  ``DYNRUNNER_PUBLISH_SRC_ROOT`` (default ``/app/out-tmp``) and a
  *destination* directory at ``DYNRUNNER_PUBLISH_DST_ROOT`` (default
  ``/app/out-network``). The slurm wrapper hard-codes these at the
  exact default paths via ``-v`` mounts; consumers in other
  deployment shapes (pure local dev, custom containerisation) can
  override either via env. ``src_root`` is read from env and passed
  through to the native call (it needs it for the cross-FS staging
  sibling placement and the under-root canonicalize check);
  ``dst_root`` is exposed via :func:`dst_root` for callers that want
  to build mirrored destinations themselves, and for the run-start
  stale-tmp sweep.
* A worker writes intermediate output anywhere under ``src_root``
  using a path scheme of its choosing.
* ``task.publish(src, dst)`` atomically delivers the file to the
  caller-chosen ``dst``. Cross-FS staging→destination is supported
  (typical: tmpfs → NFS) — the native crate handles the
  copy/fsync/rename fallback.
* ``task.publish_all(pairs)`` delivers EXACTLY the given ``(src,
  dst)`` pairs as one set-atomic transaction.

There is NO implicit src→dst mirroring: the destination is always
explicit. A caller that wants destinations mirrored from a
staging-relative layout builds the ``(src, dst)`` list itself (e.g.
``[(s, dst_root() / s.relative_to(src_root)) for s in srcs]``).

Failures surface as ``PublishError`` from the native module.
Consumers catch and decide; the framework does not auto-retry.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Tuple, Union

from .._native import PublishError as PublishError
from .._native import publish_all as _native_publish_all
from .._native import publish_one as _native_publish_one
from .._native import sweep_stale_tmps as _native_sweep_stale_tmps


PathLike = Union[str, os.PathLike]

ENV_SRC_ROOT = "DYNRUNNER_PUBLISH_SRC_ROOT"
ENV_DST_ROOT = "DYNRUNNER_PUBLISH_DST_ROOT"
DEFAULT_SRC_ROOT = "/app/out-tmp"
DEFAULT_DST_ROOT = "/app/out-network"


def _roots() -> tuple[Path, Path]:
    """Read the staging/destination roots from env, falling back to
    the slurm-wrapper defaults. Both reads are O(getenv); the
    indirection exists so tests can pin a tmpdir via env without
    touching module state.
    """
    src_root = os.environ.get(ENV_SRC_ROOT, DEFAULT_SRC_ROOT)
    dst_root = os.environ.get(ENV_DST_ROOT, DEFAULT_DST_ROOT)
    return Path(src_root), Path(dst_root)


def publish(src: PathLike, dst: PathLike) -> Path:
    """Atomically deliver ``src`` to the explicit ``dst``.

    See module docstring for the (src_root, dst_root) contract.
    ``dst`` is required — there is no implicit src_root→dst_root
    mirroring; a caller wanting a mirrored destination computes it
    from :func:`dst_root` and passes it explicitly. ``src_root`` is
    read from env and forwarded to the native call for its cross-FS
    staging and under-root validation.

    Raises ``PublishError`` on any failure (path validation,
    cross-FS copy, fsync, rename, unlink).

    Returns the resolved destination ``Path`` (the verbatim ``dst``).
    The return is the single source of truth for "where did this
    file land": callers wiring the destination into downstream
    bookkeeping (e.g. ``Task.publish(..., key=...)``'s keyed-outputs
    accumulator) capture this.
    """
    src_p, dst_p = Path(src), Path(dst)
    _native_publish_one(src_p, dst_p, _roots()[0])
    return dst_p


def publish_all(pairs: Iterable[Tuple[PathLike, PathLike]]) -> None:
    """Publish exactly the given ``(src, dst)`` pairs as one
    set-atomic staged transaction.

    Each pair is delivered verbatim — there is NO implicit
    src_root→dst_root mirroring. A caller wanting mirrored
    destinations builds the list itself (see module docstring).
    The single process-wide ``src_root`` (read once from env) is
    forwarded to the native batch for its cross-FS staging sibling
    placement and the under-root canonicalize check.

    The native layer stages ALL srcs (the slow cross-FS copies)
    before committing ANY of the renames back-to-back under a signal
    mask, so an interruption during staging leaves only
    ``.publish-tmp`` siblings — never a partial set of final files.
    The partial window shrinks to the masked, sub-millisecond rename
    phase the native layer owns. One call == one transaction over the
    whole list.

    This is the bulk file-move path; it does not feed the
    keyed-outputs accumulator — keyed outputs remain
    ``Task.publish(key=)`` / ``Task.publish_string``.

    An empty iterable is a no-op (the native batch over an empty list
    is trivially the empty transaction).
    """
    items: list[tuple[Path, Path]] = [
        (Path(src), Path(dst)) for src, dst in pairs
    ]
    if not items:
        return
    _native_publish_all(items, _roots()[0])


def sweep_stale_tmps(dest_root: PathLike) -> int:
    """Reap stale ``.publish-tmp`` siblings left in ``dest_root`` by a
    prior worker run that was hard-killed mid-rename-phase.

    Thin pass-through to the native sweep, which is own-host /
    dead-pid scoped (NFS-safe across secondaries sharing the dest).
    Returns the number of stale temps removed. A missing ``dest_root``
    is a no-op returning 0.
    """
    return _native_sweep_stale_tmps(Path(dest_root))


def dst_root() -> Path:
    """The configured destination root (``DYNRUNNER_PUBLISH_DST_ROOT``,
    falling back to the slurm-wrapper default). The single source of
    truth callers (e.g. the run-start sweep) use to find the dir
    where published files — and any crash-leftover temps — live.
    """
    return _roots()[1]


__all__ = [
    "ENV_SRC_ROOT",
    "ENV_DST_ROOT",
    "DEFAULT_SRC_ROOT",
    "DEFAULT_DST_ROOT",
    "PublishError",
    "publish",
    "publish_all",
    "sweep_stale_tmps",
    "dst_root",
]
