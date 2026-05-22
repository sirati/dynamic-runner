"""Atomic stage→destination publish — Python-side glue.

Single concern: resolve the staging contract (src_root → dst_root)
from the worker's environment and hand a fully-qualified
``(src, dst, src_root)`` triple to the native ``publish_one`` call.

The contract:

* The deployment layer mounts a *staging* directory at
  ``DYNRUNNER_PUBLISH_SRC_ROOT`` (default ``/app/out-tmp``) and a
  *destination* directory at ``DYNRUNNER_PUBLISH_DST_ROOT`` (default
  ``/app/out-network``). The slurm wrapper hard-codes these at the
  exact default paths via ``-v`` mounts; consumers in other
  deployment shapes (pure local dev, custom containerisation) can
  override either via env.
* A worker writes intermediate output anywhere under ``src_root``
  using a path scheme of its choosing.
* ``task.publish(src)`` mirrors the relative-from-src_root path
  under ``dst_root`` and atomically delivers the file. Cross-FS
  staging→destination is supported (typical: tmpfs → NFS) — the
  native crate handles the copy/fsync/rename fallback.
* ``task.publish(src, dst=...)`` lets the caller override the
  destination path when the staging shape doesn't mirror the
  destination shape (e.g. flatten, rename, project).

Failures surface as ``PublishError`` from the native module.
Consumers catch and decide; the framework does not auto-retry.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Union

from .._native import PublishError as PublishError
from .._native import publish_one as _native_publish_one


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


def _resolve(src: PathLike, dst: PathLike | None) -> tuple[Path, Path, Path]:
    """Compute the ``(src, dst, src_root)`` triple the native call
    expects. Mirrors ``src``'s position under ``src_root`` into
    ``dst_root`` when ``dst`` is omitted; otherwise honours the
    explicit ``dst`` verbatim.

    The caller-visible error here is only the no-explicit-dst case
    where ``src`` resolves outside ``src_root`` — surfaced as
    ``PublishError`` so the consumer's ``except`` clause covers
    both Python-side path-misuse and Rust-side I/O failures.
    """
    src_root, dst_root = _roots()
    src_path = Path(src)
    if dst is not None:
        return src_path, Path(dst), src_root

    # Resolve before relative_to so a symlink under src_root
    # doesn't bypass the check by canonicalising elsewhere — the
    # native layer also re-validates via canonicalize, so this
    # serves as the dst-derivation guardrail rather than the only
    # security check.
    try:
        src_resolved = src_path.resolve(strict=True)
        src_root_resolved = src_root.resolve(strict=True)
        rel = src_resolved.relative_to(src_root_resolved)
    except (FileNotFoundError, ValueError) as e:
        raise PublishError(
            f"cannot derive destination: {src_path} not under {src_root}: {e}"
        ) from e
    return src_path, dst_root / rel, src_root


def publish(src: PathLike, dst: PathLike | None = None) -> Path:
    """Atomically deliver ``src`` to its destination.

    See module docstring for the (src_root, dst_root) contract.
    Raises ``PublishError`` on any failure (path validation,
    cross-FS copy, fsync, rename, unlink).

    Returns the resolved destination ``Path`` that ``src`` was
    delivered to — the verbatim ``dst`` when the caller supplied
    one, otherwise the mirrored ``dst_root / (src - src_root)``.
    The return is single source of truth for "where did this
    file land": callers wiring the destination into downstream
    bookkeeping (e.g. ``Task.publish(..., key=...)``'s keyed-outputs
    accumulator) MUST capture this rather than re-deriving, since
    only this module owns the resolution contract.
    """
    src_p, dst_p, src_root_p = _resolve(src, dst)
    _native_publish_one(src_p, dst_p, src_root_p)
    return dst_p


def publish_all(*srcs: PathLike) -> None:
    """Publish multiple files in sequence. Stops at the first
    failure — the partial-success state is exactly what
    ``publish`` left behind on the file system: every src
    processed before the failure is delivered, the failing src
    and any after it are untouched. Consumers that want
    all-or-nothing semantics must compose that themselves
    (publish into a sibling dst tree, then atomically swap the
    tree root).
    """
    for src in srcs:
        publish(src)


__all__ = [
    "ENV_SRC_ROOT",
    "ENV_DST_ROOT",
    "DEFAULT_SRC_ROOT",
    "DEFAULT_DST_ROOT",
    "PublishError",
    "publish",
    "publish_all",
]
