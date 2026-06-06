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
    """Publish multiple files as one staged transaction.

    Resolves every ``src`` to its mirrored destination under
    ``dst_root`` up front (the ``src_root`` is process-wide, read
    once), then hands the whole ``[(src, dst), ...]`` batch to the
    native ``publish_all`` in a single call. The native layer stages
    ALL srcs (the slow cross-FS copies) before committing ANY of the
    renames back-to-back, so an interruption during staging leaves
    only ``.publish-tmp`` siblings — never a partial set of final
    files. The partial window shrinks to the masked, sub-millisecond
    rename phase the native layer owns.

    Destination derivation is identical to ``publish``: each ``src``
    must lie under ``src_root`` (else ``PublishError``). This is the
    bulk file-move path; it does not feed the keyed-outputs
    accumulator — keyed outputs remain ``Task.publish(key=)`` /
    ``Task.publish_string``.

    An empty call is a no-op (the native batch over an empty list is
    trivially the empty transaction).
    """
    if not srcs:
        return
    # _resolve returns the process-wide src_root identically for every
    # src (it's read from env once per call, same value). Capture it
    # from the first resolution and pass the (src, dst) pairs to the
    # native batch — the resolution contract stays owned here, the
    # transaction semantics stay owned by the native layer.
    items: list[tuple[Path, Path]] = []
    src_root: Path | None = None
    for src in srcs:
        src_p, dst_p, root_p = _resolve(src, None)
        src_root = root_p
        items.append((src_p, dst_p))
    _native_publish_all(items, src_root)


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
