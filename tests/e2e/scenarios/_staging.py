"""Per-scenario filesystem staging.

Single concern: shape the source/output/publish-{src,dst} tmpdir
quartet. Every scenario that uses the canonical
:mod:`tests.e2e.test_consumer` topology consumes this helper; scenarios
with bespoke needs (e.g. publish-atomic's giant input file) compose on
top of it.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ._base import DispatchPaths


def stage_inputs(
    tmp_root: Path,
    num_tasks: int,
    *,
    payload_size_bytes: int = 0,
    label: str = "",
) -> DispatchPaths:
    """Materialise N input files plus the four staging dirs.

    Parameters
    ----------
    tmp_root
        Per-scenario temp root (driver-allocated).
    num_tasks
        Number of ``input-{i}.txt`` files to create.
    payload_size_bytes
        Minimum size of each input file. The default zero produces
        a small textual payload; scenarios that want to stress
        upload bandwidth (publish-atomic kill-mid-write) bump this.
    label
        Optional sub-label for nested mkdtemp prefixes — useful when
        a scenario calls this twice for two different plans (e.g.
        already-done's initial vs rerun) so the dirs have telling
        names in ``--keep-tmp`` mode.
    """
    suffix = f"-{label}" if label else ""
    source = Path(tempfile.mkdtemp(prefix=f"src{suffix}-", dir=tmp_root))
    output = Path(tempfile.mkdtemp(prefix=f"out{suffix}-", dir=tmp_root))
    publish_src = Path(tempfile.mkdtemp(prefix=f"pubsrc{suffix}-", dir=tmp_root))
    publish_dst = Path(tempfile.mkdtemp(prefix=f"pubdst{suffix}-", dir=tmp_root))

    for i in range(num_tasks):
        body = f"input-{i}-payload\n".encode()
        if payload_size_bytes > len(body):
            # Pad to the requested size with a deterministic
            # filler. The worker doesn't care about the bytes —
            # only the size matters for upload-throughput tests.
            body = body + (b"x" * (payload_size_bytes - len(body)))
        (source / f"input-{i}.txt").write_bytes(body)

    return DispatchPaths(
        source=source,
        output=output,
        publish_src=publish_src,
        publish_dst=publish_dst,
    )


__all__ = ["stage_inputs"]
