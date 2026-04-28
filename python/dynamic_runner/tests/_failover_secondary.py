"""Subprocess entrypoint for failover-test secondaries.

Reads `--secondary <primary_url> --secondary-id <id>
--secondary-quic-port <port>`, connects via RustSecondaryCoordinator,
and processes work until the kill-marker fires (if set) or the run
completes.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from pathlib import Path

import dynamic_runner as _rs


def _watch_kill_marker(secondary_id: str) -> None:
    """If the test wrote a die-at-secs file for this secondary, sleep
    that long and then SIGKILL ourselves. This simulates a hard crash.
    """
    marker_dir = os.environ.get("DB_FAILOVER_TEST_KILL_MARKER")
    if not marker_dir:
        return
    p = Path(marker_dir) / f"{secondary_id}.die_at_secs"
    if not p.exists():
        return
    try:
        delay = float(p.read_text().strip())
    except ValueError:
        return

    def _killer():
        time.sleep(delay)
        os.kill(os.getpid(), 9)

    threading.Thread(target=_killer, daemon=True).start()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secondary", required=True, help="primary URL (tcp://host:port)")
    parser.add_argument("--secondary-id", required=True)
    parser.add_argument("--secondary-quic-port", type=int, default=0)
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()

    _watch_kill_marker(args.secondary_id)

    cfg = _rs.SecondaryConfig(
        secondary_id=args.secondary_id,
        num_workers=2,
        max_resources=_rs.ResourceMap({"memory": 1 * 1024 * 1024 * 1024}),
    )

    from . import _failover_stub_worker

    src = Path("/tmp") / f"db-failover-{args.secondary_id}-src"
    out = Path("/tmp") / f"db-failover-{args.secondary_id}-out"
    src.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)

    _rs.run_secondary(
        cfg,
        args.secondary,
        _failover_stub_worker,
        args,
        str(src),
        str(out),
        skip_existing=args.skip_existing,
    )


if __name__ == "__main__":
    main()
    sys.exit(0)
