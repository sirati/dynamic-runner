"""Worker subprocess for `test_phases.py`.

Speaks the manager-worker dispatch protocol (relative-path → `done`),
and on every dispatched item appends one JSON line to a shared log
file. The path the manager sends carries the item's
`(phase_id, type_id, affinity_id_or_NONE, index)` tag — encoded by
`_PhasedTask.discover_items` — so the post-mortem in the test sees
exactly what hit each worker.

The framework's subprocess factory builds the worker argv:

  python -m <worker_module>
    --dynamic_queue <fd>            # socketpair mode
    --source ... --output ...
    --log-file <output>/logs/.../worker_<id>.log
    [task-specific cmd_args appended by build_worker_command_args]

We append `--phases-log <path>` from `_PhasedTask.build_worker_command_args`
and parse the worker_id out of `--log-file` so each dispatch line
records who ran it.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import time
from pathlib import Path

# Field separator inside the encoded item path. Use a multi-character
# separator that cannot appear inside any field value the test passes
# (phase / type / affinity ids stick to letters + digits).
PATH_SEP = "--SEP--"
# Affinity sentinel for items with `affinity_id=None` — the path
# parser distinguishes this from a real affinity id of "NONE".
NONE_AFFINITY = "FREEPOOL"


def _parse_worker_id_from_log_file(log_file: str | None) -> str:
    if not log_file:
        return "?"
    m = re.search(r"worker_(\d+)\.log$", log_file)
    return m.group(1) if m else "?"


def _decode(relative_path: str) -> dict:
    """Reverse of `_PhasedTask.discover_items`'s path encoding."""
    name = Path(relative_path).name
    parts = name.split(PATH_SEP)
    if len(parts) != 4:
        return {"phase": "?", "type": "?", "affinity": "?", "index": -1, "raw": name}
    phase, ttype, affinity, index = parts
    return {
        "phase": phase,
        "type": ttype,
        "affinity": None if affinity == NONE_AFFINITY else affinity,
        "index": int(index) if index.isdigit() else -1,
    }


def _run_protocol(
    conn: socket.socket,
    log_path: Path,
    worker_id: str,
    kill_phase: str | None,
    kill_marker: Path | None,
) -> None:
    """Read commands, log dispatches, reply done. Optionally self-kill.

    `kill_phase` (when set) names a phase whose first item triggers
    ``os._exit(137)`` IF the shared `kill_marker` file does not yet
    exist. The worker creates the marker as a side effect of
    crashing — so only the first worker to hit the kill phase dies,
    and subsequent workers (including the respawned slot replaying
    the requeued item) sail through. Without this guard every K
    item would crash its worker, which exercises retry but not the
    targeted requeue path.

    We exit BEFORE replying `done`, so the manager observes a worker
    disconnect and re-queues the in-flight item.
    """
    conn.sendall(b"ready\n")
    buf = b""
    while True:
        try:
            data = conn.recv(4096)
        except OSError:
            break
        if not data:
            break
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            cmd = line.decode("utf-8").strip()
            if not cmd:
                continue
            if cmd == "stop":
                conn.close()
                return
            decoded = _decode(cmd)
            record = {
                "worker_id": worker_id,
                "phase": decoded["phase"],
                "type": decoded["type"],
                "affinity": decoded["affinity"],
                "index": decoded["index"],
                # `time.time()` (not `time.monotonic()`) is required:
                # each worker is a separate process and `monotonic()`
                # has a per-process origin, so cross-worker comparisons
                # are meaningless. Wall-clock time is safe enough for
                # phase-ordering assertions on a single host.
                "ts": time.time(),
                "pid": os.getpid(),
            }
            with open(log_path, "a") as f:
                f.write(json.dumps(record) + "\n")
                f.flush()
            if (
                kill_phase is not None
                and decoded["phase"] == kill_phase
                and kill_marker is not None
                and not kill_marker.exists()
            ):
                # First crash wins — touch the marker (best effort:
                # the file may not be flushed before _exit, but mkdir
                # + atomic open are cheap and racy at worst, which is
                # acceptable for a regression test).
                try:
                    kill_marker.parent.mkdir(parents=True, exist_ok=True)
                    kill_marker.touch()
                except OSError:
                    pass
                # Crash mid-dispatch: don't reply, exit hard.
                # The manager's worker-death path requeues the item.
                os._exit(137)
            # Tiny dispatch latency so the four-phase ordering test has a
            # non-zero gap between successive items (helps make the
            # `max(A.ts) <= min(B.ts)` assertion robust).
            time.sleep(0.01)
            conn.sendall(b"done:0:0\n")
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dynamic_queue", type=int)
    group.add_argument("--socket-path", type=str)
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--log-file", type=str)
    parser.add_argument("--skip_existing", action="store_true")
    parser.add_argument("--phases-log", type=str, required=True,
                        help="Shared dispatch log path (one JSON line per item).")
    parser.add_argument("--phases-kill-phase", type=str, default=None,
                        help="If set, exit hard on the first item of this phase.")
    parser.add_argument("--phases-kill-marker", type=str, default=None,
                        help=(
                            "Sentinel file path. The first worker to hit the "
                            "kill phase touches it and crashes; subsequent "
                            "workers see it and dispatch normally. Required "
                            "if --phases-kill-phase is set."
                        ))
    args, _unknown = parser.parse_known_args()

    worker_id = _parse_worker_id_from_log_file(args.log_file)
    log_path = Path(args.phases_log)
    kill_marker = Path(args.phases_kill_marker) if args.phases_kill_marker else None

    if args.dynamic_queue is not None:
        conn = socket.socket(fileno=args.dynamic_queue)
    else:
        conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        # Wait for socket file to appear.
        deadline = time.time() + 10
        while not os.path.exists(args.socket_path):
            if time.time() > deadline:
                raise TimeoutError(
                    f"Socket {args.socket_path} did not appear within 10s"
                )
            time.sleep(0.05)
        conn.connect(args.socket_path)

    _run_protocol(conn, log_path, worker_id, args.phases_kill_phase, kill_marker)
    return 0


if __name__ == "__main__":
    sys.exit(main())
