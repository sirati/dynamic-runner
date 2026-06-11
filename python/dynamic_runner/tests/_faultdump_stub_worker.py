"""Stub worker module for the worker fault-dump subprocess tests.

Runs the REAL ``dynamic_runner.worker.run`` entry — the code under test
is the runtime's faulthandler registration, so the subprocess must go
through the genuine entry point, not a hand-rolled protocol loop.

The handler is driven by the task payload (a JSON object):

* ``marker``  — path of a file to touch when the handler STARTS, so the
  parent test knows the task is in flight before it raises a signal.
* ``barrier`` — path the handler then polls for; the handler returns
  (Done) only once the parent creates it. This sequences
  "signal lands MID-TASK" deterministically: marker → parent signals →
  parent creates barrier → handler completes.
* ``abort``   — when truthy, the handler calls ``os.abort()`` (fatal
  SIGABRT) instead of completing — the fatal-signal dump case.

Kept minimal: no consumer logic, no extra CLI flags — the default
worker argparser (``--socket-path``/``--dynamic_queue`` + optional
``--log-file``) is exactly the surface under test.
"""

from __future__ import annotations

import os
import time

from dynamic_runner.worker import Task, run, task_function


@task_function
def handle(task: Task) -> None:
    payload = task.payload if isinstance(task.payload, dict) else {}
    if payload.get("abort"):
        os.abort()
    marker = payload.get("marker")
    if marker:
        with open(marker, "w") as fh:
            fh.write("in-flight\n")
    barrier = payload.get("barrier")
    if barrier:
        deadline = time.monotonic() + 30.0
        while not os.path.exists(barrier):
            if time.monotonic() > deadline:
                raise RuntimeError("barrier file never appeared")
            time.sleep(0.05)
    return None


if __name__ == "__main__":
    run()
