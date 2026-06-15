"""Workload package for the #547 chunked-spawn e2e test.

Single concern: own the Python-side shape of the workload that drives a
>256-task ``spawn_tasks`` burst at the primary, so the shell harness
``test-547-chunking.sh`` can simply ``python -m test_547_workload.driver``
and grep the primary log afterwards. The Rust framework owns the actual
chunking; this package owns ONLY the workload shape that triggers it.
"""
