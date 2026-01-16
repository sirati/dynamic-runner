import os
import subprocess


def estimate_memory(binary_size: int) -> int:
    """Estimate memory consumption: 100MB + 10*binary_size + binary_size^2 / 100MB."""
    base = 100 * 1024 * 1024  # 100MB
    linear = 10 * binary_size
    quadratic = (binary_size * binary_size) // (100 * 1024 * 1024)
    return base + linear + quadratic


def get_actual_memory_usage() -> int:
    """Get current process tree memory usage."""
    try:
        pid = os.getpid()
        result = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
        if result.returncode == 0:
            # RSS is in KB
            return int(result.stdout.strip()) * 1024
    except Exception:
        pass
    return 0
