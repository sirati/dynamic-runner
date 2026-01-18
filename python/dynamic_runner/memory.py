import os
import subprocess


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


def get_free_system_memory() -> int:
    """Get free system memory in bytes."""
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    # MemAvailable is in KB
                    kb = int(line.split()[1])
                    return kb * 1024
    except Exception:
        pass
    return 0
