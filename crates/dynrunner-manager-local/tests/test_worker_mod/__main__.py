"""Minimal test worker module for Rust integration tests.

Speaks the dynamic_queue protocol:
  Manager -> Worker: "<relative_path>\n" or "stop\n"
  Worker -> Manager: "ready\n", "done:W:F\n", "error:TYPE:MSG\n"

Supports two connection modes:
  --dynamic_queue <fd>      : socketpair mode (inherited file descriptor)
  --socket-path <path>      : named socket mode (connect to Unix domain socket)
"""
import argparse
import socket
import time


def run_protocol(conn):
    """Run the worker protocol on an established connection."""
    conn.sendall(b"ready\n")

    buf = b""
    while True:
        try:
            data = conn.recv(4096)
        except Exception:
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
            # Any non-stop command is a relative path to process
            time.sleep(0.01)
            conn.sendall(b"done:0:0\n")

    conn.close()


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dynamic_queue", type=int)
    group.add_argument("--socket-path", type=str)
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--log-file", type=str)
    parser.add_argument("--skip_existing", action="store_true")
    args, unknown = parser.parse_known_args()

    if args.dynamic_queue is not None:
        # Socketpair mode: use inherited file descriptor
        conn = socket.socket(fileno=args.dynamic_queue)
    else:
        # Named socket mode: connect to Unix domain socket
        conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        # Wait for socket file to appear (with timeout)
        import os
        timeout = 10
        start = time.time()
        while not os.path.exists(args.socket_path):
            if time.time() - start > timeout:
                raise TimeoutError(
                    f"Socket {args.socket_path} did not appear within {timeout}s"
                )
            time.sleep(0.05)
        conn.connect(args.socket_path)

    run_protocol(conn)


if __name__ == "__main__":
    main()
