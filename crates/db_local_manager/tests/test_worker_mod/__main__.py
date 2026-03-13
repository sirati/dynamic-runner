"""Minimal test worker module for Rust integration tests.

Speaks the dynamic_queue protocol:
  Manager -> Worker: "<relative_path>\n" or "stop\n"
  Worker -> Manager: "ready\n", "done:W:F\n", "error:TYPE:MSG\n"
"""
import argparse
import os
import socket
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dynamic_queue", type=int, required=True)
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--log-file", type=str)
    parser.add_argument("--skip_existing", action="store_true")
    args, unknown = parser.parse_known_args()

    sock = socket.socket(fileno=args.dynamic_queue)

    # Send ready
    sock.sendall(b"ready\n")

    buf = b""
    while True:
        try:
            data = sock.recv(4096)
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
                sock.close()
                return
            # Any non-stop command is a relative path to process
            time.sleep(0.01)
            sock.sendall(b"done:0:0\n")

    sock.close()


if __name__ == "__main__":
    main()
