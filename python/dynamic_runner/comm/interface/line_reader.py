"""Single buffered line-framing reader over one connected socket.

Single concern: own ALL reads from a worker-side connected socket so
blocking command reads (the runtime's read/run/respond loop) and
non-blocking drains (``Task.poll_messages()`` mid-task) share ONE
buffer and line framing survives the interleaving.

Why this exists: the historical shape mixed a ``socket.makefile("r")``
buffered reader (blocking path) with raw ``recv(1024)`` calls
(non-blocking path). The two paths each hold private buffered state,
so interleaving them loses bytes — the file object's read-ahead is
invisible to ``recv`` and vice versa — and the raw path additionally
treated one ``recv`` chunk as exactly one line, which corrupts framing
the moment a frame spans chunks (a 100 KiB custom-message frame always
does) or two frames share a chunk. One reader, one buffer, correct
line framing in both modes.

The reader never parses: callers hand completed lines to
``parse_command`` / ``parse_response``. It is intentionally transport-
dumb — errors and EOF both surface as ``None`` after flushing any
buffered partial line, mirroring the prior ``readline()`` semantics.
"""
from __future__ import annotations

import socket


#: Per-read chunk size. Large enough that a max-size custom-message
#: frame (~180 KiB framed) needs only a few reads; small enough to not
#: matter allocation-wise.
_RECV_CHUNK = 65536


class SocketLineReader:
    """Buffered ``\\n``-framed reader over a connected socket."""

    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._buf = bytearray()
        #: Set once EOF was observed; further reads return buffered
        #: lines only.
        self._eof = False

    def read_line(self, blocking: bool = True) -> str | None:
        """Return the next complete line (including its newline), or
        ``None``.

        ``None`` means:
          * non-blocking and no COMPLETE line is buffered yet (any
            partial data stays buffered for the next call — framing is
            never lost), or
          * EOF / transport error with no buffered bytes (the
            channel-closed signal the runtime loop breaks on).

        At EOF with a buffered partial line, the partial line is
        surfaced once for a best-effort parse — the historical
        ``readline()`` behaviour the Rust framing layer mirrors too.
        """
        while True:
            newline = self._buf.find(b"\n")
            if newline >= 0:
                line = bytes(self._buf[: newline + 1])
                del self._buf[: newline + 1]
                return line.decode("utf-8", errors="replace")
            if self._eof:
                if self._buf:
                    line = bytes(self._buf)
                    self._buf.clear()
                    return line.decode("utf-8", errors="replace")
                return None
            try:
                self._sock.setblocking(blocking)
                try:
                    chunk = self._sock.recv(_RECV_CHUNK)
                finally:
                    if not blocking:
                        # Restore the historical always-blocking
                        # resting state for any other socket user.
                        self._sock.setblocking(True)
            except BlockingIOError:
                # Non-blocking and nothing available: the partial
                # frame (if any) stays buffered.
                return None
            except (BrokenPipeError, ConnectionResetError, OSError):
                # Transport error == channel closed; flush any
                # partial on the next iteration via the EOF branch.
                self._eof = True
                continue
            if not chunk:
                self._eof = True
                continue
            self._buf.extend(chunk)
