//! Bounded wire framing for the manager-side response reader.
//!
//! Single concern: read one line-delimited response frame off an
//! `AsyncBufRead` transport WITHOUT letting a runaway frame wedge or
//! exhaust the manager, and map an over-limit frame onto the
//! protocol's EXISTING loud-failure vocabulary so no consumer above
//! the transport needs to know this guard exists.
//!
//! # Why this exists (#364)
//!
//! A worker that publishes an enormous inline output emits a `done:`
//! frame the size of that output. The API layer rejects oversize
//! values up front (`Task.publish_string` raises before anything hits
//! the wire — see `dynrunner_core::INLINE_VALUE_HARD_CAP_BYTES`), but
//! the wire reader must not TRUST the sender: a worker predating the
//! API check, a hand-rolled worker loop, or pathological multi-key
//! accumulation can still produce an arbitrarily large frame. The
//! failure mode this module forbids is the silent one — consuming the
//! frame and then dropping it with no reply leaves the worker wedged
//! forever and the task stranded (assigned-never-terminal).
//!
//! # Contract
//!
//! [`recv_response_bounded`] reads one `\n`-terminated frame up to
//! [`MAX_RESPONSE_FRAME_BYTES`]:
//!
//! * Under the cap → parsed via [`codec::parse_response`], byte-for-
//!   byte identical to the previous unbounded `read_line` path.
//! * Over the cap → the remainder of the line is DRAINED in fixed-size
//!   chunks (the oversize payload never accumulates in memory), the
//!   stream is left positioned at the next frame (recoverable), and a
//!   synthesized [`Response::Error`] with
//!   [`ErrorType::NonRecoverable`] naming the actual size and the
//!   limit is returned. The protocol state machine
//!   (`RunnerProtocol::poll_status`) already maps that onto
//!   `PollResult::Disconnected`, so the existing manager machinery
//!   fails the task loudly and releases/restarts the blocked worker —
//!   no special casing anywhere above this function.
//! * EOF with no buffered bytes → `None` (connection closed), EOF with
//!   a partial line → best-effort parse of the partial line; both
//!   preserve the prior `read_line` semantics.
//!
//! Commands (manager→worker) are NOT bounded here: dispatch frames are
//! produced by the framework itself and their inline-output content is
//! already capped upstream by the same per-value hard cap, so the
//! worker-side reader keeps the legacy unbounded read.

use dynrunner_core::ErrorType;
use tokio::io::AsyncBufReadExt;

use crate::codec;
use crate::command::Response;

/// Defense-in-depth cap on one manager-bound response frame (64 MiB).
///
/// Deliberately HIGHER than the API-level per-value publish limit
/// (`dynrunner_core::INLINE_VALUE_HARD_CAP_BYTES`, 16 MiB): the API
/// check is the contract surface workers see; this guard only catches
/// senders that bypassed it (legacy workers, hand-rolled loops,
/// many-key accumulation). 4× the per-value cap leaves generous room
/// for a multi-key accumulator plus JSON-escaping overhead while still
/// bounding the manager's per-frame memory and keeping the failure
/// loud instead of letting an absurd frame ride the mesh.
pub const MAX_RESPONSE_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// One bounded line-read outcome. Crate-internal: the public surface
/// is [`recv_response_bounded`] / [`ResponseFrameReader`], which map
/// these onto the protocol's response vocabulary.
enum BoundedLine {
    /// A complete (or EOF-truncated, matching `read_line`) frame under
    /// the cap.
    Line(String),
    /// The frame exceeded `cap`. `len` is the total line length
    /// observed while draining (best-effort: an EOF mid-drain reports
    /// the bytes seen up to it). The stream is positioned after the
    /// terminating newline (or at EOF).
    Oversize { len: usize },
    /// Clean EOF before any byte of a new frame.
    Eof,
}

/// Persistent partial-frame state for [`read_line_bounded`] — the
/// piece that makes the bounded read CANCEL-SAFE.
///
/// The accumulating buffer (and the mid-drain counter for an
/// oversize line) used to be locals of the read future; dropping the
/// future between `fill_buf` awaits (e.g. a losing `tokio::select!`
/// arm) destroyed the already-consumed prefix of the in-flight frame
/// and corrupted framing on the next read. Holding the state OUTSIDE
/// the future (on the transport, via [`ResponseFrameReader`])
/// preserves it across cancellation: every mutation happens
/// synchronously between awaits, `fill_buf` itself only peeks the
/// `BufRead`'s internal buffer, and `consume` is synchronous — so a
/// dropped future leaves `(reader, state)` at a consistent resume
/// point. This is the cancel-safety contract
/// `dynrunner_core::MessageReceiver` documents; the manager-side
/// socket transports need it because the per-task poll now races the
/// transport read against the custom-message outbox
/// (`RunnerProtocol::poll_status_with_custom_outbox`).
#[derive(Debug, Default)]
pub struct FrameReadState {
    /// Bytes of the current frame consumed so far (no newline yet).
    buf: Vec<u8>,
    /// `Some(total_bytes_seen)` while discarding the remainder of an
    /// over-cap line; the next call resumes the drain.
    drain: Option<usize>,
}

/// Read one `\n`-terminated line, accumulating at most `cap` bytes.
/// On overflow, switches to a drain loop that discards (but counts)
/// the rest of the line so the stream stays frame-aligned.
///
/// Cancel-safe with respect to `state`: see [`FrameReadState`].
async fn read_line_bounded<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    cap: usize,
    state: &mut FrameReadState,
) -> std::io::Result<BoundedLine> {
    // Resume an interrupted oversize-drain first (a cancelled call
    // may have left the discard loop mid-line).
    if let Some(mut total) = state.drain {
        loop {
            let chunk = reader.fill_buf().await?;
            if chunk.is_empty() {
                state.drain = None;
                return Ok(BoundedLine::Oversize { len: total });
            }
            let pos = chunk.iter().position(|&b| b == b'\n');
            let consume = match pos {
                Some(p) => p + 1,
                None => chunk.len(),
            };
            total += consume;
            reader.consume(consume);
            state.drain = Some(total);
            if pos.is_some() {
                state.drain = None;
                return Ok(BoundedLine::Oversize { len: total });
            }
        }
    }
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            // EOF. Preserve `read_line` semantics: no buffered bytes
            // means a clean close; a partial line is surfaced for a
            // best-effort parse.
            return if state.buf.is_empty() {
                Ok(BoundedLine::Eof)
            } else {
                Ok(BoundedLine::Line(into_utf8(std::mem::take(
                    &mut state.buf,
                ))?))
            };
        }
        let newline_pos = available.iter().position(|&b| b == b'\n');
        let take = match newline_pos {
            Some(pos) => pos + 1, // include the newline, like read_line
            None => available.len(),
        };
        if state.buf.len() + take > cap {
            // Overflow: drain the rest of the line without
            // accumulating it. Count what we already buffered plus
            // everything drained so the error can name the real size.
            let mut total = state.buf.len();
            state.buf = Vec::new();
            state.drain = Some(total);
            loop {
                let chunk = reader.fill_buf().await?;
                if chunk.is_empty() {
                    // EOF mid-drain (sender died mid-frame). Still
                    // report the oversize — loud beats silent, and the
                    // bytes seen already prove the violation.
                    state.drain = None;
                    return Ok(BoundedLine::Oversize { len: total });
                }
                let pos = chunk.iter().position(|&b| b == b'\n');
                let consume = match pos {
                    Some(p) => p + 1,
                    None => chunk.len(),
                };
                total += consume;
                reader.consume(consume);
                state.drain = Some(total);
                if pos.is_some() {
                    state.drain = None;
                    return Ok(BoundedLine::Oversize { len: total });
                }
            }
        }
        state.buf.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline_pos.is_some() {
            return Ok(BoundedLine::Line(into_utf8(std::mem::take(
                &mut state.buf,
            ))?));
        }
    }
}

/// Strict UTF-8 conversion matching `read_line`'s behaviour: invalid
/// UTF-8 is an `InvalidData` I/O error (the caller maps any I/O error
/// to a transport disconnect, exactly as before).
fn into_utf8(buf: Vec<u8>) -> std::io::Result<String> {
    String::from_utf8(buf).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        )
    })
}

/// Build the loud reject for an over-limit frame. Carries
/// [`ErrorType::NonRecoverable`] so `poll_status` routes it through
/// `PollResult::Disconnected` — task fails with this message, the
/// existing machinery releases/restarts the worker.
fn oversize_response(len: usize) -> Response {
    Response::Error {
        error_type: ErrorType::NonRecoverable,
        message: format!(
            "worker response frame of {len} bytes exceeds the protocol limit of \
             {MAX_RESPONSE_FRAME_BYTES} bytes; inline published outputs are capped \
             at {} bytes per value (Task.publish_string) — write bulk artifacts to \
             the staging dir and use Task.publish(src, key=...) instead",
            dynrunner_core::INLINE_VALUE_HARD_CAP_BYTES
        ),
    }
}

/// Receive one response frame with the [`MAX_RESPONSE_FRAME_BYTES`]
/// guard. Drop-in body for every manager-end transport's
/// `MessageReceiver<Response>::recv`:
///
/// * `None` — connection closed / transport error / unparseable frame
///   (unchanged classification; the NON-EOF causes are now logged, see
///   below).
/// * `Some(parsed)` — frame under the cap (unchanged).
/// * `Some(Error(NonRecoverable, …))` — frame over the cap; the stream
///   is drained past the offending line and left recoverable.
///
/// The silent-branch rule (#366 adjacent-hazard): every `None` used to
/// look like a clean EOF to the caller, so a PARSE failure (garbage on
/// the response stream) masqueraded as "worker disconnected" with zero
/// trace of the real cause. The collapse to `None` stays — the
/// protocol state machine's disconnect handling is the right recovery
/// either way — but the parse-failure and I/O-error paths now WARN
/// with the evidence (frame length + truncated prefix / the error)
/// before classifying, so a phantom-disconnect diagnosis takes one log
/// line instead of a packet capture. A clean EOF stays silent.
///
/// NOT cancel-safe: the partial-frame buffer is a per-call local
/// (fresh [`FrameReadState`] each invocation), so dropping the
/// returned future mid-frame loses the consumed prefix. Callers that
/// race this read in a `select!` must hold the state across calls —
/// use [`ResponseFrameReader`] instead (the manager-side socket
/// transports do).
pub async fn recv_response_bounded<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Option<Response> {
    let mut state = FrameReadState::default();
    ResponseFrameReader { state: &mut state }.recv(reader).await
}

/// Cancel-safe bounded response reader: borrows a caller-held
/// [`FrameReadState`] so a cancelled `recv` future resumes the same
/// in-flight frame on the next call. The manager-side transports own
/// one `FrameReadState` per connection and build this view per
/// `recv` call.
pub struct ResponseFrameReader<'a> {
    pub state: &'a mut FrameReadState,
}

impl ResponseFrameReader<'_> {
    /// Receive one response frame with the
    /// [`MAX_RESPONSE_FRAME_BYTES`] guard — same outcome mapping as
    /// [`recv_response_bounded`] (including the #366 loud non-EOF
    /// classification: parse failures and I/O errors WARN before
    /// collapsing to `None`; a clean EOF stays silent), plus
    /// cancellation safety via the borrowed state.
    pub async fn recv<R: tokio::io::AsyncBufRead + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> Option<Response> {
        match read_line_bounded(reader, MAX_RESPONSE_FRAME_BYTES, self.state).await {
            Ok(BoundedLine::Eof) => None,
            Ok(BoundedLine::Line(line)) => {
                let parsed = codec::parse_response(&line);
                if parsed.is_none() {
                    tracing::warn!(
                        frame_bytes = line.len(),
                        frame_prefix = %line.chars().take(128).collect::<String>(),
                        "worker response frame is unparseable (NOT a clean \
                         disconnect); treating the connection as broken"
                    );
                }
                parsed
            }
            Ok(BoundedLine::Oversize { len }) => Some(oversize_response(len)),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "I/O error reading worker response frame (NOT a clean \
                     disconnect); treating the connection as broken"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    /// Under-cap frames parse byte-identically to the legacy path.
    #[tokio::test]
    async fn under_cap_frame_parses() {
        let data = b"done:hello\n".to_vec();
        let mut reader = BufReader::new(std::io::Cursor::new(data));
        let resp = recv_response_bounded(&mut reader).await;
        match resp {
            Some(Response::Done { result_data }) => {
                assert_eq!(result_data.unwrap(), b"hello");
            }
            other => panic!("expected Done, got {other:?}"),
        }
    }

    /// Clean EOF maps to None (connection closed).
    #[tokio::test]
    async fn eof_maps_to_none() {
        let mut reader = BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        assert!(recv_response_bounded(&mut reader).await.is_none());
    }

    /// An over-cap line yields the synthesized NonRecoverable error
    /// naming the actual size and the limit, AND the stream is left
    /// frame-aligned: the next frame still parses (recoverability).
    #[tokio::test]
    async fn oversize_line_is_rejected_loudly_and_stream_recovers() {
        // Build: one line just over the cap, then a healthy frame.
        // Use the small-cap internal helper to avoid a >64MiB alloc;
        // the public-constant path is covered by the integration test
        // in dynrunner-transport-socket (real socketpair, real cap).
        let cap = 1024;
        let mut data = Vec::new();
        data.extend_from_slice(b"done:");
        data.extend_from_slice(&vec![b'A'; 2000]);
        data.push(b'\n');
        let oversize_line_len = data.len();
        data.extend_from_slice(b"ready\n");
        let mut reader = BufReader::new(std::io::Cursor::new(data));

        let first = read_line_bounded(&mut reader, cap, &mut FrameReadState::default())
            .await
            .unwrap();
        match first {
            BoundedLine::Oversize { len } => assert_eq!(len, oversize_line_len),
            BoundedLine::Line(_) | BoundedLine::Eof => {
                panic!("over-cap line must report Oversize")
            }
        }
        // Stream recovered: the next frame parses normally.
        let second = recv_response_bounded(&mut reader).await;
        assert!(matches!(second, Some(Response::Ready)));
    }

    /// EOF in the middle of draining an oversize line still reports
    /// the violation (loud beats silent).
    #[tokio::test]
    async fn eof_mid_drain_still_reports_oversize() {
        let cap = 64;
        let mut data = Vec::new();
        data.extend_from_slice(b"done:");
        data.extend_from_slice(&vec![b'A'; 500]);
        // No terminating newline: sender died mid-frame.
        let total = data.len();
        let mut reader = BufReader::new(std::io::Cursor::new(data));
        let out = read_line_bounded(&mut reader, cap, &mut FrameReadState::default())
            .await
            .unwrap();
        match out {
            BoundedLine::Oversize { len } => assert_eq!(len, total),
            BoundedLine::Line(_) | BoundedLine::Eof => {
                panic!("expected Oversize on EOF mid-drain")
            }
        }
    }

    /// The synthesized reject carries NonRecoverable (so poll_status
    /// routes it through Disconnected → restart) and names both the
    /// actual size and the limit.
    #[test]
    fn oversize_response_shape() {
        let resp = oversize_response(70_000_000);
        match resp {
            Response::Error {
                error_type,
                message,
            } => {
                assert_eq!(error_type, ErrorType::NonRecoverable);
                assert!(message.contains("70000000"));
                assert!(message.contains(&MAX_RESPONSE_FRAME_BYTES.to_string()));
                assert!(message.contains("publish"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    /// CANCEL-SAFETY pin for [`ResponseFrameReader`]: a `recv` future
    /// dropped mid-frame (after the first chunk of a multi-chunk line
    /// was consumed) must NOT lose the consumed prefix — the next
    /// `recv` against the SAME `FrameReadState` resumes and yields
    /// the complete frame. This is the property the custom-outbox
    /// `select!` in `RunnerProtocol::poll_status_with_custom_outbox`
    /// relies on.
    #[tokio::test]
    async fn frame_reader_resumes_after_cancelled_recv() {
        use tokio::io::AsyncRead;

        /// Reader that yields `Pending` once after each chunk so the
        /// test can deterministically cancel between chunks.
        struct ChunkedReader {
            chunks: Vec<Vec<u8>>,
            pending_next: bool,
        }
        impl AsyncRead for ChunkedReader {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if self.pending_next {
                    self.pending_next = false;
                    cx.waker().wake_by_ref();
                    return std::task::Poll::Pending;
                }
                if let Some(chunk) = self.chunks.first().cloned() {
                    buf.put_slice(&chunk);
                    self.chunks.remove(0);
                    self.pending_next = true;
                    std::task::Poll::Ready(Ok(()))
                } else {
                    std::task::Poll::Ready(Ok(()))
                }
            }
        }

        // One frame split across two chunks; the BufReader surfaces
        // them as two fill_buf rounds with a Pending in between.
        let mut reader = BufReader::new(ChunkedReader {
            chunks: vec![b"done:he".to_vec(), b"llo\nready\n".to_vec()],
            pending_next: false,
        });
        let mut state = FrameReadState::default();

        // First recv: poll ONCE (consumes chunk 1 into the state),
        // then DROP the future — the select!-loses-the-race shape.
        {
            let mut frame_reader = ResponseFrameReader { state: &mut state };
            let mut fut = std::pin::pin!(frame_reader.recv(&mut reader));
            let poll = futures_poll_once(fut.as_mut()).await;
            assert!(poll.is_none(), "first poll must be Pending (chunk gap)");
            // fut dropped here, mid-frame.
        }

        // Second recv against the SAME state: must resume the frame,
        // not corrupt it.
        let resp = ResponseFrameReader { state: &mut state }
            .recv(&mut reader)
            .await;
        match resp {
            Some(Response::Done { result_data }) => {
                assert_eq!(result_data.unwrap(), b"hello");
            }
            other => panic!("expected resumed Done frame, got {other:?}"),
        }
        // And the stream stays frame-aligned for the next frame.
        let next = ResponseFrameReader { state: &mut state }
            .recv(&mut reader)
            .await;
        assert!(matches!(next, Some(Response::Ready)));
    }

    /// Poll a future exactly once; `Some(v)` if Ready, `None` if Pending.
    async fn futures_poll_once<F: std::future::Future>(fut: std::pin::Pin<&mut F>) -> Option<F::Output> {
        struct PollOnce<'a, F>(Option<std::pin::Pin<&'a mut F>>);
        impl<'a, F: std::future::Future> std::future::Future for PollOnce<'a, F> {
            type Output = Option<F::Output>;
            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                let inner = self.0.take().expect("polled after completion");
                match inner.poll(cx) {
                    std::task::Poll::Ready(v) => std::task::Poll::Ready(Some(v)),
                    std::task::Poll::Pending => std::task::Poll::Ready(None),
                }
            }
        }
        PollOnce(Some(fut)).await
    }

    /// A partial line at EOF (no newline, under cap) is surfaced for a
    /// best-effort parse — matching the prior `read_line` behaviour.
    #[tokio::test]
    async fn partial_line_at_eof_parses_best_effort() {
        let mut reader = BufReader::new(std::io::Cursor::new(b"ready".to_vec()));
        let resp = recv_response_bounded(&mut reader).await;
        assert!(matches!(resp, Some(Response::Ready)));
    }
}
