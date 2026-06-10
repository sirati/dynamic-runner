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
/// is [`recv_response_bounded`], which maps these onto the protocol's
/// response vocabulary.
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

/// Read one `\n`-terminated line, accumulating at most `cap` bytes.
/// On overflow, switches to a drain loop that discards (but counts)
/// the rest of the line so the stream stays frame-aligned.
async fn read_line_bounded<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    cap: usize,
) -> std::io::Result<BoundedLine> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            // EOF. Preserve `read_line` semantics: no buffered bytes
            // means a clean close; a partial line is surfaced for a
            // best-effort parse.
            return if buf.is_empty() {
                Ok(BoundedLine::Eof)
            } else {
                Ok(BoundedLine::Line(into_utf8(buf)?))
            };
        }
        let newline_pos = available.iter().position(|&b| b == b'\n');
        let take = match newline_pos {
            Some(pos) => pos + 1, // include the newline, like read_line
            None => available.len(),
        };
        if buf.len() + take > cap {
            // Overflow: drain the rest of the line without
            // accumulating it. Count what we already buffered plus
            // everything drained so the error can name the real size.
            let mut total = buf.len();
            drop(buf);
            loop {
                let chunk = reader.fill_buf().await?;
                if chunk.is_empty() {
                    // EOF mid-drain (sender died mid-frame). Still
                    // report the oversize — loud beats silent, and the
                    // bytes seen already prove the violation.
                    return Ok(BoundedLine::Oversize { len: total });
                }
                let pos = chunk.iter().position(|&b| b == b'\n');
                let consume = match pos {
                    Some(p) => p + 1,
                    None => chunk.len(),
                };
                total += consume;
                reader.consume(consume);
                if pos.is_some() {
                    return Ok(BoundedLine::Oversize { len: total });
                }
            }
        }
        buf.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline_pos.is_some() {
            return Ok(BoundedLine::Line(into_utf8(buf)?));
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
/// * `None` — connection closed / transport error (unchanged).
/// * `Some(parsed)` — frame under the cap (unchanged).
/// * `Some(Error(NonRecoverable, …))` — frame over the cap; the stream
///   is drained past the offending line and left recoverable.
pub async fn recv_response_bounded<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Option<Response> {
    match read_line_bounded(reader, MAX_RESPONSE_FRAME_BYTES).await {
        Ok(BoundedLine::Eof) => None,
        Ok(BoundedLine::Line(line)) => codec::parse_response(&line),
        Ok(BoundedLine::Oversize { len }) => Some(oversize_response(len)),
        Err(_) => None,
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

        let first = read_line_bounded(&mut reader, cap).await.unwrap();
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
        let out = read_line_bounded(&mut reader, cap).await.unwrap();
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

    /// A partial line at EOF (no newline, under cap) is surfaced for a
    /// best-effort parse — matching the prior `read_line` behaviour.
    #[tokio::test]
    async fn partial_line_at_eof_parses_best_effort() {
        let mut reader = BufReader::new(std::io::Cursor::new(b"ready".to_vec()));
        let resp = recv_response_bounded(&mut reader).await;
        assert!(matches!(resp, Some(Response::Ready)));
    }
}
