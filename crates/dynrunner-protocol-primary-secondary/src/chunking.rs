//! Chunked transfer of ONE oversized chunk-eligible framework frame
//! (`DistributedMessage::FrameChunk` — see the variant doc for the wire
//! contract and the eligibility law).
//!
//! # Single concern
//!
//! The transfer MECHANISM only: how one serialized frame's bytes are
//! split into `FrameChunk` messages ([`split_frame`]) and how a receiver
//! reassembles them back into the original bytes
//! ([`ChunkReassembler`]). The size POLICY — the wire cap, the chunk
//! budget derived from it, WHEN to engage chunking, and what to log —
//! is owned by the transport's framing layer (the
//! `dynrunner-transport-quic` `framing` module), which is this module's
//! only production consumer. Keeping the mechanism here (next to the
//! codec) keeps it transport-agnostic: any leg that frames
//! `DistributedMessage`s can adopt it without re-implementing the state
//! machine.
//!
//! # Ordering / loss model
//!
//! Chunks of one transfer are written back-to-back by the sending
//! writer pump on ONE ordered, reliable leg (a QUIC stream / a
//! WebSocket connection), so in the absence of faults they arrive
//! contiguous and in order. Every deviation — an index gap, a foreign
//! mid-transfer chunk, a superseding transfer, a checksum mismatch — is
//! therefore transfer-fatal: the reassembler abandons the partial and
//! reports it ONCE (the caller's single loud WARN; never a silent
//! partial). There is NO chunk-level retransmit: the transfer's
//! TRIGGER (the anti-entropy digest cadence, a re-issued bootstrap
//! pull) is the bounded retry, and the abandoned bytes are simply
//! re-pulled. Duplicate already-consumed indexes are ignored
//! (idempotent under at-least-once delivery).

use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use dynrunner_core::Identifier;

use crate::messages::DistributedMessage;

/// FNV-1a 64-bit over `bytes`. Chosen over `DefaultHasher` because the
/// checksum CROSSES THE WIRE between processes: FNV-1a's constants are
/// part of this module's wire contract (deterministic across builds),
/// while `DefaultHasher`'s algorithm is explicitly unspecified.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Process-wide monotonic transfer-id source. Uniqueness is only needed
/// PER SENDING PROCESS (the reassembler is per-connection and a
/// connection has one remote sender), so a process-global counter
/// suffices; starting at 1 keeps 0 free as an "impossible id" for
/// debugging.
static NEXT_TRANSFER_ID: AtomicU64 = AtomicU64::new(1);

/// Split one oversized serialized frame (`json_bytes` — the frame's
/// JSON body, WITHOUT the 4-byte length prefix, which is per-leg
/// framing) into `FrameChunk` messages whose raw slices are at most
/// `raw_slice_bytes` long. `template` supplies the chunk envelopes'
/// `sender_id`/`timestamp` (the chunked frame's own, so operator logs
/// correlate).
///
/// The caller (the framing layer) owns the policy decision to call
/// this — frame over the wire cap AND [`DistributedMessage::chunk_eligible`]
/// — and serializes each returned message itself (each is under the cap
/// by the caller's budget arithmetic; see the framing layer's
/// compile-time pin).
///
/// `raw_slice_bytes` must be ≥ 1; `json_bytes` must be non-empty (an
/// empty frame is never oversize).
pub fn split_frame<I: Identifier>(
    template: &DistributedMessage<I>,
    json_bytes: &[u8],
    raw_slice_bytes: usize,
) -> Vec<DistributedMessage<I>> {
    assert!(raw_slice_bytes >= 1, "chunk slice budget must be >= 1");
    assert!(!json_bytes.is_empty(), "cannot chunk an empty frame");
    let transfer_id = NEXT_TRANSFER_ID.fetch_add(1, Ordering::Relaxed);
    let checksum = fnv1a64(json_bytes);
    let total = json_bytes.len().div_ceil(raw_slice_bytes);
    let total_u32 =
        u32::try_from(total).expect("chunk count exceeds u32 (frame astronomically large)");
    let sender_id = template.sender_id().to_string();
    let timestamp = template.timestamp();
    json_bytes
        .chunks(raw_slice_bytes)
        .enumerate()
        .map(|(index, slice)| DistributedMessage::FrameChunk {
            target: None,
            sender_id: sender_id.clone(),
            timestamp,
            transfer_id,
            index: index as u32,
            total: total_u32,
            checksum,
            payload_b64: BASE64.encode(slice),
        })
        .collect()
}

/// One abandoned partial transfer — the receiver-side caller logs it
/// ONCE at WARN (the bounded-loud contract: a wedged/missing chunk is
/// never a silent partial).
#[derive(Debug)]
pub struct AbandonedTransfer {
    pub transfer_id: u64,
    /// Raw bytes accumulated before abandonment.
    pub buffered_bytes: usize,
    /// Chunks consumed before abandonment.
    pub chunks_received: u32,
    /// Why the partial was discarded.
    pub reason: String,
}

/// Outcome of feeding one `FrameChunk` to the reassembler.
#[derive(Debug)]
pub enum ChunkOutcome {
    /// Chunk consumed; the transfer is still in progress (also returned
    /// for an idempotently-ignored duplicate of an already-consumed
    /// index).
    Incomplete,
    /// The final chunk landed and the checksum verified: the original
    /// frame's JSON bytes, ready for `codec::deserialize_message`.
    Complete(Vec<u8>),
    /// This chunk was rejected (malformed fields / bad base64 / index
    /// gap / checksum mismatch / over the reassembly cap). Any partial
    /// it invalidated is surfaced via [`ChunkIngest::abandoned`].
    Rejected { reason: String },
}

/// Result of [`ChunkReassembler::ingest`]: the per-chunk outcome plus
/// at most one abandonment notice (a rejected chunk can both kill the
/// in-progress partial AND itself be rejected; a superseding transfer
/// kills the partial while itself starting cleanly).
#[derive(Debug)]
pub struct ChunkIngest {
    pub abandoned: Option<AbandonedTransfer>,
    pub outcome: ChunkOutcome,
}

/// In-progress transfer state.
#[derive(Debug)]
struct Partial {
    transfer_id: u64,
    total: u32,
    checksum: u64,
    next_index: u32,
    buf: Vec<u8>,
}

/// Per-connection reassembler for `FrameChunk` transfers.
///
/// Owned by each framed-IO reader pump (one per connection): chunks of
/// one transfer always travel one leg, so there is at most ONE
/// in-progress transfer per reassembler and no cross-connection
/// interleaving to track. Dropping the reassembler (the connection
/// tearing down) discards any partial — the transfer's higher-level
/// trigger re-pulls.
#[derive(Debug, Default)]
pub struct ChunkReassembler {
    partial: Option<Partial>,
    /// Hard cap on the reassembled payload (`max_payload_bytes == 0`
    /// means "no cap"); the framing layer passes its policy value so a
    /// corrupt/malicious `total` cannot buffer unbounded memory.
    max_payload_bytes: usize,
}

impl ChunkReassembler {
    pub fn new(max_payload_bytes: usize) -> Self {
        Self {
            partial: None,
            max_payload_bytes,
        }
    }

    /// Whether a partial transfer is currently buffered.
    pub fn in_progress(&self) -> bool {
        self.partial.is_some()
    }

    fn abandon(&mut self, reason: &str) -> Option<AbandonedTransfer> {
        self.partial.take().map(|p| AbandonedTransfer {
            transfer_id: p.transfer_id,
            buffered_bytes: p.buf.len(),
            chunks_received: p.next_index,
            reason: reason.to_string(),
        })
    }

    /// Feed one `FrameChunk`'s fields. See [`ChunkIngest`] for the
    /// outcome contract; the module doc spells the ordered-leg fault
    /// model every rule below derives from.
    pub fn ingest(
        &mut self,
        transfer_id: u64,
        index: u32,
        total: u32,
        checksum: u64,
        payload_b64: &str,
    ) -> ChunkIngest {
        // Field sanity before any state change.
        if total == 0 || index >= total {
            let abandoned = if self.partial.as_ref().is_some_and(|p| p.transfer_id == transfer_id) {
                self.abandon("malformed chunk fields in the same transfer")
            } else {
                None
            };
            return ChunkIngest {
                abandoned,
                outcome: ChunkOutcome::Rejected {
                    reason: format!("malformed chunk fields: index={index} total={total}"),
                },
            };
        }
        let raw = match BASE64.decode(payload_b64) {
            Ok(raw) => raw,
            Err(e) => {
                // A corrupt slice kills its own transfer (the bytes are
                // unrecoverable); a foreign transfer's partial survives.
                let abandoned = if self.partial.as_ref().is_some_and(|p| p.transfer_id == transfer_id)
                {
                    self.abandon("corrupt base64 chunk in the same transfer")
                } else {
                    None
                };
                return ChunkIngest {
                    abandoned,
                    outcome: ChunkOutcome::Rejected {
                        reason: format!("chunk payload is not valid base64: {e}"),
                    },
                };
            }
        };

        // Transfer bookkeeping: continue the in-progress one, supersede
        // it (a NEW transfer beginning at index 0 proves the sender gave
        // up on the old one), or reject an unknown mid-transfer chunk.
        let mut abandoned = None;
        let continues_partial = self
            .partial
            .as_ref()
            .is_some_and(|p| p.transfer_id == transfer_id);
        if !continues_partial {
            if self.partial.is_some() {
                abandoned = self.abandon("superseded by a newer transfer on this connection");
            }
            if index != 0 {
                return ChunkIngest {
                    abandoned,
                    outcome: ChunkOutcome::Rejected {
                        reason: format!(
                            "chunk {index}/{total} of unknown transfer {transfer_id} \
                             (joined mid-transfer; cannot reassemble)"
                        ),
                    },
                };
            }
            self.partial = Some(Partial {
                transfer_id,
                total,
                checksum,
                next_index: 0,
                buf: Vec::new(),
            });
        }
        let (p_total, p_checksum, p_next, p_buffered) = {
            let p = self.partial.as_ref().expect("partial set above");
            (p.total, p.checksum, p.next_index, p.buf.len())
        };

        // Constancy of the transfer header across its chunks.
        if p_total != total || p_checksum != checksum {
            let abandoned2 = self.abandon("inconsistent total/checksum across chunks");
            return ChunkIngest {
                abandoned: abandoned.or(abandoned2),
                outcome: ChunkOutcome::Rejected {
                    reason: "chunk header (total/checksum) changed mid-transfer".into(),
                },
            };
        }
        // Idempotency: a duplicate of an already-consumed index is a
        // NoOp (at-least-once tolerance), not a fault.
        if index < p_next {
            return ChunkIngest {
                abandoned,
                outcome: ChunkOutcome::Incomplete,
            };
        }
        // An index GAP on an ordered leg means bytes are gone for good.
        if index > p_next {
            let abandoned2 = self.abandon("index gap (lost chunk) on an ordered leg");
            return ChunkIngest {
                abandoned: abandoned.or(abandoned2),
                outcome: ChunkOutcome::Rejected {
                    reason: format!("chunk index gap: expected {p_next}, got {index}"),
                },
            };
        }
        // Reassembly-size cap: reject before buffering past the policy
        // limit (0 = uncapped).
        if self.max_payload_bytes != 0 && p_buffered + raw.len() > self.max_payload_bytes {
            let limit = self.max_payload_bytes;
            let abandoned2 = self.abandon("reassembled payload exceeds the reassembly cap");
            return ChunkIngest {
                abandoned: abandoned.or(abandoned2),
                outcome: ChunkOutcome::Rejected {
                    reason: format!("reassembled payload exceeds the {limit}-byte reassembly cap"),
                },
            };
        }
        {
            let p = self.partial.as_mut().expect("partial set above");
            p.buf.extend_from_slice(&raw);
            p.next_index += 1;
            if p.next_index < p.total {
                return ChunkIngest {
                    abandoned,
                    outcome: ChunkOutcome::Incomplete,
                };
            }
        }
        // Final chunk: verify integrity, hand the bytes back.
        let done = self.partial.take().expect("final chunk of a live partial");
        let actual = fnv1a64(&done.buf);
        if actual != done.checksum {
            return ChunkIngest {
                abandoned,
                outcome: ChunkOutcome::Rejected {
                    reason: format!(
                        "reassembled checksum mismatch: expected {:#x}, got {actual:#x} \
                         over {} bytes",
                        done.checksum,
                        done.buf.len()
                    ),
                },
            };
        }
        ChunkIngest {
            abandoned,
            outcome: ChunkOutcome::Complete(done.buf),
        }
    }
}

#[cfg(test)]
#[path = "chunking_tests.rs"]
mod tests;
