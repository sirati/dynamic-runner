//! The liveness-beacon wire datagram.
//!
//! Single concern: encode/decode the one tiny UDP payload the beacon
//! sends and the listener reads. Deliberately the smallest possible
//! framing — one datagram == one liveness assertion "node `id` (instance
//! `token`) is alive". No streaming, no length-prefix chain: a UDP
//! datagram is already message-framed, so the whole payload IS one
//! message.
//!
//! # Trust model
//!
//! The `token` is a cheap per-run sanity discriminator, NOT a
//! cryptographic authenticator. The owner's compute fabric is trusted
//! compute-to-compute (firewalled inter-compute is unsupported), exactly
//! like the QUIC mesh's own trust assumption. The token only guards
//! against a stale beacon from a PRIOR run (a process that outlived its
//! run and kept sending) refreshing the death-clock of a same-id node in
//! a NEW run — the listener drops a datagram whose token does not match
//! the run it was told to expect.
//!
//! # Layout
//!
//! ```text
//! byte 0      : MAGIC (0xB1) — reject anything that isn't ours
//! byte 1      : VERSION (1)  — forward-compat discriminator
//! bytes 2..10 : token (u64, big-endian)
//! bytes 10..  : node-id (UTF-8, the remaining bytes)
//! ```
//!
//! A datagram with no node-id bytes (len < 11) or a bad magic/version is
//! rejected by [`decode`]; a node-id is bounded by the caller's id length
//! (the framework's node ids are short — `secondary-N` / `SETUP_NODE_ID`)
//! and by the listener's fixed recv buffer.

/// First byte of every liveness datagram. Lets the listener reject a
/// stray packet (port reuse, a scanner) without a parse attempt.
const MAGIC: u8 = 0xB1;

/// Payload format version. Bumped only on a breaking layout change; the
/// listener rejects an unrecognised version rather than mis-parsing.
const VERSION: u8 = 1;

/// Fixed header length: MAGIC + VERSION + token(8).
const HEADER_LEN: usize = 1 + 1 + 8;

/// One decoded liveness assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivenessDatagram {
    /// The asserting node's logical id (the same id the primary keys its
    /// death-clock on — `record_keepalive(id)`).
    pub node_id: String,
    /// The per-run instance token (cheap stale-run discriminator).
    pub token: u64,
}

/// Encode an assertion into the wire bytes.
pub fn encode(node_id: &str, token: u64) -> Vec<u8> {
    let id_bytes = node_id.as_bytes();
    let mut buf = Vec::with_capacity(HEADER_LEN + id_bytes.len());
    buf.push(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&token.to_be_bytes());
    buf.extend_from_slice(id_bytes);
    buf
}

/// Decode wire bytes back into an assertion, or `None` if the datagram is
/// not a well-formed liveness payload (bad magic/version, too short, or
/// non-UTF-8 id). A `None` is dropped silently by the listener — a
/// malformed packet is never a liveness signal.
pub fn decode(bytes: &[u8]) -> Option<LivenessDatagram> {
    if bytes.len() <= HEADER_LEN {
        return None;
    }
    if bytes[0] != MAGIC || bytes[1] != VERSION {
        return None;
    }
    let token = u64::from_be_bytes(bytes[2..HEADER_LEN].try_into().ok()?);
    let node_id = std::str::from_utf8(&bytes[HEADER_LEN..]).ok()?.to_string();
    Some(LivenessDatagram { node_id, token })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode→decode is the identity on a well-formed assertion.
    #[test]
    fn roundtrip() {
        let bytes = encode("secondary-3", 0xDEAD_BEEF_CAFE_0042);
        let decoded = decode(&bytes).expect("well-formed datagram decodes");
        assert_eq!(decoded.node_id, "secondary-3");
        assert_eq!(decoded.token, 0xDEAD_BEEF_CAFE_0042);
    }

    /// A datagram with no id bytes is rejected (a liveness assertion MUST
    /// name the node it asserts).
    #[test]
    fn empty_id_rejected() {
        let mut bytes = vec![MAGIC, VERSION];
        bytes.extend_from_slice(&7u64.to_be_bytes());
        assert_eq!(decode(&bytes), None);
    }

    /// A foreign packet on the port (wrong magic) is rejected without a
    /// parse attempt.
    #[test]
    fn bad_magic_rejected() {
        let mut bytes = encode("secondary-0", 1);
        bytes[0] = 0x00;
        assert_eq!(decode(&bytes), None);
    }

    /// An unrecognised version is rejected rather than mis-parsed.
    #[test]
    fn bad_version_rejected() {
        let mut bytes = encode("secondary-0", 1);
        bytes[1] = 0xFF;
        assert_eq!(decode(&bytes), None);
    }

    /// A truncated header (shorter than MAGIC+VERSION+token) is rejected.
    #[test]
    fn truncated_rejected() {
        assert_eq!(decode(&[MAGIC, VERSION, 0, 0]), None);
        assert_eq!(decode(&[]), None);
    }
}
