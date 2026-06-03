//! Inline base64 STANDARD-with-padding encoder/decoder used for the
//! `cert_pem_b64` envelope field. Hand-rolled to avoid pulling in the
//! `base64` crate on the slurm preparation path; both functions are
//! `pub(super)` and consumed by [`builder`](super::builder) (encode)
//! and [`parse`](super::parse) (decode).

use super::types::PeerInfoError;

/// Trivial base64 STANDARD-with-padding encoder. Pulled in inline
/// to avoid adding the `base64` crate dep just for the cert blob —
/// the crate is small but the dep adds compile cost on a path
/// (slurm preparation) that already builds for every consumer.
///
/// We don't validate the input PEM — the caller already has a
/// validated cert (CertExchange round-trip succeeded). Any byte
/// stream is round-trippable through this pair.
pub(super) fn encode_b64(s: &str) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut i = 0;
    while i + 3 <= bytes.len() {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i + 1] as u32) << 8) | (bytes[i + 2] as u32);
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        out.push(TABLE[(n & 0x3f) as usize] as char);
        i += 3;
    }
    let rem = bytes.len() - i;
    if rem == 1 {
        let n = (bytes[i] as u32) << 16;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i + 1] as u32) << 8);
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        out.push('=');
    }
    out
}

pub(super) fn decode_b64(s: &str) -> Result<String, PeerInfoError> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(PeerInfoError::InvalidCert(format!(
            "length {} not a multiple of 4",
            bytes.len()
        )));
    }
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut i = 0;
    while i < bytes.len() {
        let q = &bytes[i..i + 4];
        let pad = q.iter().filter(|&&b| b == b'=').count();
        if pad > 2 || (pad > 0 && i + 4 != bytes.len()) {
            return Err(PeerInfoError::InvalidCert(format!(
                "misplaced padding at index {i}"
            )));
        }
        let a =
            val(q[0]).ok_or_else(|| PeerInfoError::InvalidCert(format!("invalid char at {i}")))?;
        let b = val(q[1])
            .ok_or_else(|| PeerInfoError::InvalidCert(format!("invalid char at {}", i + 1)))?;
        let c = if q[2] == b'=' {
            0
        } else {
            val(q[2])
                .ok_or_else(|| PeerInfoError::InvalidCert(format!("invalid char at {}", i + 2)))?
        };
        let d = if q[3] == b'=' {
            0
        } else {
            val(q[3])
                .ok_or_else(|| PeerInfoError::InvalidCert(format!("invalid char at {}", i + 3)))?
        };
        let n = ((a as u32) << 18) | ((b as u32) << 12) | ((c as u32) << 6) | (d as u32);
        out.push(((n >> 16) & 0xff) as u8);
        if q[2] != b'=' {
            out.push(((n >> 8) & 0xff) as u8);
        }
        if q[3] != b'=' {
            out.push((n & 0xff) as u8);
        }
        i += 4;
    }
    String::from_utf8(out).map_err(|e| PeerInfoError::InvalidCert(format!("utf8: {e}")))
}
