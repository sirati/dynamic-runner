use db_comm_api_base::Identifier;
use crate::messages::DistributedMessage;

/// Serialize a distributed message to a length-prefixed JSON frame.
///
/// Wire format: 4-byte big-endian length prefix + JSON bytes.
/// This matches the Python protocol which uses length-prefixed JSON.
pub fn serialize_message<I: Identifier>(msg: &DistributedMessage<I>) -> Result<Vec<u8>, String> {
    let json = serde_json::to_string(msg).map_err(|e| e.to_string())?;
    let json_bytes = json.as_bytes();
    let len = json_bytes.len() as u32;
    let mut buf = Vec::with_capacity(4 + json_bytes.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(json_bytes);
    Ok(buf)
}

/// Deserialize a distributed message from JSON bytes (without length prefix).
pub fn deserialize_message<I: Identifier>(json_bytes: &[u8]) -> Result<DistributedMessage<I>, String> {
    serde_json::from_slice(json_bytes).map_err(|e| e.to_string())
}

/// Extract one message from a buffer that may contain length-prefixed frames.
///
/// Returns (message, bytes_consumed) or None if not enough data.
pub fn decode_frame<I: Identifier>(buf: &[u8]) -> Result<Option<(DistributedMessage<I>, usize)>, String> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let msg = deserialize_message(&buf[4..4 + len])?;
    Ok(Some((msg, 4 + len)))
}



#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
