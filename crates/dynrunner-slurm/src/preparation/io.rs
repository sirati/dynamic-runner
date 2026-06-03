//! Connection-info file readers: [`read_peer_info_file`] (the
//! late-joiner bootstrap entry point used by Step 8's
//! `join_running_cluster`) plus a `#[cfg(test)]` shim
//! [`parse_connection_uri`] kept here so existing in-crate tests keep
//! their assertion shape. Both delegate to the canonical parser in
//! [`crate::peer_info`].

use crate::peer_info::{PeerInfoRecord, parse as parse_peer_info};

use super::options::{InfoFileReader, PrepError};

#[cfg(test)]
pub(super) fn parse_connection_uri(line: &str) -> Result<(String, u16), String> {
    let uri = crate::peer_info::parse_v1_uri(line).map_err(|e| e.to_string())?;
    Ok((uri.host, uri.port))
}

/// Read a connection-info file from `path` via the supplied
/// [`InfoFileReader`] and parse it as a full v1/v2 [`PeerInfoRecord`].
///
/// Returns `Ok(None)` if the file does not exist yet (matching the
/// watcher's polling semantics). Used by Step 8's late-joiner
/// bootstrap to harvest a directory of records without re-implementing
/// the format. Pure relative to the reader trait — no direct fs/IO.
pub async fn read_peer_info_file<R: InfoFileReader>(
    reader: &R,
    path: String,
) -> Result<Option<PeerInfoRecord>, PrepError> {
    let stdout = reader.read(path.clone()).await?;
    let Some(text) = stdout else {
        return Ok(None);
    };
    if text.trim().is_empty() {
        return Ok(None);
    }
    let record = parse_peer_info(&text).map_err(|e| PrepError::InfoParse {
        secondary_id: path,
        message: e.to_string(),
    })?;
    Ok(Some(record))
}
