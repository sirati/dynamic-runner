//! The `secondary-{n}` id encoding — ONE encoder/decoder shared by the
//! mint site and the failover-time `next_secondary_id` derive.
//!
//! Single concern: the textual shape of a minted secondary id. Both
//! [`PrimaryCoordinator::mint_secondary_id`](crate::primary::PrimaryCoordinator)
//! (which formats the next id) and the hydrate-time roster reconstruction
//! (which parses every known id to compute `max + 1`) go through this
//! pair, so the format and its inverse can never drift apart.

/// Prefix every minted secondary id carries.
const SECONDARY_ID_PREFIX: &str = "secondary-";

/// Format the monotonic index `n` into its `secondary-{n}` id.
pub(crate) fn format_secondary_id(n: u32) -> String {
    format!("{SECONDARY_ID_PREFIX}{n}")
}

/// Inverse of [`format_secondary_id`]: parse the index back out of a
/// `secondary-{n}` id, or `None` if the id does not match the encoding.
pub(crate) fn parse_secondary_index(id: &str) -> Option<u32> {
    id.strip_prefix(SECONDARY_ID_PREFIX)
        .and_then(|n| n.parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_then_parse_roundtrips() {
        for n in [0u32, 1, 7, 42, u32::MAX] {
            assert_eq!(parse_secondary_index(&format_secondary_id(n)), Some(n));
        }
    }

    #[test]
    fn parse_rejects_non_matching_ids() {
        assert_eq!(parse_secondary_index("observer-3"), None);
        assert_eq!(parse_secondary_index("secondary-"), None);
        assert_eq!(parse_secondary_index("secondary-x"), None);
        assert_eq!(parse_secondary_index("primary"), None);
    }
}
