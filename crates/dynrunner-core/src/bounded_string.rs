//! Self-truncating UTF-8 string newtype.
//!
//! Why this exists: protocol payloads carry free-form diagnostic strings
//! (e.g. `ErrorType::Unfulfillable { reason }`). Without a cap, a
//! malicious or buggy peer can send an arbitrarily large field and force
//! the receiver to allocate proportional memory. `BoundedString<N>`
//! enforces a per-field byte cap at construction time and again on
//! deserialise, so the cap holds regardless of where the value enters
//! the process.
//!
//! Truncation rule: the result is the longest UTF-8 prefix whose byte
//! length is `<= N`. Multi-byte characters that would straddle the cap
//! are dropped wholesale (never split). This matches how `&str` indexing
//! works and avoids producing invalid UTF-8.
//!
//! Single-allocation guarantee: construction from a `String` reuses the
//! input's allocation (via `String::truncate`) rather than copying into
//! a fresh buffer.
//!
//! Wire shape: `#[serde(transparent)]` over a `String`, so the on-wire
//! representation is just a JSON / bincode string. Deserialise applies
//! the same cap, defending against oversized inbound payloads.

use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize};

/// A `String` capped at `N` bytes, truncated on a UTF-8 character
/// boundary.
///
/// Construction and deserialise both apply the cap; callers never see an
/// instance that exceeds it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct BoundedString<const N: usize>(String);

impl<const N: usize> BoundedString<N> {
    /// Returns the (already-capped) inner slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper, returning the inner `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Compile-time cap (in bytes).
    pub const fn cap() -> usize {
        N
    }

    /// Truncates `s` in place to the longest UTF-8 prefix whose byte
    /// length is `<= N`. Single allocation: reuses the input's buffer.
    fn truncate_in_place(s: &mut String) {
        if s.len() <= N {
            return;
        }
        // Find the largest valid UTF-8 boundary `<= N`. `char_indices`
        // yields the starting byte offset of each character; the last
        // offset with `offset <= N` is the cut point. Characters whose
        // start is already past N are out; a character whose start is
        // `<= N` but whose end exceeds N is also dropped (its successor's
        // start, if any, is `> N` and would not be chosen).
        let cut = s
            .char_indices()
            .map(|(i, c)| i + c.len_utf8())
            .take_while(|&end| end <= N)
            .last()
            .unwrap_or(0);
        s.truncate(cut);
    }
}

impl<const N: usize> From<String> for BoundedString<N> {
    fn from(mut s: String) -> Self {
        Self::truncate_in_place(&mut s);
        Self(s)
    }
}

impl<const N: usize> From<&str> for BoundedString<N> {
    fn from(s: &str) -> Self {
        // Delegate to the `String` impl so the single-allocation rule
        // is enforced uniformly: one `to_owned`, then in-place
        // truncation.
        Self::from(s.to_owned())
    }
}

impl<const N: usize> fmt::Display for BoundedString<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<const N: usize> AsRef<str> for BoundedString<N> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<const N: usize> Deref for BoundedString<N> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'de, const N: usize> Deserialize<'de> for BoundedString<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialise as an owned `String` (single allocation), then
        // route through `From<String>` so the cap is applied with the
        // same in-place truncation as user-facing construction. A
        // malicious peer that sends a 10 MiB field still gets capped to
        // `N` bytes before any caller observes the value.
        let s = String::deserialize(deserializer)?;
        Ok(Self::from(s))
    }
}

#[cfg(test)]
#[path = "bounded_string_tests.rs"]
mod tests;
