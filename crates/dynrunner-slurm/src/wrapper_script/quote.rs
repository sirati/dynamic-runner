//! Inline shell-quoting and random-suffix helpers used by both the
//! secondary-mode generator and the image-validation generator.
//! [`bash_quote`] matches Python's `shlex.quote` semantics;
//! [`rand_hex8`] derives an 8-hex-digit suffix from the system clock
//! for per-run temp paths.

/// Bash-quote a string the way Python's `shlex.quote` does:
/// safe chars (`[A-Za-z0-9@%+=:,./_-]`) and non-empty input pass
/// through verbatim; everything else is wrapped in single quotes
/// with internal `'` replaced by `'\''`. The empty string becomes
/// `''` to avoid silent collapse on the bash side.
pub(super) fn bash_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    let safe = s
        .bytes()
        .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'_' | b'-'));
    if safe {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// 8-hex-char random suffix using `/dev/urandom` (4 bytes of
/// entropy). Mirrors Python's `secrets.token_hex(4)`. Falls back
/// to a hash of the system time if /dev/urandom is unreadable
/// (extremely unlikely on Linux).
pub(super) fn rand_hex8() -> String {
    use std::io::Read;
    let mut buf = [0u8; 4];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom")
        && f.read_exact(&mut buf).is_ok()
    {
        return format!(
            "{:02x}{:02x}{:02x}{:02x}",
            buf[0], buf[1], buf[2], buf[3]
        );
    }
    // Fallback: hash of nanoseconds-since-epoch — not cryptographic
    // but identical entropy semantics for the suffix's purpose
    // (avoid two parallel jobs sharing /tmp/asm-XXXX).
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut h = DefaultHasher::new();
    h.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    format!("{:08x}", h.finish() as u32)
}
