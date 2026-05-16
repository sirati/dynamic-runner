use super::*;

#[test]
fn shorter_than_cap_passes_through() {
    let s: BoundedString<2048> = "hello".to_string().into();
    assert_eq!(s.as_str(), "hello");
}

#[test]
fn exactly_cap_passes_through() {
    let raw = "a".repeat(2048);
    let s: BoundedString<2048> = raw.clone().into();
    assert_eq!(s.as_str(), raw.as_str());
    assert_eq!(s.as_str().len(), 2048);
}

#[test]
fn ascii_truncates_to_cap() {
    let raw = "a".repeat(4097);
    let s: BoundedString<2048> = raw.into();
    assert_eq!(s.as_str().len(), 2048);
    assert!(s.as_str().chars().all(|c| c == 'a'));
}

#[test]
fn utf8_boundary_respected() {
    // 'é' is 2 bytes in UTF-8. With cap=3 and input "éé" (4 bytes),
    // the longest valid prefix `<= 3` bytes is just the first "é"
    // (2 bytes). The second character would push the length to 4,
    // so it is dropped entirely. The cut never lands inside a
    // multi-byte sequence.
    let s: BoundedString<3> = "éé".to_string().into();
    assert_eq!(s.as_str(), "é");
    assert_eq!(s.as_str().len(), 2);
}

#[test]
fn utf8_truncate_at_2048_drops_partial_multibyte() {
    // Build "a" * 2047 followed by a 2-byte char. Total: 2049 bytes.
    // Cap=2048. The 2-byte char straddles bytes 2047..2049 — its
    // start is `<= 2048` but its end is past. Drop it; keep the
    // 2047-byte ASCII run.
    let mut raw = "a".repeat(2047);
    raw.push('é');
    assert_eq!(raw.len(), 2049);

    let s: BoundedString<2048> = raw.into();
    assert_eq!(s.as_str().len(), 2047);
    assert!(s.as_str().chars().all(|c| c == 'a'));
}

#[test]
fn empty_string_round_trips() {
    let s: BoundedString<2048> = String::new().into();
    assert_eq!(s.as_str(), "");
}

#[test]
fn display_and_deref_match() {
    let s: BoundedString<16> = "hello".to_string().into();
    assert_eq!(format!("{}", s), "hello");
    let r: &str = &s;
    assert_eq!(r, "hello");
    let a: &str = s.as_ref();
    assert_eq!(a, "hello");
}

#[test]
fn serialise_is_transparent_string() {
    let s: BoundedString<2048> = "wire".to_string().into();
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(json, "\"wire\"");
}

#[test]
fn deserialise_caps_oversize_input() {
    // Build a JSON string literal that exceeds the cap: 4097 ASCII
    // characters. After deserialise, the resulting `BoundedString<2048>`
    // must hold at most 2048 bytes, regardless of what the wire said.
    let body = "a".repeat(4097);
    let json = format!("\"{}\"", body);
    let s: BoundedString<2048> = serde_json::from_str(&json).unwrap();
    assert_eq!(s.as_str().len(), 2048);
}

#[test]
fn deserialise_caps_at_utf8_boundary() {
    // Encode "é" * 1500 (3000 bytes) as JSON. Cap=2048. The deserialiser
    // must cut on a character boundary — total bytes after cut is even
    // (each 'é' is 2 bytes), and parsing the result must still yield
    // valid UTF-8 (which the `BoundedString` invariant guarantees by
    // construction).
    let body = "é".repeat(1500);
    let json = serde_json::to_string(&body).unwrap();
    let s: BoundedString<2048> = serde_json::from_str(&json).unwrap();
    assert!(s.as_str().len() <= 2048);
    // Even byte length: no split mid-character.
    assert_eq!(s.as_str().len() % 2, 0);
    assert!(s.as_str().chars().all(|c| c == 'é'));
}

#[test]
fn json_roundtrip_within_cap() {
    let original: BoundedString<2048> = "round-trip".to_string().into();
    let json = serde_json::to_string(&original).unwrap();
    let parsed: BoundedString<2048> = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, original);
}
