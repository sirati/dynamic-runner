use super::b64::{decode_b64, encode_b64};
use super::{
    Builder, PeerInfoError, PeerInfoVersion, ReadDirError, parse, parse_v1_uri, read_dir_v2,
};

/// v1 contents (line 1 only) parse to a `V1` record. The legacy-URI
/// fields are populated, envelope fields are all `None`.
#[test]
fn parse_v1_legacy_uri_only() {
    let r = parse("tcp://node03:54321\n").unwrap();
    assert_eq!(r.version, PeerInfoVersion::V1);
    assert_eq!(r.legacy_uri.host, "node03");
    assert_eq!(r.legacy_uri.port, 54321);
    assert!(r.secondary_id.is_none());
    assert!(r.cert_pem.is_none());
    assert!(r.ipv4.is_none());
    assert!(r.ipv6.is_none());
    assert!(r.quic_port.is_none());
    assert!(r.is_observer.is_none());
}

/// v2 contents parse all envelope keys; absent keys remain `None`.
#[test]
fn parse_v2_full_envelope() {
    let contents = "\
tcp://compute1:40001
version=2
secondary_id=secondary-7
cert_pem_b64=SGVsbG8gV29ybGQ=
ipv4=10.0.0.5
ipv6=fd00::1
quic_port=51200
is_observer=true
";
    let r = parse(contents).unwrap();
    assert_eq!(r.version, PeerInfoVersion::V2);
    assert_eq!(r.legacy_uri.host, "compute1");
    assert_eq!(r.legacy_uri.port, 40001);
    assert_eq!(r.secondary_id.as_deref(), Some("secondary-7"));
    assert_eq!(r.cert_pem.as_deref(), Some("Hello World"));
    assert_eq!(r.ipv4.as_deref(), Some("10.0.0.5"));
    assert_eq!(r.ipv6.as_deref(), Some("fd00::1"));
    assert_eq!(r.quic_port, Some(51200));
    assert_eq!(r.is_observer, Some(true));
}

/// is_observer=false explicitly false (defaults are caller-side).
#[test]
fn parse_v2_is_observer_false() {
    let r = parse("tcp://h:1\nversion=2\nis_observer=false\n").unwrap();
    assert_eq!(r.is_observer, Some(false));
}

/// Blank lines in the envelope are tolerated (writer-side neatness
/// could insert them; reader should ignore).
#[test]
fn parse_v2_blank_lines_tolerated() {
    let r = parse("tcp://h:1\n\nversion=2\n\nipv4=10.0.0.1\n\n").unwrap();
    assert_eq!(r.ipv4.as_deref(), Some("10.0.0.1"));
}

/// An envelope line without `=` is a structured parse error, not
/// silently dropped — a typo'd key would otherwise pass unnoticed.
#[test]
fn parse_malformed_envelope_line_errors() {
    let err = parse("tcp://h:1\nversion 2\n").unwrap_err();
    assert!(matches!(err, PeerInfoError::MalformedEnvelopeLine(_)));
}

/// is_observer must be `true` / `false` exactly; any other value
/// surfaces a typed error so a typo doesn't silently default.
#[test]
fn parse_is_observer_invalid_value() {
    let err = parse("tcp://h:1\nversion=2\nis_observer=yes\n").unwrap_err();
    assert!(matches!(err, PeerInfoError::InvalidIsObserver(_)));
}

/// Empty file → `Empty` error. Single-line URI is the minimum
/// well-formed v1 input.
#[test]
fn parse_empty_is_error() {
    let err = parse("").unwrap_err();
    assert!(matches!(
        err,
        PeerInfoError::InvalidUri(_) | PeerInfoError::Empty
    ));
}

/// Line 1 is required and must be a parsable URI; line-1 garbage
/// surfaces as `InvalidUri`.
#[test]
fn parse_garbage_line1_errors() {
    let err = parse("not a uri\nversion=2\n").unwrap_err();
    assert!(matches!(err, PeerInfoError::InvalidUri(_)));
}

/// Builder round-trips: format → parse yields an equivalent record
/// (modulo version, which the builder always marks v2).
#[test]
fn builder_round_trip() {
    let b = Builder::new("node-x", 40000)
        .secondary_id("sec-3")
        .cert_pem("-----BEGIN CERT-----\nMIIB...\n-----END CERT-----\n")
        .ipv4("10.0.0.5")
        .quic_port(51200)
        .is_observer(true);
    let s = b.format();
    let r = parse(&s).unwrap();
    assert_eq!(r.version, PeerInfoVersion::V2);
    assert_eq!(r.legacy_uri.host, "node-x");
    assert_eq!(r.legacy_uri.port, 40000);
    assert_eq!(r.secondary_id.as_deref(), Some("sec-3"));
    assert_eq!(
        r.cert_pem.as_deref(),
        Some("-----BEGIN CERT-----\nMIIB...\n-----END CERT-----\n")
    );
    assert_eq!(r.ipv4.as_deref(), Some("10.0.0.5"));
    assert!(r.ipv6.is_none());
    assert_eq!(r.quic_port, Some(51200));
    assert_eq!(r.is_observer, Some(true));
}

/// Full-shape round-trip: build a record with a NON-DEFAULT value for
/// EVERY field (every envelope key present, `ipv6` set, `is_observer`
/// = true rather than the absent/false default), `format()` then
/// `parse()`, and assert each field survives. A symmetric test on an
/// empty / partial shape would pass even if format/parse silently
/// dropped a field — so every field carries a distinctive non-default
/// value here, making any format-vs-parse drift a failed assertion.
/// Complements the format-side exhaustive-destructure guard, which
/// catches a field omitted from the serialiser; this test catches the
/// inverse — a field the parser silently fails to recover.
#[test]
fn builder_round_trip_all_fields_non_default() {
    let b = Builder::new("node-full", 40123)
        .secondary_id("sec-roundtrip")
        .cert_pem("-----BEGIN CERTIFICATE-----\nMIIBfull\n-----END CERTIFICATE-----\n")
        .ipv4("10.1.2.3")
        .ipv6("fd00::dead:beef")
        .quic_port(52345)
        .is_observer(true);
    let s = b.format();
    let r = parse(&s).unwrap();
    // `version` is always V2 out of `format()` (it emits the marker).
    assert_eq!(r.version, PeerInfoVersion::V2);
    // host / tunnel_port project onto the legacy URI line.
    assert_eq!(r.legacy_uri.host, "node-full");
    assert_eq!(r.legacy_uri.port, 40123);
    assert_eq!(r.secondary_id.as_deref(), Some("sec-roundtrip"));
    assert_eq!(
        r.cert_pem.as_deref(),
        Some("-----BEGIN CERTIFICATE-----\nMIIBfull\n-----END CERTIFICATE-----\n")
    );
    assert_eq!(r.ipv4.as_deref(), Some("10.1.2.3"));
    assert_eq!(r.ipv6.as_deref(), Some("fd00::dead:beef"));
    assert_eq!(r.quic_port, Some(52345));
    assert_eq!(r.is_observer, Some(true));
}

/// Forward-compat: a v1 reader on a v2 file (i.e. parsing only
/// line 1 via `parse_v1_uri`) still resolves the legacy URI
/// — protects gateway preparation against a v2 wrapper output.
#[test]
fn v1_reader_on_v2_file_works() {
    let v2 = "tcp://compute1:40001\nversion=2\nis_observer=true\n";
    let first_line = v2.split('\n').next().unwrap();
    let uri = parse_v1_uri(first_line).unwrap();
    assert_eq!(uri.host, "compute1");
    assert_eq!(uri.port, 40001);
}

/// base64 round-trip on a non-ASCII byte sequence (covers the
/// inline encoder's correctness for arbitrary PEM contents).
#[test]
fn b64_round_trip_arbitrary_bytes() {
    let inputs = [
        "",
        "f",
        "fo",
        "foo",
        "foob",
        "fooba",
        "foobar",
        "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
    ];
    for s in inputs {
        let enc = encode_b64(s);
        let dec = decode_b64(&enc).unwrap();
        assert_eq!(s, dec, "round-trip failed for {s:?}");
    }
}

/// Cert with bad base64 surfaces a typed error.
#[test]
fn cert_pem_b64_garbage_errors() {
    let err = parse("tcp://h:1\nversion=2\ncert_pem_b64=not!base64!!\n").unwrap_err();
    assert!(matches!(err, PeerInfoError::InvalidCert(_)));
}

/// quic_port out-of-range surfaces InvalidQuicPort with the source
/// `ParseIntError`.
#[test]
fn quic_port_out_of_range_errors() {
    let err = parse("tcp://h:1\nversion=2\nquic_port=999999\n").unwrap_err();
    assert!(matches!(err, PeerInfoError::InvalidQuicPort { .. }));
}

/// Unknown envelope keys are tolerated (forward-compat with future
/// v3 keys).
#[test]
fn parse_unknown_envelope_keys_tolerated() {
    let contents = "tcp://h:1\nversion=2\nipv4=10.0.0.1\nfuture_v3_key=value\n";
    let r = parse(contents).unwrap();
    assert_eq!(r.ipv4.as_deref(), Some("10.0.0.1"));
}

/// `read_dir_v2` returns every v2 `*.info` file in the directory.
/// Drives the Step 9 late-joiner bootstrap: each surviving record
/// is a candidate seed for `join_running_cluster`.
#[test]
fn read_dir_v2_collects_v2_records() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("sec-01.info"),
        "tcp://compute1:40001\nversion=2\nsecondary_id=sec-01\nquic_port=51200\nipv4=10.0.0.1\n",
    )
    .unwrap();
    std::fs::write(
        tmp.path().join("sec-02.info"),
        "tcp://compute2:40002\nversion=2\nsecondary_id=sec-02\nquic_port=51201\nipv6=fd00::2\n",
    )
    .unwrap();

    let mut records = read_dir_v2(tmp.path()).unwrap();
    // Directory enumeration order is OS-dependent; sort by
    // secondary_id so the assertion is deterministic.
    records.sort_by(|a, b| a.secondary_id.cmp(&b.secondary_id));
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].secondary_id.as_deref(), Some("sec-01"));
    assert_eq!(records[0].quic_port, Some(51200));
    assert_eq!(records[1].secondary_id.as_deref(), Some("sec-02"));
    assert_eq!(records[1].quic_port, Some(51201));
}

/// v1 files in the dir are silently dropped — they carry no cert /
/// quic_port, so a snapshot joiner cannot dial them. The mixed
/// case (some v1, some v2) yields the v2 subset, matching the
/// mid-upgrade scenario the parser docs call out.
#[test]
fn read_dir_v2_filters_v1_records() {
    let tmp = tempfile::TempDir::new().unwrap();
    // Legacy v1 file: line-1 URI only.
    std::fs::write(tmp.path().join("legacy.info"), "tcp://oldnode:40000\n").unwrap();
    // v2 file: full envelope.
    std::fs::write(
        tmp.path().join("modern.info"),
        "tcp://newnode:40001\nversion=2\nsecondary_id=modern\nquic_port=51200\n",
    )
    .unwrap();

    let records = read_dir_v2(tmp.path()).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].secondary_id.as_deref(), Some("modern"));
    assert_eq!(records[0].version, PeerInfoVersion::V2);
}

/// Non-`.info` files (a stray log, a backup) are silently skipped —
/// the SLURM connection-info dir may share space with other
/// per-run artefacts; the helper must not error on neighbours.
#[test]
fn read_dir_v2_ignores_non_info_files() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("README.txt"), "ignore me\n").unwrap();
    std::fs::write(tmp.path().join("backup.info.bak"), "garbage\n").unwrap();
    std::fs::write(
        tmp.path().join("sec.info"),
        "tcp://h:1\nversion=2\nsecondary_id=sec\nquic_port=51200\n",
    )
    .unwrap();

    let records = read_dir_v2(tmp.path()).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].secondary_id.as_deref(), Some("sec"));
}

/// All-v1 dir surfaces `NoV2Records` rather than returning an
/// empty Vec — fails loud so the late-joiner doesn't silently hang
/// on `join_running_cluster`'s connect window.
#[test]
fn read_dir_v2_all_v1_errors() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("a.info"), "tcp://h1:1\n").unwrap();
    std::fs::write(tmp.path().join("b.info"), "tcp://h2:2\n").unwrap();

    let err = read_dir_v2(tmp.path()).unwrap_err();
    assert!(
        matches!(err, ReadDirError::NoV2Records { .. }),
        "expected NoV2Records, got {err:?}"
    );
}

/// Empty dir → `NoV2Records`. Same operator-visible signal as
/// all-v1 case.
#[test]
fn read_dir_v2_empty_errors() {
    let tmp = tempfile::TempDir::new().unwrap();
    let err = read_dir_v2(tmp.path()).unwrap_err();
    assert!(matches!(err, ReadDirError::NoV2Records { .. }));
}

/// Non-existent dir surfaces `Io { NotFound, .. }` rather than the
/// generic "no v2 records" — operator sees the underlying OS
/// error.
#[test]
fn read_dir_v2_missing_dir_io_error() {
    let err = read_dir_v2("/this/path/does/not/exist/peerinfodir").unwrap_err();
    assert!(matches!(err, ReadDirError::Io { .. }));
}

/// A malformed v2 file is a typed `Parse` error carrying the
/// offending file path — operator can `vim` straight to it
/// without re-discovering which file fails.
#[test]
fn read_dir_v2_malformed_file_parse_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("good.info"), "tcp://h:1\nversion=2\n").unwrap();
    std::fs::write(tmp.path().join("bad.info"), "not-a-uri\n").unwrap();

    let err = read_dir_v2(tmp.path()).unwrap_err();
    match err {
        ReadDirError::Parse { path, .. } => {
            assert!(path.ends_with("bad.info"), "got path {path}");
        }
        other => panic!("expected Parse error, got {other:?}"),
    }
}
