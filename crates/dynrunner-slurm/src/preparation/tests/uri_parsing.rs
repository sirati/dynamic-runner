//! Tests for `parse_connection_uri` — the post-Step-7 shim delegating
//! to `peer_info::parse_v1_uri`.

use crate::preparation::io::parse_connection_uri;

#[test]
fn parse_uri_tcp() {
    let (h, p) = parse_connection_uri("tcp://node03:54321").unwrap();
    assert_eq!(h, "node03");
    assert_eq!(p, 54321);
}

#[test]
fn parse_uri_with_trailing_newline() {
    let (h, p) = parse_connection_uri("tcp://compute1:1234\n").unwrap();
    assert_eq!(h, "compute1");
    assert_eq!(p, 1234);
}

#[test]
fn parse_uri_quic_scheme() {
    let (h, p) = parse_connection_uri("quic://compute2.cluster.local:60001").unwrap();
    assert_eq!(h, "compute2.cluster.local");
    assert_eq!(p, 60001);
}

#[test]
fn parse_uri_missing_port() {
    let err = parse_connection_uri("tcp://nodeonly").unwrap_err();
    assert!(err.contains("missing port"), "got {err}");
}

#[test]
fn parse_uri_garbage() {
    let err = parse_connection_uri("not a uri at all").unwrap_err();
    // Post-Step-7: error message comes from `peer_info::parse_v1_uri`
    // (the shim delegates), shape is `line 1 is not a valid URI: …`
    // — the substring "not a valid URI" is the load-bearing marker.
    assert!(err.contains("not a valid URI"), "got {err}");
}
