//! Unit tests for `parse_cert_pem` — the PEM→DER bridge used by the
//! peer transport's TLS configuration. The `Err` strings are
//! load-bearing: `dial::dial_peer` surfaces them verbatim as the
//! `reasons=` field of the no-valid-cert WARN, so each failure class
//! must name its specific cause (the pre-fix empty `reasons=` bug).

use super::super::util::parse_cert_pem;
use crate::certs::CertPair;

#[test]
fn parse_cert_pem_works() {
    let cert = CertPair::generate("test").unwrap();
    let der = parse_cert_pem(&cert.cert_pem);
    assert_eq!(der.expect("valid PEM parses").as_ref(), cert.cert_der.as_ref());
}

#[test]
fn parse_cert_pem_empty_names_the_absent_cert() {
    let reason = parse_cert_pem("").expect_err("empty cert field must fail");
    assert!(
        reason.contains("carries no certificate"),
        "the absent-cert reason must say the record has no cert: {reason}"
    );
}

#[test]
fn parse_cert_pem_garbage_names_the_missing_block() {
    let reason = parse_cert_pem("not a cert").expect_err("non-PEM input must fail");
    assert!(
        reason.contains("no CERTIFICATE PEM block"),
        "the malformed-cert reason must name the missing PEM block: {reason}"
    );
}

#[test]
fn parse_cert_pem_bad_base64_names_the_decode_failure() {
    let pem = "-----BEGIN CERTIFICATE-----\n!!!not-base64!!!\n-----END CERTIFICATE-----\n";
    let reason = parse_cert_pem(pem).expect_err("corrupt base64 must fail");
    assert!(
        reason.contains("base64 failed to decode"),
        "the corrupt-cert reason must name the base64 failure: {reason}"
    );
}
