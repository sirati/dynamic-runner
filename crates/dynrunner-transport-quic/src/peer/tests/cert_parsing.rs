//! Unit tests for `parse_cert_pem` — the PEM→DER bridge used by the
//! peer transport's TLS configuration.

use super::super::util::parse_cert_pem;
use crate::certs::CertPair;

#[test]
fn parse_cert_pem_works() {
    let cert = CertPair::generate("test").unwrap();
    let der = parse_cert_pem(&cert.cert_pem);
    assert!(der.is_some());
    assert_eq!(der.unwrap().as_ref(), cert.cert_der.as_ref());
}

#[test]
fn parse_cert_pem_empty_returns_none() {
    assert!(parse_cert_pem("").is_none());
    assert!(parse_cert_pem("not a cert").is_none());
}
