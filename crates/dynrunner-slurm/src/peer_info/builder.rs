//! [`Builder`]: programmatic construction of a v2 peer-info record
//! plus the [`Builder::format`] method that emits the on-disk string
//! (legacy URI line followed by the envelope). Inverse of
//! [`parse`](super::parse_impl::parse).

use std::fmt::Write as _;

use super::b64::encode_b64;

/// Builder for a v2 record. Owns the `(host, tunnel_port)` legacy URI
/// alongside the envelope fields. Producing the final on-disk string
/// goes through [`Builder::format`] so the file shape (line 1 then
/// envelope) is centralised here, not duplicated across writers.
#[derive(Debug, Clone)]
pub struct Builder {
    pub host: String,
    pub tunnel_port: u16,
    pub secondary_id: Option<String>,
    pub cert_pem: Option<String>,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
    pub quic_port: Option<u16>,
    pub is_observer: Option<bool>,
}

impl Builder {
    /// Construct a builder with only the legacy URI populated. Other
    /// fields default to `None`; callers fluently set the ones they
    /// know.
    pub fn new(host: impl Into<String>, tunnel_port: u16) -> Self {
        Self {
            host: host.into(),
            tunnel_port,
            secondary_id: None,
            cert_pem: None,
            ipv4: None,
            ipv6: None,
            quic_port: None,
            is_observer: None,
        }
    }

    pub fn secondary_id(mut self, s: impl Into<String>) -> Self {
        self.secondary_id = Some(s.into());
        self
    }

    pub fn cert_pem(mut self, s: impl Into<String>) -> Self {
        self.cert_pem = Some(s.into());
        self
    }

    pub fn ipv4(mut self, s: impl Into<String>) -> Self {
        self.ipv4 = Some(s.into());
        self
    }

    pub fn ipv6(mut self, s: impl Into<String>) -> Self {
        self.ipv6 = Some(s.into());
        self
    }

    pub fn quic_port(mut self, p: u16) -> Self {
        self.quic_port = Some(p);
        self
    }

    pub fn is_observer(mut self, b: bool) -> Self {
        self.is_observer = Some(b);
        self
    }

    /// Render the on-disk string. Line 1 is the legacy URI; lines 2+
    /// are the envelope, version-key first then alphabetical (so a
    /// `diff` of two files is deterministic). Trailing newline.
    pub fn format(&self) -> String {
        // Exhaustive destructure (NO `..` rest pattern) — the structural
        // completeness guard for the format side. Every `Builder` field
        // is NAMED here, so adding a future field is a COMPILE ERROR at
        // this site until the developer either emits it into the envelope
        // or explicitly classifies it as not-on-the-wire. This is the only
        // mechanism that catches a field silently omitted from the
        // serialised record (the bug this exists to prevent); the sibling
        // `peer_info` round-trip test guards the inverse drift on `parse`.
        let Builder {
            host,
            tunnel_port,
            secondary_id,
            cert_pem,
            ipv4,
            ipv6,
            quic_port,
            is_observer,
        } = self;
        let mut out = String::with_capacity(256);
        // Line 1: legacy URI. `tcp://` is the framework convention
        // for SSH-reverse-tunnel mode (see preparation.rs's
        // back-compat reader). Writers in other modes can swap the
        // scheme inline via a direct line-1 string if they need to;
        // for now the only caller is the reverse-mode wrapper.
        let _ = writeln!(&mut out, "tcp://{host}:{tunnel_port}");
        let _ = writeln!(&mut out, "version=2");
        if let Some(s) = secondary_id {
            let _ = writeln!(&mut out, "secondary_id={s}");
        }
        if let Some(s) = cert_pem {
            let _ = writeln!(&mut out, "cert_pem_b64={}", encode_b64(s));
        }
        if let Some(s) = ipv4 {
            let _ = writeln!(&mut out, "ipv4={s}");
        }
        if let Some(s) = ipv6 {
            let _ = writeln!(&mut out, "ipv6={s}");
        }
        if let Some(p) = quic_port {
            let _ = writeln!(&mut out, "quic_port={p}");
        }
        if let Some(b) = is_observer {
            let _ = writeln!(&mut out, "is_observer={b}");
        }
        out
    }
}
