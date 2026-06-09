use std::marker::PhantomData;

use dynrunner_core::ResourceAmount;
use dynrunner_transport_quic::QuicConnection;

// ── ZST State Markers ──

pub struct AwaitingWelcome;
pub struct Handshaking;
pub struct CertExchanging;
pub struct PeerDiscovery;
pub struct InitialAssigning;
pub struct Operational;
pub struct ShuttingDown;

/// A secondary connection tracked by the primary, parameterised by state.
///
/// Uses the manual typestate pattern: each state is a ZST unit struct,
/// stored via `PhantomData<S>`. Transitions consume `self` and return the
/// next state, making invalid transitions a compile error.
pub struct SecondaryConnection<S> {
    pub secondary_id: String,
    pub num_workers: u32,
    pub resources: Vec<ResourceAmount>,
    pub hostname: String,
    pub quic_port: u16,
    pub cert_pem: Option<String>,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
    pub transport: Option<QuicConnection>,
    /// Task #36: observer-mode flag, received in `SecondaryWelcome`.
    /// Propagated into `peer_setup::send_peer_lists`'s
    /// `PeerConnectionInfo.is_observer` so other secondaries know
    /// to exclude this peer from `lowest_alive` candidate selection.
    pub is_observer: bool,
    /// Primary-capability marker, received in `SecondaryWelcome` (twin
    /// of `is_observer`). Under mesh-always (pillar 1) a network compute
    /// secondary always holds a peer mesh, so it advertises `true`; only an
    /// observer (or the in-process same-host secondary) advertises `false`.
    /// Persisted here so the post-
    /// mesh roster re-broadcast (`rebroadcast_full_roster`) can re-emit
    /// the exact capability the welcome carried, without consulting a
    /// second source — this typestate is the single record of every
    /// welcome-advertised capability (`num_workers`, `resources`,
    /// `is_observer`, `can_be_primary`).
    pub can_be_primary: bool,
    /// UDP port this peer's liveness-beacon listener is bound on, received
    /// in `CertExchange`. Fanned out via
    /// `peer_setup::send_peer_lists`'s `PeerConnectionInfo.liveness_port`
    /// so every node can beacon this peer once it becomes primary (the
    /// "primary on ANY peer" invariant). `None` for a pre-beacon sender.
    pub liveness_port: Option<u16>,
    _state: PhantomData<S>,
}

impl SecondaryConnection<AwaitingWelcome> {
    /// Create a new connection entry when we first learn about a secondary.
    pub fn new(secondary_id: String) -> Self {
        Self {
            secondary_id,
            num_workers: 0,
            resources: Vec::new(),
            hostname: String::new(),
            quic_port: 0,
            cert_pem: None,
            ipv4: None,
            ipv6: None,
            transport: None,
            is_observer: false,
            can_be_primary: false,
            liveness_port: None,
            _state: PhantomData,
        }
    }

    /// Transition: received welcome message with node capabilities.
    ///
    /// Faithful 1:1 carrier of the `SecondaryWelcome` wire frame's
    /// distinct scalar fields into the typestate; the arg count tracks the
    /// message shape, not an avoidable parameter sprawl (same rationale as
    /// `SecondaryLifecycle::enter_operational`).
    #[allow(clippy::too_many_arguments)]
    pub fn receive_welcome(
        mut self,
        num_workers: u32,
        resources: Vec<ResourceAmount>,
        hostname: String,
        quic_port: u16,
        cert_pem: Option<String>,
        is_observer: bool,
        can_be_primary: bool,
    ) -> SecondaryConnection<Handshaking> {
        self.num_workers = num_workers;
        self.resources = resources;
        self.hostname = hostname;
        self.quic_port = quic_port;
        self.cert_pem = cert_pem;
        self.is_observer = is_observer;
        self.can_be_primary = can_be_primary;
        SecondaryConnection {
            secondary_id: self.secondary_id,
            num_workers: self.num_workers,
            resources: self.resources,
            hostname: self.hostname,
            quic_port: self.quic_port,
            cert_pem: self.cert_pem,
            ipv4: self.ipv4,
            ipv6: self.ipv6,
            transport: self.transport,
            is_observer: self.is_observer,
            can_be_primary: self.can_be_primary,
            liveness_port: self.liveness_port,
            _state: PhantomData,
        }
    }
}

impl SecondaryConnection<Handshaking> {
    /// Transition: received certificate exchange with network info.
    pub fn receive_cert_exchange(
        mut self,
        cert_pem: String,
        ipv4: Option<String>,
        ipv6: Option<String>,
        quic_port: u16,
        liveness_port: Option<u16>,
    ) -> SecondaryConnection<CertExchanging> {
        self.cert_pem = Some(cert_pem);
        self.ipv4 = ipv4;
        self.ipv6 = ipv6;
        self.quic_port = quic_port;
        self.liveness_port = liveness_port;
        SecondaryConnection {
            secondary_id: self.secondary_id,
            num_workers: self.num_workers,
            resources: self.resources,
            hostname: self.hostname,
            quic_port: self.quic_port,
            cert_pem: self.cert_pem,
            ipv4: self.ipv4,
            ipv6: self.ipv6,
            transport: self.transport,
            is_observer: self.is_observer,
            can_be_primary: self.can_be_primary,
            liveness_port: self.liveness_port,
            _state: PhantomData,
        }
    }
}

impl SecondaryConnection<CertExchanging> {
    /// Transition: peer list sent, waiting for peer connections to complete.
    pub fn begin_peer_discovery(self) -> SecondaryConnection<PeerDiscovery> {
        SecondaryConnection {
            secondary_id: self.secondary_id,
            num_workers: self.num_workers,
            resources: self.resources,
            hostname: self.hostname,
            quic_port: self.quic_port,
            cert_pem: self.cert_pem,
            ipv4: self.ipv4,
            ipv6: self.ipv6,
            transport: self.transport,
            is_observer: self.is_observer,
            can_be_primary: self.can_be_primary,
            liveness_port: self.liveness_port,
            _state: PhantomData,
        }
    }
}

impl SecondaryConnection<PeerDiscovery> {
    /// Transition: peer connections confirmed, begin initial assignment.
    pub fn peers_ready(self) -> SecondaryConnection<InitialAssigning> {
        SecondaryConnection {
            secondary_id: self.secondary_id,
            num_workers: self.num_workers,
            resources: self.resources,
            hostname: self.hostname,
            quic_port: self.quic_port,
            cert_pem: self.cert_pem,
            ipv4: self.ipv4,
            ipv6: self.ipv6,
            transport: self.transport,
            is_observer: self.is_observer,
            can_be_primary: self.can_be_primary,
            liveness_port: self.liveness_port,
            _state: PhantomData,
        }
    }
}

impl SecondaryConnection<InitialAssigning> {
    /// Transition: initial tasks assigned, enter operational mode.
    pub fn assignments_sent(self) -> SecondaryConnection<Operational> {
        SecondaryConnection {
            secondary_id: self.secondary_id,
            num_workers: self.num_workers,
            resources: self.resources,
            hostname: self.hostname,
            quic_port: self.quic_port,
            cert_pem: self.cert_pem,
            ipv4: self.ipv4,
            ipv6: self.ipv6,
            transport: self.transport,
            is_observer: self.is_observer,
            can_be_primary: self.can_be_primary,
            liveness_port: self.liveness_port,
            _state: PhantomData,
        }
    }
}

impl SecondaryConnection<Operational> {
    /// Transition: begin shutdown.
    pub fn begin_shutdown(self) -> SecondaryConnection<ShuttingDown> {
        SecondaryConnection {
            secondary_id: self.secondary_id,
            num_workers: self.num_workers,
            resources: self.resources,
            hostname: self.hostname,
            quic_port: self.quic_port,
            cert_pem: self.cert_pem,
            ipv4: self.ipv4,
            ipv6: self.ipv6,
            transport: self.transport,
            is_observer: self.is_observer,
            can_be_primary: self.can_be_primary,
            liveness_port: self.liveness_port,
            _state: PhantomData,
        }
    }
}

// ── Common accessors (available in all states) ──

impl<S> SecondaryConnection<S> {
    pub fn id(&self) -> &str {
        &self.secondary_id
    }

    pub fn set_transport(&mut self, transport: QuicConnection) {
        self.transport = Some(transport);
    }
}

// ── Runtime Enum ──

/// Runtime wrapper so we can store connections in different states in a single Vec/HashMap.
pub enum SecondaryConnectionState {
    AwaitingWelcome(SecondaryConnection<AwaitingWelcome>),
    Handshaking(SecondaryConnection<Handshaking>),
    CertExchanging(SecondaryConnection<CertExchanging>),
    PeerDiscovery(SecondaryConnection<PeerDiscovery>),
    InitialAssigning(SecondaryConnection<InitialAssigning>),
    Operational(SecondaryConnection<Operational>),
    ShuttingDown(SecondaryConnection<ShuttingDown>),
}

impl SecondaryConnectionState {
    pub fn id(&self) -> &str {
        match self {
            Self::AwaitingWelcome(c) => c.id(),
            Self::Handshaking(c) => c.id(),
            Self::CertExchanging(c) => c.id(),
            Self::PeerDiscovery(c) => c.id(),
            Self::InitialAssigning(c) => c.id(),
            Self::Operational(c) => c.id(),
            Self::ShuttingDown(c) => c.id(),
        }
    }

    pub fn num_workers(&self) -> u32 {
        match self {
            Self::AwaitingWelcome(c) => c.num_workers,
            Self::Handshaking(c) => c.num_workers,
            Self::CertExchanging(c) => c.num_workers,
            Self::PeerDiscovery(c) => c.num_workers,
            Self::InitialAssigning(c) => c.num_workers,
            Self::Operational(c) => c.num_workers,
            Self::ShuttingDown(c) => c.num_workers,
        }
    }

    pub fn resources(&self) -> &[ResourceAmount] {
        match self {
            Self::AwaitingWelcome(c) => &c.resources,
            Self::Handshaking(c) => &c.resources,
            Self::CertExchanging(c) => &c.resources,
            Self::PeerDiscovery(c) => &c.resources,
            Self::InitialAssigning(c) => &c.resources,
            Self::Operational(c) => &c.resources,
            Self::ShuttingDown(c) => &c.resources,
        }
    }

    pub fn cert_pem(&self) -> Option<&str> {
        match self {
            Self::AwaitingWelcome(c) => c.cert_pem.as_deref(),
            Self::Handshaking(c) => c.cert_pem.as_deref(),
            Self::CertExchanging(c) => c.cert_pem.as_deref(),
            Self::PeerDiscovery(c) => c.cert_pem.as_deref(),
            Self::InitialAssigning(c) => c.cert_pem.as_deref(),
            Self::Operational(c) => c.cert_pem.as_deref(),
            Self::ShuttingDown(c) => c.cert_pem.as_deref(),
        }
    }

    pub fn ipv4(&self) -> Option<&str> {
        match self {
            Self::AwaitingWelcome(c) => c.ipv4.as_deref(),
            Self::Handshaking(c) => c.ipv4.as_deref(),
            Self::CertExchanging(c) => c.ipv4.as_deref(),
            Self::PeerDiscovery(c) => c.ipv4.as_deref(),
            Self::InitialAssigning(c) => c.ipv4.as_deref(),
            Self::Operational(c) => c.ipv4.as_deref(),
            Self::ShuttingDown(c) => c.ipv4.as_deref(),
        }
    }

    pub fn ipv6(&self) -> Option<&str> {
        match self {
            Self::AwaitingWelcome(c) => c.ipv6.as_deref(),
            Self::Handshaking(c) => c.ipv6.as_deref(),
            Self::CertExchanging(c) => c.ipv6.as_deref(),
            Self::PeerDiscovery(c) => c.ipv6.as_deref(),
            Self::InitialAssigning(c) => c.ipv6.as_deref(),
            Self::Operational(c) => c.ipv6.as_deref(),
            Self::ShuttingDown(c) => c.ipv6.as_deref(),
        }
    }

    pub fn quic_port(&self) -> u16 {
        match self {
            Self::AwaitingWelcome(c) => c.quic_port,
            Self::Handshaking(c) => c.quic_port,
            Self::CertExchanging(c) => c.quic_port,
            Self::PeerDiscovery(c) => c.quic_port,
            Self::InitialAssigning(c) => c.quic_port,
            Self::Operational(c) => c.quic_port,
            Self::ShuttingDown(c) => c.quic_port,
        }
    }

    /// Liveness-beacon UDP port this peer advertised in its
    /// `CertExchange`. `None` until cert-exchange lands (or a pre-beacon
    /// sender). Fanned out via `PeerConnectionInfo.liveness_port` so peers
    /// know where to beacon this node once it becomes primary.
    pub fn liveness_port(&self) -> Option<u16> {
        match self {
            Self::AwaitingWelcome(c) => c.liveness_port,
            Self::Handshaking(c) => c.liveness_port,
            Self::CertExchanging(c) => c.liveness_port,
            Self::PeerDiscovery(c) => c.liveness_port,
            Self::InitialAssigning(c) => c.liveness_port,
            Self::Operational(c) => c.liveness_port,
            Self::ShuttingDown(c) => c.liveness_port,
        }
    }

    /// Observer mode (task #36). False until receive_welcome carries
    /// the flag — pre-welcome states default to false, which matches
    /// the "regular secondary" wire-compat default.
    pub fn is_observer(&self) -> bool {
        match self {
            Self::AwaitingWelcome(c) => c.is_observer,
            Self::Handshaking(c) => c.is_observer,
            Self::CertExchanging(c) => c.is_observer,
            Self::PeerDiscovery(c) => c.is_observer,
            Self::InitialAssigning(c) => c.is_observer,
            Self::Operational(c) => c.is_observer,
            Self::ShuttingDown(c) => c.is_observer,
        }
    }

    /// Primary-capability marker (twin of `is_observer`). False until
    /// `receive_welcome` carries the flag — pre-welcome states default
    /// to false, the conservative non-capable default. Read by the
    /// post-mesh roster re-broadcast (`rebroadcast_full_roster`) to
    /// re-emit the exact `can_be_primary` the welcome advertised.
    pub fn can_be_primary(&self) -> bool {
        match self {
            Self::AwaitingWelcome(c) => c.can_be_primary,
            Self::Handshaking(c) => c.can_be_primary,
            Self::CertExchanging(c) => c.can_be_primary,
            Self::PeerDiscovery(c) => c.can_be_primary,
            Self::InitialAssigning(c) => c.can_be_primary,
            Self::Operational(c) => c.can_be_primary,
            Self::ShuttingDown(c) => c.can_be_primary,
        }
    }

    /// True if we have received the welcome and cert exchange.
    pub fn is_at_least_cert_exchanged(&self) -> bool {
        matches!(
            self,
            Self::CertExchanging(_)
                | Self::PeerDiscovery(_)
                | Self::InitialAssigning(_)
                | Self::Operational(_)
                | Self::ShuttingDown(_)
        )
    }

    pub fn is_operational(&self) -> bool {
        matches!(self, Self::Operational(_))
    }

    /// Access the transport (available in any state that has one).
    pub fn transport_mut(&mut self) -> Option<&mut QuicConnection> {
        match self {
            Self::AwaitingWelcome(c) => c.transport.as_mut(),
            Self::Handshaking(c) => c.transport.as_mut(),
            Self::CertExchanging(c) => c.transport.as_mut(),
            Self::PeerDiscovery(c) => c.transport.as_mut(),
            Self::InitialAssigning(c) => c.transport.as_mut(),
            Self::Operational(c) => c.transport.as_mut(),
            Self::ShuttingDown(c) => c.transport.as_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_machine_full_lifecycle() {
        let conn = SecondaryConnection::new("sec-0".into());
        assert_eq!(conn.id(), "sec-0");

        let conn = conn.receive_welcome(
            4,
            vec![ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: 16 * 1024 * 1024 * 1024,
            }],
            "node1".into(),
            5000,
            None,
            false,
            false,
        );
        assert_eq!(conn.num_workers, 4);

        let conn = conn.receive_cert_exchange(
            "CERT".into(),
            Some("10.0.0.1".into()),
            Some("2001:db8::1".into()),
            5001,
            Some(5002),
        );
        assert_eq!(conn.quic_port, 5001);
        assert_eq!(conn.liveness_port, Some(5002));
        // Both address families round-trip the typestate transition
        // unchanged — the dialer needs both to populate its
        // happy-eyeballs candidate set. Regression test for the
        // primary-side ipv6-drop bug fixed alongside this assertion.
        assert_eq!(conn.ipv4.as_deref(), Some("10.0.0.1"));
        assert_eq!(conn.ipv6.as_deref(), Some("2001:db8::1"));

        let conn = conn.begin_peer_discovery();
        assert_eq!(conn.ipv4.as_deref(), Some("10.0.0.1"));
        assert_eq!(conn.ipv6.as_deref(), Some("2001:db8::1"));
        let conn = conn.peers_ready();
        let conn = conn.assignments_sent();
        let _conn = conn.begin_shutdown();
    }

    #[test]
    fn runtime_enum_wraps_states() {
        let conn = SecondaryConnection::new("sec-1".into());
        let state = SecondaryConnectionState::AwaitingWelcome(conn);
        assert_eq!(state.id(), "sec-1");
        assert_eq!(state.num_workers(), 0);
        assert!(!state.is_operational());
    }

    #[test]
    fn runtime_enum_ipv6_getter_traverses_states() {
        // Build a connection that has both ipv4 and ipv6 set, then
        // walk it through every state-machine variant and confirm the
        // `ipv6()` accessor returns the value at each step. Pin
        // alongside `ipv4()` so a future drift between the two
        // accessors becomes a test failure.
        let conn = SecondaryConnection::new("sec-2".into());
        let conn = conn.receive_welcome(1, vec![], "h".into(), 5000, None, false, false);
        let conn = conn.receive_cert_exchange(
            "CERT".into(),
            Some("10.0.0.2".into()),
            Some("2001:db8::2".into()),
            5000,
            None,
        );

        let state = SecondaryConnectionState::CertExchanging(conn);
        assert_eq!(state.ipv4(), Some("10.0.0.2"));
        assert_eq!(state.ipv6(), Some("2001:db8::2"));

        // Step into PeerDiscovery / InitialAssigning / Operational /
        // ShuttingDown and confirm both getters keep returning the
        // original values.
        let SecondaryConnectionState::CertExchanging(c) = state else {
            unreachable!();
        };
        let state = SecondaryConnectionState::PeerDiscovery(c.begin_peer_discovery());
        assert_eq!(state.ipv6(), Some("2001:db8::2"));
        let SecondaryConnectionState::PeerDiscovery(c) = state else {
            unreachable!();
        };
        let state = SecondaryConnectionState::InitialAssigning(c.peers_ready());
        assert_eq!(state.ipv6(), Some("2001:db8::2"));
        let SecondaryConnectionState::InitialAssigning(c) = state else {
            unreachable!();
        };
        let state = SecondaryConnectionState::Operational(c.assignments_sent());
        assert_eq!(state.ipv6(), Some("2001:db8::2"));
        let SecondaryConnectionState::Operational(c) = state else {
            unreachable!();
        };
        let state = SecondaryConnectionState::ShuttingDown(c.begin_shutdown());
        assert_eq!(state.ipv6(), Some("2001:db8::2"));
    }
}
