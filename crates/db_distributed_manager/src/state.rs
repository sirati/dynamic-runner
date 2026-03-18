use std::marker::PhantomData;

use db_comm_api_base::ResourceAmount;
use db_transport_quic::QuicConnection;

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
            _state: PhantomData,
        }
    }

    /// Transition: received welcome message with node capabilities.
    pub fn receive_welcome(
        mut self,
        num_workers: u32,
        resources: Vec<ResourceAmount>,
        hostname: String,
        quic_port: u16,
        cert_pem: Option<String>,
    ) -> SecondaryConnection<Handshaking> {
        self.num_workers = num_workers;
        self.resources = resources;
        self.hostname = hostname;
        self.quic_port = quic_port;
        self.cert_pem = cert_pem;
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
    ) -> SecondaryConnection<CertExchanging> {
        self.cert_pem = Some(cert_pem);
        self.ipv4 = ipv4;
        self.ipv6 = ipv6;
        self.quic_port = quic_port;
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
            vec![ResourceAmount { kind: db_comm_api_base::ResourceKind::Memory, amount: 16 * 1024 * 1024 * 1024 }],
            "node1".into(),
            5000,
            None,
        );
        assert_eq!(conn.num_workers, 4);

        let conn = conn.receive_cert_exchange("CERT".into(), Some("10.0.0.1".into()), None, 5001);
        assert_eq!(conn.quic_port, 5001);

        let conn = conn.begin_peer_discovery();
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
}
