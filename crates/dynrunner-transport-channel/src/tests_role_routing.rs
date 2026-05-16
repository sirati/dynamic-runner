//! Step 2 role-cache write-through tests + Step 3/4 role-addressed
//! envelope routing tests. Exercises the cache plumbing
//! (`register_with_cluster_state` + `peer_for_role`), the
//! sender-side dispatch for `Address::Role(_)`, and the four
//! receiver-side cases (A unwrap / B relay+hint / C drop / D drop).
//! Kept as one ~500-line file because the role-routing machinery
//! IS one cohesive concern — every test shares the same minimal
//! `TestRegistrar` fixture and partitioning would scatter the
//! per-case invariants.

use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport, RoleChangeHookRegistrar};

use crate::mesh::peer_mesh;
use crate::tests_peer_basics::{keepalive, SendTestId};

// ── Step 2: role-table write-through cache tests ──
//
// These exercise the channel transport's `register_with_cluster_state`
// + `peer_for_role` pair through a minimal in-test registrar (the
// trait owner — `ClusterState` — lives in a downstream crate; we
// mimic only the part of its contract the transport actually
// touches). Step 3 will couple this to live `ClusterState` flow;
// for now the registrar fires the hook directly so the cache's
// write-through path is the single thing under test.

/// Minimal in-test `RoleChangeHookRegistrar` implementation that
/// holds onto the registered hook and exposes a `fire` method
/// driving it against an arbitrary `RoleTable`. Strictly enough
/// to test the transport's cache plumbing without taking a
/// dev-dep on the cluster-state crate.
type RoleTableHook = Box<
    dyn Fn(&dynrunner_protocol_primary_secondary::RoleTable) + Send + Sync + 'static,
>;

#[derive(Default)]
struct TestRegistrar {
    hooks: Vec<RoleTableHook>,
}

impl TestRegistrar {
    fn fire(&self, table: &dynrunner_protocol_primary_secondary::RoleTable) {
        for h in &self.hooks {
            h(table);
        }
    }
}

impl RoleChangeHookRegistrar for TestRegistrar {
    fn register_role_change_hook(&mut self, hook: RoleTableHook) {
        self.hooks.push(hook);
    }
}

/// After `register_with_cluster_state` runs and the registrar
/// fires with a `RoleTable { primary: Some(id), .. }`, the
/// transport's `peer_for_role(Role::Primary)` returns the same
/// id. Pins the basic write-through path.
#[tokio::test]
async fn peer_transport_role_cache_populates_via_hook() {
    use dynrunner_protocol_primary_secondary::{Role, RoleTable};

    let ids = vec!["a".to_string(), "b".to_string()];
    let transports = peer_mesh::<SendTestId>(&ids);
    let transport = &transports[0];

    assert_eq!(
        transport.peer_for_role(&Role::Primary),
        None,
        "cache empty before registration"
    );

    let mut registrar = TestRegistrar::default();
    transport.register_with_cluster_state(&mut registrar);

    // Still None until the hook actually fires — registration
    // alone does not seed; the authoritative table has to send
    // a `RoleTable` snapshot through.
    assert_eq!(transport.peer_for_role(&Role::Primary), None);

    let table = RoleTable {
        primary: Some("sec-7".to_string()),
        ..Default::default()
    };
    registrar.fire(&table);

    assert_eq!(
        transport.peer_for_role(&Role::Primary),
        Some("sec-7".to_string()),
    );
}

/// A subsequent `PrimaryChanged` (modelled here as a second
/// registrar.fire with a different holder) overwrites the
/// cache. Pins the overwrite contract — Step 3's dispatch will
/// silently misroute if the cache holds a stale id across a
/// promotion.
#[tokio::test]
async fn peer_transport_role_cache_overwrites_on_subsequent_promote() {
    use dynrunner_protocol_primary_secondary::{Role, RoleTable};

    let ids = vec!["a".to_string(), "b".to_string()];
    let transports = peer_mesh::<SendTestId>(&ids);
    let transport = &transports[0];

    let mut registrar = TestRegistrar::default();
    transport.register_with_cluster_state(&mut registrar);

    registrar.fire(&RoleTable {
        primary: Some("first-leader".to_string()),
        ..Default::default()
    });
    assert_eq!(
        transport.peer_for_role(&Role::Primary),
        Some("first-leader".to_string()),
    );

    registrar.fire(&RoleTable {
        primary: Some("second-leader".to_string()),
        ..Default::default()
    });
    assert_eq!(
        transport.peer_for_role(&Role::Primary),
        Some("second-leader".to_string()),
        "second fire must overwrite first leader",
    );

    // Clearing the primary (e.g. an unset table) clears the
    // cache entry — `peer_for_role` returns None again. This
    // is the contract the protocol-crate helper enforces by
    // `remove(&Role::Primary)` ahead of the conditional insert.
    registrar.fire(&RoleTable {
        primary: None,
        ..Default::default()
    });
    assert_eq!(transport.peer_for_role(&Role::Primary), None);
}

// ── Step 3: Address::Role(_) dispatch ──
//
// These exercise the protocol-crate default `send` impl's role
// arm through the channel transport, which now overrides
// `local_id` (so `RoleAddressed.sender_id` carries a meaningful
// value) but does not override `send`. The default impl resolves
// the role through `peer_for_role`, wraps in `RoleAddressed`, and
// calls `send_to_peer` — exactly the Step 3 contract.

/// `send(Address::Role(Role::Primary), msg)` with a populated
/// role cache routes the envelope to the cached holder. Post-
/// Step 4 the receiver unwraps the envelope when its own cache
/// agrees on the holder (Case A); the inner payload — not the
/// wrapper — is what reaches `try_recv_peer`. The wire-frame
/// shape (`RoleAddressed { sender_id, attempts: 0, … }`) is
/// pinned by the codec round-trip tests in `codec_tests.rs`
/// (the only place that observes the wrapper, since both
/// transports now unwrap on receipt).
#[tokio::test]
async fn send_role_primary_routes_via_cache() {
    use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

    let ids = vec!["A".to_string(), "B".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    // Populate BOTH A's and B's caches so Role::Primary -> "B".
    // A's cache drives the send-time route (envelope ships to B);
    // B's cache drives the recv-time decision (Case A unwrap).
    let mut registrar_a = TestRegistrar::default();
    let mut registrar_b = TestRegistrar::default();
    transports[0].register_with_cluster_state(&mut registrar_a);
    transports[1].register_with_cluster_state(&mut registrar_b);
    for r in [&registrar_a, &registrar_b] {
        r.fire(&RoleTable {
            primary: Some("B".to_string()),
            ..Default::default()
        });
    }

    // Sanity: cache populated.
    assert_eq!(
        transports[0].peer_for_role(&Role::Primary),
        Some("B".to_string())
    );

    let inner = keepalive("A");
    transports[0]
        .send(Address::Role(Role::Primary), inner.clone())
        .await
        .expect("Role(Primary) send must succeed with populated cache");

    // Case A: B's cache agrees on the holder, so the recv-side
    // intercept unwraps the envelope. B sees the inner payload
    // — not the RoleAddressed wrapper.
    let received = transports[1].try_recv_peer().expect("B must receive");
    assert_eq!(received.sender_id(), inner.sender_id());
    assert_eq!(received.msg_type(), inner.msg_type());
    // No stray loopback to A.
    assert!(
        transports[0].try_recv_peer().is_none(),
        "sender must not loopback the envelope"
    );
}

/// `send(Address::Role(_), msg)` with an empty cache returns an
/// `Err` whose message names "Role" and "cache" — the contract
/// the trait's default impl documents. No message reaches any
/// peer in this case.
#[tokio::test]
async fn send_role_unresolved_returns_err() {
    use dynrunner_protocol_primary_secondary::{Address, Role};

    let ids = vec!["A".to_string(), "B".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    // Cache deliberately NOT populated.
    let err = transports[0]
        .send(Address::Role(Role::Primary), keepalive("A"))
        .await
        .expect_err("cold cache must error");
    assert!(
        err.contains("Role"),
        "error must reference Role; got: {err}"
    );
    assert!(
        err.contains("cache"),
        "error must reference cache; got: {err}"
    );
    // No message must have reached any peer.
    assert!(transports[0].try_recv_peer().is_none());
    assert!(transports[1].try_recv_peer().is_none());
}

/// Pins the `local_id` plumbing end-to-end: A's id propagates
/// from the `peer_mesh` constructor through the transport's
/// `local_id` override into the `RoleAddressed.sender_id` wire
/// field, and through Step 4's Case-A unwrap path the
/// receiver's recv loop returns the inner payload unmodified.
/// Wire-shape detail of the wrapper's sender_id is covered by
/// the codec round-trip tests; the assertion here is the
/// observable end-to-end behavior under Case A (B's cache
/// agrees it holds Primary, so it unwraps).
#[tokio::test]
async fn send_role_envelope_round_trips_inner_payload() {
    use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

    let ids = vec!["A".to_string(), "B".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    let mut registrar_a = TestRegistrar::default();
    let mut registrar_b = TestRegistrar::default();
    transports[0].register_with_cluster_state(&mut registrar_a);
    transports[1].register_with_cluster_state(&mut registrar_b);
    for r in [&registrar_a, &registrar_b] {
        r.fire(&RoleTable {
            primary: Some("B".to_string()),
            ..Default::default()
        });
    }

    let inner = keepalive("A");
    transports[0]
        .send(Address::Role(Role::Primary), inner.clone())
        .await
        .unwrap();

    let received = transports[1].try_recv_peer().expect("B must receive");
    assert_eq!(received.sender_id(), "A");
    assert_eq!(received.msg_type(), inner.msg_type());
}

// ── Step 4: receiver-side relay-and-hint + Role::Self_ seeding ──
//
// These pin the four cases (A/B/C/D) of `decide_role_addressed`
// through the channel transport's `recv_peer` integration plus the
// construction-time seed of `Role::Self_` into the role cache.
//
// The recv-side intercept lives in `ChannelPeerTransport::
// handle_role_layer` (which calls `decide_role_addressed_with_cache`
// in the protocol crate). The decision module's own unit tests
// (`crates/dynrunner-protocol-primary-secondary/src/role_routing.rs`)
// exercise the pure decision; these tests exercise the wired
// path end-to-end.

/// Case A: a `RoleAddressed { intended_role: Primary }` envelope
/// addressed to a peer whose cache agrees that it holds Primary
/// is unwrapped — `recv_peer` returns the inner payload, not the
/// wrapper. The Step-3 sender-side wire shape is unchanged; what
/// changes here is the recv-side intercept.
#[tokio::test]
async fn role_addressed_case_a_unwraps_to_inner_payload() {
    use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

    let ids = vec!["A".to_string(), "B".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    // BOTH caches agree Primary=B; B's recv hits Case A.
    let mut reg_a = TestRegistrar::default();
    let mut reg_b = TestRegistrar::default();
    transports[0].register_with_cluster_state(&mut reg_a);
    transports[1].register_with_cluster_state(&mut reg_b);
    for r in [&reg_a, &reg_b] {
        r.fire(&RoleTable {
            primary: Some("B".to_string()),
            ..Default::default()
        });
    }

    let inner = keepalive("A");
    transports[0]
        .send(Address::Role(Role::Primary), inner.clone())
        .await
        .unwrap();

    let received = transports[1].try_recv_peer().expect("B receives");
    // Unwrapped: NOT the RoleAddressed wrapper, just the inner.
    assert!(
        matches!(received, DistributedMessage::Keepalive { .. }),
        "Case A must yield the inner payload, not the wrapper",
    );
    assert_eq!(received.sender_id(), inner.sender_id());
    // No additional traffic — no relay, no hint.
    assert!(transports[0].try_recv_peer().is_none());
}

/// Case B: A sends `Address::Role(Primary)` with its cache
/// saying Primary=B; B's cache says Primary=C; C's cache also
/// agrees Primary=C. Assert:
///   1. C receives the forwarded envelope with `attempts=1`
///      (Case A then unwraps for C, so C's `try_recv_peer`
///      yields the inner). To pin `attempts=1` we observe at
///      C's cache state pre-send vs. post-send is not enough,
///      so we additionally make C's cache empty for Primary so
///      the forwarded envelope lands at C as Case C (drop) and
///      we can intercept via C's `try_recv_peer` returning
///      nothing. But that loses the attempts-1 assertion.
///
/// Compromise: configure C's cache to agree (Primary=C) so the
/// payload reaches C unwrapped (Case A at C); the cache-warming
/// hint must arrive at A (decoded into A's role cache as
/// Primary=C). Then we verify A's Primary cache was updated
/// from B to C — pinning the hint round-trip.
///
/// To pin `attempts=1` on the forwarded envelope we additionally
/// inspect the protocol-crate's `decide_role_addressed` unit
/// test (which checks the attempts field directly).
#[tokio::test]
async fn role_addressed_case_b_relays_and_hints() {
    use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

    let ids = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    // A thinks Primary=B; B thinks Primary=C; C thinks Primary=C.
    let mut reg_a = TestRegistrar::default();
    let mut reg_b = TestRegistrar::default();
    let mut reg_c = TestRegistrar::default();
    transports[0].register_with_cluster_state(&mut reg_a);
    transports[1].register_with_cluster_state(&mut reg_b);
    transports[2].register_with_cluster_state(&mut reg_c);
    reg_a.fire(&RoleTable {
        primary: Some("B".to_string()),
        ..Default::default()
    });
    reg_b.fire(&RoleTable {
        primary: Some("C".to_string()),
        ..Default::default()
    });
    reg_c.fire(&RoleTable {
        primary: Some("C".to_string()),
        ..Default::default()
    });
    // Sanity: caches set as designed.
    assert_eq!(
        transports[0].peer_for_role(&Role::Primary),
        Some("B".to_string())
    );

    let inner = keepalive("A");
    transports[0]
        .send(Address::Role(Role::Primary), inner.clone())
        .await
        .unwrap();

    // B receives the wrapper, intercepts it (Case B): forwards
    // to C AND sends a hint back to A. Both internal sends
    // happen synchronously inside B's recv loop. Drive B's
    // recv with try_recv_peer; it should return None because
    // Case B never yields to the application layer.
    assert!(
        transports[1].try_recv_peer().is_none(),
        "B intercepts the envelope at recv-time; nothing surfaces to caller",
    );

    // C receives the forwarded envelope and (Case A on C)
    // unwraps it — try_recv_peer yields the inner.
    let at_c = transports[2].try_recv_peer().expect("C receives forwarded");
    assert!(
        matches!(at_c, DistributedMessage::Keepalive { .. }),
        "C must unwrap (Case A): {at_c:?}",
    );
    assert_eq!(at_c.sender_id(), inner.sender_id());

    // A receives the misaddress-hint and absorbs it into its
    // cache; nothing surfaces to A's caller.
    assert!(
        transports[0].try_recv_peer().is_none(),
        "hint is consumed at recv-time; never surfaced",
    );
    assert_eq!(
        transports[0].peer_for_role(&Role::Primary),
        Some("C".to_string()),
        "A's cache must be updated from B to C by the hint",
    );
}

/// Case C: receiver has no cached holder for the role → drop,
/// no relay, no hint.
#[tokio::test]
async fn role_addressed_case_c_no_holder_drops() {
    use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

    let ids = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    // A thinks Primary=B; B has no Primary in cache; C has no
    // Primary in cache. (Step 4's Role::Self_ seed populates
    // each cache with Self_=self, but Primary stays empty
    // without a hook fire.)
    let mut reg_a = TestRegistrar::default();
    transports[0].register_with_cluster_state(&mut reg_a);
    reg_a.fire(&RoleTable {
        primary: Some("B".to_string()),
        ..Default::default()
    });

    transports[0]
        .send(Address::Role(Role::Primary), keepalive("A"))
        .await
        .unwrap();

    // B intercepts the envelope; no cached holder → drop.
    assert!(
        transports[1].try_recv_peer().is_none(),
        "B should consume the envelope at recv-time (Case C drop)",
    );
    // C must not have received any relay.
    assert!(
        transports[2].try_recv_peer().is_none(),
        "no relay forwarded — no known holder to relay to",
    );
    // A must not have received any hint.
    assert!(
        transports[0].try_recv_peer().is_none(),
        "no hint sent back — Case C is a silent drop",
    );
    // A's cache stays at its pre-send value: Primary=B.
    assert_eq!(
        transports[0].peer_for_role(&Role::Primary),
        Some("B".to_string()),
        "Case C drops silently — sender's cache stays at the stale value",
    );
}

/// Case D: a forwarded envelope already at the relay-hop cap
/// (`MAX_ROLE_RELAY_ATTEMPTS`) must NOT be forwarded further,
/// even if the receiver knows a different holder. We bypass the
/// sender path here (`PeerTransport::send` only ever emits
/// `attempts=0`) and feed the envelope through B's inbox
/// directly by sending it from A using `send_to_peer` — A's
/// direct send carries whatever envelope we wrap manually.
#[tokio::test]
async fn role_addressed_case_d_max_attempts_drops() {
    use dynrunner_protocol_primary_secondary::{
        Role, RoleTable, MAX_ROLE_RELAY_ATTEMPTS,
    };

    let ids = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    // Configure B's cache so it WOULD be a Case-B candidate
    // (Primary=C != self). Without the attempts cap, B would
    // forward to C and hint back to A.
    let mut reg_b = TestRegistrar::default();
    transports[1].register_with_cluster_state(&mut reg_b);
    reg_b.fire(&RoleTable {
        primary: Some("C".to_string()),
        ..Default::default()
    });

    // Hand-craft an envelope at the cap. Sent A→B via direct
    // send_to_peer so it lands at B's inbox unaltered.
    let envelope = DistributedMessage::RoleAddressed {
        sender_id: "A".into(),
        timestamp: 1.0,
        intended_role: Role::Primary,
        payload: Box::new(keepalive("A")),
        attempts: MAX_ROLE_RELAY_ATTEMPTS,
    };
    transports[0].send_to_peer("B", envelope).await.unwrap();

    // B intercepts, Case D drops — nothing forwarded, no hint.
    assert!(transports[1].try_recv_peer().is_none());
    assert!(transports[2].try_recv_peer().is_none());
    assert!(transports[0].try_recv_peer().is_none());
}

/// Step 4 cache-init fix: `Role::Self_` must be populated with
/// the local peer id immediately at construction time, before
/// any `register_with_cluster_state` runs. Without this seed,
/// a `RoleAddressed { intended_role: Self_ }` envelope would
/// fall into Case C at the receiver (no cached holder → drop),
/// which contradicts the role's semantics ("the receiver IS by
/// definition the holder of Self_").
#[tokio::test]
async fn role_self_cache_populated_at_init() {
    use dynrunner_protocol_primary_secondary::Role;

    let ids = vec!["A".to_string(), "B".to_string()];
    let transports = peer_mesh::<SendTestId>(&ids);

    // Self_ resolves to local_id with no hook ever fired.
    assert_eq!(
        transports[0].peer_for_role(&Role::Self_),
        Some("A".to_string()),
    );
    assert_eq!(
        transports[1].peer_for_role(&Role::Self_),
        Some("B".to_string()),
    );

    // Primary, by contrast, stays cold until a hook fires.
    assert_eq!(transports[0].peer_for_role(&Role::Primary), None);
    assert_eq!(transports[1].peer_for_role(&Role::Primary), None);
}
