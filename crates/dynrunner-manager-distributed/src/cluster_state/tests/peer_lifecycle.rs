//! Tests for the peer-lifecycle apply rules and the
//! `peer_state` + `RoleTable.observers` lockstep.
//!
//! Pins the contract:
//!
//!   1. `PeerRemoved` is sticky-per-id: once Dead, always Dead. A
//!      duplicate broadcast is a NoOp; a late `PeerJoined` for the
//!      same id is dropped with a warn log (no resurrection).
//!   2. The observer-set projection is maintained in lockstep with
//!      the `peer_state` map — removal of an observer drops them
//!      from `RoleTable.observers` and fires role-change hooks.
//!
//! Also defines the `WarnCapture` tracing layer used by
//! `peer_joined_dead_is_noop` to assert on the warn-log emit.

use super::*;

// ── PeerRemoved + widened PeerJoined apply-rule tests ──
//
// These pin the peer-lifecycle contract on `ClusterState`:
//
//  1. `PeerRemoved` is sticky-per-id: once Dead, always Dead. A
//     duplicate broadcast is a NoOp; a late `PeerJoined` for the
//     same id is dropped with a warn log (no resurrection).
//  2. The observer-set projection is maintained in lockstep with
//     the `peer_state` map — removal of an observer drops them
//     from `RoleTable.observers`.

/// Local capture layer for warn-level tracing events. Scoped to
/// the cluster_state test module — we only need it for the
/// `peer_joined_dead_is_noop` warn-log assertion, so keep it
/// module-private rather than lifting into a shared test util.
struct WarnCapture {
    records: Arc<std::sync::Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        if event.metadata().target() != "dynrunner_cluster_state" {
            return;
        }
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "message" {
                    self.0 = value.to_string();
                }
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        if let Ok(mut buf) = self.records.lock() {
            buf.push(visitor.0);
        }
    }
}

/// Run `body` against a scoped subscriber that captures every
/// warn-level `dynrunner_cluster_state` event.
fn with_warn_capture<F, R>(body: F) -> (R, Vec<String>)
where
    F: FnOnce() -> R,
{
    use tracing_subscriber::layer::SubscriberExt;
    let records: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let layer = WarnCapture {
        records: Arc::clone(&records),
    };
    let subscriber = tracing_subscriber::Registry::default().with(layer);
    let out = tracing::subscriber::with_default(subscriber, body);
    let captured = records.lock().unwrap().clone();
    (out, captured)
}

/// Install a `dynrunner_cluster_state` warn-capture subscriber as the
/// thread-local default for the lifetime of the returned guard, exposing
/// the live record buffer. The async-test analog of `with_warn_capture`:
/// a test that must `await` (e.g. `tokio::time::advance`) between
/// synchronous `apply` bursts holds the guard across the awaits rather
/// than confining capture to one closure.
fn warn_capture_guard() -> (
    tracing::subscriber::DefaultGuard,
    Arc<std::sync::Mutex<Vec<String>>>,
) {
    use tracing_subscriber::layer::SubscriberExt;
    let records: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let layer = WarnCapture {
        records: Arc::clone(&records),
    };
    let subscriber = tracing_subscriber::Registry::default().with(layer);
    (tracing::subscriber::set_default(subscriber), records)
}

/// Idempotent removal: a second `PeerRemoved` for the same id is
/// a silent NoOp under sticky-per-id semantics.
#[test]
fn peer_removed_is_sticky() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p1".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::KeepaliveMiss,
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    // Re-applying PeerRemoved for the same id is a silent NoOp —
    // the entry is already Dead.
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::KeepaliveMiss,
            member_gen: 0,
        }),
        ApplyOutcome::NoOp
    );
}

/// Sticky-per-id under the cross-direction race: once a peer is
/// Dead, a late `PeerJoined` for the same id is a NoOp and emits
/// a warn log. Respawn requires a fresh id.
///
/// The warn-capture half retries a few times: sibling tests in this
/// module hit the SAME warn callsite concurrently (the generation-gate
/// tests apply non-advancing joins too), and tracing's callsite-interest
/// cache rebuild on a fresh `with_default` dispatch can race a
/// concurrent hit, dropping a single emission. The semantics assertions
/// (NoOp + projection) run on every attempt; only the log-capture is
/// retried, and the assertion itself stays strict — the warn MUST be
/// observed under the capture.
#[test]
fn peer_joined_dead_is_noop() {
    let run_once = || {
        with_warn_capture(|| {
            let mut s = ClusterState::<RunnerIdentifier>::new();
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "p1".into(),
                is_observer: false,
                can_be_primary: false,
                cap_version: Default::default(),
                member_gen: 0,
            });
            s.apply(ClusterMutation::PeerRemoved {
                id: "p1".into(),
                cause: RemovalCause::KeepaliveMiss,
                member_gen: 0,
            });
            assert_eq!(
                s.apply(ClusterMutation::PeerJoined {
                    peer_id: "p1".into(),
                    is_observer: true,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                }),
                ApplyOutcome::NoOp,
                "PeerJoined for a Dead id must be NoOp"
            );
            assert!(
                !s.role_table().observers.contains("p1"),
                "Dead peer must not appear in the observer projection",
            );
        })
        .1
    };
    let mut records = Vec::new();
    for _ in 0..5 {
        records = run_once();
        if records
            .iter()
            .any(|m| m.contains("PeerJoined for dead id at a non-advancing generation ignored"))
        {
            break;
        }
    }
    assert!(
        records
            .iter()
            .any(|m| m.contains("PeerJoined for dead id at a non-advancing generation ignored")),
        "expected warn log on PeerJoined for dead id, captured: {records:?}",
    );
}

/// Per-peer WARN throttle (#416): a removed-but-alive peer re-applies the
/// SAME non-advancing `PeerJoined` on every authenticated frame. The WARN
/// must stay (it names a real re-admission stall) but be quiet — once per
/// peer per minute, the suppressed count carried on the next emit, never
/// one line per frame (the 45+ min untrottled spam in
/// run_20260611_123632). Two distinct dead peers each get their OWN
/// throttle window (the gate is keyed by peer id).
///
/// `start_paused` so `tokio::time::advance` drives the `WarnThrottle`
/// interval deterministically; the synchronous `apply` calls read
/// `tokio::time::Instant::now()` against the paused clock.
#[tokio::test(start_paused = true)]
async fn peer_joined_dead_warn_is_throttled_per_peer() {
    let (_, records) = with_warn_capture(|| {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        // Two peers go Dead at gen 0.
        for id in ["p1", "p2"] {
            s.apply(ClusterMutation::PeerJoined {
                peer_id: id.into(),
                is_observer: false,
                can_be_primary: false,
                cap_version: Default::default(),
                member_gen: 0,
            });
            s.apply(ClusterMutation::PeerRemoved {
                id: id.into(),
                cause: RemovalCause::KeepaliveMiss,
                member_gen: 0,
            });
        }
        // Burst of non-advancing rejoins for p1 (the redial-forever
        // shape): every one is a NoOp; only the FIRST trips the WARN,
        // the rest are suppressed within the 60s window.
        for _ in 0..50 {
            assert_eq!(
                s.apply(ClusterMutation::PeerJoined {
                    peer_id: "p1".into(),
                    is_observer: false,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                }),
                ApplyOutcome::NoOp,
            );
        }
        // p2's first rejoin trips its OWN window (distinct peer key),
        // not suppressed by p1's recent emit.
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p2".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        });
    });
    let dead_warns: Vec<&String> = records
        .iter()
        .filter(|m| m.contains("PeerJoined for dead id at a non-advancing generation ignored"))
        .collect();
    assert_eq!(
        dead_warns.len(),
        2,
        "exactly one dead-rejoin WARN per peer in the first window (p1 + p2), \
         NOT one per frame; captured: {records:?}"
    );
}

/// The throttle re-opens after the interval, carrying the suppressed
/// count — the within-episode stall stays narrated minute by minute.
#[tokio::test(start_paused = true)]
async fn peer_joined_dead_warn_reopens_after_interval() {
    let (_guard, records) = warn_capture_guard();
    let rejoin = |s: &mut ClusterState<RunnerIdentifier>| {
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p1".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        });
    };
    let mut s = ClusterState::<RunnerIdentifier>::new();
    rejoin(&mut s);
    s.apply(ClusterMutation::PeerRemoved {
        id: "p1".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    // First window: emit + suppress.
    for _ in 0..5 {
        rejoin(&mut s);
    }
    // Cross the 60s interval, then one more rejoin emits again carrying
    // the suppressed count from the first window.
    tokio::time::advance(std::time::Duration::from_secs(61)).await;
    rejoin(&mut s);

    let captured = records.lock().unwrap().clone();
    let dead_warns: Vec<&String> = captured
        .iter()
        .filter(|m| m.contains("PeerJoined for dead id at a non-advancing generation ignored"))
        .collect();
    assert_eq!(
        dead_warns.len(),
        2,
        "one WARN in the first window + one after the interval re-opens; \
         captured: {captured:?}"
    );
}

// ── RE-ADMISSION lattice (the membership-generation rules) ──
//
// Production face (asm-dataset run_20260610_221140): a flood-starved
// primary authored false `PeerRemoved`s for LIVE peers; the sticky
// tombstone then blocked every later `PeerJoined`, so the cluster could
// never heal. The fix is sticky-per-GENERATION: removal at gen N,
// rejoin at gen N+1 (originated only by the primary's frame-ingest
// re-admission seam, on proof of life).

/// RED→GREEN headline: a `PeerJoined` at a STRICTLY higher generation
/// RE-ADMITS a removed id — liveness returns `Alive`, the role
/// projection includes the peer again, and the capability tombstone is
/// superseded. (Pre-fix this apply was the sticky NoOp forever.)
#[test]
fn peer_joined_at_next_gen_readmits_removed_id() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 0,
    });
    s.apply(ClusterMutation::PeerRemoved {
        id: "p1".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    assert!(!s.is_peer_alive("p1"));
    // The re-admission ticket carries gen+1 and the PRESERVED
    // advertisement from the tombstone.
    let ticket = s
        .removed_peer_readmission("p1")
        .expect("a Dead id must yield a re-admission ticket");
    assert_eq!(ticket.member_gen, 1);
    assert!(!ticket.is_observer);
    assert!(
        ticket.can_be_primary,
        "the tombstone must preserve the departed advertisement"
    );
    // The generation-advancing join re-admits.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p1".into(),
            is_observer: ticket.is_observer,
            can_be_primary: ticket.can_be_primary,
            cap_version: Default::default(),
            member_gen: ticket.member_gen,
        }),
        ApplyOutcome::Applied,
        "a generation-advancing PeerJoined must re-admit the removed id"
    );
    assert!(s.is_peer_alive("p1"), "the re-admitted peer is Alive again");
    assert_eq!(s.peer_member_gen("p1"), 1);
    assert!(
        s.role_table().can_be_primary.contains("p1"),
        "the preserved capability must re-project after re-admission"
    );
    assert!(
        s.removed_peer_readmission("p1").is_none(),
        "a live peer yields no re-admission ticket"
    );
}

/// A STALE removal (generation strictly below the re-admitted entry's)
/// must NOT re-bury the live peer — the removal targeted a superseded
/// membership incarnation.
#[test]
fn stale_removal_loses_to_readmitted_alive_entry() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    s.apply(ClusterMutation::PeerRemoved {
        id: "p1".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 1,
    });
    assert!(s.is_peer_alive("p1"));
    // A delayed/duplicated gen-0 removal (e.g. the roster re-emit of an
    // already-superseded tombstone) loses.
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::KeepaliveMiss,
            member_gen: 0,
        }),
        ApplyOutcome::NoOp,
        "a stale removal of a superseded incarnation must lose"
    );
    assert!(s.is_peer_alive("p1"), "the re-admitted peer stays Alive");
    // A removal AT the current incarnation still kills it (the genuine-
    // death path is intact).
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::KeepaliveMiss,
            member_gen: 1,
        }),
        ApplyOutcome::Applied,
        "a same-generation removal still kills the incarnation"
    );
    assert!(!s.is_peer_alive("p1"));
}

/// Reorder tolerance: a `PeerRemoved` observed BEFORE its incarnation's
/// `PeerJoined` still blocks that join (the sticky rule, per
/// generation), while the NEXT generation's join re-admits.
#[test]
fn removal_observed_before_join_blocks_same_generation_join() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Removal of gen 1 arrives first (never saw the gen-1 join).
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "p1".into(),
            cause: RemovalCause::KeepaliveMiss,
            member_gen: 1,
        }),
        ApplyOutcome::Applied
    );
    // The late gen-1 join is blocked (sticky within the incarnation).
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p1".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 1,
        }),
        ApplyOutcome::NoOp,
        "the same-generation late join must stay buried"
    );
    assert!(!s.is_peer_alive("p1"));
    // The next incarnation's join re-admits.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p1".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 2,
        }),
        ApplyOutcome::Applied
    );
    assert!(s.is_peer_alive("p1"));
}

/// Apply-order convergence: `Removed(gen 0)` and the re-admitting
/// `Joined(gen 1)` land in EITHER order and both replicas converge to
/// Alive at generation 1 (the lattice property the broadcast path
/// relies on).
#[test]
fn readmission_converges_under_reorder() {
    let seed = |s: &mut ClusterState<RunnerIdentifier>| {
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "p1".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        });
    };
    let removed = ClusterMutation::<RunnerIdentifier>::PeerRemoved {
        id: "p1".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    };
    let rejoined = ClusterMutation::<RunnerIdentifier>::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 1,
    };

    let mut a = ClusterState::<RunnerIdentifier>::new();
    seed(&mut a);
    a.apply(removed.clone());
    a.apply(rejoined.clone());

    let mut b = ClusterState::<RunnerIdentifier>::new();
    seed(&mut b);
    b.apply(rejoined);
    b.apply(removed);

    for (name, s) in [("removed-then-rejoined", &a), ("rejoined-then-removed", &b)] {
        assert!(s.is_peer_alive("p1"), "{name}: converges Alive");
        assert_eq!(s.peer_member_gen("p1"), 1, "{name}: converges to gen 1");
    }
}

/// `peer_membership` projects the three diagnostics states honestly —
/// the read the egress no-route message split consumes.
#[test]
fn peer_membership_projection_names_the_three_states() {
    use crate::cluster_state::PeerMembership;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(s.peer_membership("p1"), PeerMembership::NeverJoined);
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    assert_eq!(s.peer_membership("p1"), PeerMembership::AliveMember);
    s.apply(ClusterMutation::PeerRemoved {
        id: "p1".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    assert_eq!(s.peer_membership("p1"), PeerMembership::RemovedMember);
}

/// The widened `PeerJoined` apply rule preserves the observer-set
/// extension semantics: a new observer peer enters the projection,
/// re-application is silent, and a subsequent distinct observer
/// extends the set.
#[test]
fn peer_joined_alive_extends_observer_set() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-a".into(),
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-a".into(),
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::NoOp,
        "re-applying the same PeerJoined is idempotent NoOp"
    );
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-b".into(),
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.role_table().observers,
        HashSet::from(["obs-a".to_string(), "obs-b".to_string()]),
    );
}

/// Removing an observer drops it from `RoleTable.observers` and
/// fires role-change hooks against the post-mutation projection.
#[test]
fn peer_removed_observer_drops_from_role_table() {
    use std::sync::Mutex;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-1".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    assert!(s.role_table().observers.contains("obs-1"));

    let observed: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let observed = Arc::clone(&observed);
        s.register_role_change_hook(Box::new(move |table: &RoleTable| {
            observed.lock().unwrap().push(table.observers.len());
        }));
    }

    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "obs-1".into(),
            cause: RemovalCause::KeepaliveMiss,
            member_gen: 0,
        }),
        ApplyOutcome::Applied
    );
    assert!(
        !s.role_table().observers.contains("obs-1"),
        "PeerRemoved on an observer must drop it from RoleTable.observers"
    );
    let hook_fires = observed.lock().unwrap().clone();
    assert_eq!(
        hook_fires,
        vec![0],
        "role-change hook must fire once with the shrunk set"
    );
}

// ── Sticky-removal slurm-authoritative tiebreak (#546) ────────────────
//
// A `PeerJoined` for a `Dead` id at a non-advancing membership generation
// is the SAME-INCARNATION re-join the sticky rule normally drops. After
// #546 the apply path consults the slurm-authoritative snapshot first:
// if slurm reports the peer's job ALIVE, the dead-mark is REVERSED and
// the existing entry_gen is preserved (this is correction-of-our-error,
// not a new incarnation). Without an Alive verdict — Gone, Unknown, or
// no snapshot wired at all — the original sticky behavior holds.

use crate::authority_snapshot::test_helpers::StaticSnapshot;
use crate::authority_snapshot::PeerLifeState;
use std::collections::HashMap;

fn install_dead_at_gen(state: &mut ClusterState<RunnerIdentifier>, id: &str, member_gen: u64) {
    state.apply(ClusterMutation::PeerJoined {
        peer_id: id.into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen,
    });
    state.apply(ClusterMutation::PeerRemoved {
        id: id.into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen,
    });
}

fn snapshot_alive(ids: &[&str]) -> Arc<StaticSnapshot> {
    let map: HashMap<String, PeerLifeState> = ids
        .iter()
        .map(|i| ((*i).to_string(), PeerLifeState::Alive))
        .collect();
    Arc::new(StaticSnapshot { map, count: None, pending_resources: None })
}

#[test]
fn apply_peer_joined_reverses_sticky_when_authority_alive() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    install_dead_at_gen(&mut s, "p1", 5);
    s.set_authority_snapshot(snapshot_alive(&["p1"]));
    // Non-advancing PeerJoined while authority reports Alive: the
    // dead-mark must be REVERSED and the membership generation must
    // stay at 5 (correction-of-our-error, not a new incarnation).
    let outcome = s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 5,
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert!(s.is_peer_alive("p1"), "sticky-removal must be reversed");
}

#[test]
fn apply_peer_joined_keeps_sticky_when_authority_gone() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    install_dead_at_gen(&mut s, "p1", 5);
    let map: HashMap<String, PeerLifeState> =
        std::iter::once(("p1".to_string(), PeerLifeState::Gone)).collect();
    s.set_authority_snapshot(Arc::new(StaticSnapshot { map, count: None, pending_resources: None }));
    let outcome = s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 5,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::NoOp,
        "authority=Gone → original sticky removal stands"
    );
    assert!(!s.is_peer_alive("p1"));
}

#[test]
fn apply_peer_joined_keeps_sticky_when_authority_unknown() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    install_dead_at_gen(&mut s, "p1", 5);
    s.set_authority_snapshot(Arc::new(StaticSnapshot {
        map: HashMap::new(),
        count: None,
        pending_resources: None,
    }));
    let outcome = s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 5,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::NoOp,
        "authority=Unknown → fail-closed: original sticky removal stands"
    );
    assert!(!s.is_peer_alive("p1"));
}

#[test]
fn apply_peer_joined_keeps_sticky_when_no_authority_configured() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    install_dead_at_gen(&mut s, "p1", 5);
    // No `set_authority_snapshot` call — preserves the original
    // behavior for tests / deployments that never wire a snapshot.
    let outcome = s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 5,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::NoOp,
        "no authority snapshot → original sticky removal stands"
    );
    assert!(!s.is_peer_alive("p1"));
}
