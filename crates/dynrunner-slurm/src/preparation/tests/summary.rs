//! Tests for the honest tunnel-setup summary (#278): the headline must
//! report what was actually VERIFIED (established/expected, with the
//! missing ids named) — never claim "All N" when any tunnel failed.

use std::collections::HashMap;

use crate::preparation::summary::{TunnelSetupSummary, secondary_id};

fn map_of(entries: &[(&str, u16)]) -> HashMap<String, u16> {
    entries
        .iter()
        .map(|(id, port)| (id.to_string(), *port))
        .collect()
}

/// Complete fleet: only then may the headline say "All N".
#[test]
fn complete_fleet_reports_all() {
    let m = map_of(&[
        ("secondary-0", 40000),
        ("secondary-1", 40001),
        ("secondary-2", 40002),
    ]);
    let s = TunnelSetupSummary::new(&m, 3);
    assert!(s.is_complete());
    assert_eq!(s.established, 3);
    assert_eq!(s.expected, 3);
    assert!(s.missing.is_empty());
    assert_eq!(s.to_string(), "All 3 SSH tunnels established");
}

/// THE #278 honesty pin: 2 of 3 verified ⇒ the summary reports 2/3 and
/// NAMES the failed id — the pre-fix emit claimed "All 3" here.
#[test]
fn partial_fleet_reports_count_and_names_missing() {
    let m = map_of(&[("secondary-0", 40000), ("secondary-2", 40002)]);
    let s = TunnelSetupSummary::new(&m, 3);
    assert!(!s.is_complete(), "a partial fleet must not read complete");
    assert_eq!(s.established, 2);
    assert_eq!(s.expected, 3);
    assert_eq!(s.missing, vec!["secondary-1".to_string()]);
    assert_eq!(
        s.to_string(),
        "2/3 SSH tunnels established; missing: secondary-1"
    );
}

/// Multiple failures are all named, in index order.
#[test]
fn missing_ids_listed_in_index_order() {
    let m = map_of(&[("secondary-1", 40001)]);
    let s = TunnelSetupSummary::new(&m, 4);
    assert_eq!(
        s.missing,
        vec![
            "secondary-0".to_string(),
            "secondary-2".to_string(),
            "secondary-3".to_string(),
        ]
    );
    assert_eq!(
        s.to_string(),
        "1/4 SSH tunnels established; missing: secondary-0, secondary-2, secondary-3"
    );
}

/// Zero established (the all-failed shape `gather_under_deadline`
/// normally turns into an Err, but the summary must stay honest if it
/// ever renders).
#[test]
fn zero_established() {
    let s = TunnelSetupSummary::new(&HashMap::new(), 2);
    assert_eq!(s.established, 0);
    assert_eq!(
        s.to_string(),
        "0/2 SSH tunnels established; missing: secondary-0, secondary-1"
    );
}

/// A stray non-canonical key cannot inflate the established count: only
/// EXPECTED ids count toward it.
#[test]
fn stray_keys_do_not_inflate_established() {
    let m = map_of(&[("secondary-0", 40000), ("not-a-secondary", 1)]);
    let s = TunnelSetupSummary::new(&m, 2);
    assert_eq!(s.established, 1);
    assert_eq!(s.missing, vec!["secondary-1".to_string()]);
}

/// The canonical naming helper — the single source the watcher loop and
/// the summary's expected-id enumeration share.
#[test]
fn secondary_id_naming() {
    assert_eq!(secondary_id(0), "secondary-0");
    assert_eq!(secondary_id(17), "secondary-17");
}
