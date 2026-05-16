//! Path helpers shared across gateway implementations.
//!
//! Tilde expansion is required by every gateway that accepts user-supplied
//! "remote" paths (`~`, `~/foo`). The home directory is supplied by the caller
//! because each gateway resolves it differently:
//!   * `LocalGateway` reads `$HOME` from the local process environment.
//!   * `SshGateway` queries `echo $HOME` over the master connection.
//!
//! Keeping the substitution in one place prevents the two gateways from
//! drifting on whitespace, multi-`~` semantics, or non-leading `~` handling.

/// Replace a leading `~` in `path` with `home`. Returns `path` unchanged when
/// it does not start with `~`, or when `home` is `None` (home not yet
/// detected). Only the very first character is considered — `foo/~bar` is
/// left untouched, matching the Python reference
/// (`str.replace("~", home, 1)` after `str.startswith("~")`).
///
/// `home` is an `Option` because both gateways may be asked to expand a path
/// before their home directory is known (pre-connect, or `$HOME` unset).
/// Centralizing the fall-through here keeps the gateways from each
/// re-implementing the same `match` wrapper.
pub fn expand_tilde(path: &str, home: Option<&str>) -> String {
    match (path.strip_prefix('~'), home) {
        (Some(rest), Some(home)) => format!("{home}{rest}"),
        _ => path.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaves_non_tilde_untouched() {
        assert_eq!(expand_tilde("/abs/path", Some("/home/u")), "/abs/path");
        assert_eq!(expand_tilde("rel/path", Some("/home/u")), "rel/path");
    }

    #[test]
    fn expands_bare_tilde() {
        assert_eq!(expand_tilde("~", Some("/home/u")), "/home/u");
    }

    #[test]
    fn expands_tilde_slash() {
        assert_eq!(
            expand_tilde("~/foo/bar", Some("/home/u")),
            "/home/u/foo/bar",
        );
    }

    #[test]
    fn only_leading_tilde_expanded() {
        // Embedded `~` is data, not a home reference.
        assert_eq!(expand_tilde("foo/~bar", Some("/home/u")), "foo/~bar");
    }

    #[test]
    fn preserves_rest_when_home_has_trailing_slash() {
        // The function does not normalize separators — that is the caller's
        // job. Documented behavior: pure prefix replacement.
        assert_eq!(expand_tilde("~/foo", Some("/home/u/")), "/home/u//foo");
    }

    #[test]
    fn no_home_falls_through() {
        assert_eq!(expand_tilde("~/foo", None), "~/foo");
        assert_eq!(expand_tilde("/abs", None), "/abs");
    }
}
