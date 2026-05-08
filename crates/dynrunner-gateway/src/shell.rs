//! POSIX shell-quoting utilities shared by gateway-adjacent code.
//!
//! Single concern: produce text safe to splice into a remote (or
//! local) shell command line. The single-quote escape is the only
//! POSIX-portable form that survives every shell interpreter
//! (sh / bash / dash / zsh) without needing to know the target
//! interpreter or its locale. Used by `ssh.rs` for `find` paths and
//! by the SLURM preparation crate for `cat` paths and ssh argv
//! reproduction.

/// Wrap `s` in POSIX single quotes, escaping any embedded single
/// quote as `'\''`. Safe to interpolate into a remote shell command
/// line.
pub fn shell_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// POSIX shell-join: each part is shell-quoted, then space-separated.
/// Equivalent to Python `shlex.join(parts)`.
pub fn shell_join<I, S>(parts: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut out = String::new();
    let mut first = true;
    for p in parts {
        if !first {
            out.push(' ');
        }
        first = false;
        out.push_str(&shell_quote(p.as_ref()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_simple() {
        assert_eq!(shell_quote("/tmp/foo"), "'/tmp/foo'");
    }

    #[test]
    fn quote_embedded_single_quote() {
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn join_round_trip() {
        let v = ["ssh", "-i", "/path/with space"];
        assert_eq!(shell_join(v), "'ssh' '-i' '/path/with space'");
    }

    #[test]
    fn join_empty() {
        let v: [&str; 0] = [];
        assert_eq!(shell_join(v), "");
    }
}
