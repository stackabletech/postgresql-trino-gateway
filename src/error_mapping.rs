// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use pgwire::error::ErrorInfo;

// What goes back to the client is sanitised; what goes into the gateway's
// own logs is not. README's "Logging and information disclosure" section
// describes the resulting trade-off — operators should treat the gateway
// log stream as having the same sensitivity as Trino's own server log.

/// Maximum length of the sanitised error message returned to the client.
/// Trino errors with long single-line messages (e.g. catalogs of identifiers
/// in a "Function not found" error) can otherwise reach kilobytes; sending a
/// 30 KB error to a UI client is hostile.
const MAX_CLIENT_ERROR_LEN: usize = 512;

/// Map a Trino error message to a PostgreSQL `ErrorInfo` with an appropriate SQLSTATE code.
///
/// SQLSTATE classification runs over the full original message — Java stack
/// frames and internal class names contain useful keywords. The text returned
/// to the client is then sanitised to drop Trino-internal class FQNs, stack
/// traces, hostnames in URLs, and is capped at `MAX_CLIENT_ERROR_LEN` bytes.
///
/// We do NOT log the raw message: Trino error messages can embed literal
/// values from the user's data ("Cannot cast '2024-foo' as DATE"), and
/// AGENTS.md prohibits logging row contents. Operators needing the full
/// stack trace should look at Trino's own logs.
pub fn trino_error_to_pg(error_msg: &str) -> ErrorInfo {
    let upper = error_msg.to_ascii_uppercase();

    let sqlstate = if upper.contains("SYNTAX_ERROR") || is_syntax_position(&upper) {
        "42601" // syntax_error
    } else if upper.contains("TABLE_NOT_FOUND") || upper.contains("DOES NOT EXIST") {
        "42P01" // undefined_table
    } else if upper.contains("COLUMN_NOT_FOUND") || upper.contains("CANNOT BE RESOLVED") {
        "42703" // undefined_column
    } else if upper.contains("TYPE_MISMATCH") {
        "42846" // cannot_coerce
    } else if upper.contains("PERMISSION_DENIED") || upper.contains("ACCESS DENIED") {
        "42501" // insufficient_privilege
    } else if upper.contains("DIVISION_BY_ZERO") {
        "22012" // division_by_zero
    } else {
        "42000" // syntax_error_or_access_rule_violation (generic)
    };

    ErrorInfo::new(
        "ERROR".to_owned(),
        sqlstate.to_owned(),
        sanitise_for_client(error_msg),
    )
}

/// Strip Trino-internal noise from an error message before sending it to the
/// client.
///
/// What is removed:
/// - Trailing Java stack traces beginning at `\n\tat ` or `\n  at `.
/// - Leading Java exception class FQNs like `io.trino.spi.TrinoException: `.
///   Trino sometimes wraps exceptions, so the strip is repeated until no
///   further FQN is found.
/// - Hostnames inside `http://` and `https://` URLs (replaced with `<trino>`).
///
/// The result is capped at `MAX_CLIENT_ERROR_LEN` bytes.
///
/// What is intentionally kept (informational, not sensitive):
/// - The user-facing error description, which may include the user's own
///   SQL, identifiers from their query, and any SQLSTATE-equivalent keywords.
/// - Bare hostnames or IPs that appear outside URLs (e.g. "Worker 10.0.0.5
///   unreachable") — masking these without false positives requires a
///   regex-grade matcher, and the URL form is the common case for Trino.
fn sanitise_for_client(msg: &str) -> String {
    let truncated = msg
        .find("\n\tat ")
        .or_else(|| msg.find("\n  at "))
        .map(|idx| &msg[..idx])
        .unwrap_or(msg);

    let mut prefix_stripped = truncated;
    loop {
        let next = strip_java_exception_prefix(prefix_stripped);
        if std::ptr::eq(next, prefix_stripped) {
            break;
        }
        prefix_stripped = next;
    }
    let urls_masked = mask_url_hosts(prefix_stripped);
    let mut out = urls_masked.trim().to_owned();
    truncate_with_ellipsis(&mut out, MAX_CLIENT_ERROR_LEN);
    out
}

/// Truncate `s` to at most `max` bytes, ending on a UTF-8 char boundary, and
/// append `…` when truncation occurred. No-op if `s` already fits.
fn truncate_with_ellipsis(s: &mut String, max: usize) {
    if s.len() <= max {
        return;
    }
    // Ellipsis is 3 bytes; reserve room for it inside the budget.
    let mut cut = max.saturating_sub(3);
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    s.push('…');
}

/// If `s` starts with a Java FQN ending in `Exception`/`Error` followed by
/// `: `, drop the prefix. Recognised by a leading `io.`, `com.`, `java.`,
/// or `org.` package path.
fn strip_java_exception_prefix(s: &str) -> &str {
    let Some(colon) = s.find(": ") else {
        return s;
    };
    let prefix = &s[..colon];
    let looks_like_package = prefix.starts_with("io.")
        || prefix.starts_with("com.")
        || prefix.starts_with("java.")
        || prefix.starts_with("org.");
    let looks_like_exception = prefix.ends_with("Exception") || prefix.ends_with("Error");
    if looks_like_package && looks_like_exception {
        s[colon + 2..].trim_start()
    } else {
        s
    }
}

/// Replace the host portion of every `http://` or `https://` URL in `s` with
/// the literal `<trino>`. Stops at the next `/`, whitespace, `)`, or `,`.
/// Other Trino-side error messages occasionally include nextUri-style URLs.
fn mask_url_hosts(s: &str) -> String {
    const SCHEMES: &[&str] = &["https://", "http://"];
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    'outer: while !rest.is_empty() {
        for scheme in SCHEMES {
            if let Some(idx) = rest.find(scheme) {
                out.push_str(&rest[..idx]);
                out.push_str(scheme);
                let after = &rest[idx + scheme.len()..];
                let host_end = after
                    .find(|c: char| c == '/' || c == ')' || c == ',' || c.is_whitespace())
                    .unwrap_or(after.len());
                out.push_str("<trino>");
                rest = &after[host_end..];
                continue 'outer;
            }
        }
        out.push_str(rest);
        break;
    }
    out
}

/// Detect Trino's positional syntax error pattern like "line 1:7:".
fn is_syntax_position(upper: &str) -> bool {
    // Trino reports syntax errors as "line <N>:<N>: ..."
    upper.starts_with("LINE ")
        && upper
            .find(':')
            .and_then(|first| upper[first + 1..].find(':').map(|_| true))
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntax_error_keyword() {
        let info = trino_error_to_pg("Query failed: SYNTAX_ERROR: mismatched input");
        assert_eq!(info.code, "42601");
        assert_eq!(info.severity, "ERROR");
    }

    #[test]
    fn syntax_error_line_position() {
        let info = trino_error_to_pg("line 1:7: mismatched input 'FROM'");
        assert_eq!(info.code, "42601");
    }

    #[test]
    fn table_not_found_keyword() {
        let info = trino_error_to_pg("TABLE_NOT_FOUND: Table 'foo' not found");
        assert_eq!(info.code, "42P01");
    }

    #[test]
    fn table_does_not_exist() {
        let info = trino_error_to_pg("Table 'memory.default.missing' does not exist");
        assert_eq!(info.code, "42P01");
    }

    #[test]
    fn column_not_found() {
        let info = trino_error_to_pg("COLUMN_NOT_FOUND: Column 'x' cannot be resolved");
        assert_eq!(info.code, "42703");
    }

    #[test]
    fn column_cannot_be_resolved() {
        let info = trino_error_to_pg("Column 'x' cannot be resolved");
        assert_eq!(info.code, "42703");
    }

    #[test]
    fn type_mismatch() {
        let info = trino_error_to_pg("TYPE_MISMATCH: Cannot apply operator");
        assert_eq!(info.code, "42846");
    }

    #[test]
    fn permission_denied_keyword() {
        let info = trino_error_to_pg("PERMISSION_DENIED: Cannot access catalog");
        assert_eq!(info.code, "42501");
    }

    #[test]
    fn access_denied() {
        let info = trino_error_to_pg("Access Denied: Cannot select from table");
        assert_eq!(info.code, "42501");
    }

    #[test]
    fn division_by_zero() {
        let info = trino_error_to_pg("DIVISION_BY_ZERO");
        assert_eq!(info.code, "22012");
    }

    #[test]
    fn unknown_falls_through_to_default() {
        let info = trino_error_to_pg("Something totally unexpected happened");
        assert_eq!(info.code, "42000");
    }

    #[test]
    fn preserves_original_message_when_clean() {
        let msg = "SYNTAX_ERROR: mismatched input 'SELECT'";
        let info = trino_error_to_pg(msg);
        assert_eq!(info.message, msg);
    }

    #[test]
    fn strips_java_stack_trace() {
        let msg = "io.trino.spi.TrinoException: Table 'memory.foo' does not exist\n\
                   \tat io.trino.metadata.MetadataManager.resolveTableHandle(MetadataManager.java:412)\n\
                   \tat io.trino.sql.analyzer.StatementAnalyzer.analyze(StatementAnalyzer.java:1234)";
        let info = trino_error_to_pg(msg);
        assert_eq!(info.message, "Table 'memory.foo' does not exist");
        assert_eq!(info.code, "42P01");
    }

    #[test]
    fn strips_java_exception_class_fqn() {
        let msg = "io.trino.spi.security.AccessDeniedException: Access Denied: Cannot select from table foo.bar";
        let info = trino_error_to_pg(msg);
        assert_eq!(
            info.message,
            "Access Denied: Cannot select from table foo.bar"
        );
        assert_eq!(info.code, "42501");
    }

    #[test]
    fn keeps_messages_without_java_prefix() {
        let info = trino_error_to_pg("Column 'x' cannot be resolved");
        assert_eq!(info.message, "Column 'x' cannot be resolved");
    }

    #[test]
    fn masks_internal_hostnames_in_urls() {
        let msg = "error sending request for url (https://internal-trino-coord.prod:8443/v1/statement/abc): connection refused";
        let info = trino_error_to_pg(msg);
        assert!(
            !info.message.contains("internal-trino-coord"),
            "hostname must be masked: {}",
            info.message
        );
        assert!(info.message.contains("<trino>"));
        assert!(info.message.contains("/v1/statement/abc"));
    }

    #[test]
    fn masks_multiple_urls() {
        let msg = "fetch from https://worker-1.cluster:8443/x failed, retried via http://worker-2.cluster:8443/x";
        let info = trino_error_to_pg(msg);
        assert!(!info.message.contains("worker-1.cluster"));
        assert!(!info.message.contains("worker-2.cluster"));
        assert_eq!(info.message.matches("<trino>").count(), 2);
    }

    #[test]
    fn sqlstate_classification_uses_full_message() {
        // Sanitisation drops `io.trino.spi.TrinoException`, but the
        // SQLSTATE classifier still sees the keyword in the full string.
        let msg = "io.trino.spi.security.AccessDeniedException: PERMISSION_DENIED: foo";
        let info = trino_error_to_pg(msg);
        assert_eq!(info.code, "42501");
    }

    #[test]
    fn strips_nested_exception_wrappers() {
        let msg =
            "io.trino.spi.TrinoException: io.trino.spi.PrestoException: Table 'foo' does not exist";
        let info = trino_error_to_pg(msg);
        assert_eq!(info.message, "Table 'foo' does not exist");
    }

    #[test]
    fn caps_oversized_messages() {
        let huge = format!("Error: {}", "x".repeat(10_000));
        let info = trino_error_to_pg(&huge);
        assert!(
            info.message.len() <= MAX_CLIENT_ERROR_LEN,
            "expected <= {} bytes, got {}",
            MAX_CLIENT_ERROR_LEN,
            info.message.len()
        );
        assert!(
            info.message.ends_with('…'),
            "truncated message should end with ellipsis"
        );
    }

    #[test]
    fn truncate_respects_char_boundary() {
        // Pad with non-ASCII so the naive byte-cut would fall mid-character.
        let mut s = "ä".repeat(300); // each 'ä' is 2 bytes -> 600 bytes total
        truncate_with_ellipsis(&mut s, 50);
        assert!(s.len() <= 50);
        // Must still be valid UTF-8 (the test framework would have already
        // panicked if String::truncate had cut a code point).
        assert!(s.ends_with('…'));
    }
}
