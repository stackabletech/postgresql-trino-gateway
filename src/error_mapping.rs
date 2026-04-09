use pgwire::error::ErrorInfo;

/// Map a Trino error message to a PostgreSQL `ErrorInfo` with an appropriate SQLSTATE code.
///
/// Trino error messages are parsed for known patterns (e.g. `SYNTAX_ERROR`,
/// `TABLE_NOT_FOUND`) and translated into the closest PostgreSQL SQLSTATE code.
/// See <https://www.postgresql.org/docs/current/errcodes-appendix.html>.
pub fn trino_error_to_pg(error_msg: &str) -> ErrorInfo {
    let upper = error_msg.to_uppercase();

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

    ErrorInfo::new("ERROR".to_owned(), sqlstate.to_owned(), error_msg.to_owned())
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
    fn preserves_original_message() {
        let msg = "SYNTAX_ERROR: mismatched input 'SELECT'";
        let info = trino_error_to_pg(msg);
        assert_eq!(info.message, msg);
    }
}
