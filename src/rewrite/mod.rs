// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
mod casts;
mod functions;
mod predicates;

use sqlparser::ast::VisitMut;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Rewrites PostgreSQL-dialect SQL into Trino-compatible SQL.
///
/// # Security
///
/// This function relies on sqlparser's round-trip (parse -> transform -> to_string)
/// being semantics-preserving. On parse failure, the original SQL is returned
/// unchanged -- no partial rewrites. The rewriter only transforms AST nodes,
/// never raw strings, which prevents rewriting-induced SQL injection.
///
/// Applies the following transformations:
/// - `::` cast syntax becomes `CAST(... AS ...)`
/// - PostgreSQL type names are normalized to Trino equivalents
/// - `ILIKE` becomes `lower(x) LIKE lower(pattern)`
/// - PostgreSQL function names are mapped to Trino equivalents
///
/// If parsing fails (e.g. for `SET`, `SHOW`, `DISCARD` commands), the original
/// SQL is returned unchanged.
pub fn rewrite_sql(sql: &str) -> String {
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::new(&dialect).try_with_sql(sql) {
        Ok(mut parser) => match parser.parse_statements() {
            Ok(stmts) => stmts,
            Err(_) => return sql.to_string(),
        },
        Err(_) => return sql.to_string(),
    };

    if statements.is_empty() {
        return sql.to_string();
    }

    let mut cast_rewriter = casts::CastRewriter;
    let mut ilike_rewriter = predicates::ILikeRewriter;
    let mut fn_renamer = functions::FunctionRenamer;

    for stmt in &mut statements {
        let _ = stmt.visit(&mut cast_rewriter);
        let _ = stmt.visit(&mut ilike_rewriter);
        let _ = stmt.visit(&mut fn_renamer);
    }

    statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_double_colon_cast_rewrite() {
        let result = rewrite_sql("SELECT $1::text");
        assert!(result.contains("CAST"), "expected CAST in: {result}");
        assert!(!result.contains("::"), "unexpected :: in: {result}");
    }

    #[test]
    fn test_type_name_normalization() {
        let result = rewrite_sql("SELECT CAST(x AS text) FROM t");
        assert!(
            result.to_uppercase().contains("VARCHAR"),
            "expected VARCHAR in: {result}"
        );
    }

    #[test]
    fn test_int_type_normalization() {
        let result = rewrite_sql("SELECT $1::int4");
        assert!(
            result.to_uppercase().contains("INTEGER"),
            "expected INTEGER in: {result}"
        );
        assert!(!result.contains("::"), "unexpected :: in: {result}");
    }

    #[test]
    fn test_ilike_rewrite() {
        let result = rewrite_sql("SELECT * FROM t WHERE name ILIKE '%foo%'");
        assert!(
            result.to_lowercase().contains("lower"),
            "expected lower() in: {result}"
        );
        assert!(
            !result.to_uppercase().contains("ILIKE"),
            "unexpected ILIKE in: {result}"
        );
    }

    #[test]
    fn test_function_rename_string_agg() {
        let result = rewrite_sql("SELECT string_agg(name, ',') FROM t");
        assert!(result.contains("listagg"), "expected listagg in: {result}");
    }

    #[test]
    fn test_function_rename_log() {
        let result = rewrite_sql("SELECT log(x) FROM t");
        assert!(result.contains("log10"), "expected log10 in: {result}");
    }

    #[test]
    fn test_function_rename_log_two_args_unchanged() {
        let result = rewrite_sql("SELECT log(2, x) FROM t");
        assert!(
            !result.contains("log10"),
            "two-arg log should not be renamed: {result}"
        );
    }

    #[test]
    fn test_function_rename_trunc() {
        let result = rewrite_sql("SELECT trunc(x) FROM t");
        assert!(
            result.contains("truncate"),
            "expected truncate in: {result}"
        );
    }

    #[test]
    fn test_passthrough_clean_sql() {
        let input = "SELECT id, name FROM t WHERE id = 1";
        let result = rewrite_sql(input);
        assert!(result.contains("SELECT"), "expected SELECT in: {result}");
        assert!(result.contains("FROM t"), "expected FROM t in: {result}");
    }

    #[test]
    fn test_unparseable_sql_passes_through() {
        let input = "SET extra_float_digits = 3";
        assert_eq!(rewrite_sql(input), input);
    }

    #[test]
    fn test_show_passes_through() {
        let input = "SHOW server_version";
        // Should not panic, should return something (may parse or fall through)
        let result = rewrite_sql(input);
        assert!(!result.is_empty());
    }
}
