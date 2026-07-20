// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
mod casts;
mod functions;
mod limit_offset;
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
/// unchanged — no partial rewrites. The rewriter only transforms AST nodes,
/// never raw strings, which prevents rewriting-induced SQL injection.
///
/// Applies the following transformations:
/// - `::` cast syntax becomes `CAST(... AS ...)`
/// - PostgreSQL type names are normalized to Trino equivalents
/// - `ILIKE` becomes `lower(x) LIKE lower(pattern)`
/// - PostgreSQL function names are mapped to Trino equivalents
/// - `LIMIT n OFFSET m` is reordered into Trino order
///   (`OFFSET m FETCH FIRST n ROWS ONLY`)
///
/// If parsing fails (e.g. for `SET`, `SHOW`, `DISCARD` commands), the original
/// SQL is returned unchanged.
///
/// Multi-statement input is rejected — the caller (`query_pipeline`) splits
/// statements before reaching the rewriter, so a multi-statement string here
/// is a bug. We pass it through unchanged and Trino will surface the error.
pub fn rewrite_sql(sql: &str) -> String {
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::new(&dialect).try_with_sql(sql) {
        Ok(mut parser) => match parser.parse_statements() {
            Ok(stmts) => stmts,
            Err(_) => return sql.to_string(),
        },
        Err(_) => return sql.to_string(),
    };

    let stmt = match statements.len() {
        0 => return sql.to_string(),
        1 => &mut statements[0],
        _ => {
            // Should not happen — see the doc-comment. Logging instead of
            // joining-with-semicolons because the join was the original
            // multi-statement bug we are deliberately avoiding here.
            tracing::warn!(
                count = statements.len(),
                "rewrite_sql received multi-statement input; passing through unchanged"
            );
            return sql.to_string();
        }
    };

    let mut cast_rewriter = casts::CastRewriter;
    let mut ilike_rewriter = predicates::ILikeRewriter;
    let mut fn_renamer = functions::FunctionRenamer;
    let mut limit_offset_rewriter = limit_offset::LimitOffsetRewriter;
    let _ = stmt.visit(&mut cast_rewriter);
    let _ = stmt.visit(&mut ilike_rewriter);
    let _ = stmt.visit(&mut fn_renamer);
    let _ = stmt.visit(&mut limit_offset_rewriter);

    stmt.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each case asserts that the output contains every substring in
    /// `must_contain` (case-insensitive) and none of the substrings in
    /// `must_not_contain`.
    struct Case {
        name: &'static str,
        input: &'static str,
        must_contain: &'static [&'static str],
        must_not_contain: &'static [&'static str],
    }

    const CASES: &[Case] = &[
        Case {
            name: "double-colon cast",
            input: "SELECT $1::text",
            must_contain: &["CAST"],
            must_not_contain: &["::"],
        },
        Case {
            name: "type-name normalisation: text → VARCHAR",
            input: "SELECT CAST(x AS text) FROM t",
            must_contain: &["VARCHAR"],
            must_not_contain: &[],
        },
        Case {
            name: "int4 cast normalises to INTEGER",
            input: "SELECT $1::int4",
            must_contain: &["INTEGER"],
            must_not_contain: &["::"],
        },
        Case {
            name: "ILIKE → lower() LIKE lower()",
            input: "SELECT * FROM t WHERE name ILIKE '%foo%'",
            must_contain: &["LOWER"],
            must_not_contain: &["ILIKE"],
        },
        Case {
            name: "string_agg → listagg",
            input: "SELECT string_agg(name, ',') FROM t",
            must_contain: &["listagg"],
            must_not_contain: &[],
        },
        Case {
            name: "single-arg log → log10",
            input: "SELECT log(x) FROM t",
            must_contain: &["log10"],
            must_not_contain: &[],
        },
        Case {
            name: "two-arg log preserved",
            input: "SELECT log(2, x) FROM t",
            must_contain: &[],
            must_not_contain: &["log10"],
        },
        Case {
            name: "trunc → truncate",
            input: "SELECT trunc(x) FROM t",
            must_contain: &["truncate"],
            must_not_contain: &[],
        },
        Case {
            name: "clean SQL passes through",
            input: "SELECT id, name FROM t WHERE id = 1",
            must_contain: &["SELECT", "FROM"],
            must_not_contain: &[],
        },
        Case {
            name: "LIMIT n OFFSET m → OFFSET m FETCH FIRST n (no bare LIMIT)",
            input: "SELECT name FROM t ORDER BY name LIMIT 2 OFFSET 1",
            must_contain: &["OFFSET 1", "FETCH FIRST 2"],
            must_not_contain: &["LIMIT"],
        },
        Case {
            name: "LIMIT only is left unchanged",
            input: "SELECT name FROM t LIMIT 5",
            must_contain: &["LIMIT 5"],
            must_not_contain: &["FETCH", "OFFSET"],
        },
        Case {
            name: "OFFSET only is left unchanged",
            input: "SELECT name FROM t OFFSET 3",
            must_contain: &["OFFSET 3"],
            must_not_contain: &["FETCH", "LIMIT"],
        },
        Case {
            name: "LIMIT ALL OFFSET m → bare OFFSET (ALL dropped, no FETCH)",
            input: "SELECT name FROM t LIMIT ALL OFFSET 4",
            must_contain: &["OFFSET 4"],
            must_not_contain: &["FETCH", "LIMIT", "ALL"],
        },
        Case {
            name: "subquery LIMIT+OFFSET is reordered too",
            input: "SELECT * FROM (SELECT name FROM t ORDER BY name LIMIT 2 OFFSET 1) x",
            must_contain: &["OFFSET 1", "FETCH FIRST 2"],
            must_not_contain: &["LIMIT"],
        },
    ];

    #[test]
    fn rewrite_cases() {
        for case in CASES {
            let result = rewrite_sql(case.input);
            let upper = result.to_uppercase();
            let lower = result.to_lowercase();
            for needle in case.must_contain {
                let has = upper.contains(&needle.to_uppercase())
                    || lower.contains(&needle.to_lowercase());
                assert!(has, "[{}] expected `{}` in: {result}", case.name, needle);
            }
            for needle in case.must_not_contain {
                let has = upper.contains(&needle.to_uppercase())
                    || lower.contains(&needle.to_lowercase());
                assert!(!has, "[{}] unexpected `{}` in: {result}", case.name, needle);
            }
        }
    }

    /// `SET ...` doesn't parse via `Parser::parse_statements`; the rewriter
    /// returns the input unchanged.
    #[test]
    fn unparseable_sql_passes_through() {
        let input = "SET extra_float_digits = 3";
        assert_eq!(rewrite_sql(input), input);
    }

    /// The reordered clause must place `OFFSET` before the row-limiting
    /// `FETCH` — the whole point of the rewrite, which the substring-based
    /// `Case` table cannot assert on its own.
    #[test]
    fn limit_offset_emits_offset_before_fetch() {
        let result = rewrite_sql("SELECT name FROM t ORDER BY name LIMIT 2 OFFSET 1");
        let offset_at = result.find("OFFSET").expect("OFFSET present");
        let fetch_at = result.find("FETCH").expect("FETCH present");
        assert!(
            offset_at < fetch_at,
            "expected OFFSET before FETCH in: {result}"
        );
    }

    #[test]
    fn show_passes_through_non_empty() {
        let result = rewrite_sql("SHOW server_version");
        assert!(!result.is_empty());
    }
}
