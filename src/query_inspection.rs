// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0

//! AST-based inspection of incoming SQL for routing decisions.
//!
//! Substring matching on the raw SQL (`upper.contains("PG_TYPE")`) misroutes
//! any user query that has the catalog name in a string literal, comment, or
//! column reference. Parsing once and inspecting actual table relations and
//! function calls in the AST avoids those false positives.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectNamePart, Select,
    SelectItem, SetExpr, Statement, Value, visit_expressions, visit_relations,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Cached parse result for a single SQL string.
///
/// Successive `references_table` / `calls_function` calls on the same
/// `ParsedQuery` share one parse pass.
pub struct ParsedQuery {
    parsed: Option<Vec<Statement>>,
}

impl ParsedQuery {
    pub fn new(sql: &str) -> Self {
        Self {
            parsed: Parser::parse_sql(&PostgreSqlDialect {}, sql).ok(),
        }
    }

    /// True if any FROM/JOIN/UPDATE/etc. relation has the given table name as
    /// its trailing identifier, case-insensitively.
    ///
    /// Returns false on parse failure so callers fall through to the next
    /// dispatch step rather than treating that as a match.
    pub fn references_table(&self, name: &str) -> bool {
        let Some(stmts) = self.parsed.as_ref() else {
            return false;
        };
        let mut hit = false;
        let _: ControlFlow<()> = visit_relations(stmts, |obj| {
            if last_ident_eq(&obj.0, name) {
                hit = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        hit
    }

    /// True if a relation refers to `name` and is either unqualified (relying
    /// on search_path) or qualified with the given schema — but not qualified
    /// with a different schema.
    ///
    /// Use this for table names common enough to clash with user tables, like
    /// `information_schema.columns` or `information_schema.character_sets`.
    pub fn references_table_in_schema(&self, schema: &str, name: &str) -> bool {
        let Some(stmts) = self.parsed.as_ref() else {
            return false;
        };
        let mut hit = false;
        let _: ControlFlow<()> = visit_relations(stmts, |obj| {
            let parts = &obj.0;
            let last_matches = last_ident_eq(parts, name);
            if !last_matches {
                return ControlFlow::Continue(());
            }
            // 1-part: unqualified, matches.
            // 2-parts: schema must match.
            // 3-parts: catalog.schema.name — schema (second-to-last) must match.
            let qualified_ok = match parts.len() {
                1 => true,
                n if n >= 2 => parts
                    .get(n - 2)
                    .and_then(|p| p.as_ident())
                    .is_some_and(|id| id.value.eq_ignore_ascii_case(schema)),
                _ => false,
            };
            if qualified_ok {
                hit = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        hit
    }

    /// True if any expression is a parenthesized call to the named function.
    /// Bare identifiers (e.g. `version` as a column reference) do not match.
    pub fn calls_function(&self, name: &str) -> bool {
        let Some(stmts) = self.parsed.as_ref() else {
            return false;
        };
        let mut hit = false;
        let _: ControlFlow<()> = visit_expressions(stmts, |expr| {
            if let Expr::Function(f) = expr
                && last_ident_eq(&f.name.0, name)
            {
                hit = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        hit
    }

    /// True if `name` is referenced as a function call OR as a bare identifier.
    ///
    /// PostgreSQL allows certain niladic SQL keywords (`current_schema`,
    /// `current_user`, etc.) without parentheses. Most of those are parsed by
    /// sqlparser as `Expr::Function` with empty args; `current_schema` is the
    /// outlier — it's a regular `Expr::Identifier`. Use this method only for
    /// names where the parens-less form is well-known and unambiguous.
    pub fn calls_function_or_keyword(&self, name: &str) -> bool {
        if self.calls_function(name) {
            return true;
        }
        let Some(stmts) = self.parsed.as_ref() else {
            return false;
        };
        let mut hit = false;
        let _: ControlFlow<()> = visit_expressions(stmts, |expr| {
            if let Expr::Identifier(id) = expr
                && id.value.eq_ignore_ascii_case(name)
            {
                hit = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        hit
    }

    /// First string-literal argument passed to a call of `name`, or `None` if
    /// no such call exists or the first argument isn't a string literal.
    ///
    /// Returns the *first* match in sqlparser's traversal order. Callers that
    /// need to disambiguate multiple calls (e.g. `SELECT current_setting('a'),
    /// current_setting('b')`) should gate on `is_bare_scalar_select` first so
    /// the AST contains only a single expression.
    pub fn function_string_arg(&self, name: &str) -> Option<String> {
        let stmts = self.parsed.as_ref()?;
        let mut found: Option<String> = None;
        let _: ControlFlow<()> = visit_expressions(stmts, |expr| {
            if let Expr::Function(f) = expr
                && last_ident_eq(&f.name.0, name)
                && let Some(s) = first_string_literal_arg(&f.args)
            {
                found = Some(s.to_owned());
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        found
    }

    /// True if the query is `SELECT <expr> [AS alias]` with no FROM, WHERE,
    /// GROUP BY, ORDER BY, or other clauses, and exactly one projection item.
    ///
    /// Used to gate server-function intercepts so a column reference like
    /// `WHERE current_schema = 'public'` or a multi-call projection like
    /// `SELECT current_setting('a'), current_setting('b')` is not misrouted
    /// to a single-row stub response.
    pub fn is_bare_scalar_select(&self) -> bool {
        self.bare_select().is_some()
    }

    /// Project name aliases of the single SELECT statement, or empty on parse
    /// failure or non-SELECT statements. Used to synthesise empty result-set
    /// schemas for intercepted information_schema queries.
    pub fn select_column_names(&self) -> Vec<String> {
        let Some(stmts) = self.parsed.as_ref() else {
            return Vec::new();
        };
        let Some(Statement::Query(q)) = stmts.first() else {
            return Vec::new();
        };
        let SetExpr::Select(select) = q.body.as_ref() else {
            return Vec::new();
        };
        select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(expr) => unnamed_select_name(expr),
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                SelectItem::Wildcard(_) => "*".to_string(),
                SelectItem::QualifiedWildcard(name, _) => format!("{name}.*"),
            })
            .collect()
    }

    fn bare_select(&self) -> Option<&Select> {
        let stmts = self.parsed.as_ref()?;
        if stmts.len() != 1 {
            return None;
        }
        let Statement::Query(q) = &stmts[0] else {
            return None;
        };
        if q.with.is_some() || q.order_by.is_some() || q.limit_clause.is_some() || q.fetch.is_some()
        {
            return None;
        }
        let SetExpr::Select(select) = q.body.as_ref() else {
            return None;
        };
        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, mods) => !exprs.is_empty() || !mods.is_empty(),
            GroupByExpr::All(_) => true,
        };
        if !select.from.is_empty()
            || select.selection.is_some()
            || has_group_by
            || select.having.is_some()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.qualify.is_some()
            || select.projection.len() != 1
        {
            return None;
        }
        Some(select)
    }
}

/// Default column name PostgreSQL assigns to an unaliased SELECT expression.
/// PG strips the qualifier from `t.col` and uses the trailing identifier; for
/// other expression shapes it falls back to `?column?`.
fn unnamed_select_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::Function(f) => f
            .name
            .0
            .last()
            .and_then(|p| p.as_ident())
            .map(|id| id.value.clone())
            .unwrap_or_else(|| "?column?".to_string()),
        _ => "?column?".to_string(),
    }
}

/// True if the trailing identifier of a multi-part SQL name equals `want`,
/// case-insensitively.
///
/// `ObjectNamePart` is sqlparser's representation of one segment of a
/// dotted SQL identifier: `catalog.schema.table` parses to three
/// `ObjectNamePart`s (`catalog`, `schema`, `table`); a bare `pg_type`
/// parses to one. We compare against the *last* segment because that is
/// always the table or function name; the leading segments are catalog or
/// schema qualifiers, which `references_table_in_schema` handles
/// separately.
fn last_ident_eq(parts: &[ObjectNamePart], want: &str) -> bool {
    parts
        .last()
        .and_then(|p| p.as_ident())
        .is_some_and(|id| id.value.eq_ignore_ascii_case(want))
}

fn first_string_literal_arg(args: &FunctionArguments) -> Option<&str> {
    let FunctionArguments::List(list) = args else {
        return None;
    };
    let expr = match list.args.first()? {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => e,
        FunctionArg::ExprNamed {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => e,
        _ => return None,
    };
    if let Expr::Value(v) = expr
        && let Value::SingleQuotedString(s) = &v.value
    {
        return Some(s.as_str());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_table_matches_unqualified() {
        let q = ParsedQuery::new("SELECT * FROM pg_type");
        assert!(q.references_table("pg_type"));
        assert!(q.references_table("PG_TYPE"));
        assert!(!q.references_table("pg_class"));
    }

    #[test]
    fn references_table_matches_qualified() {
        let q = ParsedQuery::new("SELECT * FROM pg_catalog.pg_type t");
        assert!(q.references_table("pg_type"));
    }

    #[test]
    fn references_table_matches_join() {
        let q = ParsedQuery::new("SELECT a.attname FROM pg_attribute a JOIN pg_type t ON true");
        assert!(q.references_table("pg_attribute"));
        assert!(q.references_table("pg_type"));
    }

    /// The whole point: a literal containing the catalog name must not match.
    #[test]
    fn references_table_ignores_string_literals() {
        let q = ParsedQuery::new("SELECT * FROM users WHERE notes LIKE '%PG_TYPE%'");
        assert!(!q.references_table("pg_type"));
    }

    #[test]
    fn references_table_ignores_column_names() {
        let q = ParsedQuery::new("SELECT pg_type FROM users");
        assert!(!q.references_table("pg_type"));
    }

    #[test]
    fn references_table_information_schema() {
        let q =
            ParsedQuery::new("SELECT * FROM INFORMATION_SCHEMA.columns WHERE TABLE_SCHEMA = 'sf1'");
        assert!(q.references_table("columns"));
        assert!(!q.references_table("tables"));
    }

    #[test]
    fn references_table_in_schema_matches_qualified() {
        let q = ParsedQuery::new("SELECT * FROM information_schema.columns");
        assert!(q.references_table_in_schema("information_schema", "columns"));
    }

    #[test]
    fn references_table_in_schema_matches_unqualified() {
        let q = ParsedQuery::new("SELECT * FROM columns");
        assert!(q.references_table_in_schema("information_schema", "columns"));
    }

    #[test]
    fn references_table_in_schema_rejects_wrong_schema() {
        let q = ParsedQuery::new("SELECT * FROM mycustom.columns");
        assert!(!q.references_table_in_schema("information_schema", "columns"));
    }

    #[test]
    fn references_table_in_schema_matches_three_part_name() {
        let q = ParsedQuery::new("SELECT * FROM trino.information_schema.columns");
        assert!(q.references_table_in_schema("information_schema", "columns"));
    }

    #[test]
    fn calls_function_with_parens() {
        let q = ParsedQuery::new("SELECT version()");
        assert!(q.calls_function("version"));
    }

    #[test]
    fn calls_function_case_insensitive() {
        let q = ParsedQuery::new("SELECT VERSION()");
        assert!(q.calls_function("version"));
    }

    #[test]
    fn calls_function_qualified_name() {
        let q = ParsedQuery::new("SELECT pg_catalog.version()");
        assert!(q.calls_function("version"));
    }

    #[test]
    fn calls_function_ignores_string_literals() {
        let q = ParsedQuery::new("SELECT 'version()' AS x");
        assert!(!q.calls_function("version"));
    }

    /// `calls_function` requires parens; bare identifiers don't count even
    /// for known niladic forms.
    #[test]
    fn calls_function_requires_parens() {
        let q = ParsedQuery::new("SELECT current_schema");
        assert!(!q.calls_function("current_schema"));

        let q2 = ParsedQuery::new("SELECT current_schema()");
        assert!(q2.calls_function("current_schema"));
    }

    /// `calls_function_or_keyword` accepts both forms.
    #[test]
    fn calls_function_or_keyword_handles_niladic() {
        let q = ParsedQuery::new("SELECT current_schema");
        assert!(q.calls_function_or_keyword("current_schema"));
        let q2 = ParsedQuery::new("SELECT current_schema()");
        assert!(q2.calls_function_or_keyword("current_schema"));
    }

    /// `calls_function_or_keyword` will match a column reference of the same
    /// name. Callers must restrict use to well-known niladic SQL keywords.
    #[test]
    fn calls_function_or_keyword_matches_column_reference() {
        let q = ParsedQuery::new("SELECT current_schema FROM releases");
        assert!(q.calls_function_or_keyword("current_schema"));
    }

    #[test]
    fn function_string_arg_extracts_first_literal() {
        let q = ParsedQuery::new("SELECT current_setting('server_version_num')");
        assert_eq!(
            q.function_string_arg("current_setting").as_deref(),
            Some("server_version_num")
        );
    }

    #[test]
    fn function_string_arg_none_for_non_literal() {
        let q = ParsedQuery::new("SELECT current_setting(name)");
        assert_eq!(q.function_string_arg("current_setting"), None);
    }

    #[test]
    fn is_bare_scalar_select_simple_function() {
        assert!(ParsedQuery::new("SELECT version()").is_bare_scalar_select());
        assert!(ParsedQuery::new("SELECT current_schema").is_bare_scalar_select());
        assert!(ParsedQuery::new("SELECT current_setting('x')").is_bare_scalar_select());
        assert!(ParsedQuery::new("SELECT version() AS v").is_bare_scalar_select());
    }

    #[test]
    fn is_bare_scalar_select_rejects_from() {
        assert!(!ParsedQuery::new("SELECT version() FROM t").is_bare_scalar_select());
    }

    #[test]
    fn is_bare_scalar_select_rejects_where() {
        // WHERE without FROM doesn't parse, but a column reference in WHERE
        // alongside a FROM should reject. Test with FROM + WHERE.
        let q = ParsedQuery::new("SELECT 1 FROM t WHERE current_schema = 'public'");
        assert!(!q.is_bare_scalar_select());
    }

    #[test]
    fn is_bare_scalar_select_rejects_multiple_projections() {
        let q = ParsedQuery::new("SELECT current_setting('a'), current_setting('b')");
        assert!(!q.is_bare_scalar_select());
    }

    #[test]
    fn is_bare_scalar_select_rejects_cte() {
        let q = ParsedQuery::new("WITH t AS (SELECT 1) SELECT version()");
        assert!(!q.is_bare_scalar_select());
    }

    #[test]
    fn is_bare_scalar_select_rejects_multi_statement() {
        // sqlparser may or may not parse multi-statement strings; either way,
        // bare-scalar must reject.
        let q = ParsedQuery::new("SELECT version(); SELECT 1");
        assert!(!q.is_bare_scalar_select());
    }

    #[test]
    fn select_column_names_unaliased() {
        let q = ParsedQuery::new("SELECT a, b.c FROM t");
        assert_eq!(q.select_column_names(), vec!["a", "c"]);
    }

    #[test]
    fn select_column_names_aliased() {
        let q = ParsedQuery::new("SELECT 1 AS x, foo() AS y FROM t");
        assert_eq!(q.select_column_names(), vec!["x", "y"]);
    }

    #[test]
    fn select_column_names_complex_expressions_use_question_mark() {
        let q = ParsedQuery::new("SELECT 1 + 2 FROM t");
        assert_eq!(q.select_column_names(), vec!["?column?"]);
    }
}
