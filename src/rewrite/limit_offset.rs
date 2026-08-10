// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use sqlparser::ast::{Expr, Fetch, LimitClause, Query, Value, VisitorMut};
use std::ops::ControlFlow;

/// Reorders `LIMIT n OFFSET m` into a Trino-compatible form.
///
/// PostgreSQL accepts `LIMIT n OFFSET m`, but Trino's grammar requires the
/// offset to come *before* the row-limiting clause (`OFFSET m LIMIT n` or
/// `OFFSET m FETCH FIRST n ROWS ONLY`). sqlparser's `Display` for
/// [`LimitClause::LimitOffset`] always writes `LIMIT` before `OFFSET`
/// regardless of input order, so a plain round-trip keeps the order Trino
/// rejects — a `VisitorMut` on expressions cannot fix it.
///
/// Instead we exploit `Query`'s field render order: `limit_clause` is emitted
/// before `fetch`. So we leave the `OFFSET` in the limit clause and move the
/// `LIMIT` value into a `FETCH FIRST n ROWS ONLY` clause. The result renders as
/// `... OFFSET m FETCH FIRST n ROWS ONLY`, which is valid Trino and
/// semantically identical to `LIMIT n OFFSET m`. Everything is built from AST
/// nodes — no raw-string manipulation (see the "AST, never raw strings" rule in
/// `AGENTS.md`).
///
/// `LIMIT 0` is the exception: Trino accepts `LIMIT 0` but rejects
/// `FETCH FIRST 0 ROWS ONLY`, so that case is rewritten to a bare `LIMIT 0`
/// instead (see [`is_zero_literal`]).
///
/// Using [`VisitorMut::post_visit_query`] means every `Query` node is handled,
/// including subqueries and CTEs, not just the top level.
pub struct LimitOffsetRewriter;

impl VisitorMut for LimitOffsetRewriter {
    type Break = ();

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        // Don't clobber a pre-existing FETCH (would be a malformed query anyway).
        if query.fetch.is_some() {
            return ControlFlow::Continue(());
        }

        // Only the plain `LIMIT <expr> OFFSET <expr>` case: both present, no
        // ClickHouse `LIMIT BY`. `LIMIT ALL OFFSET m` parses to `limit: None`
        // (sqlparser drops `ALL`), so `.take()` yields `None` and we leave the
        // bare `OFFSET m` untouched — Trino accepts that as-is.
        let limit = match &mut query.limit_clause {
            Some(LimitClause::LimitOffset {
                limit,
                offset: Some(_),
                limit_by,
            }) if limit_by.is_empty() => limit.take(),
            _ => None,
        };

        let Some(limit) = limit else {
            return ControlFlow::Continue(());
        };

        // Trino rejects `FETCH FIRST 0 ROWS ONLY` ("FETCH FIRST row count must
        // be positive"), while `LIMIT 0` is accepted and returns no rows —
        // PostgreSQL accepts both. `LIMIT 0 OFFSET m` is empty for every `m`,
        // so we drop the `OFFSET` and keep a bare `LIMIT 0`, which needs no
        // reordering. Power BI issues `LIMIT 0` to probe result schemas, so
        // this path is hit in practice.
        if is_zero_literal(&limit) {
            query.limit_clause = Some(LimitClause::LimitOffset {
                limit: Some(limit),
                offset: None,
                limit_by: Vec::new(),
            });
            return ControlFlow::Continue(());
        }

        query.fetch = Some(Fetch {
            with_ties: false,
            percent: false,
            quantity: Some(limit),
        });

        ControlFlow::Continue(())
    }
}

/// Whether `expr` is a numeric literal equal to zero.
///
/// Only literals are recognised — a placeholder or expression that happens to
/// evaluate to zero still becomes a `FETCH` clause, which Trino rejects at
/// analysis time. Nothing we can do about that without evaluating the
/// expression ourselves.
fn is_zero_literal(expr: &Expr) -> bool {
    let Expr::Value(value) = expr else {
        return false;
    };
    match &value.value {
        Value::Number(n, _) => n.parse::<f64>().is_ok_and(|n| n == 0.0),
        _ => false,
    }
}
