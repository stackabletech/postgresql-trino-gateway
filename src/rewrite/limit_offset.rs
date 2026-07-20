// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use sqlparser::ast::{Fetch, LimitClause, Query, VisitorMut};
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

        if let Some(limit) = limit {
            query.fetch = Some(Fetch {
                with_ties: false,
                percent: false,
                quantity: Some(limit),
            });
        }

        ControlFlow::Continue(())
    }
}
