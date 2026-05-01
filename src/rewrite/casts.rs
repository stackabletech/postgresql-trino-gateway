// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use sqlparser::ast::VisitorMut;
use sqlparser::ast::{CastKind, DataType, Expr};
use std::ops::ControlFlow;

/// Rewrites `::` cast syntax to `CAST()` and normalizes PG type names to Trino equivalents.
pub struct CastRewriter;

impl VisitorMut for CastRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Cast {
            kind, data_type, ..
        } = expr
        {
            // Normalize :: to CAST() so Trino can understand it
            if *kind == CastKind::DoubleColon {
                *kind = CastKind::Cast;
            }
            normalize_data_type(data_type);
        }
        ControlFlow::Continue(())
    }
}

/// Map PostgreSQL-specific type names to their Trino equivalents.
fn normalize_data_type(dt: &mut DataType) {
    *dt = match dt {
        DataType::Text => DataType::Varchar(None),
        DataType::Int2(_) => DataType::SmallInt(None),
        DataType::Int4(_) => DataType::Integer(None),
        DataType::Int8(_) => DataType::BigInt(None),
        DataType::Float4 => DataType::Real,
        DataType::Float8 => DataType::DoublePrecision,
        DataType::Bool => DataType::Boolean,
        DataType::Bytea => DataType::Varbinary(None),
        _ => return,
    };
}
