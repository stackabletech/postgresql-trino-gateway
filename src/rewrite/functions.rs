// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use sqlparser::ast::VisitorMut;
use sqlparser::ast::{Expr, FunctionArguments, ObjectNamePart};
use std::ops::ControlFlow;

/// Renames PostgreSQL function names to their Trino equivalents.
pub struct FunctionRenamer;

impl VisitorMut for FunctionRenamer {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Function(func) = expr
            && func.name.0.len() == 1
            && let Some(ObjectNamePart::Identifier(ident)) = func.name.0.first_mut()
        {
            let lower = ident.value.to_lowercase();
            let arg_count = match &func.args {
                FunctionArguments::List(list) => list.args.len(),
                _ => 0,
            };

            let new_name = match lower.as_str() {
                "string_agg" => Some("listagg"),
                // PG `log(x)` = log base 10; Trino uses `log10(x)`.
                // PG `log(base, x)` is two-arg; leave that alone.
                "log" if arg_count == 1 => Some("log10"),
                "trunc" => Some("truncate"),
                _ => None,
            };

            if let Some(name) = new_name {
                ident.value = name.to_string();
            }
        }
        ControlFlow::Continue(())
    }
}
