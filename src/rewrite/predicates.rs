// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use sqlparser::ast::VisitorMut;
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName, Value,
};
use std::ops::ControlFlow;

/// Rewrites `ILIKE` to `lower(x) LIKE lower(pattern)` for Trino compatibility.
pub struct ILikeRewriter;

impl VisitorMut for ILikeRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        // We need to take ownership to destructure, so use a placeholder swap.
        let owned = std::mem::replace(expr, Expr::Value(Value::Null.into()));
        if let Expr::ILike {
            negated,
            expr: inner_expr,
            pattern,
            escape_char,
            any: _,
        } = owned
        {
            *expr = Expr::Like {
                negated,
                any: false,
                expr: Box::new(make_lower_call(*inner_expr)),
                pattern: Box::new(make_lower_call(*pattern)),
                escape_char,
            };
        } else {
            // Not an ILike — put it back.
            *expr = owned;
        }
        ControlFlow::Continue(())
    }
}

/// Wraps an expression in a `lower(expr)` function call.
fn make_lower_call(inner: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("lower"),
        )]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}
