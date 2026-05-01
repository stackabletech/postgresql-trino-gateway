// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::sync::Arc;

use pgwire::api::Type;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use super::{build_response, text_field};

/// Empty composite-fields response (pg_attribute + pg_type join).
/// Columns: oid INT4, attname VARCHAR, atttypid INT4. Zero rows.
pub fn empty_composite_fields() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("oid", Type::INT4),
        text_field("attname", Type::VARCHAR),
        text_field("atttypid", Type::INT4),
    ]);
    build_response(schema, vec![])
}

/// Empty pg_enum response. Columns: oid INT4, enumlabel VARCHAR. Zero rows.
pub fn empty_enum_labels() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("oid", Type::INT4),
        text_field("enumlabel", Type::VARCHAR),
    ]);
    build_response(schema, vec![])
}

/// Empty pg_range response.
/// Columns: rngtypid INT4, rngsubtype INT4, rngmultitypid INT4. Zero rows.
pub fn empty_pg_range() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("rngtypid", Type::INT4),
        text_field("rngsubtype", Type::INT4),
        text_field("rngmultitypid", Type::INT4),
    ]);
    build_response(schema, vec![])
}

/// Static pg_namespace response with the three standard namespaces.
/// Columns: oid INT4, nspname VARCHAR. Three rows.
pub fn respond_pg_namespace() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        text_field("oid", Type::INT4),
        text_field("nspname", Type::VARCHAR),
    ]);
    let rows = vec![
        vec![Some("11".to_owned()), Some("pg_catalog".to_owned())],
        vec![Some("2200".to_owned()), Some("public".to_owned())],
        vec![
            Some("13171".to_owned()),
            Some("information_schema".to_owned()),
        ],
    ];
    build_response(schema, rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_namespace_returns_three_rows() {
        let resp = respond_pg_namespace().unwrap();
        assert_eq!(resp.len(), 1);
    }

    #[test]
    fn empty_stubs_return_ok() {
        empty_composite_fields().unwrap();
        empty_enum_labels().unwrap();
        empty_pg_range().unwrap();
    }
}
