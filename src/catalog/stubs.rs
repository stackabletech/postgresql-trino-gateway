use std::sync::Arc;

use pgwire::api::results::{FieldFormat, FieldInfo, Response};
use pgwire::api::Type;
use pgwire::error::PgWireResult;

use super::build_response;

/// Empty composite fields response (pg_attribute + pg_type join).
/// Columns: oid (INT4), attname (VARCHAR), atttypid (INT4). Zero rows.
pub fn empty_composite_fields() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new("oid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("attname".to_owned(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("atttypid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
    ]);
    build_response(schema, vec![])
}

/// Empty enum labels response (pg_enum).
/// Columns: oid (INT4), enumlabel (VARCHAR). Zero rows.
pub fn empty_enum_labels() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new("oid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("enumlabel".to_owned(), None, None, Type::VARCHAR, FieldFormat::Text),
    ]);
    build_response(schema, vec![])
}

/// Empty pg_range response.
/// Columns: rngtypid (INT4), rngsubtype (INT4), rngmultitypid (INT4). Zero rows.
pub fn empty_pg_range() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new("rngtypid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("rngsubtype".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("rngmultitypid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
    ]);
    build_response(schema, vec![])
}

/// Static pg_namespace response with the three standard namespaces.
/// Columns: oid (INT4), nspname (VARCHAR). Three rows.
pub fn respond_pg_namespace() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new("oid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("nspname".to_owned(), None, None, Type::VARCHAR, FieldFormat::Text),
    ]);
    let rows = vec![
        vec![Some("11".to_owned()), Some("pg_catalog".to_owned())],
        vec![Some("2200".to_owned()), Some("public".to_owned())],
        vec![Some("13171".to_owned()), Some("information_schema".to_owned())],
    ];
    build_response(schema, rows)
}

/// Empty pg_class response. Refined in Task 7.
/// Columns: oid (INT4), relname (VARCHAR), relnamespace (INT4), relkind (VARCHAR). Zero rows.
pub fn empty_pg_class() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new("oid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("relname".to_owned(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("relnamespace".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("relkind".to_owned(), None, None, Type::VARCHAR, FieldFormat::Text),
    ]);
    build_response(schema, vec![])
}

/// Empty pg_attribute response. Refined in Task 7.
/// Columns: attrelid (INT4), attname (VARCHAR), atttypid (INT4), attnum (INT2),
///          attnotnull (BOOL), attisdropped (BOOL). Zero rows.
pub fn empty_pg_attribute() -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        FieldInfo::new("attrelid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("attname".to_owned(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("atttypid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("attnum".to_owned(), None, None, Type::INT2, FieldFormat::Text),
        FieldInfo::new("attnotnull".to_owned(), None, None, Type::BOOL, FieldFormat::Text),
        FieldInfo::new("attisdropped".to_owned(), None, None, Type::BOOL, FieldFormat::Text),
    ]);
    build_response(schema, vec![])
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
        empty_pg_class().unwrap();
        empty_pg_attribute().unwrap();
    }
}
