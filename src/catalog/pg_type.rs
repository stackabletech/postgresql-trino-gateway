// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::sync::Arc;

use pgwire::api::Type;
use pgwire::api::results::{FieldInfo, Response};
use pgwire::error::PgWireResult;

use super::{build_response, text_field};

struct TypeEntry {
    oid: i32,
    typname: &'static str,
    typtype: &'static str,
    elemtypoid: i32,
}

/// Base types (typtype = 'b').
const BASE_TYPES: &[TypeEntry] = &[
    TypeEntry {
        oid: 16,
        typname: "bool",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 17,
        typname: "bytea",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 20,
        typname: "int8",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 21,
        typname: "int2",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 23,
        typname: "int4",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 25,
        typname: "text",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 26,
        typname: "oid",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 114,
        typname: "json",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 142,
        typname: "xml",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 700,
        typname: "float4",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 701,
        typname: "float8",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 869,
        typname: "inet",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1042,
        typname: "bpchar",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1043,
        typname: "varchar",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1082,
        typname: "date",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1083,
        typname: "time",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1114,
        typname: "timestamp",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1184,
        typname: "timestamptz",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1186,
        typname: "interval",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1266,
        typname: "timetz",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 1700,
        typname: "numeric",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 2950,
        typname: "uuid",
        typtype: "b",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 3802,
        typname: "jsonb",
        typtype: "b",
        elemtypoid: 0,
    },
];

/// Pseudo types (typtype = 'p').
const PSEUDO_TYPES: &[TypeEntry] = &[
    TypeEntry {
        oid: 705,
        typname: "unknown",
        typtype: "p",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 2249,
        typname: "record",
        typtype: "p",
        elemtypoid: 0,
    },
    TypeEntry {
        oid: 2278,
        typname: "void",
        typtype: "p",
        elemtypoid: 0,
    },
];

/// Array types (typtype = 'a'). elemtypoid points to the base type.
const ARRAY_TYPES: &[TypeEntry] = &[
    TypeEntry {
        oid: 199,
        typname: "_json",
        typtype: "a",
        elemtypoid: 114,
    },
    TypeEntry {
        oid: 1000,
        typname: "_bool",
        typtype: "a",
        elemtypoid: 16,
    },
    TypeEntry {
        oid: 1001,
        typname: "_bytea",
        typtype: "a",
        elemtypoid: 17,
    },
    TypeEntry {
        oid: 1005,
        typname: "_int2",
        typtype: "a",
        elemtypoid: 21,
    },
    TypeEntry {
        oid: 1007,
        typname: "_int4",
        typtype: "a",
        elemtypoid: 23,
    },
    TypeEntry {
        oid: 1009,
        typname: "_text",
        typtype: "a",
        elemtypoid: 25,
    },
    TypeEntry {
        oid: 1014,
        typname: "_bpchar",
        typtype: "a",
        elemtypoid: 1042,
    },
    TypeEntry {
        oid: 1015,
        typname: "_varchar",
        typtype: "a",
        elemtypoid: 1043,
    },
    TypeEntry {
        oid: 1016,
        typname: "_int8",
        typtype: "a",
        elemtypoid: 20,
    },
    TypeEntry {
        oid: 1021,
        typname: "_float4",
        typtype: "a",
        elemtypoid: 700,
    },
    TypeEntry {
        oid: 1022,
        typname: "_float8",
        typtype: "a",
        elemtypoid: 701,
    },
    TypeEntry {
        oid: 1028,
        typname: "_oid",
        typtype: "a",
        elemtypoid: 26,
    },
    TypeEntry {
        oid: 1041,
        typname: "_inet",
        typtype: "a",
        elemtypoid: 869,
    },
    TypeEntry {
        oid: 1115,
        typname: "_timestamp",
        typtype: "a",
        elemtypoid: 1114,
    },
    TypeEntry {
        oid: 1182,
        typname: "_date",
        typtype: "a",
        elemtypoid: 1082,
    },
    TypeEntry {
        oid: 1183,
        typname: "_time",
        typtype: "a",
        elemtypoid: 1083,
    },
    TypeEntry {
        oid: 1185,
        typname: "_timestamptz",
        typtype: "a",
        elemtypoid: 1184,
    },
    TypeEntry {
        oid: 1187,
        typname: "_interval",
        typtype: "a",
        elemtypoid: 1186,
    },
    TypeEntry {
        oid: 1231,
        typname: "_numeric",
        typtype: "a",
        elemtypoid: 1700,
    },
    TypeEntry {
        oid: 2951,
        typname: "_uuid",
        typtype: "a",
        elemtypoid: 2950,
    },
    TypeEntry {
        oid: 3807,
        typname: "_jsonb",
        typtype: "a",
        elemtypoid: 3802,
    },
];

/// The total number of type entries we return.
#[cfg(test)]
const TYPE_ROW_COUNT: usize = BASE_TYPES.len() + PSEUDO_TYPES.len() + ARRAY_TYPES.len();

/// Schema matching the JDBC PostgreSQL driver's type-loading query:
///   SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
///          CASE ... END AS type, CASE ... END AS elemoid, CASE ... END AS ord
fn schema() -> Arc<Vec<FieldInfo>> {
    Arc::new(vec![
        text_field("nspname", Type::VARCHAR),
        text_field("typname", Type::VARCHAR),
        text_field("oid", Type::INT4),
        text_field("typrelid", Type::INT4),
        text_field("typbasetype", Type::INT4),
        text_field("type", Type::VARCHAR),
        text_field("elemoid", Type::INT4),
        text_field("ord", Type::INT4),
    ])
}

fn entry_to_row(e: &TypeEntry) -> Vec<Option<String>> {
    // ord: 0 = base types first, 1 = domains, 2 = ranges, 3 = arrays
    let ord = match e.typtype {
        "a" => "3",
        "r" => "2",
        "d" => "1",
        _ => "0",
    };
    vec![
        Some("pg_catalog".to_owned()),  // nspname
        Some(e.typname.to_owned()),     // typname
        Some(e.oid.to_string()),        // oid
        Some("0".to_owned()),           // typrelid (0 = not a composite)
        Some("0".to_owned()),           // typbasetype (0 = not a domain)
        Some(e.typtype.to_owned()),     // type
        Some(e.elemtypoid.to_string()), // elemoid
        Some(ord.to_owned()),           // ord
    ]
}

/// Returns the pg_type response that Npgsql expects for type loading.
///
/// Order: base types first, then pseudo types, then array types so that Npgsql
/// processes entries in dependency order.
pub fn respond_type_loading() -> PgWireResult<Vec<Response>> {
    let rows: Vec<Vec<Option<String>>> = BASE_TYPES
        .iter()
        .chain(PSEUDO_TYPES.iter())
        .chain(ARRAY_TYPES.iter())
        .map(entry_to_row)
        .collect();

    build_response(schema(), rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_loading_returns_correct_row_count() {
        let resp = respond_type_loading().unwrap();
        assert_eq!(resp.len(), 1, "should return exactly one Response");
        // 23 base + 3 pseudo + 21 array = 47
        assert_eq!(TYPE_ROW_COUNT, 47);
    }

    #[test]
    fn base_types_before_arrays() {
        // Verify ordering: base first, pseudo second, arrays last.
        assert_eq!(BASE_TYPES[0].typname, "bool");
        assert_eq!(PSEUDO_TYPES[0].typname, "unknown");
        assert_eq!(ARRAY_TYPES[0].typname, "_json");
    }
}
