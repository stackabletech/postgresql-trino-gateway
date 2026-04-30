// Copyright 2026 Stackable GmbH
// Licensed under the Open Software License version 3.0 (OSL-3.0).
// See LICENSE file in the project root for full license text.
use pgwire::api::Type;
use serde_json::Value;

/// Maps a Trino type string to the corresponding PostgreSQL `Type`.
///
/// Parametric types like `varchar(100)` or `decimal(10,2)` are handled
/// by stripping the parenthesized parameters before matching.
pub fn trino_type_to_pg(trino_type: &str) -> Type {
    let normalized = trino_type.trim().to_lowercase();

    // Handle array types: array(inner_type)
    if normalized.starts_with("array(") && normalized.ends_with(')') {
        let inner = &normalized[6..normalized.len() - 1];
        return scalar_to_array(&trino_type_to_pg(inner));
    }

    // Match multi-word types before stripping parameters.
    if normalized.starts_with("time with time zone") {
        return Type::TIMETZ;
    }
    if normalized.starts_with("timestamp with time zone") {
        return Type::TIMESTAMPTZ;
    }
    if normalized.starts_with("interval year to month") {
        return Type::INTERVAL;
    }
    if normalized.starts_with("interval day to second") {
        return Type::INTERVAL;
    }

    // Strip parameters for parametric types like varchar(100), decimal(10,2)
    let base = match normalized.find('(') {
        Some(idx) => normalized[..idx].trim(),
        None => normalized.as_str(),
    };

    match base {
        "boolean" => Type::BOOL,
        "tinyint" | "smallint" => Type::INT2,
        "integer" => Type::INT4,
        "bigint" => Type::INT8,
        "real" => Type::FLOAT4,
        "double" => Type::FLOAT8,
        "decimal" => Type::NUMERIC,
        "varchar" => Type::VARCHAR,
        "char" => Type::BPCHAR,
        "varbinary" => Type::BYTEA,
        "date" => Type::DATE,
        "time" => Type::TIME,
        "timestamp" => Type::TIMESTAMP,
        "interval" => Type::INTERVAL,
        "json" => Type::JSONB,
        "uuid" => Type::UUID,
        "ipaddress" => Type::INET,
        "map" | "row" => Type::JSONB,
        _ => Type::TEXT,
    }
}

/// Maps a scalar PG type to its corresponding array type.
fn scalar_to_array(scalar: &Type) -> Type {
    match *scalar {
        Type::BOOL => Type::BOOL_ARRAY,
        Type::INT2 => Type::INT2_ARRAY,
        Type::INT4 => Type::INT4_ARRAY,
        Type::INT8 => Type::INT8_ARRAY,
        Type::FLOAT4 => Type::FLOAT4_ARRAY,
        Type::FLOAT8 => Type::FLOAT8_ARRAY,
        Type::NUMERIC => Type::NUMERIC_ARRAY,
        Type::VARCHAR => Type::VARCHAR_ARRAY,
        Type::BPCHAR => Type::BPCHAR_ARRAY,
        Type::BYTEA => Type::BYTEA_ARRAY,
        Type::DATE => Type::DATE_ARRAY,
        Type::TIME => Type::TIME_ARRAY,
        Type::TIMETZ => Type::TIMETZ_ARRAY,
        Type::TIMESTAMP => Type::TIMESTAMP_ARRAY,
        Type::TIMESTAMPTZ => Type::TIMESTAMPTZ_ARRAY,
        Type::INTERVAL => Type::INTERVAL_ARRAY,
        Type::JSONB => Type::JSONB_ARRAY,
        Type::UUID => Type::UUID_ARRAY,
        Type::INET => Type::INET_ARRAY,
        Type::TEXT => Type::TEXT_ARRAY,
        // Fallback: treat as text array
        _ => Type::TEXT_ARRAY,
    }
}

/// Converts a JSON value to a PostgreSQL text-format string.
///
/// Returns `None` for `Value::Null` (representing SQL NULL).
///
/// Numeric encoding is Trino-type-aware because `serde_json::Number::to_string`
/// preserves the source JSON representation: a BIGINT serialized by Trino as
/// `42.0` would render as `"42.0"`, which PostgreSQL's int8 text parser rejects.
/// For integer target types we force integer form via `as_i64`/`as_u64` so the
/// wire value always matches the declared column type.
pub fn encode_value(value: &Value, trino_type: &str) -> Option<String> {
    let base = base_type(trino_type);
    match value {
        Value::Null => None,
        Value::Bool(true) => Some("true".to_owned()),
        Value::Bool(false) => Some("false".to_owned()),
        Value::Number(n) => Some(encode_number(n, base)),
        Value::String(s) => Some(s.clone()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

/// Extract the base Trino type name (e.g. `"varchar(25)"` → `"varchar"`).
fn base_type(trino_type: &str) -> &str {
    let trimmed = trino_type.trim();
    match trimmed.find('(') {
        Some(idx) => trimmed[..idx].trim(),
        None => trimmed,
    }
}

/// Render a JSON number as PostgreSQL text, respecting the declared type.
///
/// For integer targets, emit integer text regardless of how the JSON value was
/// typed (Trino can legitimately serialize whole numbers as `42` or `42.0`).
/// For floats, render NaN/Infinity using PostgreSQL's canonical forms.
fn encode_number(n: &serde_json::Number, base: &str) -> String {
    match base.to_lowercase().as_str() {
        "tinyint" | "smallint" | "integer" | "bigint" => {
            if let Some(i) = n.as_i64() {
                return i.to_string();
            }
            if let Some(u) = n.as_u64() {
                return u.to_string();
            }
            if let Some(f) = n.as_f64()
                && f.is_finite()
            {
                // `f as i64` saturates at the type limits, so 1e30 would
                // silently become i64::MAX and the client would see a wildly
                // wrong value. Cast only when the float demonstrably fits;
                // otherwise pass the float text through and let PostgreSQL's
                // integer parser reject it client-side. Fail-closed beats a
                // quiet data-corruption bug.
                //
                // The boundary at exactly i64::MAX is excluded because
                // `i64::MAX as f64` rounds up to 2^63 (not representable in
                // i64), so the round-trip would silently saturate.
                if f >= i64::MIN as f64 && f < i64::MAX as f64 {
                    return (f as i64).to_string();
                }
                return f.to_string();
            }
            n.to_string()
        }
        "real" | "double" => {
            if let Some(f) = n.as_f64() {
                return format_float_text(f);
            }
            n.to_string()
        }
        _ => n.to_string(),
    }
}

/// PostgreSQL's canonical text form for f64 special values.
fn format_float_text(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        }
    } else {
        f.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- trino_type_to_pg: scalar types --

    #[test]
    fn boolean() {
        assert_eq!(trino_type_to_pg("boolean"), Type::BOOL);
    }

    #[test]
    fn tinyint() {
        assert_eq!(trino_type_to_pg("tinyint"), Type::INT2);
    }

    #[test]
    fn smallint() {
        assert_eq!(trino_type_to_pg("smallint"), Type::INT2);
    }

    #[test]
    fn integer() {
        assert_eq!(trino_type_to_pg("integer"), Type::INT4);
    }

    #[test]
    fn bigint() {
        assert_eq!(trino_type_to_pg("bigint"), Type::INT8);
    }

    #[test]
    fn real() {
        assert_eq!(trino_type_to_pg("real"), Type::FLOAT4);
    }

    #[test]
    fn double() {
        assert_eq!(trino_type_to_pg("double"), Type::FLOAT8);
    }

    #[test]
    fn decimal() {
        assert_eq!(trino_type_to_pg("decimal"), Type::NUMERIC);
    }

    #[test]
    fn varchar() {
        assert_eq!(trino_type_to_pg("varchar"), Type::VARCHAR);
    }

    #[test]
    fn char_type() {
        assert_eq!(trino_type_to_pg("char"), Type::BPCHAR);
    }

    #[test]
    fn varbinary() {
        assert_eq!(trino_type_to_pg("varbinary"), Type::BYTEA);
    }

    #[test]
    fn date() {
        assert_eq!(trino_type_to_pg("date"), Type::DATE);
    }

    #[test]
    fn time() {
        assert_eq!(trino_type_to_pg("time"), Type::TIME);
    }

    #[test]
    fn time_with_time_zone() {
        assert_eq!(trino_type_to_pg("time with time zone"), Type::TIMETZ);
    }

    #[test]
    fn timestamp() {
        assert_eq!(trino_type_to_pg("timestamp"), Type::TIMESTAMP);
    }

    #[test]
    fn timestamp_with_time_zone() {
        assert_eq!(
            trino_type_to_pg("timestamp with time zone"),
            Type::TIMESTAMPTZ
        );
    }

    #[test]
    fn interval_year_to_month() {
        assert_eq!(trino_type_to_pg("interval year to month"), Type::INTERVAL);
    }

    #[test]
    fn interval_day_to_second() {
        assert_eq!(trino_type_to_pg("interval day to second"), Type::INTERVAL);
    }

    #[test]
    fn json() {
        assert_eq!(trino_type_to_pg("json"), Type::JSONB);
    }

    #[test]
    fn uuid() {
        assert_eq!(trino_type_to_pg("uuid"), Type::UUID);
    }

    #[test]
    fn ipaddress() {
        assert_eq!(trino_type_to_pg("ipaddress"), Type::INET);
    }

    #[test]
    fn map_type() {
        assert_eq!(trino_type_to_pg("map"), Type::JSONB);
    }

    #[test]
    fn row_type() {
        assert_eq!(trino_type_to_pg("row"), Type::JSONB);
    }

    // -- Parametric types --

    #[test]
    fn varchar_with_length() {
        assert_eq!(trino_type_to_pg("varchar(100)"), Type::VARCHAR);
    }

    #[test]
    fn decimal_with_precision_scale() {
        assert_eq!(trino_type_to_pg("decimal(10,2)"), Type::NUMERIC);
    }

    #[test]
    fn char_with_length() {
        assert_eq!(trino_type_to_pg("char(50)"), Type::BPCHAR);
    }

    // -- Array types --

    #[test]
    fn array_integer() {
        assert_eq!(trino_type_to_pg("array(integer)"), Type::INT4_ARRAY);
    }

    #[test]
    fn array_varchar() {
        assert_eq!(trino_type_to_pg("array(varchar)"), Type::VARCHAR_ARRAY);
    }

    #[test]
    fn array_boolean() {
        assert_eq!(trino_type_to_pg("array(boolean)"), Type::BOOL_ARRAY);
    }

    #[test]
    fn array_bigint() {
        assert_eq!(trino_type_to_pg("array(bigint)"), Type::INT8_ARRAY);
    }

    // -- Unknown types default to TEXT --

    #[test]
    fn unknown_type_defaults_to_text() {
        assert_eq!(trino_type_to_pg("hyperloglog"), Type::TEXT);
    }

    #[test]
    fn unknown_type_with_params_defaults_to_text() {
        assert_eq!(trino_type_to_pg("qdigest(double)"), Type::TEXT);
    }

    // -- Case insensitivity --

    #[test]
    fn case_insensitive() {
        assert_eq!(trino_type_to_pg("BOOLEAN"), Type::BOOL);
        assert_eq!(trino_type_to_pg("VARCHAR"), Type::VARCHAR);
        assert_eq!(trino_type_to_pg("Integer"), Type::INT4);
    }

    // -- encode_value --

    #[test]
    fn encode_null() {
        assert_eq!(encode_value(&Value::Null, "varchar"), None);
    }

    #[test]
    fn encode_bool_true() {
        assert_eq!(
            encode_value(&Value::Bool(true), "boolean"),
            Some("true".to_owned())
        );
    }

    #[test]
    fn encode_bool_false() {
        assert_eq!(
            encode_value(&Value::Bool(false), "boolean"),
            Some("false".to_owned())
        );
    }

    #[test]
    fn encode_integer_number() {
        let val = serde_json::json!(42);
        assert_eq!(encode_value(&val, "integer"), Some("42".to_owned()));
    }

    #[test]
    fn encode_float_number() {
        let val = serde_json::json!(3.14);
        assert_eq!(encode_value(&val, "double"), Some("3.14".to_owned()));
    }

    /// Regression: Trino can serialize a BIGINT whole number as JSON `42.0`;
    /// naive `Number::to_string` would emit `"42.0"`, which PG's int8 text
    /// parser rejects (it allows only decimal integer literals). Ensure we
    /// render integer text for integer target types regardless of JSON form.
    #[test]
    fn encode_bigint_from_float_json() {
        let val = serde_json::Value::Number(serde_json::Number::from_f64(42.0).unwrap());
        assert_eq!(encode_value(&val, "bigint"), Some("42".to_owned()));
    }

    #[test]
    fn encode_bigint_max() {
        let val = serde_json::json!(9223372036854775807_i64);
        assert_eq!(
            encode_value(&val, "bigint"),
            Some("9223372036854775807".to_owned())
        );
    }

    #[test]
    fn encode_bigint_as_string_passes_through() {
        // Trino sometimes sends bigint as a JSON string to preserve precision.
        let val = serde_json::json!("9223372036854775807");
        assert_eq!(
            encode_value(&val, "bigint"),
            Some("9223372036854775807".to_owned())
        );
    }

    /// serde_json rejects NaN as a `Value::Number`, so Trino sends it as the
    /// string `"NaN"`. Confirm the string-passthrough path produces the
    /// canonical PostgreSQL NaN text.
    #[test]
    fn encode_real_nan_as_pg_text() {
        let string_nan = serde_json::Value::String("NaN".to_owned());
        assert_eq!(encode_value(&string_nan, "real"), Some("NaN".to_owned()));
    }

    #[test]
    fn encode_real_infinity_from_string() {
        let val = serde_json::Value::String("Infinity".to_owned());
        assert_eq!(encode_value(&val, "real"), Some("Infinity".to_owned()));
    }

    #[test]
    fn encode_real_normal_value() {
        let val = serde_json::json!(3.14);
        assert_eq!(encode_value(&val, "real"), Some("3.14".to_owned()));
    }

    #[test]
    fn encode_integer_from_parsed_float_json() {
        // `serde_json::from_str("42.0")` yields a Float-variant Number;
        // our integer path must still render "42" for int8 target.
        let val: serde_json::Value = serde_json::from_str("42.0").unwrap();
        assert_eq!(encode_value(&val, "integer"), Some("42".to_owned()));
    }

    /// Regression: previously, an out-of-range float for a BIGINT target was
    /// silently cast via `f as i64`, which saturates to `i64::MAX`. The
    /// client would see 9223372036854775807 instead of an error. Now we emit
    /// the float as text so PostgreSQL's int8 parser rejects it client-side.
    #[test]
    fn encode_bigint_overflow_does_not_silently_saturate() {
        let val = serde_json::Value::Number(serde_json::Number::from_f64(1.0e30).unwrap());
        let encoded = encode_value(&val, "bigint").expect("must encode");
        assert_ne!(encoded, "9223372036854775807", "must not saturate to i64::MAX");
        let parsed: f64 = encoded.parse().expect("must be valid float text");
        assert!(
            (parsed - 1.0e30).abs() < 1.0e15,
            "expected ~1e30, got {parsed} (text: {encoded})"
        );
    }

    #[test]
    fn encode_bigint_negative_overflow_does_not_saturate() {
        let val = serde_json::Value::Number(serde_json::Number::from_f64(-1.0e30).unwrap());
        let encoded = encode_value(&val, "bigint").expect("must encode");
        assert_ne!(encoded, i64::MIN.to_string(), "must not saturate to i64::MIN");
        let parsed: f64 = encoded.parse().expect("must be valid float text");
        assert!(parsed < 0.0 && parsed.abs() > 1.0e29, "expected large negative, got {parsed}");
    }

    #[test]
    fn encode_bigint_in_range_float_still_casts() {
        // 9e18 fits comfortably below i64::MAX (9.22e18); should cast cleanly.
        let val = serde_json::Value::Number(serde_json::Number::from_f64(9.0e18).unwrap());
        let encoded = encode_value(&val, "bigint").expect("must encode");
        assert!(
            !encoded.contains('e') && !encoded.contains('.'),
            "in-range float should produce integer text: got {encoded}"
        );
    }

    #[test]
    fn encode_integer_from_huge_float_is_not_truncated() {
        // INT4 target with an out-of-range float: same fail-closed behaviour
        // as BIGINT. We don't try to fit the i32 range explicitly because
        // PG's int4 parser rejects anything outside its range, and the cast
        // would in any case saturate at i64::MAX before reaching the wire.
        let val = serde_json::Value::Number(serde_json::Number::from_f64(1.0e15).unwrap());
        let encoded = encode_value(&val, "integer").expect("must encode");
        // 1e15 fits in i64 so we cast; the i64 representation is far above
        // i32::MAX and PG's int4 parser will reject it at the client.
        assert!(
            encoded.parse::<i64>().is_ok(),
            "in-range-for-i64 cast should be valid integer text: got {encoded}"
        );
    }

    #[test]
    fn encode_string() {
        let val = serde_json::json!("hello world");
        assert_eq!(
            encode_value(&val, "varchar"),
            Some("hello world".to_owned())
        );
    }

    #[test]
    fn encode_array() {
        let val = serde_json::json!([1, 2, 3]);
        let result = encode_value(&val, "array(integer)");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "[1,2,3]");
    }

    #[test]
    fn encode_object() {
        let val = serde_json::json!({"key": "value"});
        let result = encode_value(&val, "map");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), r#"{"key":"value"}"#);
    }
}
