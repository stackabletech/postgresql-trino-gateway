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
pub fn encode_value(value: &Value, _trino_type: &str) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(true) => Some("true".to_owned()),
        Value::Bool(false) => Some("false".to_owned()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
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
        assert_eq!(
            trino_type_to_pg("interval year to month"),
            Type::INTERVAL
        );
    }

    #[test]
    fn interval_day_to_second() {
        assert_eq!(
            trino_type_to_pg("interval day to second"),
            Type::INTERVAL
        );
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
        assert_eq!(
            encode_value(&val, "integer"),
            Some("42".to_owned())
        );
    }

    #[test]
    fn encode_float_number() {
        let val = serde_json::json!(3.14);
        assert_eq!(
            encode_value(&val, "double"),
            Some("3.14".to_owned())
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
