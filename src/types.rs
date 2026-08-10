// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::borrow::Cow;
use std::error::Error;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::types::ToSqlText;
use pgwire::types::format::FormatOptions;
use postgres_types::{IsNull, ToSql, to_sql_checked};
use rust_decimal::Decimal;
use serde_json::Value;

/// Parametric types (`varchar(100)`, `decimal(10,2)`) are handled by
/// stripping the parenthesised parameters before matching the base name.
pub fn trino_type_to_pg(trino_type: &str) -> Type {
    let normalized = trino_type.trim().to_lowercase();

    // Handle array types: array(inner_type)
    if normalized.starts_with("array(") && normalized.ends_with(')') {
        let inner = &normalized[6..normalized.len() - 1];
        return scalar_to_array(&trino_type_to_pg(inner));
    }

    match strip_type_params(&normalized).as_str() {
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
        "time with time zone" => Type::TIMETZ,
        "timestamp" => Type::TIMESTAMP,
        "timestamp with time zone" => Type::TIMESTAMPTZ,
        "interval" | "interval year to month" | "interval day to second" => Type::INTERVAL,
        "json" => Type::JSONB,
        "uuid" => Type::UUID,
        "ipaddress" => Type::INET,
        "map" | "row" => Type::JSONB,
        _ => Type::TEXT,
    }
}

/// Remove a type's parenthesised parameters, wherever they sit in the name.
///
/// Trino spells the precision *inside* the type name (`timestamp(6) with time
/// zone`), so cutting at the first `(` and dropping the remainder would lose
/// the ` with time zone` suffix. Removing the span from the first `(` to the
/// last `)` keeps the full name and collapses nested parameters
/// (`map(varchar, array(integer))`) in one pass.
fn strip_type_params(normalized: &str) -> String {
    match (normalized.find('('), normalized.rfind(')')) {
        (Some(open), Some(close)) if close > open => {
            let mut base = String::with_capacity(normalized.len());
            base.push_str(&normalized[..open]);
            base.push_str(&normalized[close + 1..]);
            base.trim().to_owned()
        }
        _ => normalized.trim().to_owned(),
    }
}

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
///
/// All values are emitted in the PostgreSQL **text** wire format, rendered
/// verbatim. Types whose Trino rendering differs from PostgreSQL's are
/// reshaped by `text_value` before they reach here.
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

/// Encode one result-set cell into `encoder`, honoring the wire `format`
/// the client requested for this column in its `Bind` message.
///
/// Text columns keep the string-rendering path (`encode_value`). Binary
/// columns are converted to the appropriate typed Rust value so pgwire's
/// `DataRowEncoder` — which picks text vs binary from the column's
/// `FieldFormat` — emits PostgreSQL's binary wire layout. SQL NULL encodes
/// identically in both formats (a -1 length with no payload), so it is
/// handled once here regardless of format.
///
/// Binary encoding is implemented for the scalar types that binary-requesting
/// drivers (Power BI's Npgsql, Tableau/pgjdbc, tokio-postgres) actually ask
/// for: bool, the integer and floating types, numeric, date, time and
/// timestamp (with and without time zone), and the string family (whose binary
/// and text wire forms are identical). Any *other* type requested in binary
/// fails closed with SQLSTATE 0A000 rather than emitting bytes the client would
/// misread — we cannot silently fall back to text because the client decodes
/// strictly per the format it bound. See TODO.md "Binary result-format".
pub fn encode_cell(
    encoder: &mut DataRowEncoder,
    value: &Value,
    pg_type: &Type,
    trino_type: &str,
    format: FieldFormat,
) -> PgWireResult<()> {
    // NULL is format-independent on the wire (-1 length, no payload).
    if value.is_null() {
        return encoder.encode_field(&None::<&str>);
    }
    match format {
        FieldFormat::Text => encoder.encode_field(&text_value(value, pg_type, trino_type)),
        FieldFormat::Binary => encode_binary_cell(encoder, value, pg_type, trino_type),
    }
}

/// Render a non-NULL value for the PostgreSQL **text** wire format.
///
/// Trino's rendering matches PostgreSQL's for every type except the two
/// time-zone-aware ones: Trino writes a named zone or a full offset
/// (`2026-06-01 10:30:00.000000 Europe/Berlin`), PostgreSQL the session zone
/// with a compact one (`2026-06-01 08:30:00+00`). Strict client parsers, Npgsql
/// among them, reject the Trino spelling.
///
/// Unparseable values pass through unchanged. Unlike binary, a text value
/// cannot be *misread* — the client either accepts the bytes or rejects them
/// itself — so failing the result set here would only break lenient clients
/// that work today.
fn text_value(value: &Value, pg_type: &Type, trino_type: &str) -> Option<String> {
    if *pg_type == Type::TIMESTAMPTZ
        && let Some(dt) = value.as_str().and_then(parse_trino_timestamptz)
    {
        return Some(render_timestamptz_text(dt));
    }
    if *pg_type == Type::TIMETZ
        && let Some(time) = value.as_str().and_then(parse_trino_timetz)
    {
        return Some(time.to_pg_text());
    }
    encode_value(value, trino_type)
}

/// Binary-encode a non-NULL value for the column's PostgreSQL type. See
/// `encode_cell` for the supported-type policy and the fail-closed rationale.
fn encode_binary_cell(
    encoder: &mut DataRowEncoder,
    value: &Value,
    pg_type: &Type,
    trino_type: &str,
) -> PgWireResult<()> {
    let t = pg_type;
    if *t == Type::BOOL {
        encoder.encode_field(&json_to_bool(value, trino_type)?)
    } else if *t == Type::INT2 {
        let n = json_to_i64(value, trino_type)?;
        let v = i16::try_from(n).map_err(|_| out_of_range(n, "int2"))?;
        encoder.encode_field(&v)
    } else if *t == Type::INT4 {
        let n = json_to_i64(value, trino_type)?;
        let v = i32::try_from(n).map_err(|_| out_of_range(n, "int4"))?;
        encoder.encode_field(&v)
    } else if *t == Type::INT8 {
        encoder.encode_field(&json_to_i64(value, trino_type)?)
    } else if *t == Type::FLOAT4 {
        encoder.encode_field(&(json_to_f64(value, trino_type)? as f32))
    } else if *t == Type::FLOAT8 {
        encoder.encode_field(&json_to_f64(value, trino_type)?)
    } else if *t == Type::NUMERIC {
        encoder.encode_field(&json_to_decimal(value, trino_type)?)
    } else if *t == Type::TIMESTAMP {
        encoder.encode_field(&json_to_timestamp(value, trino_type)?)
    } else if *t == Type::TIMESTAMPTZ {
        encoder.encode_field(&json_to_timestamptz(value, trino_type)?)
    } else if *t == Type::DATE {
        encoder.encode_field(&json_to_date(value, trino_type)?)
    } else if *t == Type::TIME {
        encoder.encode_field(&json_to_time(value, trino_type)?)
    } else if *t == Type::TIMETZ {
        encoder.encode_field(&json_to_timetz(value, trino_type)?)
    } else if *t == Type::VARCHAR
        || *t == Type::TEXT
        || *t == Type::BPCHAR
        || *t == Type::NAME
        || *t == Type::UNKNOWN
    {
        // varchar/text/char/name share an identical text and binary wire
        // layout (the raw UTF-8 bytes), so the rendered string is already the
        // correct binary payload.
        encoder.encode_field(&encode_value(value, trino_type))
    } else {
        Err(unsupported_binary_type(pg_type, trino_type))
    }
}

fn json_to_bool(value: &Value, trino_type: &str) -> PgWireResult<bool> {
    match value {
        Value::Bool(b) => Ok(*b),
        Value::String(s) if s == "true" => Ok(true),
        Value::String(s) if s == "false" => Ok(false),
        _ => Err(conversion_error("boolean", trino_type)),
    }
}

/// Trino sends integers as JSON numbers, but occasionally as strings to
/// preserve precision. Accept both, plus whole-valued floats (`42.0`).
fn json_to_i64(value: &Value, trino_type: &str) -> PgWireResult<i64> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| n.as_f64().and_then(f64_to_i64_exact))
            .ok_or_else(|| conversion_error("integer", trino_type)),
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .ok()
            .or_else(|| s.trim().parse::<f64>().ok().and_then(f64_to_i64_exact))
            .ok_or_else(|| conversion_error("integer", trino_type)),
        _ => Err(conversion_error("integer", trino_type)),
    }
}

/// Convert a float to i64 only when it is integral and in range; otherwise
/// `None` so the caller fails closed rather than silently truncating.
fn f64_to_i64_exact(f: f64) -> Option<i64> {
    if f.fract() == 0.0 && f >= i64::MIN as f64 && f < i64::MAX as f64 {
        Some(f as i64)
    } else {
        None
    }
}

fn json_to_f64(value: &Value, trino_type: &str) -> PgWireResult<f64> {
    match value {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| conversion_error("float", trino_type)),
        // Trino serializes non-finite floats as strings.
        Value::String(s) => match s.trim() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            other => other
                .parse::<f64>()
                .map_err(|_| conversion_error("float", trino_type)),
        },
        _ => Err(conversion_error("float", trino_type)),
    }
}

/// Trino returns DECIMAL as a string to preserve precision. `from_str_exact`
/// rejects values that exceed `rust_decimal`'s ~28-digit capacity, so we fail
/// closed instead of silently losing precision.
fn json_to_decimal(value: &Value, trino_type: &str) -> PgWireResult<Decimal> {
    let s = match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return Err(conversion_error("numeric", trino_type)),
    };
    Decimal::from_str_exact(s.trim()).map_err(|_| conversion_error("numeric", trino_type))
}

fn json_to_timestamp(value: &Value, trino_type: &str) -> PgWireResult<NaiveDateTime> {
    let s = value
        .as_str()
        .ok_or_else(|| conversion_error("timestamp", trino_type))?;
    parse_naive_datetime(s.trim()).ok_or_else(|| conversion_error("timestamp", trino_type))
}

/// Trino separates date and time with a space; the ISO-8601 `T` turns up in
/// values that passed through a client library on the way in. `%.f` also
/// matches an absent fraction, so `timestamp(0)` needs no extra format.
fn parse_naive_datetime(s: &str) -> Option<NaiveDateTime> {
    let s = truncate_fraction(s);
    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
}

/// Shorten a fractional-second field longer than nine digits.
///
/// Trino goes to picosecond precision (`timestamp(12)`), while chrono's `%.f`
/// parses at most nine digits and treats the rest as trailing garbage, failing
/// the whole value. PostgreSQL stores microseconds, so the dropped digits could
/// not have reached the client anyway.
fn truncate_fraction(s: &str) -> Cow<'_, str> {
    let Some(dot) = s.find('.') else {
        return Cow::Borrowed(s);
    };
    let fraction_start = dot + 1;
    let fraction_len = s[fraction_start..]
        .bytes()
        .take_while(u8::is_ascii_digit)
        .count();
    if fraction_len <= 9 {
        return Cow::Borrowed(s);
    }
    let mut truncated = String::with_capacity(s.len());
    truncated.push_str(&s[..fraction_start + 9]);
    truncated.push_str(&s[fraction_start + fraction_len..]);
    Cow::Owned(truncated)
}

fn json_to_timestamptz(value: &Value, trino_type: &str) -> PgWireResult<DateTime<Utc>> {
    let s = value
        .as_str()
        .ok_or_else(|| conversion_error("timestamptz", trino_type))?;
    parse_trino_timestamptz(s).ok_or_else(|| conversion_error("timestamptz", trino_type))
}

/// Parse Trino's rendering of `timestamp(p) with time zone`.
///
/// Trino carries a zone per value and renders it either as a fixed offset
/// (`2026-06-01 10:30:00.000000 +02:00`, sometimes without the space) or as a
/// named zone (`2026-06-01 10:30:00.000000 Europe/Berlin`, which is also how
/// plain `UTC` arrives). PostgreSQL's `timestamptz` is a bare instant, so both
/// shapes collapse to UTC.
fn parse_trino_timestamptz(s: &str) -> Option<DateTime<Utc>> {
    let s = truncate_fraction(s.trim());
    let s = s.as_ref();
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%.f %#z",
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%.f %#z",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
    }

    let (stamp, zone) = s.rsplit_once(' ')?;
    let tz: Tz = zone.parse().ok()?;
    let naive = parse_naive_datetime(stamp.trim())?;
    // A named zone is local wall-clock time: a DST fold leaves two candidate
    // instants, a DST gap none, and the rendered text cannot say which is
    // meant. `single()` returns None for both, so the caller fails closed
    // rather than shift the value by an hour without saying so.
    tz.from_local_datetime(&naive)
        .single()
        .map(|dt| dt.with_timezone(&Utc))
}

fn json_to_date(value: &Value, trino_type: &str) -> PgWireResult<NaiveDate> {
    let s = value
        .as_str()
        .ok_or_else(|| conversion_error("date", trino_type))?
        .trim();
    NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| conversion_error("date", trino_type))
}

fn json_to_time(value: &Value, trino_type: &str) -> PgWireResult<NaiveTime> {
    let s = value
        .as_str()
        .ok_or_else(|| conversion_error("time", trino_type))?
        .trim();
    NaiveTime::parse_from_str(&truncate_fraction(s), "%H:%M:%S%.f")
        .map_err(|_| conversion_error("time", trino_type))
}

fn json_to_timetz(value: &Value, trino_type: &str) -> PgWireResult<TimeTz> {
    let s = value
        .as_str()
        .ok_or_else(|| conversion_error("timetz", trino_type))?;
    parse_trino_timetz(s).ok_or_else(|| conversion_error("timetz", trino_type))
}

/// Parse Trino's rendering of `time(p) with time zone`, e.g.
/// `10:30:00.000000+02:00`. Trino only ever carries a fixed offset for this
/// type — resolving a named zone would need a date — so unlike in
/// `parse_trino_timestamptz` the offset is mandatory.
fn parse_trino_timetz(s: &str) -> Option<TimeTz> {
    // chrono has no `timetz` equivalent, but it does validate offsets while
    // parsing a `DateTime`. Lend the value a dummy date, then keep only the
    // wall-clock time and the offset it was written with.
    let dated = format!("2000-01-01 {}", truncate_fraction(s.trim()));
    for fmt in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%.f %#z"] {
        if let Ok(dt) = DateTime::parse_from_str(&dated, fmt) {
            return Some(TimeTz {
                time: dt.time(),
                offset_east_seconds: dt.offset().local_minus_utc(),
            });
        }
    }
    None
}

/// A PostgreSQL `timetz`: a time of day plus the UTC offset it was written
/// with. No chrono type carries that pair, so `postgres-types` has no `ToSql`
/// for it either and both wire encodings live here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeTz {
    time: NaiveTime,

    /// Seconds *east* of UTC, the chrono and ISO-8601 convention — the
    /// opposite of the one on the wire (see `to_sql`).
    offset_east_seconds: i32,
}

impl TimeTz {
    /// PostgreSQL's text form, e.g. `10:30:00+02`.
    fn to_pg_text(self) -> String {
        format!(
            "{}{}{}",
            self.time.format("%H:%M:%S"),
            format_fraction(self.time.nanosecond()),
            format_utc_offset(self.offset_east_seconds)
        )
    }
}

impl ToSql for TimeTz {
    /// PostgreSQL's `timetz_send`: int64 microseconds since midnight, then the
    /// zone offset as an int32 in seconds *west* of UTC, i.e. negated against
    /// ISO-8601. Both PostgreSQL (`DecodeTimezone`) and Npgsql (`TimeTzHandler`)
    /// apply that negation, so it is the wire contract, not an implementation
    /// detail.
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let micros = self
            .time
            .signed_duration_since(NaiveTime::MIN)
            .num_microseconds()
            .ok_or("time of day does not fit in a microsecond count")?;
        out.put_i64(micros);
        out.put_i32(-self.offset_east_seconds);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIMETZ
    }

    to_sql_checked!();
}

impl ToSqlText for TimeTz {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.to_pg_text().as_bytes());
        Ok(IsNull::No)
    }
}

/// PostgreSQL renders `timestamptz` in the session time zone. The gateway pins
/// that to UTC — both the `TimeZone` startup parameter and the `SHOW timezone`
/// intercept report UTC — so every instant goes out as UTC.
fn render_timestamptz_text(dt: DateTime<Utc>) -> String {
    format!(
        "{}{}+00",
        dt.format("%Y-%m-%d %H:%M:%S"),
        format_fraction(dt.nanosecond())
    )
}

/// PostgreSQL's fractional-second spelling: microsecond resolution, trailing
/// zeros trimmed, nothing at all when the fraction is zero. A server emits
/// `10:30:00.5`, never `10:30:00.500000`; chrono's `%.f` would pad to whole
/// groups of three digits instead.
fn format_fraction(nanoseconds: u32) -> String {
    // chrono folds a leap second into the nanosecond field, which would carry
    // the fraction past six digits.
    let micros = (nanoseconds / 1_000).min(999_999);
    if micros == 0 {
        return String::new();
    }
    format!(".{micros:06}").trim_end_matches('0').to_owned()
}

/// PostgreSQL's offset spelling: `+02`, widened to `+02:30` or `+02:30:15`
/// only when the finer fields carry something.
fn format_utc_offset(seconds_east: i32) -> String {
    let sign = if seconds_east < 0 { '-' } else { '+' };
    let total = seconds_east.unsigned_abs();
    let (hours, minutes, seconds) = (total / 3600, (total % 3600) / 60, total % 60);
    if seconds != 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else if minutes != 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}")
    }
}

/// A value Trino returned could not be converted to the target PG type's
/// binary form. SQLSTATE 22P03 = invalid_binary_representation. The offending
/// value is deliberately not included (it may carry sensitive data).
fn conversion_error(target: &str, trino_type: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "22P03".to_owned(),
        format!("gateway could not encode a Trino {trino_type} value as binary {target}"),
    )))
}

/// An integer value did not fit the narrower PG integer type. SQLSTATE
/// 22003 = numeric_value_out_of_range.
fn out_of_range(value: i64, target: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "22003".to_owned(),
        format!("value {value} out of range for binary {target}"),
    )))
}

/// The client requested binary results for a column whose type the gateway
/// cannot yet encode in binary. SQLSTATE 0A000 = feature_not_supported.
fn unsupported_binary_type(pg_type: &Type, trino_type: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!(
            "binary wire format for column type {} (Trino {}) is not supported by the gateway; \
             the client requested binary results for this column",
            pg_type.name(),
            trino_type
        ),
    )))
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
        let val = serde_json::json!(3.5);
        assert_eq!(encode_value(&val, "double"), Some("3.5".to_owned()));
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
        let val = serde_json::json!(3.5);
        assert_eq!(encode_value(&val, "real"), Some("3.5".to_owned()));
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
        assert_ne!(
            encoded, "9223372036854775807",
            "must not saturate to i64::MAX"
        );
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
        assert_ne!(
            encoded,
            i64::MIN.to_string(),
            "must not saturate to i64::MIN"
        );
        let parsed: f64 = encoded.parse().expect("must be valid float text");
        assert!(
            parsed < 0.0 && parsed.abs() > 1.0e29,
            "expected large negative, got {parsed}"
        );
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

    // -- binary wire format (encode_cell) --

    use pgwire::api::results::FieldInfo;
    use serde_json::json;
    use std::sync::Arc;

    /// Encode one value as a single-column row with the given pg type and wire
    /// format, returning the field payload: `None` for SQL NULL, else the
    /// bytes after the 4-byte length prefix.
    fn encode_one(
        value: &Value,
        pg_type: Type,
        trino_type: &str,
        format: FieldFormat,
    ) -> PgWireResult<Option<Vec<u8>>> {
        let schema = Arc::new(vec![FieldInfo::new(
            "c".to_owned(),
            None,
            None,
            pg_type.clone(),
            format,
        )]);
        let mut encoder = DataRowEncoder::new(schema);
        encode_cell(&mut encoder, value, &pg_type, trino_type, format)?;
        let row = encoder.take_row();
        let data = &row.data;
        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(data[4..4 + len as usize].to_vec()))
    }

    fn bin(value: &Value, pg_type: Type, trino_type: &str) -> Vec<u8> {
        encode_one(value, pg_type, trino_type, FieldFormat::Binary)
            .expect("binary encode should succeed")
            .expect("non-null value")
    }

    #[test]
    fn binary_int4_is_big_endian_4_bytes() {
        assert_eq!(bin(&json!(42), Type::INT4, "integer"), vec![0, 0, 0, 42]);
    }

    #[test]
    fn binary_int8_is_big_endian_8_bytes() {
        assert_eq!(
            bin(&json!(1), Type::INT8, "bigint"),
            vec![0, 0, 0, 0, 0, 0, 0, 1]
        );
    }

    #[test]
    fn binary_int2_from_smallint() {
        assert_eq!(bin(&json!(7), Type::INT2, "smallint"), vec![0, 7]);
    }

    #[test]
    fn binary_int8_from_json_string() {
        // Trino may send bigint as a JSON string to preserve precision.
        assert_eq!(
            bin(&json!("255"), Type::INT8, "bigint"),
            vec![0, 0, 0, 0, 0, 0, 0, 255]
        );
    }

    #[test]
    fn binary_float8_matches_ieee754_be() {
        assert_eq!(
            bin(&json!(1.5), Type::FLOAT8, "double"),
            1.5f64.to_be_bytes().to_vec()
        );
    }

    #[test]
    fn binary_float4_matches_ieee754_be() {
        assert_eq!(
            bin(&json!(1.5), Type::FLOAT4, "real"),
            1.5f32.to_be_bytes().to_vec()
        );
    }

    #[test]
    fn binary_float8_nan_from_string() {
        // Trino serializes non-finite floats as strings.
        let bytes = bin(&json!("NaN"), Type::FLOAT8, "double");
        let f = f64::from_be_bytes(bytes.try_into().expect("8 bytes"));
        assert!(f.is_nan());
    }

    #[test]
    fn binary_bool_true_is_one_byte() {
        assert_eq!(bin(&json!(true), Type::BOOL, "boolean"), vec![1]);
        assert_eq!(bin(&json!(false), Type::BOOL, "boolean"), vec![0]);
    }

    #[test]
    fn binary_timestamp_is_micros_since_2000() {
        // PostgreSQL binary timestamp = i64 microseconds since 2000-01-01.
        let epoch = NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let ts = NaiveDate::from_ymd_opt(2026, 6, 1)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        let expected = (ts - epoch).num_microseconds().unwrap();
        let bytes = bin(
            &json!("2026-06-01 10:30:00.000000"),
            Type::TIMESTAMP,
            "timestamp(6)",
        );
        assert_eq!(
            i64::from_be_bytes(bytes.try_into().expect("8 bytes")),
            expected
        );
    }

    #[test]
    fn binary_timestamp_without_fraction_parses() {
        let bytes = bin(&json!("2026-06-01 10:30:00"), Type::TIMESTAMP, "timestamp");
        assert_eq!(bytes.len(), 8);
    }

    #[test]
    fn parametric_timestamptz_maps_to_timestamptz() {
        // Trino always reports the precision, and it sits *inside* the type
        // name, so a naive strip at the first `(` swallows the time zone.
        assert_eq!(
            trino_type_to_pg("timestamp(6) with time zone"),
            Type::TIMESTAMPTZ
        );
        assert_eq!(trino_type_to_pg("time(6) with time zone"), Type::TIMETZ);
        assert_eq!(trino_type_to_pg("timestamp(3)"), Type::TIMESTAMP);
        assert_eq!(trino_type_to_pg("time(3)"), Type::TIME);
    }

    #[test]
    fn nested_type_params_collapse_to_the_base_name() {
        assert_eq!(
            trino_type_to_pg("map(varchar, array(integer))"),
            Type::JSONB
        );
        assert_eq!(
            trino_type_to_pg("array(timestamp(6) with time zone)"),
            Type::TIMESTAMPTZ_ARRAY
        );
    }

    /// Micros since the PostgreSQL epoch (2000-01-01 00:00:00 UTC), the unit
    /// of both binary timestamp types.
    fn micros_since_pg_epoch(dt: DateTime<Utc>) -> i64 {
        let epoch = NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        (dt - epoch).num_microseconds().unwrap()
    }

    fn binary_timestamptz_micros(value: &str) -> i64 {
        let bytes = bin(
            &json!(value),
            Type::TIMESTAMPTZ,
            "timestamp(6) with time zone",
        );
        i64::from_be_bytes(bytes.try_into().expect("8 bytes"))
    }

    #[test]
    fn binary_timestamptz_accepts_every_shape_trino_renders() {
        // 2026-06-01 08:30:00Z, written four ways.
        let expected = micros_since_pg_epoch(
            NaiveDate::from_ymd_opt(2026, 6, 1)
                .unwrap()
                .and_hms_opt(8, 30, 0)
                .unwrap()
                .and_utc(),
        );
        for value in [
            "2026-06-01 08:30:00.000000 UTC",
            "2026-06-01 10:30:00.000000 Europe/Berlin",
            "2026-06-01 10:30:00.000000 +02:00",
            "2026-06-01 10:30:00.000000+02:00",
        ] {
            assert_eq!(
                binary_timestamptz_micros(value),
                expected,
                "wrong instant for {value}"
            );
        }
    }

    #[test]
    fn binary_timestamptz_without_fraction_parses() {
        assert_eq!(
            binary_timestamptz_micros("2026-06-01 08:30:00 UTC"),
            binary_timestamptz_micros("2026-06-01 08:30:00.000000 UTC")
        );
    }

    #[test]
    fn binary_timestamptz_rejects_dst_ambiguous_local_time() {
        // 2026-10-25 02:30 happens twice in Europe/Berlin, 2026-03-29 02:30 not
        // at all. Guessing an hour would be silent data corruption.
        for value in [
            "2026-10-25 02:30:00.000000 Europe/Berlin",
            "2026-03-29 02:30:00.000000 Europe/Berlin",
        ] {
            let res = encode_one(
                &json!(value),
                Type::TIMESTAMPTZ,
                "timestamp(6) with time zone",
                FieldFormat::Binary,
            );
            assert!(res.is_err(), "{value} must fail closed");
        }
    }

    #[test]
    fn binary_timestamptz_rejects_unknown_zone() {
        let res = encode_one(
            &json!("2026-06-01 10:30:00.000000 Mars/Olympus_Mons"),
            Type::TIMESTAMPTZ,
            "timestamp(6) with time zone",
            FieldFormat::Binary,
        );
        assert!(res.is_err(), "unknown zone must fail closed");
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Pins the wire layouts — notably `timetz`'s sign-inverted zone field,
    /// which no client in this stack can round-trip — against PostgreSQL 18
    /// itself. The expected byte strings are the output of:
    ///
    /// ```sql
    /// SELECT encode(timetz_send('10:30:00+02'::timetz), 'hex'),
    ///        encode(timestamptz_send('2026-06-01 10:30:00+02'::timestamptz), 'hex'),
    ///        encode(timestamp_send('2026-06-01 10:30:00'::timestamp), 'hex');
    /// ```
    #[test]
    fn binary_layouts_match_postgres_send_functions() {
        for (value, pg_type, trino_type, expected) in [
            (
                "10:30:00+02:00",
                Type::TIMETZ,
                "time(6) with time zone",
                "00000008cd0e3a00ffffe3e0",
            ),
            (
                "10:30:00-05:00",
                Type::TIMETZ,
                "time(6) with time zone",
                "00000008cd0e3a0000004650",
            ),
            (
                "2026-06-01 10:30:00.000000 +02:00",
                Type::TIMESTAMPTZ,
                "timestamp(6) with time zone",
                "0002f62bc4d8f200",
            ),
            (
                "2026-06-01 10:30:00.000000",
                Type::TIMESTAMP,
                "timestamp(6)",
                "0002f62d72003a00",
            ),
        ] {
            assert_eq!(
                hex(&bin(&json!(value), pg_type, trino_type)),
                expected,
                "wire bytes for {value}"
            );
        }
    }

    /// Trino renders more precision than chrono parses or PostgreSQL stores,
    /// so sub-microsecond digits are dropped rather than failing the value.
    #[test]
    fn picosecond_precision_truncates_to_microseconds() {
        assert_eq!(
            binary_timestamptz_micros("2026-06-01 08:30:00.123456789012 UTC"),
            binary_timestamptz_micros("2026-06-01 08:30:00.123456 UTC")
        );
        // The zone-less types hit the same limit.
        assert_eq!(
            bin(
                &json!("2026-06-01 10:30:00.123456789012"),
                Type::TIMESTAMP,
                "timestamp(12)"
            ),
            bin(
                &json!("2026-06-01 10:30:00.123456"),
                Type::TIMESTAMP,
                "timestamp(6)"
            )
        );
        assert_eq!(
            bin(&json!("10:30:00.123456789012"), Type::TIME, "time(12)"),
            bin(&json!("10:30:00.123456"), Type::TIME, "time(6)")
        );
        assert_eq!(
            bin(
                &json!("10:30:00.123456789012+02:00"),
                Type::TIMETZ,
                "time(12) with time zone"
            ),
            bin(
                &json!("10:30:00.123456+02:00"),
                Type::TIMETZ,
                "time(6) with time zone"
            )
        );
    }

    #[test]
    fn binary_timetz_is_micros_then_negated_offset() {
        // Offset counted in seconds *west* of UTC: +02:00 → -7200.
        let bytes = bin(
            &json!("10:30:00.000000+02:00"),
            Type::TIMETZ,
            "time(6) with time zone",
        );
        assert_eq!(bytes.len(), 12);
        let (micros, zone) = bytes.split_at(8);
        assert_eq!(
            i64::from_be_bytes(micros.try_into().expect("8 bytes")),
            (10 * 3600 + 30 * 60) * 1_000_000
        );
        assert_eq!(i32::from_be_bytes(zone.try_into().expect("4 bytes")), -7200);
    }

    #[test]
    fn binary_timetz_requires_an_offset() {
        // A bare time is not a `time with time zone` value; guessing UTC would
        // silently move it.
        let res = encode_one(
            &json!("10:30:00.000000"),
            Type::TIMETZ,
            "time(6) with time zone",
            FieldFormat::Binary,
        );
        assert!(res.is_err(), "offset-less timetz must fail closed");
    }

    fn text_of(value: &Value, pg_type: Type, trino_type: &str) -> String {
        let bytes = encode_one(value, pg_type, trino_type, FieldFormat::Text)
            .expect("text encode should succeed")
            .expect("non-null value");
        String::from_utf8(bytes).expect("utf-8")
    }

    #[test]
    fn text_timestamptz_is_rendered_in_utc_like_postgres() {
        // Both Trino spellings must come out in the pinned session zone.
        for value in [
            "2026-06-01 10:30:00.000000 Europe/Berlin",
            "2026-06-01 10:30:00.000000+02:00",
        ] {
            assert_eq!(
                text_of(
                    &json!(value),
                    Type::TIMESTAMPTZ,
                    "timestamp(6) with time zone"
                ),
                "2026-06-01 08:30:00+00"
            );
        }
    }

    /// Expected strings are PostgreSQL 18's own `::text` output for the same
    /// values under `SET timezone='UTC'`, the zone the gateway advertises.
    #[test]
    fn text_forms_match_postgres_output() {
        for (value, pg_type, trino_type, expected) in [
            (
                "2026-06-01 10:30:00.000000 +02:00",
                Type::TIMESTAMPTZ,
                "timestamp(6) with time zone",
                "2026-06-01 08:30:00+00",
            ),
            (
                "2026-06-01 10:30:00.500000 +02:00",
                Type::TIMESTAMPTZ,
                "timestamp(6) with time zone",
                "2026-06-01 08:30:00.5+00",
            ),
            (
                "2026-06-01 10:30:00.123456 +02:00",
                Type::TIMESTAMPTZ,
                "timestamp(6) with time zone",
                "2026-06-01 08:30:00.123456+00",
            ),
            (
                "10:30:00.000000+02:00",
                Type::TIMETZ,
                "time(6) with time zone",
                "10:30:00+02",
            ),
            (
                "10:30:00.500000+02:00",
                Type::TIMETZ,
                "time(6) with time zone",
                "10:30:00.5+02",
            ),
            (
                "10:30:00.000000+05:30",
                Type::TIMETZ,
                "time(6) with time zone",
                "10:30:00+05:30",
            ),
            (
                "10:30:00.000000-05:00",
                Type::TIMETZ,
                "time(6) with time zone",
                "10:30:00-05",
            ),
        ] {
            assert_eq!(
                text_of(&json!(value), pg_type, trino_type),
                expected,
                "text form of {value}"
            );
        }
    }

    #[test]
    fn text_timestamptz_falls_back_to_pass_through() {
        // Text cannot be misread the way binary can, so an unparseable value
        // goes out unchanged rather than failing the whole result set.
        let odd = "not a timestamp";
        assert_eq!(
            text_of(
                &json!(odd),
                Type::TIMESTAMPTZ,
                "timestamp(6) with time zone"
            ),
            odd
        );
    }

    #[test]
    fn utc_offsets_use_postgres_spelling() {
        assert_eq!(format_utc_offset(0), "+00");
        assert_eq!(format_utc_offset(2 * 3600), "+02");
        assert_eq!(format_utc_offset(-5 * 3600), "-05");
        assert_eq!(format_utc_offset(5 * 3600 + 30 * 60), "+05:30");
        assert_eq!(format_utc_offset(-(3600 + 60 + 1)), "-01:01:01");
    }

    #[test]
    fn binary_numeric_round_trips_via_decimal() {
        // 4-byte count of base-10000 digit groups means a non-empty payload;
        // we just assert it encodes (correctness of the digit layout is
        // postgres-types' responsibility) and that an over-precise value
        // fails closed.
        assert!(!bin(&json!("123.45"), Type::NUMERIC, "decimal(10,2)").is_empty());
    }

    #[test]
    fn binary_numeric_rejects_overprecise_value() {
        // Exceeds rust_decimal's capacity → fail closed rather than corrupt.
        let huge = "1".repeat(40);
        let err = encode_one(
            &json!(huge),
            Type::NUMERIC,
            "decimal(38,0)",
            FieldFormat::Binary,
        );
        assert!(err.is_err(), "over-precise decimal must fail closed");
    }

    #[test]
    fn binary_varchar_is_raw_utf8_bytes() {
        // varchar binary == text bytes.
        assert_eq!(
            bin(&json!("hello"), Type::VARCHAR, "varchar"),
            b"hello".to_vec()
        );
    }

    #[test]
    fn binary_null_is_negative_one_length() {
        // NULL is format-independent: -1 length, no payload.
        let out = encode_one(&Value::Null, Type::INT4, "integer", FieldFormat::Binary).unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn binary_int4_overflow_fails_closed() {
        let too_big = json!(i64::from(i32::MAX) + 1);
        let res = encode_one(&too_big, Type::INT4, "integer", FieldFormat::Binary);
        assert!(res.is_err(), "out-of-range int4 must fail closed");
    }

    #[test]
    fn binary_unsupported_type_fails_closed() {
        // INTERVAL has no binary encoder yet → fail closed, not silent text.
        let res = encode_one(
            &json!("1 day"),
            Type::INTERVAL,
            "interval",
            FieldFormat::Binary,
        );
        assert!(res.is_err(), "unsupported binary type must fail closed");
    }

    #[test]
    fn text_path_still_renders_strings() {
        // The text branch is unchanged: int4 in text is the ASCII digits.
        assert_eq!(
            encode_one(&json!(42), Type::INT4, "integer", FieldFormat::Text)
                .unwrap()
                .unwrap(),
            b"42".to_vec()
        );
    }

    #[test]
    fn text_path_null_is_negative_one_length() {
        let out = encode_one(&Value::Null, Type::VARCHAR, "varchar", FieldFormat::Text).unwrap();
        assert!(out.is_none());
    }
}
