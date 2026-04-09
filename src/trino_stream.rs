use std::sync::Arc;

use async_stream::stream;
use futures::Stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use serde_json::Value;
use trino_rust_client::models::{QueryResultData, Column};
use trino_rust_client::{Client, Row};

use crate::types::{encode_value, trino_type_to_pg};

/// Column metadata from Trino, used for encoding.
#[derive(Clone)]
pub struct TrinoColumn {
    pub name: String,
    pub trino_type: String,
}

impl From<&Column> for TrinoColumn {
    fn from(col: &Column) -> Self {
        TrinoColumn {
            name: col.name.clone(),
            trino_type: col.ty.clone(),
        }
    }
}

/// Build pgwire FieldInfo schema from Trino columns.
pub fn build_pg_schema(columns: &[TrinoColumn]) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        columns
            .iter()
            .map(|col| {
                FieldInfo::new(
                    col.name.clone(),
                    None,
                    None,
                    trino_type_to_pg(&col.trino_type),
                    FieldFormat::Text,
                )
            })
            .collect(),
    )
}

/// Encode one Trino row (slice of serde_json::Value) into a PG DataRow.
pub fn encode_row(
    values: &[Value],
    columns: &[TrinoColumn],
    schema: &Arc<Vec<FieldInfo>>,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (value, col) in values.iter().zip(columns.iter()) {
        encoder.encode_field(&encode_value(value, &col.trino_type))?;
    }
    Ok(encoder.take_row())
}

/// Extract rows from a QueryResultData as Vec<Vec<Value>>.
fn extract_direct_rows(data: Option<QueryResultData<Row>>) -> Vec<Vec<Value>> {
    match data {
        Some(QueryResultData::Direct(rows)) => {
            rows.into_iter().map(|row| row.into_json()).collect()
        }
        _ => Vec::new(),
    }
}

/// Submit a query to Trino and return (schema, row_stream).
///
/// The stream polls nextUri until done, yielding DataRow items suitable for
/// pgwire's QueryResponse.
pub async fn execute_trino_query(
    client: &Arc<Client>,
    sql: String,
) -> Result<(Arc<Vec<FieldInfo>>, impl Stream<Item = PgWireResult<DataRow>>), PgWireError> {
    // 1. Submit query
    let result = client
        .get::<Row>(sql)
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    // 2. Check for immediate error
    if let Some(error) = &result.error {
        return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Trino query error: {}", error.message),
        ))));
    }

    // 3. Extract column metadata
    let trino_columns: Vec<TrinoColumn> = result
        .columns
        .as_ref()
        .map(|cols| cols.iter().map(TrinoColumn::from).collect())
        .unwrap_or_default();

    if trino_columns.is_empty() {
        return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Trino query returned no column metadata",
        ))));
    }

    // 4. Build PG schema
    let schema = build_pg_schema(&trino_columns);

    // 5. Extract initial rows
    let initial_rows = extract_direct_rows(result.data);
    let mut next_uri = result.next_uri;

    // 6. Create streaming bridge
    let stream_client = Arc::clone(client);
    let stream_columns = trino_columns.clone();
    let stream_schema = schema.clone();

    let row_stream = stream! {
        // Yield initial rows
        for row_values in initial_rows {
            yield encode_row(&row_values, &stream_columns, &stream_schema);
        }

        // Poll nextUri for more data
        while let Some(url) = next_uri.take() {
            let chunk = stream_client
                .get_next::<Row>(&url)
                .await;

            match chunk {
                Ok(result) => {
                    // Check for Trino-side error
                    if let Some(error) = &result.error {
                        yield Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Trino query error: {}", error.message),
                        ))));
                        break;
                    }

                    // Extract and yield rows from this chunk
                    let rows = extract_direct_rows(result.data);
                    for row_values in rows {
                        yield encode_row(&row_values, &stream_columns, &stream_schema);
                    }

                    // Continue to next chunk or finish
                    next_uri = result.next_uri;
                }
                Err(e) => {
                    yield Err(PgWireError::ApiError(Box::new(e)));
                    break;
                }
            }
        }
    };

    Ok((schema, row_stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::api::Type;
    use serde_json::json;

    #[test]
    fn build_pg_schema_maps_columns() {
        let columns = vec![
            TrinoColumn {
                name: "id".to_owned(),
                trino_type: "integer".to_owned(),
            },
            TrinoColumn {
                name: "name".to_owned(),
                trino_type: "varchar".to_owned(),
            },
        ];
        let schema = build_pg_schema(&columns);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name(), "id");
        assert_eq!(*schema[0].datatype(), Type::INT4);
        assert_eq!(schema[1].name(), "name");
        assert_eq!(*schema[1].datatype(), Type::VARCHAR);
    }

    #[test]
    fn encode_row_with_values() {
        let columns = vec![
            TrinoColumn {
                name: "id".to_owned(),
                trino_type: "integer".to_owned(),
            },
            TrinoColumn {
                name: "name".to_owned(),
                trino_type: "varchar".to_owned(),
            },
        ];
        let schema = build_pg_schema(&columns);
        let values = vec![json!(42), json!("alice")];

        let row = encode_row(&values, &columns, &schema);
        assert!(row.is_ok());
    }

    #[test]
    fn encode_row_with_null() {
        let columns = vec![TrinoColumn {
            name: "val".to_owned(),
            trino_type: "varchar".to_owned(),
        }];
        let schema = build_pg_schema(&columns);
        let values = vec![Value::Null];

        let row = encode_row(&values, &columns, &schema);
        assert!(row.is_ok());
    }

    #[test]
    fn trino_column_from_model_column() {
        let model_col = Column {
            name: "age".to_owned(),
            ty: "bigint".to_owned(),
            type_signature: None,
        };
        let tc = TrinoColumn::from(&model_col);
        assert_eq!(tc.name, "age");
        assert_eq!(tc.trino_type, "bigint");
    }
}
