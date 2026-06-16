// SPDX-FileCopyrightText: 2026 Stackable GmbH
// SPDX-License-Identifier: OSL-3.0
use std::sync::Arc;

use async_stream::stream;
use futures::Stream;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use serde_json::Value;
use trino_rust_client::models::{Column, QueryResultData};
use trino_rust_client::{Client, Row};

use crate::session::ActiveQueryId;
use crate::types::{encode_cell, trino_type_to_pg};

#[derive(Clone)]
pub(crate) struct TrinoColumn {
    pub(crate) name: String,
    pub(crate) trino_type: String,
}

impl From<&Column> for TrinoColumn {
    fn from(col: &Column) -> Self {
        TrinoColumn {
            name: col.name.clone(),
            trino_type: col.ty.clone(),
        }
    }
}

/// Build the PG `RowDescription` schema for a Trino result set.
///
/// `result_format` is the per-column format the client bound for results;
/// `None` means all-text (the simple-query protocol, which never negotiates
/// binary). Each column's `FieldFormat` is taken from that request, so the
/// schema drives both the advertised RowDescription and the per-cell encoding
/// in `encode_row` — keeping them in lock-step.
pub(crate) fn build_pg_schema(
    columns: &[TrinoColumn],
    result_format: Option<&Format>,
) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let format = result_format.map_or(FieldFormat::Text, |f| f.format_for(idx));
                FieldInfo::new(
                    col.name.clone(),
                    None,
                    None,
                    trino_type_to_pg(&col.trino_type),
                    format,
                )
            })
            .collect(),
    )
}

pub(crate) fn encode_row(
    values: &[Value],
    columns: &[TrinoColumn],
    schema: &Arc<Vec<FieldInfo>>,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (idx, (value, col)) in values.iter().zip(columns.iter()).enumerate() {
        let field = &schema[idx];
        encode_cell(
            &mut encoder,
            value,
            field.datatype(),
            &col.trino_type,
            field.format(),
        )?;
    }
    Ok(encoder.take_row())
}

/// `None` clears the slot, matching the "idle connection: cancel is a
/// no-op" contract enforced by `cancel::GatewayCancelHandler`.
fn set_active_query_id(slot: Option<&ActiveQueryId>, id: Option<String>) {
    let Some(slot) = slot else { return };
    match slot.lock() {
        Ok(mut g) => *g = id,
        // Poison means a previous holder panicked; the data we're about
        // to overwrite is irrelevant either way.
        Err(p) => *p.into_inner() = id,
    }
}

fn extract_direct_rows(data: Option<QueryResultData<Row>>) -> Vec<Vec<Value>> {
    match data {
        Some(QueryResultData::Direct(rows)) => {
            rows.into_iter().map(|row| row.into_json()).collect()
        }
        _ => Vec::new(),
    }
}

/// Submit a query to Trino and return `(schema, row_stream)`.
///
/// # Streaming behaviour
///
/// The function does **not** buffer the full result set before yielding.
/// Trino's REST API returns results as a chain of pages: the initial POST
/// reply contains the first page (sometimes empty) and a `nextUri`; each
/// GET on `nextUri` returns the next page and another `nextUri`, until
/// `nextUri` is absent. Rows are decoded and yielded one at a time:
///
/// 1. The initial POST runs synchronously inside this function so the
///    schema (`columns`) and any first-page rows are available before we
///    return. Trino sometimes returns `columns: null` on the first reply
///    and only exposes them on a subsequent page; we poll forward on
///    behalf of the caller until columns appear.
/// 2. Once we have a schema, we return immediately. The caller (pgwire)
///    starts pulling `DataRow`s from the returned `Stream`.
/// 3. Each pull yields one decoded row from the current page. When the
///    current page is exhausted, the stream awaits the next GET and
///    continues. Memory usage is bounded by one page at a time, regardless
///    of total result-set size.
///
/// Pgwire writes each yielded `DataRow` to the client socket as it
/// arrives, so a slow client back-pressures the Trino polling loop
/// naturally — we don't queue rows in memory waiting for a recipient.
///
/// # Cancellation
///
/// If `active_query_id` is `Some`, the Trino query id from the initial
/// response is recorded into that slot before we return, so a concurrent
/// `CancelRequest` (handled in `cancel.rs`) can call
/// `trino_client.cancel(id)` against the running query. The slot is
/// cleared on stream end / drop / error via the `ClearOnDrop` guard
/// inside the stream closure.
///
/// # Error paths
///
/// User-visible errors (Trino syntax, missing table, etc.) become
/// `PgWireError::UserError` so the simple/extended-query handler turns
/// them into a `Response::Error` to the client. Connection-level errors
/// during page polling are yielded *into* the stream as the next item,
/// matching pgwire's expectation that an in-flight stream surfaces its
/// own failures rather than panicking.
pub async fn execute_trino_query(
    client: &Arc<Client>,
    sql: String,
    active_query_id: Option<&ActiveQueryId>,
    result_format: Option<&Format>,
) -> Result<
    (
        Arc<Vec<FieldInfo>>,
        impl Stream<Item = PgWireResult<DataRow>> + use<>,
    ),
    PgWireError,
> {
    tracing::trace!(trino_sql = %sql, "Trino: submitting query");
    let result = client.get::<Row>(sql).await.map_err(|e| {
        let info = crate::error_mapping::trino_error_to_pg(&e.to_string());
        PgWireError::UserError(Box::new(info))
    })?;

    // Record the query id BEFORE the error check so a query that fails
    // immediately is still cancellable while being torn down.
    // trino_client.cancel() on a finished query is a harmless no-op.
    set_active_query_id(active_query_id, Some(result.id.clone()));

    if let Some(error) = &result.error {
        set_active_query_id(active_query_id, None);
        let info = crate::error_mapping::trino_error_to_pg(&error.message);
        return Err(PgWireError::UserError(Box::new(info)));
    }

    // Trino often returns `columns: null` in the initial response and only
    // exposes them on the second page; poll nextUri until columns appear.
    let mut trino_columns: Vec<TrinoColumn> = result
        .columns
        .as_ref()
        .map(|cols| cols.iter().map(TrinoColumn::from).collect())
        .unwrap_or_default();

    let mut initial_rows = extract_direct_rows(result.data);
    let mut next_uri = result.next_uri;

    while trino_columns.is_empty() {
        match next_uri.take() {
            Some(url) => {
                let next_result = match client.get_next::<Row>(&url).await {
                    Ok(r) => r,
                    Err(e) => {
                        set_active_query_id(active_query_id, None);
                        let info = crate::error_mapping::trino_error_to_pg(&e.to_string());
                        return Err(PgWireError::UserError(Box::new(info)));
                    }
                };

                if let Some(error) = &next_result.error {
                    set_active_query_id(active_query_id, None);
                    let info = crate::error_mapping::trino_error_to_pg(&error.message);
                    return Err(PgWireError::UserError(Box::new(info)));
                }

                if let Some(cols) = &next_result.columns {
                    trino_columns = cols.iter().map(TrinoColumn::from).collect();
                }

                let mut rows = extract_direct_rows(next_result.data);
                initial_rows.append(&mut rows);
                next_uri = next_result.next_uri;
            }
            None => break,
        }
    }

    tracing::trace!(
        columns = trino_columns.len(),
        initial_rows = initial_rows.len(),
        more_pending = next_uri.is_some(),
        "Trino: initial response received"
    );

    // Empty schema means DDL/DML; the caller returns Response::Execution
    // instead of Response::Query.
    let schema = build_pg_schema(&trino_columns, result_format);

    let stream_client = Arc::clone(client);
    let stream_columns = trino_columns.clone();
    let stream_schema = schema.clone();
    // Clone the slot Arc into the stream closure so we can clear it when
    // the stream ends naturally, errors, or is dropped.
    let stream_active_query_id: Option<ActiveQueryId> = active_query_id.cloned();

    let row_stream = stream! {
        // Drop guard clears the slot whether the stream finishes normally,
        // errors out via `break`, or is dropped by the caller (e.g. when
        // the connection closes mid-stream). After clearing, a subsequent
        // CancelRequest on an idle connection sees no active query id and
        // correctly returns the "idle, ignored" path.
        struct ClearOnDrop(Option<ActiveQueryId>);
        impl Drop for ClearOnDrop {
            fn drop(&mut self) {
                set_active_query_id(self.0.as_ref(), None);
            }
        }
        let _clear_on_drop = ClearOnDrop(stream_active_query_id);

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
                        let info = crate::error_mapping::trino_error_to_pg(&error.message);
                        yield Err(PgWireError::UserError(Box::new(info)));
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
                    let info = crate::error_mapping::trino_error_to_pg(&e.to_string());
                    yield Err(PgWireError::UserError(Box::new(info)));
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
    fn set_active_query_id_writes_and_clears() {
        let slot: ActiveQueryId = Arc::new(std::sync::Mutex::new(None));
        set_active_query_id(Some(&slot), Some("q-1".to_owned()));
        assert_eq!(slot.lock().unwrap().as_deref(), Some("q-1"));
        set_active_query_id(Some(&slot), None);
        assert!(slot.lock().unwrap().is_none());
    }

    #[test]
    fn set_active_query_id_with_no_slot_is_a_noop() {
        // Just must not panic.
        set_active_query_id(None, Some("ignored".to_owned()));
        set_active_query_id(None, None);
    }

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
        let schema = build_pg_schema(&columns, None);
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
        let schema = build_pg_schema(&columns, None);
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
        let schema = build_pg_schema(&columns, None);
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
