use crate::{CoreError, DocumentId};
use config::IdType;
use replication::{ColumnValue, Operation, RowEvent, TupleData};

/// Turn a row's column names and text values into a `(RowEvent, DocumentId)`.
/// Each value becomes a `ColumnValue::Text` (or `Null`), and the `id_column`
/// is extracted and parsed into a `DocumentId`. The event is always an Insert
/// with `relation_id = 0` since these come from queries, not replication.
pub fn values_to_event(
    column_names: &[String],
    values: &[Option<String>],
    id_column: &str,
    id_type: &IdType,
) -> Result<(RowEvent, DocumentId), CoreError> {
    let mut col_values = Vec::with_capacity(values.len());
    let mut doc_id = None;

    for (i, value) in values.iter().enumerate() {
        let col_name = column_names.get(i).map(|s| s.as_str()).unwrap_or("");
        let col_value = match value {
            Some(v) => {
                if col_name == id_column {
                    doc_id = Some(DocumentId::from_text(v, id_type)?);
                }
                ColumnValue::Text(bytes::Bytes::from(v.clone()))
            }
            None => {
                if col_name == id_column {
                    return Err(CoreError::Pipeline(format!(
                        "NULL id column '{}'",
                        id_column
                    )));
                }
                ColumnValue::Null
            }
        };
        col_values.push(col_value);
    }

    let doc_id = doc_id.ok_or_else(|| {
        CoreError::Pipeline(format!("id column '{}' not found in row", id_column))
    })?;

    Ok((
        RowEvent {
            relation_id: 0,
            operation: Operation::Insert,
            new_tuple: Some(TupleData {
                columns: col_values,
            }),
            old_tuple: None,
        },
        doc_id,
    ))
}

/// Convert `tokio_postgres::Row`s into `(RowEvent, DocumentId)` pairs by
/// reading all columns as text and passing them through `values_to_event`.
pub fn pg_rows_to_events(
    rows: &[tokio_postgres::Row],
    id_column: &str,
    id_type: &IdType,
) -> Result<Vec<(RowEvent, DocumentId)>, CoreError> {
    let mut events = Vec::with_capacity(rows.len());

    for row in rows {
        let columns = row.columns();
        let col_names: Vec<String> = columns.iter().map(|c| c.name().to_owned()).collect();
        let values: Vec<Option<String>> = (0..columns.len()).map(|i| row.get(i)).collect();
        events.push(values_to_event(&col_names, &values, id_column, id_type)?);
    }

    Ok(events)
}
