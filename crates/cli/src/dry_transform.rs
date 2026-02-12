use bytes::Bytes;
use config::Config;
use puffgres_core::{Action, DocumentId, JsTransformer, Transformer};
use replication::{ColumnValue, Operation, RowEvent, TupleData};

use crate::paths::ProjectPaths;

/// Run the transform on a sample row and validate the output.
pub async fn dry_run_transform(
    paths: &ProjectPaths,
    config: &Config,
    column_names: &[String],
    values: &[Option<String>],
) -> Result<(), String> {
    let transform_path = paths.root.join(&config.transform.path);
    let transformer = JsTransformer::new(transform_path, config.id.id_type.clone());

    // Find the id column index
    let id_col_idx = column_names
        .iter()
        .position(|c| c == &config.id.column)
        .ok_or_else(|| {
            format!(
                "id column '{}' not found in sample row columns",
                config.id.column
            )
        })?;

    // Parse the id value
    let id_text = values[id_col_idx]
        .as_deref()
        .ok_or_else(|| format!("id column '{}' is NULL in sample row", config.id.column))?;

    let doc_id = DocumentId::from_text(id_text, &config.id.id_type).map_err(|e| {
        format!(
            "cannot parse id column '{}' value '{}' as {:?}: {e}",
            config.id.column, id_text, config.id.id_type
        )
    })?;

    // Build a simulated insert event
    let tuple = TupleData {
        columns: values
            .iter()
            .map(|v| match v {
                Some(s) => ColumnValue::Text(Bytes::from(s.clone())),
                None => ColumnValue::Null,
            })
            .collect(),
    };

    let event = RowEvent {
        relation_id: 0,
        operation: Operation::Insert,
        new_tuple: Some(tuple),
        old_tuple: None,
    };

    // Run the transform
    let actions = transformer
        .transform_batch(&[(&event, doc_id)])
        .await
        .map_err(|e| format!("transform dry-run failed: {e}"))?;

    // Validate output
    for action in &actions {
        if let Action::Upsert {
            vector,
            distance_metric,
            ..
        } = action
        {
            if vector.is_some() && distance_metric.is_none() {
                return Err("transform returns a vector but no distance_metric — \
                     turbopuffer requires distance_metric for namespaces with vectors"
                    .to_string());
            }
        }
    }

    Ok(())
}
