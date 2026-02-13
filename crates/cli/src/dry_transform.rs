use config::Config;
use puffgres_core::{Action, JsTransformer, Transformer, values_to_event};

use crate::paths::ProjectPaths;

/// Run the transform on a sample row and validate the output.
pub async fn dry_run_transform(
    paths: &ProjectPaths,
    config: &Config,
    column_names: &[String],
    values: &[Option<String>],
) -> Result<Vec<puffgres_core::Action>, String> {
    let transform_path = paths.root.join(&config.transform.path);
    let transformer = JsTransformer::new(transform_path, config.id.id_type.clone());

    let (event, doc_id) =
        values_to_event(column_names, values, &config.id.column, &config.id.id_type)
            .map_err(|e| format!("failed to build event from sample row: {e}"))?;

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

    Ok(actions)
}
