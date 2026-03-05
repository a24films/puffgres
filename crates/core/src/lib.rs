pub mod action;
pub mod backfill;
pub mod backoff;
pub mod error;
pub mod id;
pub mod js_transform;
pub mod mapping;
pub mod router;
pub mod row_convert;
pub mod transform;

pub use action::Action;
pub use backfill::{BackfillConfig, BackfillOutcome, BackfillResult, BackfillSink, run_backfill};
pub use backoff::{Backoff, BackoffConfig};
pub use error::CoreError;
pub use id::DocumentId;
pub use js_transform::JsTransformer;
pub use mapping::Mapping;
pub use replication::{ColumnValue, Operation, RelationCache, RelationInfo, RowEvent, TupleData};
pub use router::Router;
pub use row_convert::{pg_rows_to_events, values_to_event};
pub use transform::Transformer;

pub type Result<T> = std::result::Result<T, CoreError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::unnecessary_literal_unwrap)]
    fn result_type_alias() {
        let success: Result<i32> = Ok(42);
        assert_eq!(success.unwrap(), 42);

        let failure: Result<i32> = Err(CoreError::Pipeline("test".to_string()));
        assert!(failure.is_err());
    }

    #[test]
    fn reexports_available() {
        // Verify replication types are accessible through core
        let op = Operation::Insert;
        assert_eq!(op, Operation::Insert);

        let val = ColumnValue::Null;
        assert!(val.is_null());
    }
}
