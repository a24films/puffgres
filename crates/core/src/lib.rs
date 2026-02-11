pub mod action;
pub mod error;
pub mod id;

pub use action::Action;
pub use error::CoreError;
pub use id::DocumentId;
pub use replication::{ColumnValue, Operation, RowEvent, TupleData};

pub type Result<T> = std::result::Result<T, CoreError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
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
