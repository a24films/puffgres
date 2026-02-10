pub mod error;
pub mod event;

pub use error::ReplicationError;
pub use event::{ColumnValue, Operation, RowEvent, TupleData};

pub type Result<T> = std::result::Result<T, ReplicationError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_type_alias() {
        let success: Result<i32> = Ok(42);
        assert_eq!(success.unwrap(), 42);

        let failure: Result<i32> = Err(ReplicationError::Decoder("test".to_string()));
        assert!(failure.is_err());
    }
}
