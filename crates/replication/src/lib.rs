mod connection;
pub mod decoder;
pub mod error;
pub mod event;
pub mod relation;
pub mod stream;

pub use decoder::{
    BeginInfo, CommitInfo, DeleteInfo, InsertInfo, TruncateInfo, UpdateInfo, WalMessage,
};
pub use error::ReplicationError;
pub use event::{ColumnValue, Operation, RowEvent, TupleData};
pub use relation::{ColumnInfo, RelationCache, RelationInfo, ReplicaIdentity};
pub use stream::{
    ReplicationStream, ReplicationStreamConfig, ReplicationTransport, StreamingBatch,
};

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
