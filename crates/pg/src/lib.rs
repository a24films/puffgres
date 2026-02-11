pub mod connect;
pub mod error;
pub mod publication;
pub mod slot;

pub use error::PgError;
pub use tokio_postgres::types::PgLsn;

pub type Result<T> = std::result::Result<T, PgError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_type_alias() {
        let success: Result<i32> = Ok(42);
        assert_eq!(success.unwrap(), 42);

        let failure: Result<i32> = Err(PgError::ConnectionError("test".to_string()));
        assert!(failure.is_err());
    }
}
