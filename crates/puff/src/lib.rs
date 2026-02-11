pub mod client;
pub mod error;

pub use client::TurbopufferClient;
pub use error::PuffError;

pub type Result<T> = std::result::Result<T, PuffError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_type_alias() {
        let ok: Result<u32> = Ok(42);
        assert!(ok.is_ok());

        let err: Result<u32> = Err(PuffError::Client("test".to_string()));
        assert!(err.is_err());
    }

    #[test]
    fn reexports_accessible() {
        let _client = TurbopufferClient::new("key".to_string(), None).unwrap();
        let _err = PuffError::Client("test".to_string());
    }
}
