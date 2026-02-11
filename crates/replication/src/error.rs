use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("decoder error: {0}")]
    Decoder(String),

    #[error("relation not found: OID {0}")]
    RelationNotFound(u32),

    #[error("stream error: {0}")]
    Stream(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoder_error_display() {
        let err = ReplicationError::Decoder("bad data".to_string());
        assert_eq!(err.to_string(), "decoder error: bad data");
    }

    #[test]
    fn relation_not_found_display() {
        let err = ReplicationError::RelationNotFound(12345);
        assert_eq!(err.to_string(), "relation not found: OID 12345");
    }
}
