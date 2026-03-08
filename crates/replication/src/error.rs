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

impl ReplicationError {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            ReplicationError::Connection(_) | ReplicationError::Stream(_)
        )
    }
}

/// Signal returned from `recv_batch` when the replication stream detects that
/// a tracked relation's schema has changed. This is not an error — it signals
/// the caller to tear down and reconnect with fresh schema metadata.
#[derive(Debug, Clone)]
pub struct SchemaChanged {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
}

impl std::fmt::Display for SchemaChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "schema changed for relation {}.{} (OID {})",
            self.namespace, self.name, self.relation_id
        )
    }
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

    #[test]
    fn connection_is_transient() {
        assert!(ReplicationError::Connection("timeout".into()).is_transient());
        assert!(ReplicationError::Stream("reset".into()).is_transient());
    }

    #[test]
    fn decoder_is_permanent() {
        assert!(!ReplicationError::Decoder("bad".into()).is_transient());
        assert!(!ReplicationError::RelationNotFound(1).is_transient());
    }

    #[test]
    fn schema_changed_display() {
        let sc = SchemaChanged {
            relation_id: 42,
            namespace: "public".into(),
            name: "users".into(),
        };
        assert_eq!(
            sc.to_string(),
            "schema changed for relation public.users (OID 42)"
        );
    }
}
