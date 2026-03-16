use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub name: String,
    pub namespace: String,
    pub source: SourceConfig,
    pub id: IdConfig,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub schema: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdConfig {
    pub column: String,
    #[serde(rename = "type")]
    pub id_type: IdType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum IdType {
    Uint,
    Int,
    Uuid,
    String,
}

impl Config {
    /// Compute content hash from raw file bytes.
    ///
    /// Hashing the original bytes (rather than re-serializing) ensures the hash
    /// is stable across `toml` crate versions. Callers should pass the bytes
    /// from `fs::read(config_path)`.
    pub fn content_hash_from_bytes(raw: &[u8]) -> String {
        format!("{:x}", Sha256::digest(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_fixture(name: &str) -> Config {
        let path = format!("tests/fixtures/{}.toml", name);
        toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    #[test]
    fn parse_minimal_config() {
        let config = load_fixture("valid");
        assert_eq!(config.name, "users");
        assert_eq!(config.namespace, "users");
        assert_eq!(config.source.schema, "public");
        assert_eq!(config.source.table, "users");
        assert_eq!(config.id.column, "id");
        assert_eq!(config.id.id_type, IdType::Uint);
        assert!(config.columns.is_none());
    }

    #[test]
    fn parse_full_config() {
        let config = load_fixture("full");
        assert_eq!(config.name, "film");
        assert_eq!(config.namespace, "film");
        assert_eq!(config.source.schema, "public");
        assert_eq!(config.source.table, "films");
        assert_eq!(config.id.column, "id");
        assert_eq!(config.id.id_type, IdType::Uuid);

        let columns = config.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[0], "id");
        assert_eq!(columns[1], "title");
        assert_eq!(columns[2], "director");
        assert_eq!(columns[3], "year");
    }

    #[test]
    fn content_hash_from_bytes_consistent() {
        let bytes = std::fs::read("tests/fixtures/valid.toml").unwrap();
        let hash1 = Config::content_hash_from_bytes(&bytes);
        let hash2 = Config::content_hash_from_bytes(&bytes);

        assert_eq!(hash1, hash2);
        assert!(!hash1.is_empty());
        assert_eq!(hash1.len(), 64); // SHA-256 produces 64 hex characters
    }

    #[test]
    fn content_hash_changes_with_content() {
        let bytes1 = std::fs::read("tests/fixtures/valid.toml").unwrap();
        let bytes2 = std::fs::read("tests/fixtures/full.toml").unwrap();

        let hash1 = Config::content_hash_from_bytes(&bytes1);
        let hash2 = Config::content_hash_from_bytes(&bytes2);

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn parse_all_id_types() {
        let cases = vec![
            ("id_uint", IdType::Uint),
            ("id_int", IdType::Int),
            ("id_uuid", IdType::Uuid),
            ("id_string", IdType::String),
        ];

        for (fixture_name, expected) in cases {
            let config = load_fixture(fixture_name);
            assert_eq!(
                config.id.id_type, expected,
                "failed for fixture {fixture_name}"
            );
        }
    }
}
