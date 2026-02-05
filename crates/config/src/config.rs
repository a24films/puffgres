use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ConfigError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub name: String,
    pub version: u64,
    pub namespace: String,
    pub source: SourceConfig,
    pub id: IdConfig,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    pub transform: TransformConfig,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    pub path: String,
}

impl Config {
    /// Returns the full namespace with version suffix (e.g., "film_v2")
    pub fn full_namespace(&self) -> String {
        format!("{}_v{}", self.namespace, self.version)
    }

    /// Compute content hash for immutability checking
    pub fn content_hash(&self) -> Result<String, ConfigError> {
        let serialized = toml::to_string(self)?;
        let hash = Sha256::digest(serialized.as_bytes());
        Ok(format!("{:x}", hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let toml_str = r#"
name = "user_0001"
version = 1
namespace = "user"

[source]
schema = "public"
table = "user"

[id]
column = "id"
type = "uint"

[transform]
path = "transforms/user.ts"
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.name, "user_0001");
        assert_eq!(config.version, 1);
        assert_eq!(config.namespace, "user");
        assert_eq!(config.source.schema, "public");
        assert_eq!(config.source.table, "user");
        assert_eq!(config.id.column, "id");
        assert_eq!(config.id.id_type, IdType::Uint);
        assert!(config.columns.is_none());
        assert_eq!(config.transform.path, "transforms/user.ts");
    }

    #[test]
    fn parse_full_config() {
        let toml_str = r#"
name = "film_0002"
version = 2
namespace = "film"
columns = ["id", "title", "director", "year"]

[source]
schema = "public"
table = "films"

[id]
column = "id"
type = "uuid"

[transform]
path = "transforms/film.ts"
"#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.name, "film_0002");
        assert_eq!(config.version, 2);
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

        assert_eq!(config.transform.path, "transforms/film.ts");
    }

    #[test]
    fn full_namespace_format_correct() {
        let config = Config {
            name: "film_0002".to_string(),
            version: 2,
            namespace: "film".to_string(),
            source: SourceConfig {
                schema: "public".to_string(),
                table: "films".to_string(),
            },
            id: IdConfig {
                column: "id".to_string(),
                id_type: IdType::Uint,
            },
            columns: None,
            transform: TransformConfig {
                path: "transforms/film.ts".to_string(),
            },
        };

        assert_eq!(config.full_namespace(), "film_v2");
    }

    #[test]
    fn content_hash_consistent() {
        let config1 = Config {
            name: "film_0001".to_string(),
            version: 1,
            namespace: "film".to_string(),
            source: SourceConfig {
                schema: "public".to_string(),
                table: "films".to_string(),
            },
            id: IdConfig {
                column: "id".to_string(),
                id_type: IdType::Uint,
            },
            columns: None,
            transform: TransformConfig {
                path: "transforms/film.ts".to_string(),
            },
        };

        let config2 = config1.clone();

        let hash1 = config1.content_hash().unwrap();
        let hash2 = config2.content_hash().unwrap();

        assert_eq!(hash1, hash2);
        assert!(!hash1.is_empty());
        assert_eq!(hash1.len(), 64); // SHA-256 produces 64 hex characters
    }

    #[test]
    fn content_hash_changes_with_content() {
        let config1 = Config {
            name: "film_0001".to_string(),
            version: 1,
            namespace: "film".to_string(),
            source: SourceConfig {
                schema: "public".to_string(),
                table: "films".to_string(),
            },
            id: IdConfig {
                column: "id".to_string(),
                id_type: IdType::Uint,
            },
            columns: None,
            transform: TransformConfig {
                path: "transforms/film.ts".to_string(),
            },
        };

        let mut config2 = config1.clone();
        config2.version = 2;

        let hash1 = config1.content_hash().unwrap();
        let hash2 = config2.content_hash().unwrap();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn parse_all_id_types() {
        let test_cases = vec![
            ("uint", IdType::Uint),
            ("int", IdType::Int),
            ("uuid", IdType::Uuid),
            ("string", IdType::String),
        ];

        for (type_str, expected) in test_cases {
            let toml_str = format!(
                r#"
name = "test_0001"
version = 1
namespace = "test"

[source]
schema = "public"
table = "test"

[id]
column = "id"
type = "{}"

[transform]
path = "transforms/test.ts"
"#,
                type_str
            );

            let config: Config = toml::from_str(&toml_str).unwrap();
            assert_eq!(config.id.id_type, expected);
        }
    }
}
