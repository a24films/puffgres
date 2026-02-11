use replication::RelationCache;

use crate::RowEvent;
use crate::mapping::Mapping;

pub struct Router {
    mappings: Vec<Mapping>,
}

impl Router {
    pub fn new(mappings: Vec<Mapping>) -> Self {
        Self { mappings }
    }

    pub fn route<'a>(&'a self, event: &RowEvent, relations: &RelationCache) -> Vec<&'a Mapping> {
        let Some(relation) = relations.get(event.relation_id) else {
            return vec![];
        };
        self.mappings
            .iter()
            .filter(|m| m.matches(relation))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use config::Config;
    use replication::{
        ColumnInfo, ColumnValue, Operation, RelationInfo, ReplicaIdentity, TupleData,
    };

    fn load_fixture(name: &str) -> Config {
        let path = format!("tests/fixtures/{name}.toml");
        toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn insert_event(relation_id: u32) -> RowEvent {
        RowEvent {
            relation_id,
            operation: Operation::Insert,
            new_tuple: Some(TupleData {
                columns: vec![ColumnValue::Text(Bytes::from_static(b"1"))],
            }),
            old_tuple: None,
        }
    }

    fn users_relation() -> RelationInfo {
        RelationInfo {
            id: 16384,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnInfo {
                part_of_key: true,
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
        }
    }

    #[test]
    fn route_matches_single_mapping() {
        let router = Router::new(vec![Mapping::from_config(&load_fixture("valid"))]);
        let mut cache = RelationCache::new();
        cache.insert(users_relation());

        let matches = router.route(&insert_event(16384), &cache);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].name, "user_0001");
    }

    #[test]
    fn route_no_match_different_table() {
        let router = Router::new(vec![Mapping::from_config(&load_fixture("valid"))]);
        let mut cache = RelationCache::new();
        cache.insert(RelationInfo {
            id: 99999,
            namespace: "public".to_string(),
            name: "orders".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        });

        let matches = router.route(&insert_event(99999), &cache);
        assert!(matches.is_empty());
    }

    #[test]
    fn route_unknown_relation_id() {
        let router = Router::new(vec![Mapping::from_config(&load_fixture("valid"))]);
        let cache = RelationCache::new();

        let matches = router.route(&insert_event(99999), &cache);
        assert!(matches.is_empty());
    }

    #[test]
    fn route_multiple_mappings_same_table() {
        let config = load_fixture("valid");
        let router = Router::new(vec![
            Mapping::from_config(&config),
            Mapping::from_config(&config),
        ]);
        let mut cache = RelationCache::new();
        cache.insert(users_relation());

        let matches = router.route(&insert_event(16384), &cache);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn route_empty_router() {
        let router = Router::new(vec![]);
        let mut cache = RelationCache::new();
        cache.insert(users_relation());

        let matches = router.route(&insert_event(16384), &cache);
        assert!(matches.is_empty());
    }
}
