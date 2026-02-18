use std::collections::HashMap;

use replication::RelationCache;

use crate::mapping::Mapping;
use crate::{DocumentId, RowEvent};

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

    /// Takes a batch of Postgres events, finds which configs each event applies
    /// to, and returns pairs like config_name -> Vec<(event, id)>. Basically,
    /// it shows which configs need to pay attention to which event/id pairings.
    ///
    /// Returns pairs like:
    ///
    /// ```text
    /// {
    ///   "user_0001": [(event1, id1), (event2, id2), (event3, id3)],
    ///   "film_0001": [(event4, id4)],
    /// }
    /// ```
    ///
    /// Events with unknown relations or unparseable IDs are skipped with a warning.
    pub fn route_batch<'a>(
        &'a self,
        events: &'a [RowEvent],
        relations: &RelationCache,
    ) -> HashMap<&'a str, Vec<(&'a RowEvent, DocumentId)>> {
        let mut result: HashMap<&str, Vec<(&RowEvent, DocumentId)>> = HashMap::new();

        for event in events {
            let Some(relation) = relations.get(event.relation_id) else {
                continue;
            };

            for mapping in &self.mappings {
                if !mapping.matches(relation) {
                    continue;
                }

                match mapping.extract_id(event, relation) {
                    Ok(id) => {
                        result
                            .entry(mapping.name.as_str())
                            .or_default()
                            .push((event, id));
                    }
                    Err(e) => {
                        tracing::warn!(config = %mapping.name, error = %e, "failed to extract ID");
                    }
                }
            }
        }

        result
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

    #[test]
    fn route_batch_groups_by_config() {
        let router = Router::new(vec![Mapping::from_config(&load_fixture("valid"))]);
        let mut cache = RelationCache::new();
        cache.insert(users_relation());

        let events = vec![insert_event(16384), insert_event(16384)];
        let grouped = router.route_batch(&events, &cache);

        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped["user_0001"].len(), 2);
    }

    #[test]
    fn route_batch_skips_unmatched_events() {
        let router = Router::new(vec![Mapping::from_config(&load_fixture("valid"))]);
        let mut cache = RelationCache::new();
        cache.insert(users_relation());

        let events = vec![insert_event(16384), insert_event(99999)];
        let grouped = router.route_batch(&events, &cache);

        assert_eq!(grouped["user_0001"].len(), 1);
    }

    #[test]
    fn route_batch_empty() {
        let router = Router::new(vec![Mapping::from_config(&load_fixture("valid"))]);
        let cache = RelationCache::new();

        let grouped = router.route_batch(&[], &cache);
        assert!(grouped.is_empty());
    }

    #[test]
    fn route_batch_skips_null_id() {
        let router = Router::new(vec![Mapping::from_config(&load_fixture("valid"))]);
        let mut cache = RelationCache::new();
        cache.insert(users_relation());

        let events = vec![RowEvent {
            relation_id: 16384,
            operation: Operation::Insert,
            new_tuple: Some(TupleData {
                columns: vec![ColumnValue::Null],
            }),
            old_tuple: None,
        }];
        let grouped = router.route_batch(&events, &cache);
        assert!(grouped.is_empty());
    }

    #[test]
    fn route_batch_extracts_correct_ids() {
        let router = Router::new(vec![Mapping::from_config(&load_fixture("valid"))]);
        let mut cache = RelationCache::new();
        cache.insert(users_relation());

        let events = vec![
            RowEvent {
                relation_id: 16384,
                operation: Operation::Insert,
                new_tuple: Some(TupleData {
                    columns: vec![ColumnValue::Text(Bytes::from_static(b"42"))],
                }),
                old_tuple: None,
            },
            RowEvent {
                relation_id: 16384,
                operation: Operation::Insert,
                new_tuple: Some(TupleData {
                    columns: vec![ColumnValue::Text(Bytes::from_static(b"99"))],
                }),
                old_tuple: None,
            },
        ];
        let grouped = router.route_batch(&events, &cache);
        let ids: Vec<_> = grouped["user_0001"]
            .iter()
            .map(|(_, id)| id.clone())
            .collect();
        assert_eq!(ids, vec![DocumentId::Uint(42), DocumentId::Uint(99)]);
    }
}
