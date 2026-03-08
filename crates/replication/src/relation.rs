use std::collections::HashMap;

/// Replica identity setting for a relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    /// Primary key columns in old tuple for Update/Delete.
    Default,
    /// No old tuple data.
    Nothing,
    /// Entire old row in old tuple for Update/Delete.
    Full,
    /// Columns from a named index in old tuple.
    Index,
}

impl ReplicaIdentity {
    pub fn from_pgoutput_byte(byte: u8) -> Self {
        match byte {
            b'd' => Self::Default,
            b'n' => Self::Nothing,
            b'f' => Self::Full,
            b'i' => Self::Index,
            _ => Self::Default,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub part_of_key: bool,
    pub name: String,
    /// PostgreSQL type OID. These are set by Postgres, e.g. 23 for int4, 25 for text, 2950 for uuid.
    pub type_oid: u32,
    /// Type modifier (e.g., varchar length). -1 if unset.
    pub type_modifier: i32,
}

#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<ColumnInfo>,
}

// Postgres doesn't send table metadata (schema name, table name, column names/types) with every
// update. Instead, it keys relations on a relation_id that is unique per table.
// It guarantees it will send a Relation message (containing all table metadata) BEFORE any DML
// referencing that relation, but we need to store it so we can look it up when rows arrive. It
// also re-sends a Relation message whenever the schema changes.
// We can keep this in memory because every time the ReplicationStream reconnects, the cache is
// wiped and Postgres will re-send all Relation messages from scratch.
#[derive(Debug, Default)]
pub struct RelationCache {
    relations: HashMap<u32, RelationInfo>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the given relation already exists in the cache with
    /// different column metadata, indicating a schema change (e.g. ALTER TABLE).
    pub fn schema_changed(&self, relation: &RelationInfo) -> bool {
        self.relations
            .get(&relation.id)
            .is_some_and(|existing| existing.columns != relation.columns)
    }

    /// Returns `true` only if the schema change affects columns in `watched`.
    /// Additive changes (new columns not in the watched set) are silently accepted.
    /// If `watched` is empty, any column change is considered breaking.
    pub fn schema_changed_for_columns(&self, relation: &RelationInfo, watched: &[String]) -> bool {
        let Some(existing) = self.relations.get(&relation.id) else {
            return false;
        };
        if existing.columns == relation.columns {
            return false;
        }
        if watched.is_empty() {
            // No explicit column list → all columns are watched
            return true;
        }
        // Build a map of column name → (type_oid, type_modifier, part_of_key) for each
        let old: HashMap<&str, (&ColumnInfo,)> = existing
            .columns
            .iter()
            .map(|c| (c.name.as_str(), (c,)))
            .collect();
        let new: HashMap<&str, (&ColumnInfo,)> = relation
            .columns
            .iter()
            .map(|c| (c.name.as_str(), (c,)))
            .collect();

        for col_name in watched {
            let old_col = old.get(col_name.as_str());
            let new_col = new.get(col_name.as_str());
            match (old_col, new_col) {
                (Some((o,)), Some((n,))) if o != n => return true, // type/modifier changed
                (Some(_), None) => return true,                    // column dropped
                (None, Some(_)) => {} // column added (was missing before)
                _ => {}
            }
        }
        false
    }

    pub fn insert(&mut self, relation: RelationInfo) {
        self.relations.insert(relation.id, relation);
    }

    pub fn get(&self, relation_id: u32) -> Option<&RelationInfo> {
        self.relations.get(&relation_id)
    }

    pub fn clear(&mut self) {
        self.relations.clear();
    }

    pub fn len(&self) -> usize {
        self.relations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &RelationInfo> {
        self.relations.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_identity_from_byte() {
        assert_eq!(
            ReplicaIdentity::from_pgoutput_byte(b'd'),
            ReplicaIdentity::Default
        );
        assert_eq!(
            ReplicaIdentity::from_pgoutput_byte(b'n'),
            ReplicaIdentity::Nothing
        );
        assert_eq!(
            ReplicaIdentity::from_pgoutput_byte(b'f'),
            ReplicaIdentity::Full
        );
        assert_eq!(
            ReplicaIdentity::from_pgoutput_byte(b'i'),
            ReplicaIdentity::Index
        );
        assert_eq!(
            ReplicaIdentity::from_pgoutput_byte(b'?'),
            ReplicaIdentity::Default
        );
    }

    #[test]
    fn cache_insert_and_get() {
        let mut cache = RelationCache::new();
        assert!(cache.is_empty());

        cache.insert(RelationInfo {
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
        });

        assert_eq!(cache.len(), 1);
        let cached = cache.get(16384).unwrap();
        assert_eq!(cached.name, "users");
        assert!(cached.columns[0].part_of_key);
    }

    #[test]
    fn schema_changed_detects_column_diff() {
        let mut cache = RelationCache::new();

        let original = RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnInfo {
                part_of_key: true,
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
        };
        cache.insert(original);

        // Same columns → no change
        let same = RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnInfo {
                part_of_key: true,
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
        };
        assert!(!cache.schema_changed(&same));

        // Added column → change detected
        let added_column = RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                ColumnInfo {
                    part_of_key: true,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnInfo {
                    part_of_key: false,
                    name: "email".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        assert!(cache.schema_changed(&added_column));

        // New relation (not in cache) → no change
        let new_relation = RelationInfo {
            id: 99,
            namespace: "public".to_string(),
            name: "orders".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        };
        assert!(!cache.schema_changed(&new_relation));
    }

    #[test]
    fn cache_overwrite_on_schema_change() {
        let mut cache = RelationCache::new();

        cache.insert(RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "old_name".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        });
        cache.insert(RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "new_name".to_string(),
            replica_identity: ReplicaIdentity::Full,
            columns: vec![],
        });

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(1).unwrap().name, "new_name");
    }

    #[test]
    fn cache_clear_on_reconnect() {
        let mut cache = RelationCache::new();
        cache.insert(RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "t".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        });

        cache.clear();
        assert!(cache.is_empty());
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn schema_changed_for_columns_additive_is_safe() {
        let mut cache = RelationCache::new();
        let col_id = ColumnInfo {
            part_of_key: true,
            name: "id".to_string(),
            type_oid: 23,
            type_modifier: -1,
        };
        cache.insert(RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![col_id.clone()],
        });

        // Adding a column NOT in the watched set → not breaking
        let with_email = RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                col_id.clone(),
                ColumnInfo {
                    part_of_key: false,
                    name: "email".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        let watched = vec!["id".to_string()];
        assert!(!cache.schema_changed_for_columns(&with_email, &watched));
    }

    #[test]
    fn schema_changed_for_columns_watched_column_dropped() {
        let mut cache = RelationCache::new();
        cache.insert(RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                ColumnInfo {
                    part_of_key: true,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnInfo {
                    part_of_key: false,
                    name: "email".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        });

        // Dropping a watched column → breaking
        let without_email = RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnInfo {
                part_of_key: true,
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
        };
        let watched = vec!["id".to_string(), "email".to_string()];
        assert!(cache.schema_changed_for_columns(&without_email, &watched));
    }

    #[test]
    fn schema_changed_for_columns_type_change_is_breaking() {
        let mut cache = RelationCache::new();
        cache.insert(RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnInfo {
                part_of_key: true,
                name: "id".to_string(),
                type_oid: 23, // int4
                type_modifier: -1,
            }],
        });

        // Changing type of a watched column → breaking
        let type_changed = RelationInfo {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnInfo {
                part_of_key: true,
                name: "id".to_string(),
                type_oid: 20, // int8
                type_modifier: -1,
            }],
        };
        let watched = vec!["id".to_string()];
        assert!(cache.schema_changed_for_columns(&type_changed, &watched));
    }
}
