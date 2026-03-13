#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use replication::{ColumnInfo, RelationCache, RelationInfo, ReplicaIdentity};

#[derive(Debug, Arbitrary)]
struct FuzzRelation {
    id: u32,
    namespace: String,
    name: String,
    columns: Vec<FuzzColumn>,
}

#[derive(Debug, Arbitrary)]
struct FuzzColumn {
    part_of_key: bool,
    name: String,
    type_oid: u32,
    type_modifier: i32,
}

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    relations: Vec<FuzzRelation>,
    watched_columns: Vec<String>,
}

impl FuzzRelation {
    fn to_relation_info(&self) -> RelationInfo {
        RelationInfo {
            id: self.id,
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            replica_identity: ReplicaIdentity::Default,
            columns: self
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    part_of_key: c.part_of_key,
                    name: c.name.clone(),
                    type_oid: c.type_oid,
                    type_modifier: c.type_modifier,
                })
                .collect(),
        }
    }
}

fuzz_target!(|input: FuzzInput| {
    let mut cache = RelationCache::new();

    for rel in &input.relations {
        let info = rel.to_relation_info();

        // These should never panic
        let _ = cache.schema_changed(&info);
        let _ = cache.schema_changed_for_columns(&info, &input.watched_columns);
        let _ = cache.get(info.id);
        let _ = cache.len();
        let _ = cache.is_empty();

        cache.insert(info);
    }

    // Iterate after all inserts
    for _ in cache.iter() {}

    cache.clear();
    assert!(cache.is_empty());
});
