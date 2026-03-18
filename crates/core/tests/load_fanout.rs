//! Load test: multi-config fanout and sustained throughput.
//! Run with: cargo test -p puffgres-core --test load_fanout -- --ignored --nocapture

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config::IdType;
use replication::{
    ColumnInfo, ColumnValue, Operation, RelationCache, RelationInfo, ReplicaIdentity, RowEvent,
    TupleData,
};
use serde_json::json;

use puffgres_core::{Action, CoreError, DocumentId, Mapping, Router, Transformer};

fn make_relation(id: u32, table: &str) -> RelationInfo {
    RelationInfo {
        id,
        namespace: "public".to_string(),
        name: table.to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![ColumnInfo {
            part_of_key: true,
            name: "id".to_string(),
            type_oid: 23,
            type_modifier: -1,
        }],
    }
}

fn make_mapping(name: &str, table: &str) -> Mapping {
    Mapping {
        name: name.to_string(),
        namespace: format!("ns_{name}"),
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        id_column: "id".to_string(),
        id_type: IdType::Uint,
        columns: None,
    }
}

fn insert_event(relation_id: u32, id_val: u64) -> RowEvent {
    let id_str = id_val.to_string();
    RowEvent {
        relation_id,
        operation: Operation::Insert,
        new_tuple: Some(Arc::new(TupleData {
            columns: vec![ColumnValue::Text(Bytes::from(id_str))],
        })),
        old_tuple: None,
    }
}

struct PassthroughTransformer;

impl Transformer for PassthroughTransformer {
    fn transform_batch<'a>(
        &'a self,
        events: &'a [(&'a RowEvent, DocumentId)],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Action>, CoreError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(events
                .iter()
                .map(|(event, id)| match event.operation {
                    Operation::Delete => Action::Delete { id: id.clone() },
                    _ => Action::Upsert {
                        id: id.clone(),
                        document: json!({}),
                        vector: None,
                        distance_metric: None,
                        schema: None,
                    },
                })
                .collect())
        })
    }
}

#[test]
#[ignore]
fn fanout_100_tables_1000_configs() {
    let num_tables = 100u32;
    let configs_per_table = 10;
    let events_per_table = 100;

    let mut cache = RelationCache::new();
    let mut mappings = Vec::new();

    for t in 0..num_tables {
        let table_name = format!("table_{t:03}");
        cache.insert(make_relation(t + 1, &table_name));
        for c in 0..configs_per_table {
            mappings.push(make_mapping(&format!("config_{t:03}_{c}"), &table_name));
        }
    }

    let router = Router::new(mappings);
    let total_configs = num_tables as usize * configs_per_table;
    println!("Setup: {num_tables} tables, {total_configs} configs");

    let mut events: Vec<RowEvent> = Vec::new();
    for t in 0..num_tables {
        for i in 0..events_per_table {
            events.push(insert_event(t + 1, (t as u64) * 1000 + i));
        }
    }
    let total_events = events.len();

    let start = Instant::now();
    let routed = router.route_batch(&events, &cache);
    let elapsed = start.elapsed();

    let events_per_sec = total_events as f64 / elapsed.as_secs_f64();
    let total_routed: usize = routed.values().map(|v| v.len()).sum();
    let expected_routed = total_events * configs_per_table;

    println!("Routing {total_events} events to {total_configs} configs:");
    println!("  Elapsed:        {elapsed:.2?}");
    println!("  Events/sec:     {events_per_sec:.0}");
    println!("  Total routed:   {total_routed} (expected {expected_routed})");
    println!("  Unique configs: {}", routed.len());

    assert_eq!(total_routed, expected_routed);
    assert_eq!(routed.len(), total_configs);

    // Verify each config got exactly events_per_table events
    for (config_name, events) in &routed {
        assert_eq!(
            events.len(),
            events_per_table as usize,
            "config {config_name} got {} events, expected {events_per_table}",
            events.len()
        );
    }
}

#[test]
#[ignore]
fn fanout_scaling_comparison() {
    let events_per_table = 1000;
    let mut cache = RelationCache::new();
    cache.insert(make_relation(1, "test_table"));

    let mut events: Vec<RowEvent> = Vec::new();
    for i in 0..events_per_table {
        events.push(insert_event(1, i));
    }

    println!(
        "\n{:<12} {:>12} {:>12} {:>15}",
        "Configs", "Elapsed", "Events/sec", "Routed total"
    );
    println!("{}", "-".repeat(55));

    for &num_configs in &[1, 10, 100, 1000] {
        let mappings: Vec<_> = (0..num_configs)
            .map(|c| make_mapping(&format!("cfg_{c}"), "test_table"))
            .collect();
        let router = Router::new(mappings);

        let start = Instant::now();
        let routed = router.route_batch(&events, &cache);
        let elapsed = start.elapsed();

        let total_routed: usize = routed.values().map(|v| v.len()).sum();
        let eps = events_per_table as f64 / elapsed.as_secs_f64();

        println!(
            "{:<12} {:>12.2?} {:>12.0} {:>15}",
            num_configs, elapsed, eps, total_routed
        );

        assert_eq!(total_routed, events_per_table as usize * num_configs);
    }
}

#[test]
#[ignore]
fn sustained_routing_60s() {
    let configs = 3;
    let batch_size = 10;
    let target_duration = Duration::from_secs(60);

    let mut cache = RelationCache::new();
    cache.insert(make_relation(1, "test_table"));

    let mappings: Vec<_> = (0..configs)
        .map(|c| make_mapping(&format!("cfg_{c}"), "test_table"))
        .collect();
    let router = Router::new(mappings);

    let start = Instant::now();
    let mut total_events = 0u64;
    let mut total_routed = 0u64;
    let mut batch_count = 0u64;

    while start.elapsed() < target_duration {
        let events: Vec<RowEvent> = (0..batch_size)
            .map(|i| insert_event(1, total_events + i as u64))
            .collect();

        let routed = router.route_batch(&events, &cache);
        total_routed += routed.values().map(|v| v.len() as u64).sum::<u64>();
        total_events += batch_size as u64;
        batch_count += 1;
    }

    let elapsed = start.elapsed();
    let eps = total_events as f64 / elapsed.as_secs_f64();

    println!("--- sustained routing ---");
    println!("  Duration:      {elapsed:.2?}");
    println!("  Total events:  {total_events}");
    println!("  Total routed:  {total_routed}");
    println!("  Batches:       {batch_count}");
    println!("  Events/sec:    {eps:.0}");
    println!(
        "  Routed/event:  {:.1}",
        total_routed as f64 / total_events as f64
    );

    assert_eq!(
        total_routed,
        total_events * configs as u64,
        "each event should route to {configs} configs"
    );
}

#[tokio::test]
#[ignore]
async fn sustained_end_to_end_fanout_60s() {
    let num_tables = 100u32;
    let configs_per_table = 10usize;
    let batch_size = 100usize;
    let target_duration = Duration::from_secs(60);

    let mut cache = RelationCache::new();
    let mut mappings = Vec::new();

    for t in 0..num_tables {
        let table_name = format!("table_{t:03}");
        cache.insert(make_relation(t + 1, &table_name));
        for c in 0..configs_per_table {
            mappings.push(make_mapping(&format!("config_{t:03}_{c}"), &table_name));
        }
    }

    let total_configs = num_tables as usize * configs_per_table;
    let router = Router::new(mappings);
    let transformer = PassthroughTransformer;

    let start = Instant::now();
    let mut total_events = 0u64;
    let mut total_routed = 0u64;
    let mut total_actions = 0u64;
    let mut batch_count = 0u64;
    let mut next_relation = 1u32;

    while start.elapsed() < target_duration {
        let relation_id = next_relation;
        next_relation = if next_relation == num_tables {
            1
        } else {
            next_relation + 1
        };

        let events: Vec<RowEvent> = (0..batch_size)
            .map(|i| insert_event(relation_id, total_events + i as u64))
            .collect();

        let routed = router.route_batch(&events, &cache);
        total_routed += routed.values().map(|v| v.len() as u64).sum::<u64>();

        for routed_events in routed.values() {
            let actions = transformer
                .transform_batch(routed_events.as_slice())
                .await
                .unwrap();
            total_actions += actions.len() as u64;
        }

        total_events += batch_size as u64;
        batch_count += 1;
    }

    let elapsed = start.elapsed();
    let source_eps = total_events as f64 / elapsed.as_secs_f64();
    let routed_eps = total_routed as f64 / elapsed.as_secs_f64();

    println!("--- sustained end-to-end fanout ---");
    println!("  Duration:            {elapsed:.2?}");
    println!("  Tables:              {num_tables}");
    println!("  Configs/table:       {configs_per_table}");
    println!("  Total configs:       {total_configs}");
    println!("  Source events:       {total_events}");
    println!("  Routed deliveries:   {total_routed}");
    println!("  Actions produced:    {total_actions}");
    println!("  Batches:             {batch_count}");
    println!("  Source events/sec:   {source_eps:.0}");
    println!("  Routed deliveries/s: {routed_eps:.0}");
    println!(
        "  Routed/event:        {:.1}",
        total_routed as f64 / total_events as f64
    );

    assert_eq!(total_routed, total_events * configs_per_table as u64);
    assert_eq!(total_actions, total_routed);
}
