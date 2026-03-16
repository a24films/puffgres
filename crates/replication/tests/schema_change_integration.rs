use std::collections::HashMap;
use std::time::Duration;

use pg::connect::connect;
use pg::publication::ensure_publication;
use pg::slot::ensure_slot;
use pg::test_utils::setup_postgres_logical;
use replication::{
    BatchResult, Operation, ReplicationStream, ReplicationStreamConfig, SchemaChanged,
    StreamingBatch,
};

const SLOT: &str = "schema_change_slot";
const PUB: &str = "schema_change_pub";

/// Wait for `recv_batch` to return a `Batch` with at least one event,
/// retrying on empty keep-alive batches up to a timeout.
async fn recv_batch_with_events(
    stream: &mut ReplicationStream,
    timeout: Duration,
) -> replication::Result<Option<StreamingBatch>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for a batch with events");
        }

        match tokio::time::timeout(remaining, stream.recv_batch()).await {
            Ok(Ok(Some(BatchResult::Batch(batch)))) if !batch.events.is_empty() => {
                return Ok(Some(batch));
            }
            Ok(Ok(Some(BatchResult::Batch(_)))) => continue,
            Ok(Ok(Some(_))) => continue,
            Ok(Ok(None)) => return Ok(None),
            Ok(Err(e)) => return Err(e),
            Err(_) => panic!("timed out waiting for a batch with events"),
        }
    }
}

/// Wait for `recv_batch` to return `BatchResult::SchemaChanged`.
/// Empty batches and non-empty batches are consumed along the way.
async fn recv_until_schema_changed(
    stream: &mut ReplicationStream,
    timeout: Duration,
) -> Option<SchemaChanged> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for SchemaChanged signal");
        }

        match tokio::time::timeout(remaining, stream.recv_batch()).await {
            Ok(Ok(Some(BatchResult::SchemaChanged(sc)))) => return Some(sc),
            Ok(Err(e)) => panic!("unexpected error: {e}"),
            Ok(Ok(Some(_))) => continue, // consume batches
            Ok(Ok(None)) => return None,
            Err(_) => panic!("timed out waiting for SchemaChanged signal"),
        }
    }
}

#[tokio::test]
async fn schema_change_triggers_error_on_alter_table() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    // Create table, slot, and publication
    client
        .execute(
            "CREATE TABLE test_schema (id INT PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();

    ensure_slot(&client, SLOT).await.unwrap();
    ensure_publication(&client, PUB, &["test_schema".to_string()])
        .await
        .unwrap();

    // Insert a row so the first batch has WAL activity (establishes relation in cache)
    client
        .execute(
            "INSERT INTO test_schema (id, name) VALUES (1, 'alice')",
            &[],
        )
        .await
        .unwrap();

    // Connect the replication stream
    let mut stream = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: SLOT.to_string(),
        publication_name: PUB.to_string(),
        start_lsn: None,
        status_interval: Duration::from_secs(10),
        max_transaction_events: None,
        sub_batch_size: None,
        watched_columns: HashMap::new(),
    })
    .await
    .unwrap();

    // Consume the first batch — populates the relation cache
    let batch = recv_batch_with_events(&mut stream, Duration::from_secs(10))
        .await
        .unwrap()
        .expect("expected a batch with events");
    assert!(!batch.events.is_empty());
    stream.ack();

    // Verify initial cache: 2 columns (id, name)
    let cache = stream.relation_cache();
    let initial_relation = cache
        .iter()
        .find(|r| r.name == "test_schema")
        .expect("test_schema should be in relation cache");
    assert_eq!(initial_relation.columns.len(), 2);

    // ALTER TABLE — add a column
    client
        .execute("ALTER TABLE test_schema ADD COLUMN email TEXT", &[])
        .await
        .unwrap();

    // Insert another row to force Postgres to re-send the Relation message
    client
        .execute(
            "INSERT INTO test_schema (id, name, email) VALUES (2, 'bob', 'bob@example.com')",
            &[],
        )
        .await
        .unwrap();

    // recv_batch should return SchemaChanged
    let sc = recv_until_schema_changed(&mut stream, Duration::from_secs(10))
        .await
        .expect("expected SchemaChanged signal");
    assert_eq!(sc.namespace, "public");
    assert_eq!(sc.name, "test_schema");

    // Verify the relation cache was updated with the new column
    let cache = stream.relation_cache();
    let updated_relation = cache
        .iter()
        .find(|r| r.name == "test_schema")
        .expect("test_schema should still be in relation cache");
    assert_eq!(
        updated_relation.columns.len(),
        3,
        "should have 3 columns after ALTER TABLE ADD COLUMN"
    );
    assert_eq!(updated_relation.columns[2].name, "email");
}

#[tokio::test]
async fn reconnect_after_schema_change_resumes_streaming() {
    let ctx = setup_postgres_logical().await;
    let client = connect(&ctx.connection_string).await.unwrap();

    // Create table, slot, and publication
    client
        .execute(
            "CREATE TABLE test_schema_reconnect (id INT PRIMARY KEY, name TEXT)",
            &[],
        )
        .await
        .unwrap();

    let slot = "schema_reconnect_slot";
    let pub_name = "schema_reconnect_pub";

    ensure_slot(&client, slot).await.unwrap();
    ensure_publication(&client, pub_name, &["test_schema_reconnect".to_string()])
        .await
        .unwrap();

    // Insert a row to establish WAL activity
    client
        .execute(
            "INSERT INTO test_schema_reconnect (id, name) VALUES (1, 'alice')",
            &[],
        )
        .await
        .unwrap();

    // Connect stream #1
    let mut stream = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: None,
        status_interval: Duration::from_secs(10),
        max_transaction_events: None,
        sub_batch_size: None,
        watched_columns: HashMap::new(),
    })
    .await
    .unwrap();

    // Consume the first batch to populate the cache
    let batch = recv_batch_with_events(&mut stream, Duration::from_secs(10))
        .await
        .unwrap()
        .expect("expected a batch");
    let last_acked_lsn = batch.ack_lsn;
    stream.ack();

    // ALTER TABLE
    client
        .execute(
            "ALTER TABLE test_schema_reconnect ADD COLUMN email TEXT",
            &[],
        )
        .await
        .unwrap();

    // Insert a row to trigger the Relation re-send
    client
        .execute(
            "INSERT INTO test_schema_reconnect (id, name, email) VALUES (2, 'bob', 'bob@example.com')",
            &[],
        )
        .await
        .unwrap();

    // Get SchemaChanged signal
    let sc = recv_until_schema_changed(&mut stream, Duration::from_secs(10)).await;
    assert!(sc.is_some(), "expected SchemaChanged signal");

    // Drop stream #1, reconnect from the last acked LSN (simulating the run.rs reconnect loop).
    // Replication slots retain all un-acked WAL, so when we reconnect from the last
    // checkpointed LSN we will receive all events between the disconnect and the present.
    drop(stream);

    // Terminate any stale backend holding the slot
    pg::slot::terminate_active_slot_backend(&client, slot)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut stream2 = ReplicationStream::connect(ReplicationStreamConfig {
        connection_string: ctx.connection_url.clone(),
        slot_name: slot.to_string(),
        publication_name: pub_name.to_string(),
        start_lsn: Some(last_acked_lsn),
        status_interval: Duration::from_secs(10),
        max_transaction_events: None,
        sub_batch_size: None,
        watched_columns: HashMap::new(),
    })
    .await
    .unwrap();

    // Do NOT insert any new rows yet — first verify that bob's row (inserted before
    // the SchemaChanged disconnect, but never acked) is replayed on the reconnected
    // stream. This is the critical assertion: no messages are dropped during reconnection.
    let batch = recv_batch_with_events(&mut stream2, Duration::from_secs(10))
        .await
        .unwrap()
        .expect("expected bob's batch to be replayed on reconnected stream");

    let bob_event = batch.events.iter().find(|e| {
        e.operation == Operation::Insert
            && e.new_tuple
                .as_ref()
                .is_some_and(|t| t.columns.first().and_then(|c| c.as_bytes()) == Some(b"2"))
    });
    assert!(
        bob_event.is_some(),
        "bob's row (id=2) should be replayed on reconnect — was inserted before \
         disconnect but never acked. Got events: {:?}",
        batch
            .events
            .iter()
            .map(|e| format!("{:?}", e.operation))
            .collect::<Vec<_>>(),
    );

    // Verify the new relation cache has 3 columns
    let cache = stream2.relation_cache();
    let relation = cache
        .iter()
        .find(|r| r.name == "test_schema_reconnect")
        .expect("test_schema_reconnect should be in relation cache");
    assert_eq!(
        relation.columns.len(),
        3,
        "reconnected stream should see the altered schema with 3 columns"
    );
    assert_eq!(relation.columns[2].name, "email");
}
