use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use std::future::Future;
use std::pin::Pin;

use config::IdType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use std::sync::Arc;

use crate::transform::Transformer;
use crate::{Action, CoreError, DocumentId};
use replication::{Operation, RowEvent};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Extract column values from a RowEvent tuple as text strings.
///
/// When `reindex` is provided, columns are reordered according to the mapping
/// (e.g., from WAL order to schema.ts order). Out-of-bounds indices produce
/// null values with a warning.
fn extract_columns(event: &RowEvent, reindex: Option<&[usize]>) -> Vec<Option<String>> {
    let tuple = event.new_tuple.as_ref().or(event.old_tuple.as_ref());
    tuple
        .map(|t| {
            let col_to_string = |col: &replication::ColumnValue| {
                col.as_bytes()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_string())
            };
            if let Some(reindex) = reindex {
                reindex
                    .iter()
                    .map(|&i| {
                        if i >= t.columns.len() {
                            tracing::warn!(
                                index = i,
                                columns = t.columns.len(),
                                "column reindex out of bounds, producing null"
                            );
                        }
                        t.columns.get(i).and_then(&col_to_string)
                    })
                    .collect()
            } else {
                t.columns.iter().map(col_to_string).collect()
            }
        })
        .unwrap_or_default()
}

// Wire types for the JS boundary. These are the JSON shapes that cross the
// subprocess stdin/stdout. They're intentionally decoupled from the internal
// Action/RowEvent types so the JS contract can evolve independently.

/// Event serialized to JSON and written to the transform script's stdin.
#[derive(Serialize)]
struct JsEvent {
    operation: &'static str,
    id: Value,
    columns: Vec<Option<String>>,
}

/// Action deserialized from the transform script's stdout.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum JsAction {
    Upsert {
        id: Value,
        document: Value,
        #[serde(default)]
        vector: Option<Vec<f32>>,
        #[serde(default)]
        distance_metric: Option<String>,
        #[serde(default)]
        schema: Option<HashMap<String, Value>>,
    },
    Delete {
        id: Value,
    },
    Skip {},
}

struct ChildProcess {
    child: Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    stderr_lines: Arc<Mutex<Vec<String>>>,
}

/// A [`Transformer`] that delegates to a user-supplied TypeScript/JavaScript
/// file by spawning `pnpx tsx <script>` as a persistent subprocess.
///
/// Communication uses newline-delimited JSON (NDJSON): each batch is written
/// as a single JSON array line to stdin, and the script responds with a single
/// JSON array line on stdout. The process is kept alive across batches.
///
/// If a batch times out (default 30s), the child is killed and respawned.
///
/// Holds one transform subprocess per lane (`concurrency` children).
/// Events are partitioned by [`DocumentId`] so same-id edits stay on one
/// child (ordered) while distinct ids can run on different children in
/// parallel. With concurrency = 1 this is a single child, matching the previous serial behavior
pub struct JsTransformer {
    script_path: PathBuf,
    id_type: IdType,
    /// When set, reindexes WAL tuple columns to match the generated schema order.
    column_reindex: Option<Vec<usize>>,
    timeout: Duration,
    /// One mutexed child per lane. Lane `i` always uses `processes[i]`.
    processes: Vec<Mutex<Option<ChildProcess>>>,
}

impl JsTransformer {
    pub fn new(script_path: PathBuf, id_type: IdType) -> Self {
        Self::new_with_timeout(script_path, id_type, DEFAULT_TIMEOUT)
    }

    pub fn new_with_timeout(script_path: PathBuf, id_type: IdType, timeout: Duration) -> Self {
        Self::new_with_timeout_and_concurrency(script_path, id_type, timeout, 1)
    }

    pub fn new_with_timeout_and_concurrency(
        script_path: PathBuf,
        id_type: IdType,
        timeout: Duration,
        concurrency: usize,
    ) -> Self {
        let n = concurrency.max(1);
        Self {
            script_path,
            id_type,
            column_reindex: None,
            timeout,
            processes: (0..n).map(|_| Mutex::new(None)).collect(),
        }
    }

    /// Create a transformer with a column reindex mapping.
    pub fn with_column_reindex(
        script_path: PathBuf,
        id_type: IdType,
        column_reindex: Vec<usize>,
    ) -> Self {
        Self::with_column_reindex_and_timeout(script_path, id_type, column_reindex, DEFAULT_TIMEOUT)
    }

    pub fn with_column_reindex_and_timeout(
        script_path: PathBuf,
        id_type: IdType,
        column_reindex: Vec<usize>,
        timeout: Duration,
    ) -> Self {
        Self::with_column_reindex_timeout_and_concurrency(
            script_path,
            id_type,
            column_reindex,
            timeout,
            1,
        )
    }

    pub fn with_column_reindex_timeout_and_concurrency(
        script_path: PathBuf,
        id_type: IdType,
        column_reindex: Vec<usize>,
        timeout: Duration,
        concurrency: usize,
    ) -> Self {
        let n = concurrency.max(1);
        Self {
            script_path,
            id_type,
            column_reindex: Some(column_reindex),
            timeout,
            processes: (0..n).map(|_| Mutex::new(None)).collect(),
        }
    }

    pub fn concurrency(&self) -> usize {
        self.processes.len()
    }

    fn spawn_child(&self) -> Result<ChildProcess, CoreError> {
        let node_options = match std::env::var("NODE_OPTIONS") {
            Ok(existing)
                if existing
                    .split_whitespace()
                    .any(|arg| arg == "--no-deprecation") =>
            {
                existing
            }
            Ok(existing) if existing.trim().is_empty() => "--no-deprecation".to_string(),
            Ok(existing) => format!("{existing} --no-deprecation"),
            Err(_) => "--no-deprecation".to_string(),
        };

        let mut child = Command::new("pnpx")
            .arg("tsx")
            .arg(&self.script_path)
            .env("NODE_OPTIONS", node_options)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| CoreError::pipeline(format!("failed to spawn pnpx tsx: {e}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CoreError::pipeline("failed to open stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CoreError::pipeline("failed to open stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| CoreError::pipeline("failed to open stderr".to_string()))?;

        // Drain stderr in a background task so the OS pipe buffer doesn't fill
        // and block the child process. Lines are collected into a shared buffer
        // (capped at 100 lines) so they can be reported if the child exits
        // unexpectedly.
        const MAX_STDERR_LINES: usize = 100;
        let stderr_lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let stderr_lines_handle = Arc::clone(&stderr_lines);
        tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::warn!(target: "transform_stderr", "{}", line);
                let mut buf = stderr_lines_handle.lock().await;
                if buf.len() >= MAX_STDERR_LINES {
                    buf.remove(0);
                }
                buf.push(line);
            }
        });

        Ok(ChildProcess {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_lines,
        })
    }

    /// Get or spawn the child process for a lane.
    async fn ensure_process(&self, lane: usize) -> Result<(), CoreError> {
        let mut guard = self.processes[lane].lock().await;
        if guard.is_none() {
            *guard = Some(self.spawn_child()?);
        }
        Ok(())
    }

    /// Kill the child for a lane and spawn a fresh one.
    async fn respawn(&self, lane: usize) -> Result<(), CoreError> {
        let mut guard = self.processes[lane].lock().await;
        if let Some(mut proc) = guard.take() {
            let _ = proc.child.kill().await;
        }
        *guard = Some(self.spawn_child()?);
        Ok(())
    }

    fn serialize_events(&self, events: &[(&RowEvent, DocumentId)]) -> Result<String, CoreError> {
        let js_events: Vec<JsEvent> = events
            .iter()
            .map(|(event, id)| {
                let operation = match event.operation {
                    Operation::Insert => "insert",
                    Operation::Update => "update",
                    Operation::Delete => "delete",
                };

                let id_value = match id {
                    DocumentId::Uint(n) => Value::Number((*n).into()),
                    DocumentId::Int(n) => Value::Number((*n).into()),
                    DocumentId::Uuid(u) => Value::String(u.to_string()),
                    DocumentId::String(s) => Value::String(s.clone()),
                };

                let columns = extract_columns(event, self.column_reindex.as_deref());

                JsEvent {
                    operation,
                    id: id_value,
                    columns,
                }
            })
            .collect();

        serde_json::to_string(&js_events)
            .map_err(|e| CoreError::pipeline(format!("failed to serialize events: {e}")))
    }

    fn parse_actions(&self, output: &str) -> Result<Vec<Action>, CoreError> {
        let js_actions: Vec<JsAction> = serde_json::from_str(output)
            .map_err(|e| CoreError::pipeline(format!("failed to parse transform output: {e}")))?;

        js_actions
            .into_iter()
            .map(|action| match action {
                JsAction::Upsert {
                    id,
                    document,
                    vector,
                    distance_metric,
                    schema,
                } => {
                    let doc_id = DocumentId::from_value(&id, &self.id_type)?;
                    Ok(Action::Upsert {
                        id: doc_id,
                        document,
                        vector,
                        distance_metric,
                        schema,
                    })
                }
                JsAction::Delete { id } => {
                    let doc_id = DocumentId::from_value(&id, &self.id_type)?;
                    Ok(Action::Delete { id: doc_id })
                }
                JsAction::Skip {} => Ok(Action::Skip),
            })
            .collect()
    }

    /// Send a batch over NDJSON and read the response, with timeout.
    ///
    /// On a process error (broken pipe, unexpected EOF) the child is respawned
    /// and the **same batch is retried once** on the fresh process. Timeouts
    /// are not retried because they likely indicate a problem with the script
    /// itself rather than a transient child crash.
    async fn send_batch_to_process(&self, lane: usize, input: &str) -> Result<String, CoreError> {
        self.ensure_process(lane).await?;

        let result = self.try_send(lane, input).await;

        match result {
            Ok(Ok(line)) => Ok(line),
            Ok(Err(_)) => {
                // Process error — respawn and retry this batch once
                self.respawn(lane).await?;
                match self.try_send(lane, input).await {
                    Ok(Ok(line)) => Ok(line),
                    Ok(Err(e)) => {
                        self.respawn(lane).await?;
                        Err(e)
                    }
                    Err(_) => {
                        self.respawn(lane).await?;
                        Err(CoreError::pipeline(format!(
                            "transform timed out after {}s",
                            self.timeout.as_secs()
                        )))
                    }
                }
            }
            Err(_) => {
                // Timeout — kill and respawn but don't retry
                self.respawn(lane).await?;
                Err(CoreError::pipeline(format!(
                    "transform timed out after {}s",
                    self.timeout.as_secs()
                )))
            }
        }
    }

    /// Attempt a single send/receive cycle on the child for `lane`.
    async fn try_send(
        &self,
        lane: usize,
        input: &str,
    ) -> Result<Result<String, CoreError>, tokio::time::error::Elapsed> {
        let mut guard = self.processes[lane].lock().await;
        let proc = guard
            .as_mut()
            .expect("process should exist after ensure");

        let fut = async {
            // Write JSON array + newline
            proc.stdin
                .write_all(input.as_bytes())
                .await
                .map_err(|e| CoreError::pipeline(format!("failed to write to stdin: {e}")))?;
            proc.stdin
                .write_all(b"\n")
                .await
                .map_err(|e| CoreError::pipeline(format!("failed to write newline: {e}")))?;
            proc.stdin
                .flush()
                .await
                .map_err(|e| CoreError::pipeline(format!("failed to flush stdin: {e}")))?;

            // Read one line of response
            let mut line = String::new();
            proc.stdout
                .read_line(&mut line)
                .await
                .map_err(|e| CoreError::pipeline(format!("failed to read from stdout: {e}")))?;

            if line.is_empty() {
                // stdout EOF — the child process exited. Read any stderr
                // collected by the background drain task so the caller gets
                // an actionable error instead of an opaque "closed stdout"
                // message.
                let stderr_collected = proc.stderr_lines.lock().await;
                let stderr_text = stderr_collected.join("\n");
                let stderr_snippet = stderr_text.trim();

                let exit_status = proc.child.try_wait().ok().flatten();

                let mut msg = String::from("transform process exited unexpectedly");
                if let Some(status) = exit_status {
                    msg.push_str(&format!(" ({})", status));
                }
                if !stderr_snippet.is_empty() {
                    // Cap stderr to avoid flooding the error with huge stack traces.
                    let truncated: &str = match stderr_snippet.floor_char_boundary(2048) {
                        bound if bound < stderr_snippet.len() => &stderr_snippet[..bound],
                        _ => stderr_snippet,
                    };
                    msg.push_str(&format!(":\n{truncated}"));
                }
                if stderr_snippet.contains("ERR_MODULE_NOT_FOUND")
                    || stderr_snippet.contains("Cannot find package")
                {
                    msg.push_str(
                        "\n\nhint: a package failed to resolve — run `pnpm install` in your \
                         puffgres project directory to install transform dependencies. \
                         If puffgres lives inside an existing pnpm workspace, run \
                         `pnpm install --ignore-workspace` so its own dependencies are installed \
                         (a plain `pnpm install` installs the workspace and skips them).",
                    );
                }

                return Err(CoreError::pipeline(msg));
            }

            Ok(line)
        };

        tokio::time::timeout(self.timeout, fut).await
    }

    /// Partition events by document id into lanes, preserving arrival order
    /// within each lane. Each item is `(original_index, event, id)`. Empty
    /// lanes are omitted from the result.
    fn partition_lanes<'a>(
        &self,
        events: &'a [(&'a RowEvent, DocumentId)],
    ) -> Vec<(usize, Vec<(usize, &'a RowEvent, DocumentId)>)> {
        let n = self.concurrency();
        let mut lanes: Vec<Vec<(usize, &RowEvent, DocumentId)>> =
            (0..n).map(|_| Vec::new()).collect();
        for (idx, &(event, ref id)) in events.iter().enumerate() {
            lanes[id.lane(n)].push((idx, event, id.clone()));
        }
        lanes
            .into_iter()
            .enumerate()
            .filter(|(_, evs)| !evs.is_empty())
            .collect()
    }

    async fn transform_lane(
        &self,
        lane: usize,
        events: &[(&RowEvent, DocumentId)],
    ) -> Result<Vec<Action>, CoreError> {
        let input = self.serialize_events(events)?;
        let output = self.send_batch_to_process(lane, &input).await?;
        match self.parse_actions(output.trim()) {
            Ok(actions) => Ok(actions),
            Err(e) => {
                // Parse failure may mean the child emitted extra/malformed
                // output. Respawn to realign the request/response framing.
                self.respawn(lane).await?;
                Err(e)
            }
        }
    }

    /// Transform one lane, tagging each action with its original batch index.
    ///
    /// The script must return exactly one action per input event. That 1:1
    /// pairing is how we put concurrent lane results back into batch order:
    /// action i belongs to event i, even if the transform remaps document ids.
    async fn transform_lane_indexed(
        &self,
        lane: usize,
        events: &[(usize, &RowEvent, DocumentId)],
    ) -> Result<Vec<(usize, Action)>, CoreError> {
        let bare: Vec<(&RowEvent, DocumentId)> = events
            .iter()
            .map(|(_, event, id)| (*event, id.clone()))
            .collect();
        let actions = self.transform_lane(lane, &bare).await?;
        if actions.len() != events.len() {
            return Err(CoreError::pipeline(format!(
                "transform returned {} actions for {} events (need one action per event when transform_concurrency > 1)",
                actions.len(),
                events.len()
            )));
        }
        Ok(events
            .iter()
            .map(|(idx, _, _)| *idx)
            .zip(actions)
            .collect())
    }
}

impl Drop for JsTransformer {
    fn drop(&mut self) {
        // Best-effort kill. We can't await here, so just start the kill.
        for slot in &mut self.processes {
            if let Some(mut proc) = slot.get_mut().take() {
                let _ = proc.child.start_kill();
            }
        }
    }
}

impl Transformer for JsTransformer {
    fn transform_batch<'a>(
        &'a self,
        events: &'a [(&'a RowEvent, DocumentId)],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Action>, CoreError>> + Send + 'a>> {
        Box::pin(async move {
            if events.is_empty() {
                return Ok(Vec::new());
            }

            // Single lane (default): one round trip, identical to the old path.
            if self.concurrency() == 1 {
                return self.transform_lane(0, events).await;
            }

            let partitions = self.partition_lanes(events);
            // One non-empty partition: use that lane alone (skip join overhead;
            // common for tiny OLTP transactions).
            if partitions.len() == 1 {
                let (lane, lane_events) = &partitions[0];
                let bare: Vec<(&RowEvent, DocumentId)> = lane_events
                    .iter()
                    .map(|(_, event, id)| (*event, id.clone()))
                    .collect();
                return self.transform_lane(*lane, &bare).await;
            }

            let futs = partitions
                .iter()
                .map(|(lane, lane_events)| self.transform_lane_indexed(*lane, lane_events));
            // Finish every lane before returning (batch latency follows the
            // slowest lane; a failure must not cancel another mid round trip).
            let lane_results = futures::future::join_all(futs).await;
            let mut indexed = Vec::new();
            let mut first_err = None;
            for result in lane_results {
                match result {
                    Ok(pairs) => indexed.extend(pairs),
                    Err(e) if first_err.is_none() => first_err = Some(e),
                    Err(_) => {}
                }
            }
            if let Some(e) = first_err {
                return Err(e);
            }
            // Restore original batch order so remapped turbopuffer ids still
            // apply in WAL/event order across lanes.
            indexed.sort_by_key(|(idx, _)| *idx);
            Ok(indexed.into_iter().map(|(_, action)| action).collect())
        })
    }
}

/// A [`Transformer`] that passes through raw column values as the document,
/// skipping the subprocess entirely. Used when no `transform.ts` exists.
///
/// Column names must be provided so that the document is emitted as a JSON
/// object (required by the write path in `puff::client`).
pub struct PassthroughTransformer {
    column_names: Vec<String>,
    column_reindex: Option<Vec<usize>>,
}

impl PassthroughTransformer {
    pub fn new(column_names: Vec<String>) -> Self {
        Self {
            column_names,
            column_reindex: None,
        }
    }

    pub fn with_column_reindex(column_names: Vec<String>, column_reindex: Vec<usize>) -> Self {
        Self {
            column_names,
            column_reindex: Some(column_reindex),
        }
    }

    fn columns_to_values(&self, event: &RowEvent) -> Vec<Option<String>> {
        extract_columns(event, self.column_reindex.as_deref())
    }
}

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
                    _ => {
                        let columns = self.columns_to_values(event);
                        let document = Value::Object(
                            self.column_names
                                .iter()
                                .zip(columns.into_iter())
                                .map(|(name, val)| {
                                    (name.clone(), val.map(Value::String).unwrap_or(Value::Null))
                                })
                                .collect(),
                        );
                        Action::Upsert {
                            id: id.clone(),
                            document,
                            vector: None,
                            distance_metric: None,
                            schema: None,
                        }
                    }
                })
                .collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use config::IdType;
    use replication::{ColumnValue, TupleData};
    use serde_json::json;
    use std::sync::Arc;

    fn make_transformer() -> JsTransformer {
        JsTransformer::new(PathBuf::from("transform.ts"), IdType::Uint)
    }

    fn make_event(op: Operation, cols: Vec<&str>) -> RowEvent {
        RowEvent {
            relation_id: 1,
            operation: op,
            new_tuple: Some(Arc::new(TupleData {
                columns: cols
                    .into_iter()
                    .map(|s| ColumnValue::Text(Bytes::from(s.to_string())))
                    .collect(),
            })),
            old_tuple: None,
        }
    }

    #[test]
    fn serialize_insert_event() {
        let t = make_transformer();
        let event = make_event(Operation::Insert, vec!["1", "hello"]);
        let id = DocumentId::Uint(1);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["operation"], "insert");
        assert_eq!(parsed[0]["id"], 1);
        assert_eq!(parsed[0]["columns"], json!(["1", "hello"]));
    }

    #[test]
    fn serialize_delete_uses_old_tuple() {
        let t = make_transformer();
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Delete,
            new_tuple: None,
            old_tuple: Some(Arc::new(TupleData {
                columns: vec![ColumnValue::Text(Bytes::from("42"))],
            })),
        };
        let id = DocumentId::Uint(42);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["operation"], "delete");
        assert_eq!(parsed[0]["columns"], json!(["42"]));
    }

    #[test]
    fn serialize_null_columns() {
        let t = make_transformer();
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Insert,
            new_tuple: Some(Arc::new(TupleData {
                columns: vec![
                    ColumnValue::Text(Bytes::from("a")),
                    ColumnValue::Null,
                    ColumnValue::Text(Bytes::from("c")),
                ],
            })),
            old_tuple: None,
        };
        let id = DocumentId::Uint(1);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["columns"], json!(["a", null, "c"]));
    }

    #[test]
    fn serialize_no_tuple_gives_empty_columns() {
        let t = make_transformer();
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Delete,
            new_tuple: None,
            old_tuple: None,
        };
        let id = DocumentId::Uint(1);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["columns"], json!([]));
    }

    #[test]
    fn serialize_string_id() {
        let t = JsTransformer::new(PathBuf::from("t.ts"), IdType::String);
        let event = make_event(Operation::Insert, vec!["val"]);
        let id = DocumentId::String("abc".to_string());

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["id"], "abc");
    }

    #[test]
    fn serialize_uuid_id() {
        let t = JsTransformer::new(PathBuf::from("t.ts"), IdType::Uuid);
        let event = make_event(Operation::Insert, vec!["val"]);
        let uuid: uuid::Uuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let id = DocumentId::Uuid(uuid);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["id"], "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn serialize_batch() {
        let t = make_transformer();
        let e1 = make_event(Operation::Insert, vec!["a"]);
        let e2 = make_event(Operation::Update, vec!["b"]);

        let json_str = t
            .serialize_events(&[(&e1, DocumentId::Uint(1)), (&e2, DocumentId::Uint(2))])
            .unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["operation"], "insert");
        assert_eq!(parsed[1]["operation"], "update");
    }

    #[test]
    fn concurrency_defaults_to_one() {
        let t = make_transformer();
        assert_eq!(t.concurrency(), 1);
    }

    #[test]
    fn partition_lanes_keeps_same_id_together_in_order() {
        let t = JsTransformer::new_with_timeout_and_concurrency(
            PathBuf::from("t.ts"),
            IdType::Uint,
            DEFAULT_TIMEOUT,
            4,
        );
        let e1 = make_event(Operation::Insert, vec!["a"]);
        let e2 = make_event(Operation::Update, vec!["b"]);
        let e3 = make_event(Operation::Update, vec!["c"]);
        let events = [
            (&e1, DocumentId::Uint(42)),
            (&e2, DocumentId::Uint(99)),
            (&e3, DocumentId::Uint(42)),
        ];

        let partitions = t.partition_lanes(&events);
        let lane_42 = DocumentId::Uint(42).lane(4);
        let lane_99 = DocumentId::Uint(99).lane(4);

        let part_42 = partitions
            .iter()
            .find(|(lane, _)| *lane == lane_42)
            .expect("lane for id 42");
        assert_eq!(part_42.1.len(), 2);
        assert_eq!(part_42.1[0].0, 0); // original batch index
        assert_eq!(part_42.1[1].0, 2);
        assert_eq!(part_42.1[0].2, DocumentId::Uint(42));
        assert_eq!(part_42.1[1].2, DocumentId::Uint(42));
        // Arrival order within the lane: insert then update.
        assert_eq!(part_42.1[0].1.operation, Operation::Insert);
        assert_eq!(part_42.1[1].1.operation, Operation::Update);

        if lane_42 != lane_99 {
            let part_99 = partitions
                .iter()
                .find(|(lane, _)| *lane == lane_99)
                .expect("lane for id 99");
            assert_eq!(part_99.1.len(), 1);
            assert_eq!(part_99.1[0].0, 1);
            assert_eq!(part_99.1[0].2, DocumentId::Uint(99));
        }
    }

    #[test]
    fn merge_indexed_actions_restores_batch_order() {
        // Simulate two lanes finishing out of order with remapped targets:
        // source indices 0 then 1 must win over lane-number concat order.
        let mut indexed = vec![
            (1, Action::Delete {
                id: DocumentId::String("shared".into()),
            }),
            (0, Action::Upsert {
                id: DocumentId::String("shared".into()),
                document: json!({}),
                vector: None,
                distance_metric: None,
                schema: None,
            }),
        ];
        indexed.sort_by_key(|(idx, _)| *idx);
        let actions: Vec<_> = indexed.into_iter().map(|(_, a)| a).collect();
        assert!(matches!(
            &actions[0],
            Action::Upsert {
                id: DocumentId::String(s),
                ..
            } if s == "shared"
        ));
        assert!(matches!(
            &actions[1],
            Action::Delete {
                id: DocumentId::String(s)
            } if s == "shared"
        ));
    }

    #[test]
    fn parse_upsert_action() {
        let t = make_transformer();
        let output = r#"[{"type":"upsert","id":1,"document":{"name":"test"}}]"#;
        let actions = t.parse_actions(output).unwrap();

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Upsert {
                id,
                document,
                vector,
                ..
            } => {
                assert_eq!(*id, DocumentId::Uint(1));
                assert_eq!(*document, json!({"name": "test"}));
                assert!(vector.is_none());
            }
            _ => panic!("expected Upsert"),
        }
    }

    #[test]
    fn parse_upsert_with_vector() {
        let t = make_transformer();
        let output = r#"[{"type":"upsert","id":1,"document":{},"vector":[0.1,0.2,0.3]}]"#;
        let actions = t.parse_actions(output).unwrap();

        match &actions[0] {
            Action::Upsert { vector, .. } => {
                assert_eq!(vector.as_ref().unwrap(), &vec![0.1, 0.2, 0.3]);
            }
            _ => panic!("expected Upsert"),
        }
    }

    #[test]
    fn parse_delete_action() {
        let t = make_transformer();
        let output = r#"[{"type":"delete","id":42}]"#;
        let actions = t.parse_actions(output).unwrap();

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Delete { id } => assert_eq!(*id, DocumentId::Uint(42)),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn parse_skip_action() {
        let t = make_transformer();
        let output = r#"[{"type":"skip"}]"#;
        let actions = t.parse_actions(output).unwrap();

        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Skip));
    }

    #[test]
    fn parse_mixed_actions() {
        let t = make_transformer();
        let output = r#"[
            {"type":"upsert","id":1,"document":{"a":1}},
            {"type":"skip"},
            {"type":"delete","id":2}
        ]"#;
        let actions = t.parse_actions(output).unwrap();

        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], Action::Upsert { .. }));
        assert!(matches!(actions[1], Action::Skip));
        assert!(matches!(actions[2], Action::Delete { .. }));
    }

    #[test]
    fn parse_invalid_json() {
        let t = make_transformer();
        let err = t.parse_actions("not json").unwrap_err();
        assert!(err.to_string().contains("failed to parse transform output"));
    }

    #[test]
    fn parse_unknown_action_type() {
        let t = make_transformer();
        let err = t.parse_actions(r#"[{"type":"explode"}]"#).unwrap_err();
        assert!(err.to_string().contains("failed to parse transform output"));
    }

    #[test]
    fn parse_missing_document_field() {
        let t = make_transformer();
        let err = t
            .parse_actions(r#"[{"type":"upsert","id":1}]"#)
            .unwrap_err();
        assert!(err.to_string().contains("failed to parse transform output"));
    }

    #[test]
    fn parse_invalid_id_type() {
        let t = make_transformer(); // expects Uint
        let err = t
            .parse_actions(r#"[{"type":"upsert","id":"not-a-number","document":{}}]"#)
            .unwrap_err();
        assert!(err.to_string().contains("cannot parse"));
    }

    #[test]
    fn serialize_with_column_reindex() {
        let t = JsTransformer::with_column_reindex(
            PathBuf::from("transform.ts"),
            IdType::Uint,
            vec![1, 0],
        );
        let event = make_event(Operation::Insert, vec!["42", "alice", "alice@example.com"]);
        let id = DocumentId::Uint(42);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["columns"], json!(["alice", "42"]));
    }

    #[test]
    fn serialize_with_column_reindex_handles_nulls() {
        let t = JsTransformer::with_column_reindex(
            PathBuf::from("transform.ts"),
            IdType::Uint,
            vec![2, 0],
        );
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Insert,
            new_tuple: Some(Arc::new(TupleData {
                columns: vec![
                    ColumnValue::Text(Bytes::from("1")),
                    ColumnValue::Null,
                    ColumnValue::Null,
                ],
            })),
            old_tuple: None,
        };
        let id = DocumentId::Uint(1);

        let json_str = t.serialize_events(&[(&event, id)]).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed[0]["columns"], json!([null, "1"]));
    }

    #[test]
    fn new_with_timeout_uses_custom_timeout() {
        let t = JsTransformer::new_with_timeout(
            PathBuf::from("transform.ts"),
            IdType::Uint,
            Duration::from_secs(45),
        );

        assert_eq!(t.timeout, Duration::from_secs(45));
    }

    #[tokio::test]
    async fn passthrough_insert_returns_upsert() {
        let t = PassthroughTransformer::new(vec!["col0".into(), "col1".into()]);
        let event = make_event(Operation::Insert, vec!["hello", "world"]);
        let id = DocumentId::Uint(1);

        let actions = t.transform_batch(&[(&event, id)]).await.unwrap();
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Upsert { id, document, .. } => {
                assert_eq!(*id, DocumentId::Uint(1));
                assert_eq!(*document, json!({"col0": "hello", "col1": "world"}));
            }
            _ => panic!("expected Upsert"),
        }
    }

    #[tokio::test]
    async fn passthrough_delete_returns_delete() {
        let t = PassthroughTransformer::new(vec!["col0".into()]);
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Delete,
            new_tuple: None,
            old_tuple: Some(Arc::new(TupleData {
                columns: vec![ColumnValue::Text(Bytes::from("42"))],
            })),
        };
        let id = DocumentId::Uint(42);

        let actions = t.transform_batch(&[(&event, id)]).await.unwrap();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Delete { .. }));
    }

    #[tokio::test]
    async fn passthrough_handles_nulls() {
        let t = PassthroughTransformer::new(vec!["a".into(), "b".into(), "c".into()]);
        let event = RowEvent {
            relation_id: 1,
            operation: Operation::Insert,
            new_tuple: Some(Arc::new(TupleData {
                columns: vec![
                    ColumnValue::Text(Bytes::from("a")),
                    ColumnValue::Null,
                    ColumnValue::Text(Bytes::from("c")),
                ],
            })),
            old_tuple: None,
        };
        let id = DocumentId::Uint(1);

        let actions = t.transform_batch(&[(&event, id)]).await.unwrap();
        match &actions[0] {
            Action::Upsert { document, .. } => {
                assert_eq!(*document, json!({"a": "a", "b": null, "c": "c"}));
            }
            _ => panic!("expected Upsert"),
        }
    }
}
