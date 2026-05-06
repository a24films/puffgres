use std::time::{SystemTime, UNIX_EPOCH};

use pg::connect::{PgConnection, quote_identifier};
use pg::schema_bootstrap::{PUFFGRES_SCHEMA, ensure_schema, ensure_state_tables};

use crate::StateError;

pub struct PostgresStateStore {
    connection: PgConnection,
    schema_name: String,
}

impl PostgresStateStore {
    pub async fn connect(connection_string: &str) -> Result<Self, StateError> {
        Self::connect_with_schema(connection_string, PUFFGRES_SCHEMA).await
    }

    pub async fn connect_with_schema(
        connection_string: &str,
        schema_name: &str,
    ) -> Result<Self, StateError> {
        let connection = pg::connect::connect(connection_string).await?;
        ensure_schema(&connection, schema_name).await?;
        ensure_state_tables(&connection, schema_name).await?;

        Ok(Self {
            connection,
            schema_name: schema_name.to_string(),
        })
    }

    pub fn client(&self) -> &pg::Client {
        &self.connection
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub async fn verify_startup_roundtrip(&self) -> Result<(), StateError> {
        let pid = std::process::id();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StateError::InvalidState(format!("system clock error: {e}")))?
            .as_millis();
        let probe_key = format!("startup_probe_{}_{}", pid, ts);
        let probe_value = format!("{}-{}", pid, ts);
        let schema = quote_identifier(&self.schema_name);

        let upsert = format!(
            "INSERT INTO {schema}.runtime_state (key, value, updated_at)
             VALUES ($1, $2, $3)
             ON CONFLICT(key) DO UPDATE
             SET value = excluded.value, updated_at = excluded.updated_at"
        );
        let select =
            format!("SELECT value FROM {schema}.runtime_state WHERE key = $1");
        let delete =
            format!("DELETE FROM {schema}.runtime_state WHERE key = $1");
        let updated_at = chrono::Utc::now().timestamp_millis();

        self.connection
            .execute(&upsert, &[&probe_key, &probe_value, &updated_at])
            .await
            .map_err(pg::PgError::from)?;

        let stored = self
            .connection
            .query_one(&select, &[&probe_key])
            .await
            .map_err(pg::PgError::from)?
            .get::<_, String>(0);

        let _ = self.connection.execute(&delete, &[&probe_key]).await;

        if stored != probe_value {
            return Err(StateError::InvalidState(format!(
                "postgres state roundtrip verification failed for schema '{}'",
                self.schema_name
            )));
        }

        tracing::info!(
            state_schema = %self.schema_name,
            "postgres state startup roundtrip check passed"
        );

        Ok(())
    }
}
