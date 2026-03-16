use std::ops::Deref;
use std::sync::Arc;

use rustls::ClientConfig;
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use tokio_postgres_rustls_improved::MakeRustlsConnect;

use crate::{PgError, Result};

/// A Postgres connection that owns both the client and the background task
/// driving the connection. When dropped, the background task is aborted so
/// the connection is cleaned up instead of being left dangling.
pub struct PgConnection {
    client: Client,
    _handle: JoinHandle<()>,
}

impl Deref for PgConnection {
    type Target = Client;
    fn deref(&self) -> &Client {
        &self.client
    }
}

impl PgConnection {
    fn new<S, T>(client: Client, connection: tokio_postgres::Connection<S, T>) -> Self
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(
                    error = %e,
                    error_debug = ?e,
                    "postgres connection error",
                );
            }
        });
        PgConnection {
            client,
            _handle: handle,
        }
    }
}

impl Drop for PgConnection {
    fn drop(&mut self) {
        self._handle.abort();
    }
}

pub async fn connect(connection_string: &str) -> Result<PgConnection> {
    if requires_tls(connection_string) {
        let config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| PgError::ConnectionError(format!("TLS config error: {}", e)))?
                .with_root_certificates(root_certs())
                .with_no_client_auth();

        let connector = MakeRustlsConnect::new(config);
        let (client, connection) = tokio_postgres::connect(connection_string, connector)
            .await
            .map_err(|e| PgError::ConnectionError(format!("Failed to connect: {}", e)))?;

        Ok(PgConnection::new(client, connection))
    } else {
        let (client, connection) =
            tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
                .await
                .map_err(|e| PgError::ConnectionError(format!("Failed to connect: {}", e)))?;

        Ok(PgConnection::new(client, connection))
    }
}

fn root_certs() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    roots
}

fn requires_tls(connection_string: &str) -> bool {
    // Parse as URL to extract sslmode from query params, avoiding false matches
    // on passwords or other URL components that happen to contain "sslmode=".
    if let Ok(url) = url::Url::parse(connection_string) {
        return url.query_pairs().any(|(k, v)| {
            k == "sslmode" && matches!(v.as_ref(), "require" | "verify-ca" | "verify-full")
        });
    }
    // Fallback for non-URL connection strings: check key=value pairs
    connection_string
        .split(|c: char| c.is_whitespace())
        .filter_map(|part| part.split_once('='))
        .any(|(k, v)| k == "sslmode" && matches!(v, "require" | "verify-ca" | "verify-full"))
}

pub async fn validate_tables(client: &Client, tables: &[(&str, &str)]) -> Result<()> {
    for (schema, table) in tables {
        let query = r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name = $2
            )
        "#;

        let row = client
            .query_one(query, &[schema, table])
            .await
            .map_err(|e| {
                PgError::from_query_err(
                    format!(
                        "Failed to check if table {}.{} exists: {}",
                        schema, table, e
                    ),
                    &e,
                )
            })?;

        let exists: bool = row.get(0);
        if !exists {
            return Err(PgError::TableNotFound {
                schema: schema.to_string(),
                table: table.to_string(),
            });
        }

        let read_query = format!(
            "SELECT 1 FROM {}.{} LIMIT 1",
            quote_identifier(schema),
            quote_identifier(table)
        );

        client.query(&read_query, &[]).await.map_err(|e| {
            PgError::from_query_err(
                format!("Failed to read from table {}.{}: {}", schema, table, e),
                &e,
            )
        })?;
    }

    Ok(())
}

pub fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_identifier("simple"), "\"simple\"");
        assert_eq!(quote_identifier("with\"quote"), "\"with\"\"quote\"");
        assert_eq!(quote_identifier("CamelCase"), "\"CamelCase\"");
    }

    #[test]
    fn tls_required_for_sslmode_require() {
        assert!(requires_tls(
            "postgresql://user:pass@host/db?sslmode=require"
        ));
    }

    #[test]
    fn tls_required_for_verify_full() {
        assert!(requires_tls(
            "postgresql://user:pass@host/db?sslmode=verify-full"
        ));
    }

    #[test]
    fn tls_not_required_for_prefer() {
        assert!(!requires_tls(
            "postgresql://user:pass@host/db?sslmode=prefer"
        ));
    }

    #[test]
    fn tls_not_required_when_absent() {
        assert!(!requires_tls("postgresql://user:pass@host/db"));
    }

    #[test]
    fn tls_not_triggered_by_password_containing_sslmode() {
        assert!(!requires_tls("postgresql://user:sslmode=require@host/db"));
    }

    #[test]
    fn tls_with_key_value_format() {
        assert!(requires_tls(
            "host=db.example.com sslmode=require dbname=mydb"
        ));
    }

    #[test]
    fn tls_required_when_later_param_overrides_prefer() {
        assert!(requires_tls(
            "postgresql://user:pass@host/db?sslmode=prefer&sslmode=require"
        ));
    }
}
