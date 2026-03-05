use std::sync::Arc;

use rustls::ClientConfig;
use tokio_postgres::Client;
use tokio_postgres_rustls_improved::MakeRustlsConnect;

use crate::{PgError, Result};

pub async fn connect(connection_string: &str) -> Result<Client> {
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

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(
                    error = %e,
                    error_debug = ?e,
                    "postgres connection error",
                );
            }
        });

        Ok(client)
    } else {
        let (client, connection) =
            tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
                .await
                .map_err(|e| PgError::ConnectionError(format!("Failed to connect: {}", e)))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(
                    error = %e,
                    error_debug = ?e,
                    "postgres connection error",
                );
            }
        });

        Ok(client)
    }
}

fn root_certs() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    roots
}

fn requires_tls(connection_string: &str) -> bool {
    connection_string.contains("sslmode=require")
        || connection_string.contains("sslmode=verify-ca")
        || connection_string.contains("sslmode=verify-full")
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
                PgError::QueryError(format!(
                    "Failed to check if table {}.{} exists: {}",
                    schema, table, e
                ))
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
            PgError::QueryError(format!(
                "Failed to read from table {}.{}: {}",
                schema, table, e
            ))
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
}
