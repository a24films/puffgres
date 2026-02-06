use tokio_postgres::{Client, NoTls};

use crate::{PgError, Result};

pub async fn connect(connection_string: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
        .await
        .map_err(|e| PgError::ConnectionError(format!("Failed to connect: {}", e)))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
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
            return Err(PgError::QueryError(format!(
                "Table {}.{} does not exist",
                schema, table
            )));
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

fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("simple"), "\"simple\"");
        assert_eq!(quote_identifier("with\"quote"), "\"with\"\"quote\"");
        assert_eq!(quote_identifier("CamelCase"), "\"CamelCase\"");
    }
}
