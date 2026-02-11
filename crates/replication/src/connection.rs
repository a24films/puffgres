use crate::{ReplicationError, Result};

pub(crate) struct ParsedConnection {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub sslmode: Option<String>,
}

pub(crate) fn parse_connection_string(s: &str) -> Result<ParsedConnection> {
    let url = url::Url::parse(s)
        .map_err(|e| ReplicationError::Connection(format!("invalid connection string: {e}")))?;

    let scheme = url.scheme();
    if scheme != "postgresql" && scheme != "postgres" {
        return Err(ReplicationError::Connection(format!(
            "unsupported scheme: {scheme} (expected postgresql:// or postgres://)"
        )));
    }

    let user = url.username();

    let sslmode = url
        .query_pairs()
        .find(|(k, _)| k == "sslmode")
        .map(|(_, v)| v.to_string());

    Ok(ParsedConnection {
        host: url.host_str().unwrap_or("localhost").to_string(),
        port: url.port().unwrap_or(5432),
        user: if user.is_empty() {
            "postgres".to_string()
        } else {
            user.to_string()
        },
        password: url.password().unwrap_or("").to_string(),
        database: {
            let path = url.path().trim_start_matches('/');
            if path.is_empty() {
                "postgres".to_string()
            } else {
                path.to_string()
            }
        },
        sslmode,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_url() {
        let parsed = parse_connection_string("postgresql://user:pass@myhost:5433/mydb").unwrap();
        assert_eq!(parsed.host, "myhost");
        assert_eq!(parsed.port, 5433);
        assert_eq!(parsed.user, "user");
        assert_eq!(parsed.password, "pass");
        assert_eq!(parsed.database, "mydb");
    }

    #[test]
    fn parse_minimal_url() {
        let parsed = parse_connection_string("postgresql://localhost/mydb").unwrap();
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 5432);
        assert_eq!(parsed.user, "postgres");
        assert_eq!(parsed.password, "");
        assert_eq!(parsed.database, "mydb");
    }

    #[test]
    fn parse_postgres_scheme() {
        let parsed = parse_connection_string("postgres://user:pw@host:5432/db").unwrap();
        assert_eq!(parsed.host, "host");
        assert_eq!(parsed.user, "user");
    }

    #[test]
    fn parse_defaults() {
        let parsed = parse_connection_string("postgresql://localhost").unwrap();
        assert_eq!(parsed.port, 5432);
        assert_eq!(parsed.user, "postgres");
        assert_eq!(parsed.database, "postgres");
    }

    #[test]
    fn reject_bad_scheme() {
        assert!(parse_connection_string("http://localhost/db").is_err());
    }

    #[test]
    fn reject_invalid_url() {
        assert!(parse_connection_string("not a url").is_err());
    }
}
