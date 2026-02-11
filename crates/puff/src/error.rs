use thiserror::Error;

#[derive(Debug, Error)]
pub enum PuffError {
    #[error("client error: {0}")]
    Client(String),

    #[error("api error (status {status}): {message}")]
    Api { status: u16, message: String },

    #[error("http error: {0}")]
    Http(String),

    #[error("json error: {0}")]
    Json(String),
}

impl From<rs_puff::Error> for PuffError {
    fn from(err: rs_puff::Error) -> Self {
        match err {
            rs_puff::Error::Api { status, message } => PuffError::Api { status, message },
            rs_puff::Error::Http(e) => PuffError::Http(e.to_string()),
            rs_puff::Error::Json(e) => PuffError::Json(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_error_display() {
        let err = PuffError::Client("bad config".to_string());
        assert_eq!(err.to_string(), "client error: bad config");
    }

    #[test]
    fn api_error_display() {
        let err = PuffError::Api {
            status: 404,
            message: "not found".to_string(),
        };
        assert_eq!(err.to_string(), "api error (status 404): not found");
    }

    #[test]
    fn http_error_display() {
        let err = PuffError::Http("connection refused".to_string());
        assert_eq!(err.to_string(), "http error: connection refused");
    }

    #[test]
    fn json_error_display() {
        let err = PuffError::Json("invalid json".to_string());
        assert_eq!(err.to_string(), "json error: invalid json");
    }
}
