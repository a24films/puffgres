use chrono::{DateTime, TimeZone, Utc};

/// Convert a `DateTime<Utc>` to epoch milliseconds.
pub fn to_millis(dt: &DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

/// Convert epoch milliseconds to `DateTime<Utc>`.
/// Returns `None` if the value is out of range.
pub fn from_millis(millis: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(millis).single()
}
