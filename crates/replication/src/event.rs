use bytes::Bytes;

// In the original implementation, we would parse replication stream values in all cases.
// This meant, for every wire operation, we'd do a bunch of work, primarily copying WAL
// bytes to a String and parsing them into int/float/bool based on PG column type. In
// essence, we were doing a bunch of decoding on events we didn't even end up consuming
// ultimately. Instead of doing this, we now keep a reference into the original WAL buffer
// and move the pointer forward when the decoder needs a message. At scale there can be
// thousands of rows per second, so we create a ton of garbage with the initial approach.
// This optimization was not my idea! Rather, I got it from looking at how pg-walstream
// (github.com/isdaniel/pg-walstream) handles this case. It's licensed under BSD-3, so
// was totally fine to use the same pattern, and it's rewritten here.

/// A single column value from a WAL tuple.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    /// Column is SQL NULL.
    Null,
    /// Column was not modified (unchanged TOAST value). Only appears in UPDATE
    /// old-tuples when the column is stored out-of-line and wasn't touched.
    Unchanged,
    /// Text-format value — the normal case for pgoutput logical replication.
    Text(Bytes),
    /// Binary-format value (requires `binary = true` on the publication, PG 14+).
    Binary(Bytes),
}

impl ColumnValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn is_unchanged(&self) -> bool {
        matches!(self, Self::Unchanged)
    }

    /// Returns the raw bytes if this is a Text or Binary value.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Text(b) | Self::Binary(b) => Some(b),
            _ => None,
        }
    }
}

/// A complete row of column values from a WAL message.
#[derive(Debug, Clone)]
pub struct TupleData {
    pub columns: Vec<ColumnValue>,
}

/// The type of DML operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

/// A decoded row-level change event.
#[derive(Debug, Clone)]
pub struct RowEvent {
    pub relation_id: u32,
    pub operation: Operation,
    /// New tuple data (present for Insert and Update).
    pub new_tuple: Option<TupleData>,
    /// Old tuple data (present for Delete; present for Update when
    /// REPLICA IDENTITY is FULL or when key columns are sent).
    pub old_tuple: Option<TupleData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_value_null() {
        let col = ColumnValue::Null;
        assert!(col.is_null());
        assert!(!col.is_unchanged());
        assert!(col.as_bytes().is_none());
    }

    #[test]
    fn column_value_text() {
        let col = ColumnValue::Text(Bytes::from_static(b"hello"));
        assert!(!col.is_null());
        assert_eq!(col.as_bytes(), Some(b"hello" as &[u8]));
    }

    #[test]
    fn column_value_binary() {
        let col = ColumnValue::Binary(Bytes::from_static(b"\x00\x01\x02"));
        assert_eq!(col.as_bytes().unwrap().len(), 3);
    }

    #[test]
    fn column_value_unchanged() {
        let col = ColumnValue::Unchanged;
        assert!(col.is_unchanged());
        assert!(col.as_bytes().is_none());
    }

    #[test]
    fn operation_equality() {
        assert_eq!(Operation::Insert, Operation::Insert);
        assert_ne!(Operation::Insert, Operation::Delete);
    }

    #[test]
    fn row_event_insert() {
        let event = RowEvent {
            relation_id: 16384,
            operation: Operation::Insert,
            new_tuple: Some(TupleData {
                columns: vec![ColumnValue::Text(Bytes::from_static(b"1"))],
            }),
            old_tuple: None,
        };
        assert_eq!(event.operation, Operation::Insert);
        assert!(event.new_tuple.is_some());
        assert!(event.old_tuple.is_none());
    }
}
