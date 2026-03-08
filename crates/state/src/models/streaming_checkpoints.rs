use diesel::prelude::*;

use crate::schema::streaming_checkpoints;

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = streaming_checkpoints)]
pub struct StreamingCheckpointRow {
    pub config_name: String,
    pub lsn: i64,
    pub events_processed: i64,
    pub updated_at: i64,
}

#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = streaming_checkpoints)]
pub struct NewStreamingCheckpoint<'a> {
    pub config_name: &'a str,
    pub lsn: i64,
    pub events_processed: i64,
    pub updated_at: i64,
}
