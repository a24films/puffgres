use diesel::prelude::*;

use crate::schema::backfill_progress;

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = backfill_progress)]
pub struct BackfillProgressRow {
    pub config_name: String,
    pub last_id: Option<String>,
    pub total_rows: Option<i64>,
    pub processed_rows: i64,
    pub status: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub error_message: Option<String>,
    pub watermark_lsn: Option<i64>,
}

#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = backfill_progress)]
pub struct NewBackfillProgress<'a> {
    pub config_name: &'a str,
    pub last_id: Option<&'a str>,
    pub total_rows: Option<i64>,
    pub processed_rows: i64,
    pub status: &'a str,
    pub started_at: Option<&'a str>,
    pub completed_at: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub watermark_lsn: Option<i64>,
}
