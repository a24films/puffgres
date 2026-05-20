use diesel::prelude::*;

use crate::pg_lsn::Lsn;
use crate::schema::backfill_progress;

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = backfill_progress)]
pub struct BackfillProgressRow {
    pub config_name: String,
    pub last_id: Option<String>,
    pub total_rows: Option<i64>,
    pub processed_rows: i64,
    pub status: String,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub error_message: Option<String>,
    pub watermark_lsn: Option<Lsn>,
}

#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = backfill_progress)]
pub struct NewBackfillProgress<'a> {
    pub config_name: &'a str,
    pub last_id: Option<&'a str>,
    pub total_rows: Option<i64>,
    pub processed_rows: i64,
    pub status: &'a str,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub error_message: Option<&'a str>,
    pub watermark_lsn: Option<Lsn>,
}
