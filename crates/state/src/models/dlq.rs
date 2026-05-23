use diesel::prelude::*;

use crate::pg_lsn::Lsn;
use crate::schema::dlq;

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = dlq)]
pub struct DlqRow {
    pub id: i64,
    pub config_name: String,
    pub lsn: Lsn,
    pub doc_id: Option<String>,
    pub operation: Option<String>,
    pub error_message: String,
    pub error_kind: String,
    pub retry_count: i32,
    pub created_at: i64,
    pub last_retry_at: Option<i64>,
    pub permanent_at: Option<i64>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = dlq)]
pub struct NewDlqEntry<'a> {
    pub config_name: &'a str,
    pub lsn: Lsn,
    pub doc_id: Option<&'a str>,
    pub operation: Option<&'a str>,
    pub error_message: &'a str,
    pub error_kind: &'a str,
    pub retry_count: i32,
    pub created_at: i64,
    pub last_retry_at: Option<i64>,
    pub permanent_at: Option<i64>,
}
