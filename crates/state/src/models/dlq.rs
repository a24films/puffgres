use diesel::prelude::*;

use crate::schema::dlq;

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = dlq)]
pub struct DlqRow {
    pub id: i64,
    pub config_name: String,
    pub lsn: i64,
    pub event_json: String,
    pub doc_id: Option<String>,
    pub error_message: String,
    pub error_kind: String,
    pub retry_count: i32,
    pub created_at: String,
    pub last_retry_at: Option<String>,
    pub permanent_at: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = dlq)]
pub struct NewDlqEntry<'a> {
    pub config_name: &'a str,
    pub lsn: i64,
    pub event_json: &'a str,
    pub doc_id: Option<&'a str>,
    pub error_message: &'a str,
    pub error_kind: &'a str,
    pub retry_count: i32,
    pub created_at: &'a str,
    pub last_retry_at: Option<&'a str>,
    pub permanent_at: Option<&'a str>,
}
