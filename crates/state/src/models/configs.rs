use diesel::prelude::*;

use crate::schema::configs;

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = configs)]
pub struct ConfigRow {
    pub name: String,
    pub namespace: String,
    pub content_hash: String,
    pub transform_hash: Option<String>,
    pub applied_at: String,
    pub tombstone_applied_at: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = configs)]
pub struct NewConfig<'a> {
    pub name: &'a str,
    pub namespace: &'a str,
    pub content_hash: &'a str,
    pub transform_hash: Option<&'a str>,
    pub applied_at: &'a str,
    pub tombstone_applied_at: Option<&'a str>,
}
