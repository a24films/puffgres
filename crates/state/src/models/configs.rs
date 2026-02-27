use diesel::prelude::*;

use crate::schema::configs;

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = configs)]
pub struct ConfigRow {
    pub name: String,
    pub version: i64,
    pub namespace: String,
    pub content_hash: String,
    pub transform_hash: Option<String>,
    pub applied_at: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = configs)]
pub struct NewConfig<'a> {
    pub name: &'a str,
    pub version: i64,
    pub namespace: &'a str,
    pub content_hash: &'a str,
    pub transform_hash: Option<&'a str>,
    pub applied_at: &'a str,
}
