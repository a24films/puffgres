// Hand-maintained — references the custom pg_lsn SQL type, so diesel print-schema cannot regenerate this file.

diesel::table! {
    use diesel::sql_types::*;
    use crate::pg_lsn::PgLsn;

    backfill_progress (config_name) {
        config_name -> Text,
        last_id -> Nullable<Text>,
        total_rows -> Nullable<BigInt>,
        processed_rows -> BigInt,
        status -> Text,
        started_at -> Nullable<BigInt>,
        completed_at -> Nullable<BigInt>,
        error_message -> Nullable<Text>,
        watermark_lsn -> Nullable<PgLsn>,
    }
}

diesel::table! {
    configs (name) {
        name -> Text,
        namespace -> Text,
        content_hash -> Text,
        transform_hash -> Nullable<Text>,
        applied_at -> BigInt,
        tombstone_applied_at -> Nullable<BigInt>,
        namespace_prefix -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::pg_lsn::PgLsn;

    dlq (id) {
        id -> BigInt,
        config_name -> Text,
        lsn -> PgLsn,
        doc_id -> Nullable<Text>,
        operation -> Nullable<Text>,
        error_message -> Text,
        error_kind -> Text,
        retry_count -> Integer,
        created_at -> BigInt,
        last_retry_at -> Nullable<BigInt>,
        permanent_at -> Nullable<BigInt>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::pg_lsn::PgLsn;

    streaming_checkpoints (config_name) {
        config_name -> Text,
        lsn -> PgLsn,
        events_processed -> BigInt,
        updated_at -> BigInt,
    }
}

diesel::joinable!(backfill_progress -> configs (config_name));
diesel::joinable!(dlq -> configs (config_name));
diesel::joinable!(streaming_checkpoints -> configs (config_name));

diesel::allow_tables_to_appear_in_same_query!(
    backfill_progress,
    configs,
    dlq,
    streaming_checkpoints,
);
