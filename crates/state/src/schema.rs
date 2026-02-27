// @generated automatically by Diesel CLI.

diesel::table! {
    backfill_progress (config_name) {
        config_name -> Text,
        last_id -> Nullable<Text>,
        total_rows -> Nullable<BigInt>,
        processed_rows -> BigInt,
        status -> Text,
        started_at -> Nullable<Text>,
        completed_at -> Nullable<Text>,
        error_message -> Nullable<Text>,
        watermark_lsn -> Nullable<BigInt>,
    }
}

diesel::table! {
    configs (name) {
        name -> Text,
        namespace -> Text,
        content_hash -> Text,
        transform_hash -> Nullable<Text>,
        applied_at -> Text,
    }
}

diesel::table! {
    dlq (id) {
        id -> BigInt,
        config_name -> Text,
        lsn -> BigInt,
        event_json -> Text,
        doc_id -> Nullable<Text>,
        error_message -> Text,
        error_kind -> Text,
        retry_count -> Integer,
        created_at -> Text,
        last_retry_at -> Nullable<Text>,
        permanent_at -> Nullable<Text>,
    }
}

diesel::table! {
    streaming_checkpoints (config_name) {
        config_name -> Text,
        lsn -> BigInt,
        events_processed -> BigInt,
        updated_at -> Text,
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
