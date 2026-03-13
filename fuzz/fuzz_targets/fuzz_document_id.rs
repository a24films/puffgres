#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use puffgres_core::DocumentId;
use serde_json::Value;

#[derive(Debug, Arbitrary)]
enum FuzzIdType {
    Uint,
    Int,
    Uuid,
    String,
}

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    text: String,
    id_type: FuzzIdType,
}

impl FuzzInput {
    fn config_id_type(&self) -> config::IdType {
        match self.id_type {
            FuzzIdType::Uint => config::IdType::Uint,
            FuzzIdType::Int => config::IdType::Int,
            FuzzIdType::Uuid => config::IdType::Uuid,
            FuzzIdType::String => config::IdType::String,
        }
    }
}

fuzz_target!(|input: FuzzInput| {
    let id_type = input.config_id_type();

    // 1. from_text should never panic
    let result = DocumentId::from_text(&input.text, &id_type);

    // 2. If parsing succeeds, Display -> from_text should round-trip
    if let Ok(ref id) = result {
        let displayed = id.to_string();
        let reparsed = DocumentId::from_text(&displayed, &id_type);
        assert!(
            reparsed.is_ok(),
            "round-trip failed: {id:?} -> \"{displayed}\" -> {reparsed:?}"
        );
        assert_eq!(
            result.unwrap(),
            reparsed.unwrap(),
            "round-trip produced different value"
        );
    }

    // 3. from_value should never panic on arbitrary JSON values
    let json_values = [
        Value::String(input.text.clone()),
        Value::Null,
        Value::Bool(true),
        serde_json::json!(42),
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!(u64::MAX),
    ];
    for val in &json_values {
        let _ = DocumentId::from_value(val, &id_type);
    }
});
