//! Realistic fake data for dry-running transforms in `puffgres check`.
//!
//! Built on the [`fake`] crate. Each value is seeded from the column name, so a
//! column always gets the same value across runs (stable output, testable)
//! while neighbouring columns differ. Values are rendered the way Postgres
//! emits them with a `::text` cast — which is exactly what the generated
//! `parseRow` expects.
//!
//! Postgres types are classified with the same [`tpuf_scalar_type`] mapping the
//! schema generator uses, so there's a single source of truth for "what kind of
//! value is this column" rather than a second list of type names here.

use config::IdType;
use fake::Fake;
use fake::faker::company::en::CompanyName;
use fake::faker::lorem::en::Words;
use fake::rand::SeedableRng;
use fake::rand::rngs::StdRng;
use fake::uuid::UUIDv4;

use crate::generate::tpuf_scalar_type;

/// Seed a deterministic RNG from the column name via FNV-1a, so the same column
/// always produces the same fake value.
fn rng(column: &str) -> StdRng {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in column.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    StdRng::seed_from_u64(h)
}

/// Generate a fake id value that parses under the configured [`IdType`].
pub fn fake_id(id_type: &IdType, column: &str) -> String {
    let mut r = rng(column);
    match id_type {
        IdType::Uint | IdType::Int => (1i64..1_000_000)
            .fake_with_rng::<i64, _>(&mut r)
            .to_string(),
        IdType::Uuid | IdType::String => UUIDv4.fake_with_rng::<String, _>(&mut r),
    }
}

/// Generate a fake `::text` value for a column of the given Postgres type.
///
/// `column` is the column name, used only to seed the value. `udt_name` is the
/// resolved Postgres type (e.g. `int4`, `uuid`, `text[]`).
pub fn fake_value(column: &str, udt_name: &str) -> String {
    if let Some(element) = udt_name.strip_suffix("[]") {
        // Postgres array literal: numbers/bools/json are bare, text is quoted.
        let a = fake_scalar(&format!("{column}.0"), element);
        let b = fake_scalar(&format!("{column}.1"), element);
        return match tpuf_scalar_type(element) {
            "string" | "uuid" => format!("{{\"{a}\",\"{b}\"}}"),
            _ => format!("{{{a},{b}}}"),
        };
    }
    fake_scalar(column, udt_name)
}

fn fake_scalar(column: &str, udt_name: &str) -> String {
    let mut r = rng(column);
    match tpuf_scalar_type(udt_name) {
        "int" => (0i64..1000).fake_with_rng::<i64, _>(&mut r).to_string(),
        "float" => format!("{:.2}", (0.0f64..10_000.0).fake_with_rng::<f64, _>(&mut r)),
        "bool" => fake::Faker.fake_with_rng::<bool, _>(&mut r).to_string(),
        "uuid" => UUIDv4.fake_with_rng::<String, _>(&mut r),
        "json" => {
            let name = CompanyName().fake_with_rng::<String, _>(&mut r);
            serde_json::json!({ "name": name }).to_string()
        }
        // "string" and anything unrecognised.
        _ => Words(2..4)
            .fake_with_rng::<Vec<String>, _>(&mut r)
            .join(" "),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use puffgres_core::DocumentId;

    #[test]
    fn deterministic() {
        assert_eq!(
            fake_value("buyer_name", "text"),
            fake_value("buyer_name", "text")
        );
        assert_eq!(fake_id(&IdType::Uint, "id"), fake_id(&IdType::Uint, "id"));
    }

    #[test]
    fn distinct_columns_differ() {
        assert_ne!(fake_value("first", "text"), fake_value("second", "text"));
    }

    #[test]
    fn fake_ids_parse_for_their_type() {
        for ty in [IdType::Uint, IdType::Int, IdType::Uuid, IdType::String] {
            let v = fake_id(&ty, "id");
            assert!(
                DocumentId::from_text(&v, &ty).is_ok(),
                "{ty:?} produced unparseable id {v:?}"
            );
        }
    }

    #[test]
    fn integers_parse() {
        // resolve_column_info yields catalog names (int2/int4/int8), not aliases.
        for ty in ["int2", "int4", "int8"] {
            assert!(fake_value("c", ty).parse::<i64>().is_ok(), "type {ty}");
        }
    }

    #[test]
    fn floats_parse() {
        for ty in ["float4", "float8", "numeric"] {
            assert!(fake_value("c", ty).parse::<f64>().is_ok(), "type {ty}");
        }
    }

    #[test]
    fn bool_is_true_or_false() {
        let v = fake_value("flag", "bool");
        assert!(v == "true" || v == "false");
    }

    #[test]
    fn uuid_is_valid() {
        let v = fake_value("c", "uuid");
        assert!(DocumentId::from_text(&v, &IdType::Uuid).is_ok(), "got {v}");
    }

    #[test]
    fn json_is_valid() {
        let v = fake_value("c", "jsonb");
        assert!(
            serde_json::from_str::<serde_json::Value>(&v).is_ok(),
            "got {v}"
        );
    }

    #[test]
    fn array_is_braced() {
        let v = fake_value("tags", "text[]");
        assert!(v.starts_with('{') && v.ends_with('}'), "got {v}");
    }
}
