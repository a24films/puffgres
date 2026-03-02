use crate::Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

impl Config {
    /// Validate config structure (no DB connection needed)
    pub fn validate(&self) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        if !is_valid_identifier(&self.name) {
            errors.push(ValidationError {
                field: "name".into(),
                message: "Name must be valid (alphanumeric, underscore)".into(),
            });
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

fn is_valid_identifier(s: &str) -> bool {
    s.chars().next().map_or(false, |first| {
        !first.is_numeric() && (first.is_alphanumeric() || first == '_')
    }) && s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_fixture(name: &str) -> Config {
        let path = format!("tests/fixtures/{}.toml", name);
        toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    #[test]
    fn test_valid_config_passes() {
        let config = load_fixture("valid");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_name_fails() {
        let config = load_fixture("invalid_name");
        let errors = config.validate().unwrap_err();

        assert!(errors.iter().any(|e| e.field == "name"));
    }

    #[test]
    fn test_is_valid_identifier() {
        // Valid
        assert!(is_valid_identifier("user_0001"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("CamelCase"));

        // Invalid
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("123invalid"));
        assert!(!is_valid_identifier("invalid-name"));
    }
}
