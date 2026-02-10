use crate::PgError;

pub fn parse_lsn(s: &str) -> Result<u64, PgError> {
    let parts: Vec<&str> = s.split('/').collect();

    if parts.len() != 2 {
        return Err(PgError::ReplicationError(
            format!("Invalid LSN format: expected 'X/Y', got '{}'", s)
        ));
    }

    let upper = u32::from_str_radix(parts[0], 16)
        .map_err(|e| PgError::ReplicationError(
            format!("Failed to parse upper LSN component '{}': {}", parts[0], e)
        ))?;

    let lower = u32::from_str_radix(parts[1], 16)
        .map_err(|e| PgError::ReplicationError(
            format!("Failed to parse lower LSN component '{}': {}", parts[1], e)
        ))?;

    Ok(((upper as u64) << 32) | (lower as u64))
}

pub fn format_lsn(lsn: u64) -> String {
    let upper = (lsn >> 32) as u32;
    let lower = (lsn & 0xFFFFFFFF) as u32;
    format!("{:X}/{:X}", upper, lower)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_zero_lsn() {
        let lsn = parse_lsn("0/0").unwrap();
        assert_eq!(lsn, 0);
    }

    #[test]
    fn parse_simple_lsn() {
        let lsn = parse_lsn("0/16B3BE8").unwrap();
        assert_eq!(lsn, 23804904);
    }

    #[test]
    fn parse_lsn_with_upper_component() {
        let lsn = parse_lsn("1/0").unwrap();
        assert_eq!(lsn, 1_u64 << 32);
    }

    #[test]
    fn parse_lsn_with_both_components() {
        let lsn = parse_lsn("1/ABCD1234").unwrap();
        assert_eq!(lsn, (1_u64 << 32) | 0xABCD1234);
    }

    #[test]
    fn parse_max_lsn() {
        let lsn = parse_lsn("FFFFFFFF/FFFFFFFF").unwrap();
        assert_eq!(lsn, u64::MAX);
    }

    #[test]
    fn parse_lowercase_hex() {
        let lsn = parse_lsn("0/abcd1234").unwrap();
        assert_eq!(lsn, 0xABCD1234);
    }

    #[test]
    fn parse_mixed_case_hex() {
        let lsn = parse_lsn("AbC/DeF123").unwrap();
        assert_eq!(lsn, (0xABC_u64 << 32) | 0xDEF123);
    }

    #[test]
    fn parse_invalid_format_no_slash() {
        let result = parse_lsn("0");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid LSN format"));
    }

    #[test]
    fn parse_invalid_format_too_many_slashes() {
        let result = parse_lsn("0/1/2");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid LSN format"));
    }

    #[test]
    fn parse_invalid_hex_upper() {
        let result = parse_lsn("XYZ/0");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse upper LSN component"));
    }

    #[test]
    fn parse_invalid_hex_lower() {
        let result = parse_lsn("0/XYZ");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse lower LSN component"));
    }

    #[test]
    fn parse_empty_string() {
        let result = parse_lsn("");
        assert!(result.is_err());
    }

    #[test]
    fn format_zero_lsn() {
        assert_eq!(format_lsn(0), "0/0");
    }

    #[test]
    fn format_simple_lsn() {
        assert_eq!(format_lsn(23804904), "0/16B3BE8");
    }

    #[test]
    fn format_lsn_with_upper_component() {
        assert_eq!(format_lsn(1_u64 << 32), "1/0");
    }

    #[test]
    fn format_lsn_with_both_components() {
        let lsn = (1_u64 << 32) | 0xABCD1234;
        assert_eq!(format_lsn(lsn), "1/ABCD1234");
    }

    #[test]
    fn format_max_lsn() {
        assert_eq!(format_lsn(u64::MAX), "FFFFFFFF/FFFFFFFF");
    }

    #[test]
    fn round_trip_zero() {
        let original = "0/0";
        let lsn = parse_lsn(original).unwrap();
        let formatted = format_lsn(lsn);
        assert_eq!(formatted, "0/0");
    }

    #[test]
    fn round_trip_simple() {
        let lsn = parse_lsn("0/16B3BE8").unwrap();
        let formatted = format_lsn(lsn);
        let reparsed = parse_lsn(&formatted).unwrap();
        assert_eq!(lsn, reparsed);
    }

    #[test]
    fn round_trip_with_upper() {
        let lsn = parse_lsn("A1B2C3D4/E5F6A7B8").unwrap();
        let formatted = format_lsn(lsn);
        let reparsed = parse_lsn(&formatted).unwrap();
        assert_eq!(lsn, reparsed);
    }

    #[test]
    fn round_trip_max() {
        let lsn = parse_lsn("FFFFFFFF/FFFFFFFF").unwrap();
        let formatted = format_lsn(lsn);
        let reparsed = parse_lsn(&formatted).unwrap();
        assert_eq!(lsn, reparsed);
    }

    #[test]
    fn round_trip_preserves_uppercase() {
        let lsn = parse_lsn("abc/def").unwrap();
        let formatted = format_lsn(lsn);
        assert_eq!(formatted, "ABC/DEF");
    }
}
