use anyhow::Result;
use chrono::NaiveDate;
use serde_json::Value;

pub fn extract_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing or invalid '{}' field", key))
}

pub fn extract_optional_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub fn extract_optional_date(value: &Value, key: &str) -> Option<NaiveDate> {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
}

pub fn extract_date(value: &Value, key: &str) -> Result<NaiveDate> {
    let date_str = value
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing or invalid '{}' field", key))?;
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|e| anyhow::anyhow!("Invalid date for '{}': {}", key, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_string_valid() {
        let val = json!({"name": "Lightning Bolt"});
        assert_eq!(extract_string(&val, "name").unwrap(), "Lightning Bolt");
    }

    #[test]
    fn test_extract_string_missing() {
        let val = json!({"other": "value"});
        assert!(extract_string(&val, "name").is_err());
    }

    #[test]
    fn test_extract_optional_string_present() {
        let val = json!({"artist": "Chris Rahn"});
        assert_eq!(
            extract_optional_string(&val, "artist"),
            Some("Chris Rahn".to_string())
        );
    }

    #[test]
    fn test_extract_optional_string_missing() {
        let val = json!({"other": "value"});
        assert_eq!(extract_optional_string(&val, "artist"), None);
    }

    #[test]
    fn test_extract_date_valid() {
        let val = json!({"releaseDate": "2024-01-15"});
        let date = extract_date(&val, "releaseDate").unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    }

    #[test]
    fn test_extract_date_invalid() {
        let val = json!({"releaseDate": "not-a-date"});
        assert!(extract_date(&val, "releaseDate").is_err());
    }

    #[test]
    fn test_extract_date_missing() {
        let val = json!({"other": "value"});
        assert!(extract_date(&val, "releaseDate").is_err());
    }
}
