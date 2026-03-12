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

pub fn extract_date(value: &Value, key: &str) -> Result<NaiveDate> {
    let date_str = value
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing or invalid '{}' field", key))?;
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|e| anyhow::anyhow!("Invalid date for '{}': {}", key, e))
}
