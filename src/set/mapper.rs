use crate::{set::domain::Set, utils::json};
use anyhow::Result;
use serde_json::Value;

pub struct SetMapper;

impl SetMapper {
    pub fn map_mtg_json_to_set(set_data: &Value) -> Result<Set> {
        let code = json::extract_string(set_data, "code")?.to_lowercase();
        let block = json::extract_optional_string(set_data, "block");
        let keyrune_code = json::extract_string(set_data, "keyruneCode")?.to_lowercase();
        let name = json::extract_string(set_data, "name")?;
        let parent_code = match json::extract_optional_string(set_data, "parentCode") {
            Some(pc) => Some(pc.to_lowercase()),
            None => None,
        };
        let release_date = json::extract_date(set_data, "releaseDate")?;
        let set_type = json::extract_string(set_data, "type")?;
        let is_online_only = set_data
            .get("isOnlineOnly")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let is_foreign_only = set_data
            .get("isForeignOnly")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // sizes updated after ingestion, during transformation
        Ok(Set {
            code,
            base_size: 0,
            block,
            is_foreign_only,
            is_main: true,
            is_online_only,
            keyrune_code,
            name,
            parent_code,
            release_date,
            set_type,
            total_size: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use serde_json::json;

    fn create_valid_set_json() -> Value {
        json!({
            "code": "LEA",
            "keyruneCode": "LEA",
            "name": "Limited Edition Alpha",
            "releaseDate": "1993-08-05",
            "type": "core",
            "block": "Core Set",
            "parentCode": "LEA",
            "isOnlineOnly": false,
            "isForeignOnly": false
        })
    }

    #[test]
    fn test_map_mtg_json_to_set() {
        let json = create_valid_set_json();
        let set = SetMapper::map_mtg_json_to_set(&json).unwrap();
        assert_eq!(set.code, "lea");
        assert_eq!(set.name, "Limited Edition Alpha");
        assert_eq!(set.keyrune_code, "lea");
        assert_eq!(set.set_type, "core");
        assert_eq!(set.block, Some("Core Set".to_string()));
        assert_eq!(set.parent_code, Some("lea".to_string()));
        assert_eq!(
            set.release_date,
            NaiveDate::from_ymd_opt(1993, 8, 5).unwrap()
        );
        assert!(!set.is_online_only);
        assert!(!set.is_foreign_only);
        assert_eq!(set.base_size, 0);
        assert_eq!(set.total_size, 0);
    }

    #[test]
    fn test_map_mtg_json_to_set_missing_field_fails() {
        let json = json!({
            "code": "TST",
            "name": "Test Set"
            // missing keyruneCode, releaseDate, type
        });
        assert!(SetMapper::map_mtg_json_to_set(&json).is_err());
    }
}
