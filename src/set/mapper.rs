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
