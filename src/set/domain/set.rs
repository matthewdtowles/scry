use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow, Serialize, Deserialize)]
pub struct Set {
    pub code: String,
    pub base_size: i32,
    pub block: Option<String>,
    pub keyrune_code: String,
    pub name: String,
    pub parent_code: Option<String>,
    pub release_date: NaiveDate,

    #[sqlx(rename = "type")]
    pub set_type: String,
    pub total_size: i32,

    #[sqlx(default)]
    pub is_main: bool,

    #[sqlx(skip)]
    pub is_online_only: bool,
    #[sqlx(skip)]
    pub is_foreign_only: bool,
}

impl Set {
    /// Should this set be filtered out of inclusion in DB?
    pub fn should_filter(&self) -> bool {
        if self.is_online_only || self.is_foreign_only || self.set_type == "memorabilia" {
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_set() -> Set {
        Set {
            code: "tst".to_string(),
            base_size: 0,
            block: Some("tst".to_string()),
            keyrune_code: "tst".to_string(),
            name: "Test Set".to_string(),
            parent_code: Some("tst".to_string()),
            is_foreign_only: false,
            is_main: true,
            is_online_only: false,
            release_date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            set_type: "core".to_string(),
            total_size: 0,
        }
    }

    #[test]
    fn test_should_filter_online() {
        let mut set: Set = create_test_set();
        assert!(!set.should_filter());

        set.is_online_only = true;
        assert!(set.should_filter());
    }

    #[test]
    fn test_should_filter_foreign() {
        let mut set: Set = create_test_set();
        assert!(!set.should_filter());

        set.is_foreign_only = true;
        assert!(set.should_filter());
    }

    #[test]
    fn test_should_filter_memorabilia() {
        let mut set: Set = create_test_set();
        assert!(!set.should_filter());

        set.set_type = "memorabilia".to_string();
        assert!(set.should_filter());
    }
}
