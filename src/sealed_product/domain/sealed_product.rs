use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow, Serialize, Deserialize)]
pub struct SealedProduct {
    pub uuid: String,
    pub name: String,
    pub set_code: String,
    pub category: Option<String>,
    pub subtype: Option<String>,
    pub card_count: Option<i32>,
    pub product_size: Option<i32>,
    pub release_date: Option<NaiveDate>,
    pub contents_summary: Option<String>,
    pub tcgplayer_product_id: Option<String>,
}

impl SealedProduct {
    /// Filter out online-only products (MTGO/Arena redemption products within paper sets)
    pub fn is_online_only(&self) -> bool {
        let name_upper = self.name.to_uppercase();
        name_upper.contains("MTGO") || name_upper.contains("ARENA")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_product() -> SealedProduct {
        SealedProduct {
            uuid: "test-uuid".to_string(),
            name: "Draft Booster Box".to_string(),
            set_code: "blb".to_string(),
            category: Some("booster_box".to_string()),
            subtype: Some("draft".to_string()),
            card_count: Some(540),
            product_size: Some(36),
            release_date: NaiveDate::from_ymd_opt(2024, 8, 2),
            contents_summary: Some("36x Draft Booster Pack".to_string()),
            tcgplayer_product_id: Some("541185".to_string()),
        }
    }

    #[test]
    fn test_is_online_only_false_for_paper() {
        let product = create_test_product();
        assert!(!product.is_online_only());
    }

    #[test]
    fn test_is_online_only_true_for_mtgo() {
        let mut product = create_test_product();
        product.name = "Bloomburrow MTGO Redemption".to_string();
        assert!(product.is_online_only());
    }

    #[test]
    fn test_is_online_only_true_for_arena() {
        let mut product = create_test_product();
        product.name = "Arena Starter Kit".to_string();
        assert!(product.is_online_only());
    }
}
