use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow, Serialize, Deserialize)]
pub struct SealedProductPrice {
    pub sealed_product_uuid: String,
    pub price: Decimal,
    pub date: NaiveDate,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_sealed_product_price() {
        let price = SealedProductPrice {
            sealed_product_uuid: "test-uuid".to_string(),
            price: Decimal::new(9999, 2),
            date: NaiveDate::from_ymd_opt(2024, 8, 2).unwrap(),
        };
        assert_eq!(price.sealed_product_uuid, "test-uuid");
        assert_eq!(price.price, Decimal::new(9999, 2));
    }
}
