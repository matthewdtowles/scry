use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow, Serialize, Deserialize)]
pub struct SetPrice {
    pub set_code: String,
    pub base_price: Decimal,
    pub total_price: Decimal,
    pub base_price_all: Decimal,
    pub total_price_all: Decimal,
    pub date: NaiveDate,
}
