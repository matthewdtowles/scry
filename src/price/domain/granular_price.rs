use crate::price::domain::Price;
use anyhow::{bail, Result};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// A single price point as ingested, with no averaging: one provider, one
/// retail|buylist type, one finish, one condition, one day. Mirrors a
/// `granular_price` row in the web DB (owned there; scry only writes it).
///
/// `condition` is "NM" by convention for sources with no grade (MTGJSON); the
/// Card Kingdom direct feed (Tier B) supplies real conditions + `qty`.
#[derive(Clone, Debug, FromRow, Serialize, Deserialize, PartialEq)]
pub struct GranularPrice {
    pub card_id: String,
    pub provider: String,
    pub price_type: String,
    pub finish: String,
    pub condition: String,
    pub date: NaiveDate,
    pub price: Decimal,
    pub qty: Option<i32>,
}

impl GranularPrice {
    pub const DEFAULT_CONDITION: &str = "NM";

    pub fn new(
        card_id: String,
        provider: String,
        price_type: String,
        finish: String,
        condition: String,
        date: NaiveDate,
        price: Decimal,
        qty: Option<i32>,
    ) -> Result<Self> {
        if price < Decimal::ZERO {
            bail!("Granular price cannot be negative");
        }
        if let Some(q) = qty {
            if q < 0 {
                bail!("Granular price quantity cannot be negative");
            }
        }
        Ok(Self {
            card_id,
            provider,
            price_type,
            finish,
            condition,
            date,
            price,
            qty,
        })
    }
}

/// A card's full price contribution from one ingest pass: the granular rows for
/// every provider/type/finish, plus the derived averaged retail price that
/// feeds the existing `price` table. Both come from the same stream pass so the
/// `price` table stays exactly as before while the granular store fills.
#[derive(Clone, Debug, Default)]
pub struct CardPrices {
    pub average: Option<Price>,
    pub granular: Vec<GranularPrice>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
    }

    fn valid(price: Decimal, qty: Option<i32>) -> Result<GranularPrice> {
        GranularPrice::new(
            "card-123".to_string(),
            "cardkingdom".to_string(),
            "buylist".to_string(),
            "normal".to_string(),
            GranularPrice::DEFAULT_CONDITION.to_string(),
            date(),
            price,
            qty,
        )
    }

    #[test]
    fn test_new_valid() {
        // 3.50
        assert!(valid(Decimal::new(350, 2), Some(12)).is_ok());
    }

    #[test]
    fn test_new_zero_price_ok() {
        assert!(valid(Decimal::ZERO, None).is_ok());
    }

    #[test]
    fn test_new_negative_price_fails() {
        assert!(valid(Decimal::from(-1), None).is_err());
    }

    #[test]
    fn test_new_negative_qty_fails() {
        assert!(valid(Decimal::from(1), Some(-1)).is_err());
    }
}
