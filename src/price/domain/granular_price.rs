use crate::price::domain::Price;
use anyhow::{bail, Result};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// A single price point as ingested, with no averaging: one provider, one
/// retail|buylist type, one finish, one condition, one day. Mirrors a row in
/// the web DB's `granular_price` (current per-vendor offer) and
/// `granular_price_history` (dated series) — owned there; scry writes and
/// prunes both.
///
/// `condition` is "NM" by convention for sources with no grade (MTGJSON, the
/// only Tier A source). Buy quantity and real conditions arrive with Tier B
/// (Card Kingdom direct) and are modelled then, not carried here.
#[derive(Clone, Debug, FromRow, Serialize, Deserialize, PartialEq)]
pub struct GranularPrice {
    pub card_id: String,
    pub provider: String,
    pub price_type: String,
    pub finish: String,
    pub condition: String,
    pub date: NaiveDate,
    pub price: Decimal,
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
    ) -> Result<Self> {
        if price < Decimal::ZERO {
            bail!("Granular price cannot be negative");
        }
        Ok(Self {
            card_id,
            provider,
            price_type,
            finish,
            condition,
            date,
            price,
        })
    }
}

/// A card's full price contribution from one stream pass: the granular rows for
/// every provider/type/finish/date, plus the derived averaged retail price(s)
/// that feed the existing `price`/`price_history` tables. Both come from the
/// same pass so those tables stay exactly as before while the granular store
/// fills. `averages` holds one entry per date — 0 or 1 for today's ingest, many
/// for the historical (multi-date) ingest.
#[derive(Clone, Debug, Default)]
pub struct CardPrices {
    pub averages: Vec<Price>,
    pub granular: Vec<GranularPrice>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
    }

    fn valid(price: Decimal) -> Result<GranularPrice> {
        GranularPrice::new(
            "card-123".to_string(),
            "cardkingdom".to_string(),
            "buylist".to_string(),
            "normal".to_string(),
            GranularPrice::DEFAULT_CONDITION.to_string(),
            date(),
            price,
        )
    }

    #[test]
    fn test_new_valid() {
        // 3.50
        assert!(valid(Decimal::new(350, 2)).is_ok());
    }

    #[test]
    fn test_new_zero_price_ok() {
        assert!(valid(Decimal::ZERO).is_ok());
    }

    #[test]
    fn test_new_negative_price_fails() {
        assert!(valid(Decimal::from(-1)).is_err());
    }
}
