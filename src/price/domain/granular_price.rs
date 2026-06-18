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
/// only Tier A source).
///
/// `qty` is the vendor's live buy quantity (Tier B, Card Kingdom direct);
/// `None` for sources that don't carry one (MTGJSON). Upserts write it
/// last-writer-wins (`qty = EXCLUDED.qty`), so it reads NULL ("unknown")
/// unless the most recent writer provided it -- a stale quantity is worse
/// than none for actionable offers.
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
            qty: None,
        })
    }

    /// Order rows by the storage index key (`card_id, provider, price_type,
    /// finish, condition, date`) so a bulk upsert inserts into the btree in key
    /// order - sequential leaf access instead of the random-UUID order the
    /// stream delivers, which is the dominant write cost on the large granular
    /// tables. `date` is constant within a daily run but is included so the
    /// historical (multi-date) pass is ordered too.
    pub fn sort_for_bulk_write(rows: &mut [GranularPrice]) {
        rows.sort_unstable_by(|a, b| {
            a.card_id
                .cmp(&b.card_id)
                .then_with(|| a.provider.cmp(&b.provider))
                .then_with(|| a.price_type.cmp(&b.price_type))
                .then_with(|| a.finish.cmp(&b.finish))
                .then_with(|| a.condition.cmp(&b.condition))
                .then_with(|| a.date.cmp(&b.date))
        });
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

    #[test]
    fn sort_for_bulk_write_orders_by_index_key() {
        let mk = |card: &str, provider: &str| {
            GranularPrice::new(
                card.to_string(),
                provider.to_string(),
                "retail".to_string(),
                "normal".to_string(),
                GranularPrice::DEFAULT_CONDITION.to_string(),
                date(),
                Decimal::ONE,
            )
            .unwrap()
        };
        let mut rows = vec![
            mk("c2", "tcgplayer"),
            mk("c1", "tcgplayer"),
            mk("c1", "cardkingdom"),
        ];
        GranularPrice::sort_for_bulk_write(&mut rows);
        let keys: Vec<(&str, &str)> = rows
            .iter()
            .map(|r| (r.card_id.as_str(), r.provider.as_str()))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("c1", "cardkingdom"),
                ("c1", "tcgplayer"),
                ("c2", "tcgplayer"),
            ]
        );
    }
}
