use crate::price::domain::Price;
use anyhow::Result;
use chrono::NaiveDate;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tracing::warn;

/// Accumulates prices from multiple providers to calculate averages
///
/// Used during price ingestion to aggregate data from TCGPlayer, Card Kingdom, etc.
#[derive(Debug, Default)]
pub struct PriceAccumulator {
    foil_sum: f64,
    foil_count: usize,
    normal_sum: f64,
    normal_count: usize,
    date: Option<String>,
}

impl PriceAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a foil price to the accumulator
    pub fn add_foil(&mut self, value: f64) {
        self.foil_sum += value;
        self.foil_count += 1;
    }

    /// Add a normal price to the accumulator
    pub fn add_normal(&mut self, value: f64) {
        self.normal_sum += value;
        self.normal_count += 1;
    }

    /// Set the price date (from provider data)
    pub fn set_date(&mut self, date: String) {
        self.date = Some(date);
    }

    /// Calculate average foil price
    pub fn average_foil(&self) -> Option<f64> {
        if self.foil_count > 0 {
            Some(self.foil_sum / self.foil_count as f64)
        } else {
            None
        }
    }

    /// Calculate average normal price
    pub fn average_normal(&self) -> Option<f64> {
        if self.normal_count > 0 {
            Some(self.normal_sum / self.normal_count as f64)
        } else {
            None
        }
    }

    /// Convert accumulated data into a Price entity.
    ///
    /// Returns an error (which callers skip) when no valid provider date was
    /// accumulated, rather than dating the row `1970-01-01` — epoch rows poison
    /// `clean_up_prices` and `price_history` retention.
    pub fn into_price(self, card_id: String) -> Result<Price> {
        let Some(date) = self
            .date
            .as_deref()
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        else {
            warn!(
                "Skipping price for card {}: missing or unparseable date {:?}",
                card_id, self.date
            );
            return Err(anyhow::anyhow!("no valid price date for card {card_id}"));
        };
        let foil = self.average_foil().and_then(Decimal::from_f64);
        let normal = self.average_normal().and_then(Decimal::from_f64);
        Price::new(card_id, foil, normal, date)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulator_foil_only() {
        let mut acc = PriceAccumulator::new();
        acc.add_foil(10.0);
        acc.add_foil(12.0);
        acc.add_foil(14.0);

        assert_eq!(acc.average_foil(), Some(12.0));
        assert_eq!(acc.average_normal(), None);
    }

    #[test]
    fn test_accumulator_both_prices() {
        let mut acc = PriceAccumulator::new();
        acc.add_foil(10.0);
        acc.add_foil(12.0);
        acc.add_normal(5.0);
        acc.add_normal(7.0);

        assert_eq!(acc.average_foil(), Some(11.0));
        assert_eq!(acc.average_normal(), Some(6.0));
    }

    #[test]
    fn test_accumulator_empty() {
        let acc = PriceAccumulator::new();
        assert_eq!(acc.average_foil(), None);
        assert_eq!(acc.average_normal(), None);
    }

    #[test]
    fn test_into_price_valid() {
        let mut acc = PriceAccumulator::new();
        acc.add_foil(10.0);
        acc.set_date("2024-01-15".to_string());

        let price = acc.into_price("card-123".to_string()).unwrap();
        assert_eq!(price.card_id, "card-123");
        assert!(price.foil.is_some());
        assert_eq!(price.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    }

    #[test]
    fn test_into_price_no_prices_fails() {
        let acc = PriceAccumulator::new();
        let result = acc.into_price("card-123".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_into_price_missing_date_skips() {
        // Has a price but no provider date — must not fall back to epoch.
        let mut acc = PriceAccumulator::new();
        acc.add_normal(5.0);
        assert!(acc.into_price("card-123".to_string()).is_err());
    }

    #[test]
    fn test_into_price_unparseable_date_skips() {
        let mut acc = PriceAccumulator::new();
        acc.add_normal(5.0);
        acc.set_date("not-a-date".to_string());
        assert!(acc.into_price("card-123".to_string()).is_err());
    }
}
