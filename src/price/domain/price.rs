use anyhow::{bail, Result};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow, Serialize, Deserialize)]
pub struct Price {
    pub card_id: String,
    pub foil: Option<Decimal>,
    pub normal: Option<Decimal>,
    pub date: NaiveDate,
}

impl Price {
    /// Create a new Price with validation
    pub fn new(
        card_id: String,
        foil: Option<Decimal>,
        normal: Option<Decimal>,
        date: NaiveDate,
    ) -> Result<Self> {
        if foil.is_none() && normal.is_none() {
            bail!("Price must have at least one value (foil or normal)");
        }
        if let Some(f) = foil {
            if f < Decimal::ZERO {
                bail!("Foil price cannot be negative");
            }
        }
        if let Some(n) = normal {
            if n < Decimal::ZERO {
                bail!("Normal price cannot be negative");
            }
        }
        Ok(Self {
            card_id,
            foil,
            normal,
            date,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_valid_price() {
        let price = Price::new(
            "card-123".to_string(),
            Some(Decimal::from(10)),
            Some(Decimal::from(5)),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        );
        assert!(price.is_ok());
    }

    #[test]
    fn test_new_foil_only() {
        let price = Price::new(
            "card-123".to_string(),
            Some(Decimal::from(10)),
            None,
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        );
        assert!(price.is_ok());
    }

    #[test]
    fn test_new_normal_only() {
        let price = Price::new(
            "card-123".to_string(),
            None,
            Some(Decimal::from(5)),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        );
        assert!(price.is_ok());
    }

    #[test]
    fn test_new_no_prices_fails() {
        let price = Price::new(
            "card-123".to_string(),
            None,
            None,
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        );
        assert!(price.is_err());
    }

    #[test]
    fn test_new_negative_foil_fails() {
        let price = Price::new(
            "card-123".to_string(),
            Some(Decimal::from(-10)),
            Some(Decimal::from(5)),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        );
        assert!(price.is_err());
    }
}
