use anyhow::{bail, Result};
use chrono::{Duration, NaiveDate, Timelike};
use chrono_tz::America::New_York;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow, Serialize, Deserialize)]
pub struct Price {
    pub id: Option<i32>,
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
            id: None,
            card_id,
            foil,
            normal,
            date,
        })
    }

    /// Calculate the expected latest available price date
    ///
    /// MTG JSON updates prices at 10 AM EST. Before that time, yesterday's
    /// prices are the latest available.
    pub fn expected_latest_available_date() -> NaiveDate {
        let est_now = chrono::Utc::now().with_timezone(&New_York);
        const EST_UPDATE_HOUR: u32 = 10;
        if est_now.hour() >= EST_UPDATE_HOUR {
            est_now.date_naive()
        } else {
            est_now.date_naive() - Duration::days(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

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

    #[test]
    fn test_expected_latest_available_date() {
        // This test depends on current time, so we just check it returns a valid date
        let date = Price::expected_latest_available_date();
        assert!(date.year() >= 2024);
    }
}
