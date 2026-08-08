use anyhow::{bail, Result};
use chrono::{DateTime, Duration, NaiveDate, Timelike, Utc};
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

    /// Calculate the expected latest available price date.
    ///
    /// MTGJSON rebuilds the bulk files once a day at ~06:10 UTC - observed
    /// 2026-08-07, where `AllPricesToday.json`, `AllPrintings.json` and
    /// `AllPrices.json` all carried `last-modified` between 06:09:55 and
    /// 06:10:14 UTC. Before that hour the newest prices that exist anywhere
    /// are yesterday's, so expecting today's would be wrong.
    ///
    /// Expressed in UTC rather than US/Eastern: the publish time tracks UTC,
    /// so an Eastern cutover would drift by an hour across DST and misjudge
    /// the window for half the year. The extra ~50 minutes of margin absorbs
    /// a build that runs late.
    pub fn expected_latest_available_date() -> NaiveDate {
        Self::expected_latest_available_date_at(chrono::Utc::now())
    }

    /// The cutover itself, taking `now` so the boundary is testable. Splitting
    /// this out is what lets the tests below pin the hour: reading the clock
    /// directly, the only thing a test could assert is that some date came
    /// back.
    fn expected_latest_available_date_at(now: DateTime<Utc>) -> NaiveDate {
        const PUBLISH_HOUR_UTC: u32 = 7;
        if now.hour() >= PUBLISH_HOUR_UTC {
            now.date_naive()
        } else {
            now.date_naive() - Duration::days(1)
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

    /// A UTC instant on 2026-08-07, the day the ~06:10 publish was observed.
    fn at(hour: u32, minute: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(2026, 8, 7)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
            .and_utc()
    }

    #[test]
    fn test_expected_latest_available_date_uses_current_clock() {
        // Wired to the real clock; the boundary itself is pinned below.
        let date = Price::expected_latest_available_date();
        assert!(date.year() >= 2024);
    }

    #[test]
    fn test_before_publish_hour_expects_yesterday() {
        // 02:00 UTC is when the ingest used to run - four hours before MTGJSON
        // published, so the newest prices that existed were the prior day's.
        let expected = NaiveDate::from_ymd_opt(2026, 8, 6).unwrap();
        assert_eq!(Price::expected_latest_available_date_at(at(2, 0)), expected);
        assert_eq!(
            Price::expected_latest_available_date_at(at(6, 59)),
            expected
        );
    }

    #[test]
    fn test_at_and_after_publish_hour_expects_today() {
        // 07:00 UTC is the cutover; 08:00 is where the ingest cron now runs.
        let expected = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();
        assert_eq!(Price::expected_latest_available_date_at(at(7, 0)), expected);
        assert_eq!(Price::expected_latest_available_date_at(at(8, 0)), expected);
        assert_eq!(
            Price::expected_latest_available_date_at(at(23, 59)),
            expected
        );
    }

    #[test]
    fn test_publish_hour_leaves_margin_after_the_observed_build() {
        // The build lands ~06:10 UTC. Just after it, the cutover has not yet
        // flipped - deliberate margin for a late build, not an off-by-one.
        assert_eq!(
            Price::expected_latest_available_date_at(at(6, 15)),
            NaiveDate::from_ymd_opt(2026, 8, 6).unwrap()
        );
    }
}
