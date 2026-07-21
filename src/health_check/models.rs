use tracing::info;

/// Maximum age, in days, of the newest `price` row before the catalog counts
/// as stale. The ingest cron runs daily, so a larger gap means at least one
/// run failed to write prices - which is otherwise invisible until someone
/// notices the site is showing old data.
pub const MAX_PRICE_AGE_DAYS: i64 = 1;

#[derive(Debug)]
pub struct BasicHealthStatus {
    pub card_count: i64,
    /// Days between today and the newest `price` row.
    pub price_age_days: i64,
    pub price_count: i64,
    pub set_count: i64,
}

impl BasicHealthStatus {
    /// True when prices are older than a daily ingest should ever leave them.
    pub fn is_stale(&self) -> bool {
        self.price_age_days > MAX_PRICE_AGE_DAYS
    }

    pub fn display(&self) {
        info!("=== SYSTEM HEALTH REPORT ===");
        info!("Cards in database: {}", self.card_count);
        info!("Current prices: {}", self.price_count);
        info!("Newest price row: {} day(s) old", self.price_age_days);
        info!("Sets in database: {}", self.set_count);
        info!("=== END HEALTH REPORT ===");
    }
}

#[derive(Debug)]
pub struct DetailedHealthStatus {
    pub basic: BasicHealthStatus,
    pub cards_with_prices: i64,
    pub cards_without_prices: i64,
}

impl DetailedHealthStatus {
    pub fn display(&self) {
        self.basic.display();
        info!("=== DETAILED HEALTH CHECK ===");
        info!("Cards with prices: {}", self.cards_with_prices);
        info!("Cards without prices: {}", self.cards_without_prices);
        info!("=== END DETAILED REPORT ===");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(price_age_days: i64) -> BasicHealthStatus {
        BasicHealthStatus {
            card_count: 100,
            price_age_days,
            price_count: 100,
            set_count: 10,
        }
    }

    #[test]
    fn test_same_day_prices_are_fresh() {
        assert!(!status(0).is_stale());
    }

    #[test]
    fn test_one_day_old_prices_are_fresh() {
        // The ingest runs at 02:00 UTC against the prior day's price file, so
        // a one-day gap is the normal steady state, not a failure.
        assert!(!status(MAX_PRICE_AGE_DAYS).is_stale());
    }

    #[test]
    fn test_two_day_old_prices_are_stale() {
        // The July 2026 incident: two consecutive ingests died mid-run and
        // nothing surfaced it.
        assert!(status(MAX_PRICE_AGE_DAYS + 1).is_stale());
    }
}
