use chrono::{NaiveDate, Utc};

/// The current calendar date in UTC.
///
/// Every "today" in the codebase goes through here so that snapshots, price
/// rows, and retention cutoffs all agree on the day. UTC is chosen deliberately:
/// the production cron all runs in UTC and MTGJSON dates are treated as UTC, so
/// a container's local timezone must not shift what scry considers "today".
/// (The MTGJSON 10 AM EST availability cutoff is a separate concern - see
/// `Price::expected_latest_available_date`.)
pub fn today() -> NaiveDate {
    Utc::now().date_naive()
}
