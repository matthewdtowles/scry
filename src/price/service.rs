use crate::price::cardkingdom::{granular_from_ck_products, CkPricelistEventProcessor, CkProduct};
use crate::price::domain::{CardPrices, Price};
use crate::price::event_processor::PriceEventProcessor;
use crate::price::repository::PriceRepository;
use crate::price::write_timings::{timed, WriteTimings};
use crate::utils::{clock, JsonStreamParser};
use crate::{database::ConnectionPool, utils::http_client::HttpClient};
use anyhow::Result;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// How fresh the averaged `price` table is against MTGJSON's publishing
/// schedule.
///
/// A pair rather than a bare bool because this gets reported to a human:
/// "newest priced build is 2026-08-20, expected 2026-08-21" says what to go
/// look at, where "not current" does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceCurrency {
    /// The date on the newest row in `price`, or `None` when the table is empty.
    pub newest: Option<NaiveDate>,
    /// The build date MTGJSON should have published by now.
    pub expected: NaiveDate,
}

impl PriceCurrency {
    /// `>=`, not `==`. A run straddling the publish hour can hold data newer
    /// than an expectation computed a moment later, and prices ahead of the
    /// window are not a staleness problem. This is the semantics #71 settled
    /// on; keep it.
    pub fn is_current(&self) -> bool {
        self.newest.is_some_and(|d| d >= self.expected)
    }

    /// The whole alert line, or `None` when the table is current.
    ///
    /// Returns the complete sentence rather than a fragment a caller prefixes.
    /// An earlier revision had the caller write "MTGJSON's price feed has not
    /// advanced: {fragment}", which was wrong for the empty-table case - the
    /// feed advancing has nothing to do with a `price` table that holds no rows
    /// at all, and that is a far more alarming state than a skipped upstream
    /// build. Owning the full wording here means the two cases cannot be
    /// described as the same thing, and callers cannot drift from each other.
    pub fn alert(&self) -> Option<String> {
        if self.is_current() {
            return None;
        }
        Some(match self.newest {
            Some(newest) => format!(
                "MTGJSON's price feed has not advanced - newest priced build is {newest}, \
                 expected {}. The ingest itself succeeded; prices are unchanged because \
                 upstream published no new build.",
                self.expected
            ),
            None => format!(
                "the price table is EMPTY after a completed ingest - expected a {} build. \
                 This is not a skipped upstream build; no prices are being served at all.",
                self.expected
            ),
        })
    }
}

const BATCH_SIZE: usize = 500;
/// Emit an info-level progress line roughly every this many cards so a slow but
/// healthy stream is visible at the default `scry=info` verbosity.
const PROGRESS_LOG_EVERY: usize = 20_000;

pub struct RetentionResult {
    pub weekly_deleted: i64,
    pub monthly_deleted: i64,
    pub total_deleted: i64,
}

#[derive(Debug, Default)]
pub struct CkDirectStats {
    pub rows_saved: i64,
    pub unmatched: u64,
}

pub struct PriceService {
    client: Arc<HttpClient>,
    repository: PriceRepository,
}

impl PriceService {
    pub fn new(db: Arc<ConnectionPool>, http_client: Arc<HttpClient>) -> Self {
        Self {
            client: http_client,
            repository: PriceRepository::new(db),
        }
    }

    pub async fn fetch_prices_for_card_ids(
        &self,
        card_ids: &[String],
    ) -> Result<HashMap<String, (Option<Decimal>, Option<Decimal>)>> {
        self.repository.fetch_prices_for_card_ids(card_ids).await
    }

    pub async fn update_price_foil_if_null(
        &self,
        card_id: &str,
        new_foil: &Decimal,
    ) -> Result<i64> {
        self.repository
            .update_price_foil_if_null(card_id, new_foil)
            .await
    }

    pub async fn insert_price_for_card(
        &self,
        card_id: &str,
        normal: Option<Decimal>,
        foil: Option<Decimal>,
    ) -> Result<i64> {
        self.repository
            .insert_price_for_card(card_id, normal, foil)
            .await
    }

    pub async fn fetch_price_count(&self) -> Result<i64> {
        self.repository.price_count().await
    }

    pub async fn fetch_price_history_count(&self) -> Result<i64> {
        self.repository.price_history_count().await
    }

    /// Ingest today's prices into the averaged `price` / `price_history` tables.
    /// The per-vendor granular store is no longer written here - CK-direct is the
    /// sole granular writer (ROADMAP 10.10). A hard write/stream failure aborts.
    pub async fn ingest_all_today(&self) -> Result<()> {
        debug!("Start ingestion of all prices");
        let byte_stream = self.client.all_today_prices_stream().await?;
        debug!("Received byte stream for today's prices.");
        let valid_card_ids = self.repository.fetch_all_card_ids().await?;

        let event_processor = PriceEventProcessor::new(BATCH_SIZE);
        let timings = WriteTimings::default();
        let mut cards_seen = 0usize;
        let mut next_log = PROGRESS_LOG_EVERY;

        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        json_stream_parser
            .parse_stream(byte_stream, |batch| {
                cards_seen += batch.len();
                if cards_seen >= next_log {
                    info!("Ingested {} card prices so far...", cards_seen);
                    next_log += PROGRESS_LOG_EVERY;
                }
                Box::pin(self.save_prices(batch, &valid_card_ids, &timings))
            })
            .await?;
        info!("Finished ingesting prices for {} cards.", cards_seen);
        timings.log_summary("ingest_all_today");

        Ok(())
    }

    /// Backfill historical averaged prices into `price_history`. A hard
    /// write/stream failure aborts.
    pub async fn ingest_all_historical(&self) -> Result<()> {
        debug!("Start ingestion of all historical prices");
        let byte_stream = self.client.all_prices_stream().await?;
        debug!("Received byte stream for historical prices.");
        let valid_card_ids = self.repository.fetch_all_card_ids().await?;

        let event_processor = PriceEventProcessor::new_historical(BATCH_SIZE);
        let timings = WriteTimings::default();
        let mut cards_seen = 0usize;
        let mut next_log = PROGRESS_LOG_EVERY;

        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        json_stream_parser
            .parse_stream(byte_stream, |batch| {
                cards_seen += batch.len();
                if cards_seen >= next_log {
                    info!("Ingested {} historical card prices so far...", cards_seen);
                    next_log += PROGRESS_LOG_EVERY;
                }
                Box::pin(self.save_price_history_only(batch, &valid_card_ids, &timings))
            })
            .await?;
        info!(
            "Finished ingesting historical prices for {} cards.",
            cards_seen
        );
        timings.log_summary("ingest_all_historical");

        Ok(())
    }

    /// Ingest Card Kingdom's direct pricelist: live buylist offers
    /// (`price_buy` + `qty_buying`), matched to cards via `scryfall_id`. Must
    /// run AFTER the MTGJSON ingest so the CK-direct row overwrites the
    /// indicative MTGJSON CK row on the shared granular key (last-writer-wins
    /// upsert). Hard failures return Err; the caller treats the whole
    /// CK-direct pass as best-effort enrichment.
    pub async fn ingest_cardkingdom_direct(&self) -> Result<CkDirectStats> {
        debug!("Start Card Kingdom direct pricelist ingestion");
        let scryfall_map = self.repository.fetch_scryfall_card_id_map().await?;
        if scryfall_map.is_empty() {
            warn!("No cards carry a scryfall_id; skipping CK-direct ingest.");
            return Ok(CkDirectStats::default());
        }
        let byte_stream = self.client.cardkingdom_pricelist_stream().await?;
        let today = clock::today();
        let rows_saved = AtomicI64::new(0);
        let unmatched = AtomicU64::new(0);
        let timings = WriteTimings::default();

        let event_processor = CkPricelistEventProcessor::new(BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        json_stream_parser
            .parse_stream(byte_stream, |batch| {
                Box::pin(self.save_ck_batch(
                    batch,
                    &scryfall_map,
                    today,
                    &rows_saved,
                    &unmatched,
                    &timings,
                ))
            })
            .await?;
        timings.log_summary("ingest_cardkingdom_direct");

        Ok(CkDirectStats {
            rows_saved: rows_saved.load(Ordering::Relaxed),
            unmatched: unmatched.load(Ordering::Relaxed),
        })
    }

    async fn save_ck_batch(
        &self,
        products: Vec<CkProduct>,
        scryfall_map: &HashMap<String, String>,
        date: NaiveDate,
        rows_saved: &AtomicI64,
        unmatched: &AtomicU64,
        timings: &WriteTimings,
    ) -> Result<()> {
        let (rows, batch_unmatched) = granular_from_ck_products(products, scryfall_map, date);
        unmatched.fetch_add(batch_unmatched, Ordering::Relaxed);
        if rows.is_empty() {
            return Ok(());
        }
        let saved = timed(
            &timings.granular_price,
            self.repository.save_granular_prices(&rows),
        )
        .await?;
        rows_saved.fetch_add(saved, Ordering::Relaxed);
        Ok(())
    }

    /// Remove all prices older than the latest price date, in one statement (§5).
    pub async fn clean_up_prices(&self) -> Result<()> {
        let deleted = self.repository.delete_prices_before_latest().await?;
        if deleted == 0 {
            info!("No old prices found in price table.");
        } else {
            info!("Removed {deleted} old price rows.");
        }
        Ok(())
    }

    /// Whether `price` reflects the newest MTGJSON build, and the two dates
    /// behind that verdict.
    ///
    /// `clean_up_prices` drops every date but the newest, so `newest` is the
    /// build the last successful ingest actually wrote - which is exactly what
    /// `scry health` later measures against `CURRENT_DATE`.
    pub async fn price_currency(&self) -> Result<PriceCurrency> {
        let price_dates = self.repository.fetch_price_dates().await?;
        Ok(PriceCurrency {
            newest: price_dates.iter().max().copied(),
            expected: Price::expected_latest_available_date(),
        })
    }

    pub async fn fetch_history_size(&self) -> Result<String> {
        self.repository.price_history_size().await
    }

    pub async fn apply_retention(&self) -> Result<RetentionResult> {
        info!("Starting retention cleanup on price_history");

        let weekly_deleted = self.repository.apply_weekly_retention().await?;
        info!("Weekly period: deleted {} rows", weekly_deleted);

        let monthly_deleted = self.repository.apply_monthly_retention().await?;
        info!("Monthly period: deleted {} rows", monthly_deleted);

        let total_deleted = weekly_deleted + monthly_deleted;
        Ok(RetentionResult {
            weekly_deleted,
            monthly_deleted,
            total_deleted,
        })
    }

    pub async fn truncate_history(&self) -> Result<()> {
        self.repository.truncate_price_history().await
    }

    pub async fn update_price_change_weekly(&self) -> Result<i64> {
        self.repository.update_price_change_weekly().await
    }

    async fn save_price_history_only(
        &self,
        card_prices: Vec<CardPrices>,
        valid_card_ids: &std::collections::HashSet<String>,
        timings: &WriteTimings,
    ) -> Result<()> {
        // Historical backfill: averaged prices -> price_history.
        let mut history: Vec<Price> = Vec::new();
        for cp in card_prices {
            for avg in cp.averages {
                if valid_card_ids.contains(&avg.card_id) {
                    history.push(avg);
                }
            }
        }

        if !history.is_empty() {
            let history_count = timed(
                &timings.price_history,
                self.repository.save_price_history(&history),
            )
            .await?;
            debug!("Saved batch of {} prices to history table.", history_count);
        }
        Ok(())
    }

    async fn save_prices(
        &self,
        card_prices: Vec<CardPrices>,
        valid_card_ids: &std::collections::HashSet<String>,
        timings: &WriteTimings,
    ) -> Result<()> {
        // Derive the averaged retail price per card for the price/price_history
        // tables, filtering to known card ids. The per-vendor granular rows the
        // event processor also emits are intentionally dropped - CK-direct is the
        // sole granular writer now (ROADMAP 10.10).
        let mut averages: Vec<Price> = Vec::new();
        for cp in card_prices {
            for avg in cp.averages {
                if valid_card_ids.contains(&avg.card_id) {
                    averages.push(avg);
                }
            }
        }

        if !averages.is_empty() {
            let saved_count = timed(&timings.price, self.repository.save_prices(&averages)).await?;
            debug!("Saved batch of {} prices to price table.", saved_count);
            let history_count = timed(
                &timings.price_history,
                self.repository.save_price_history(&averages),
            )
            .await?;
            debug!("Saved batch of {} prices to history table.", history_count);
        }
        Ok(())
    }
}

#[cfg(test)]
mod currency_tests {
    use super::*;

    fn d(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, day).unwrap()
    }

    #[test]
    fn a_feed_that_advanced_is_current() {
        let c = PriceCurrency {
            newest: Some(d(21)),
            expected: d(21),
        };
        assert!(c.is_current());
    }

    /// The shape of the 2026-08-21 run: the ingest fetched, parsed and wrote,
    /// but the build it was served was the one it already had. Every step
    /// succeeded, which is exactly why nothing below it raises and why this has
    /// to be noticed here.
    #[test]
    fn a_feed_that_did_not_advance_names_both_dates() {
        let c = PriceCurrency {
            newest: Some(d(20)),
            expected: d(21),
        };
        assert!(!c.is_current());
        let alert = c.alert().expect("a stalled feed must alert");
        assert!(alert.contains("has not advanced"), "{alert}");
        assert!(alert.contains("2026-08-20"), "{alert}");
        assert!(alert.contains("2026-08-21"), "{alert}");
    }

    /// A current table must not produce an alert at all - this is what keeps a
    /// healthy night silent instead of mailing every morning.
    #[test]
    fn a_current_feed_produces_no_alert() {
        let c = PriceCurrency {
            newest: Some(d(21)),
            expected: d(21),
        };
        assert_eq!(c.alert(), None);
    }

    /// #71's semantics, and the reason this is `>=` and not `==`: a run that
    /// straddles the publish hour can hold data newer than an expectation
    /// computed a moment later. Being ahead of the window is not staleness.
    /// Flip `is_current` to `==` and this test fails.
    #[test]
    fn a_feed_ahead_of_the_window_is_still_current() {
        let c = PriceCurrency {
            newest: Some(d(22)),
            expected: d(21),
        };
        assert!(c.is_current());
    }

    /// An empty table is a different problem from a skipped upstream build, and
    /// must not be reported as one. Collapse the two branches of `alert()` into
    /// a single message and this fails.
    #[test]
    fn an_empty_price_table_is_not_described_as_a_stalled_feed() {
        let c = PriceCurrency {
            newest: None,
            expected: d(21),
        };
        assert!(!c.is_current());
        let alert = c.alert().expect("an empty table must alert");
        assert!(alert.contains("EMPTY"), "{alert}");
        assert!(alert.contains("2026-08-21"), "{alert}");
        assert!(
            !alert.contains("has not advanced"),
            "an empty table is not a stalled feed: {alert}"
        );
    }
}
