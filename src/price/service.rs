use crate::price::cardkingdom::{granular_from_ck_products, CkPricelistEventProcessor, CkProduct};
use crate::price::domain::{CardPrices, GranularPrice, Price};
use crate::price::event_processor::PriceEventProcessor;
use crate::price::historical_event_processor::HistoricalPriceEventProcessor;
use crate::price::repository::PriceRepository;
use crate::price::write_timings::{timed, WriteTimings};
use crate::utils::JsonStreamParser;
use crate::{database::ConnectionPool, utils::http_client::HttpClient};
use anyhow::Result;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

const BATCH_SIZE: usize = 500;
/// Emit an info-level progress line roughly every this many cards so a slow but
/// healthy stream is visible at the default `scry=info` verbosity.
const PROGRESS_LOG_EVERY: usize = 20_000;

pub struct RetentionResult {
    pub weekly_deleted: i64,
    pub monthly_deleted: i64,
    pub granular_weekly_deleted: i64,
    pub granular_monthly_deleted: i64,
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

    /// Ingest today's prices. Returns the number of best-effort granular write
    /// failures (0 = clean). Granular (per-vendor) writes must not abort the
    /// averaged price/price_history refresh (the critical path) or the rest of
    /// the stream, so they are tallied rather than propagated; the caller decides
    /// how to surface a non-zero count AFTER its own post-ingest steps run. A
    /// hard failure (stream/network, or an averaged price/price_history write)
    /// still returns Err and aborts.
    pub async fn ingest_all_today(&self) -> Result<u64> {
        debug!("Start ingestion of all prices");
        let byte_stream = self.client.all_today_prices_stream().await?;
        debug!("Received byte stream for today's prices.");
        let valid_card_ids = self.repository.fetch_all_card_ids().await?;

        let event_processor = PriceEventProcessor::new(BATCH_SIZE);
        let granular_failures = AtomicU64::new(0);
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
                Box::pin(self.save_prices(batch, &valid_card_ids, &granular_failures, &timings))
            })
            .await?;
        info!("Finished ingesting prices for {} cards.", cards_seen);
        timings.log_summary("ingest_all_today");

        Ok(granular_failures.load(Ordering::Relaxed))
    }

    /// Backfill historical prices. Returns the count of best-effort granular
    /// history write failures (0 = clean); same policy as `ingest_all_today`.
    pub async fn ingest_all_historical(&self) -> Result<u64> {
        debug!("Start ingestion of all historical prices");
        let byte_stream = self.client.all_prices_stream().await?;
        debug!("Received byte stream for historical prices.");
        let valid_card_ids = self.repository.fetch_all_card_ids().await?;

        let event_processor = HistoricalPriceEventProcessor::new(BATCH_SIZE);
        let granular_failures = AtomicU64::new(0);
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
                Box::pin(self.save_price_history_only(
                    batch,
                    &valid_card_ids,
                    &granular_failures,
                    &timings,
                ))
            })
            .await?;
        info!(
            "Finished ingesting historical prices for {} cards.",
            cards_seen
        );
        timings.log_summary("ingest_all_historical");

        Ok(granular_failures.load(Ordering::Relaxed))
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
        let today = chrono::Utc::now().date_naive();
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
        timed(
            &timings.granular_price_history,
            self.repository.save_granular_price_history(&rows),
        )
        .await?;
        rows_saved.fetch_add(saved, Ordering::Relaxed);
        Ok(())
    }

    pub async fn delete_all(&self) -> Result<i64> {
        info!("Deleting all prices.");
        self.repository.delete_all().await
    }

    /// Remove all old prices from db
    pub async fn clean_up_prices(&self) -> Result<()> {
        let mut price_dates = self.repository.fetch_price_dates().await?;
        if price_dates.is_empty() {
            warn!("No dates found in price table.");
            return Ok(());
        }
        if let Some(max_date) = price_dates.iter().max() {
            let max_date = max_date.clone();
            price_dates.retain(|d| d != &max_date);
        }
        if price_dates.is_empty() {
            info!("No old prices found in price table.");
            return Ok(());
        }
        for d in price_dates {
            self.repository.delete_by_date(d).await?;
        }
        Ok(())
    }

    pub async fn prices_are_current(&self) -> Result<bool> {
        let price_dates = self.repository.fetch_price_dates().await?;
        let expected_date = Price::expected_latest_available_date();
        Ok(price_dates.iter().max().map(|d| *d) == Some(expected_date))
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

        let granular_weekly_deleted = self.repository.apply_granular_weekly_retention().await?;
        info!(
            "Granular weekly period: deleted {} rows",
            granular_weekly_deleted
        );

        let granular_monthly_deleted = self.repository.apply_granular_monthly_retention().await?;
        info!(
            "Granular monthly period: deleted {} rows",
            granular_monthly_deleted
        );

        let total_deleted =
            weekly_deleted + monthly_deleted + granular_weekly_deleted + granular_monthly_deleted;
        Ok(RetentionResult {
            weekly_deleted,
            monthly_deleted,
            granular_weekly_deleted,
            granular_monthly_deleted,
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
        granular_failures: &AtomicU64,
        timings: &WriteTimings,
    ) -> Result<()> {
        // Historical pass: averaged prices -> price_history (unchanged), and the
        // same multi-date granular rows backfill granular_price_history.
        let mut history: Vec<Price> = Vec::new();
        let mut granular: Vec<GranularPrice> = Vec::new();
        for cp in card_prices {
            for avg in cp.averages {
                if valid_card_ids.contains(&avg.card_id) {
                    history.push(avg);
                }
            }
            for row in cp.granular {
                if valid_card_ids.contains(&row.card_id) {
                    granular.push(row);
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
        if !granular.is_empty() {
            // Best-effort (see ingest_all_historical).
            match timed(
                &timings.granular_price_history,
                self.repository.save_granular_price_history(&granular),
            )
            .await
            {
                Ok(history_count) => {
                    debug!("Saved batch of {} granular history rows.", history_count)
                }
                Err(e) => {
                    Self::note_granular_failure(granular_failures, "granular_price_history", &e)
                }
            }
        }
        Ok(())
    }

    async fn save_prices(
        &self,
        card_prices: Vec<CardPrices>,
        valid_card_ids: &std::collections::HashSet<String>,
        granular_failures: &AtomicU64,
        timings: &WriteTimings,
    ) -> Result<()> {
        // Split the per-card bundle into derived averages (for the existing
        // price/price_history tables) and granular rows (for granular_price),
        // filtering both to known card ids in a single pass.
        let mut averages: Vec<Price> = Vec::new();
        let mut granular: Vec<GranularPrice> = Vec::new();
        for cp in card_prices {
            for avg in cp.averages {
                if valid_card_ids.contains(&avg.card_id) {
                    averages.push(avg);
                }
            }
            for row in cp.granular {
                if valid_card_ids.contains(&row.card_id) {
                    granular.push(row);
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
        if !granular.is_empty() {
            // Best-effort and independent per table: a failure on the secondary
            // per-vendor store must not stall the averaged price refresh above or
            // the rest of the stream (see ingest_all_today).
            match timed(
                &timings.granular_price,
                self.repository.save_granular_prices(&granular),
            )
            .await
            {
                Ok(current_count) => debug!("Saved {} current granular rows.", current_count),
                Err(e) => Self::note_granular_failure(granular_failures, "granular_price", &e),
            }
            match timed(
                &timings.granular_price_history,
                self.repository.save_granular_price_history(&granular),
            )
            .await
            {
                Ok(history_count) => debug!("Saved {} granular history rows.", history_count),
                Err(e) => {
                    Self::note_granular_failure(granular_failures, "granular_price_history", &e)
                }
            }
        }
        Ok(())
    }

    /// Record a best-effort granular write failure: log the first one at ERROR
    /// with full detail, throttle the rest to debug (a structural failure repeats
    /// every batch and would otherwise flood the log), and bump the counter the
    /// caller checks once the stream completes.
    fn note_granular_failure(counter: &AtomicU64, table: &str, err: &anyhow::Error) {
        let prev = counter.fetch_add(1, Ordering::Relaxed);
        if prev == 0 {
            error!("Best-effort {table} write failed (price refresh continues): {err:?}");
        } else {
            debug!(
                "Best-effort {table} write failed again (#{}): {err}",
                prev + 1
            );
        }
    }
}
