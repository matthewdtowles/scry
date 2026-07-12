use crate::price::cardkingdom::{granular_from_ck_products, CkPricelistEventProcessor, CkProduct};
use crate::price::domain::{CardPrices, Price};
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
use tracing::{debug, info, warn};

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

        let event_processor = HistoricalPriceEventProcessor::new(BATCH_SIZE);
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
        rows_saved.fetch_add(saved, Ordering::Relaxed);
        Ok(())
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

        let total_deleted = weekly_deleted + monthly_deleted;
        Ok(RetentionResult {
            weekly_deleted,
            monthly_deleted,
            total_deleted,
        })
    }

    /// Apply the same tiered retention to `granular_price_history` (CK-direct
    /// buylist writes grow it daily without bound otherwise). Returns
    /// `(weekly_deleted, monthly_deleted)`. Logging is left to the caller,
    /// matching `SetService`/`PortfolioService` retention.
    pub async fn apply_granular_retention(&self) -> Result<(i64, i64)> {
        let weekly = self.repository.apply_granular_weekly_retention().await?;
        let monthly = self.repository.apply_granular_monthly_retention().await?;
        Ok((weekly, monthly))
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
