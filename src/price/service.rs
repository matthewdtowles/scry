use crate::price::domain::{CardPrices, GranularPrice, Price};
use crate::price::event_processor::PriceEventProcessor;
use crate::price::historical_event_processor::HistoricalPriceEventProcessor;
use crate::price::repository::PriceRepository;
use crate::utils::JsonStreamParser;
use crate::{database::ConnectionPool, utils::http_client::HttpClient};
use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

const BATCH_SIZE: usize = 500;

pub struct RetentionResult {
    pub weekly_deleted: i64,
    pub monthly_deleted: i64,
    pub granular_weekly_deleted: i64,
    pub granular_monthly_deleted: i64,
    pub total_deleted: i64,
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

    pub async fn ingest_all_today(&self) -> Result<()> {
        debug!("Start ingestion of all prices");
        let byte_stream = self.client.all_today_prices_stream().await?;
        debug!("Received byte stream for today's prices.");
        let valid_card_ids = self.repository.fetch_all_card_ids().await?;

        let event_processor = PriceEventProcessor::new(BATCH_SIZE);

        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        json_stream_parser
            .parse_stream(byte_stream, |batch| {
                Box::pin(self.save_prices(batch, &valid_card_ids))
            })
            .await?;
        Ok(())
    }

    pub async fn ingest_all_historical(&self) -> Result<()> {
        debug!("Start ingestion of all historical prices");
        let byte_stream = self.client.all_prices_stream().await?;
        debug!("Received byte stream for historical prices.");
        let valid_card_ids = self.repository.fetch_all_card_ids().await?;

        let event_processor = HistoricalPriceEventProcessor::new(BATCH_SIZE);

        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        json_stream_parser
            .parse_stream(byte_stream, |batch| {
                Box::pin(self.save_price_history_only(batch, &valid_card_ids))
            })
            .await?;
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
        info!("Granular weekly period: deleted {} rows", granular_weekly_deleted);

        let granular_monthly_deleted = self.repository.apply_granular_monthly_retention().await?;
        info!("Granular monthly period: deleted {} rows", granular_monthly_deleted);

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
    ) -> Result<()> {
        // Historical pass: averaged prices -> price_history (unchanged), and the
        // same multi-date granular rows backfill granular_price.
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
            let history_count = self.repository.save_price_history(&history).await?;
            debug!("Saved batch of {} prices to history table.", history_count);
        }
        if !granular.is_empty() {
            let granular_count = self.repository.save_granular_prices(&granular).await?;
            debug!("Saved batch of {} granular price rows.", granular_count);
        }
        Ok(())
    }

    async fn save_prices(
        &self,
        card_prices: Vec<CardPrices>,
        valid_card_ids: &std::collections::HashSet<String>,
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
            let saved_count = self.repository.save_prices(&averages).await?;
            debug!("Saved batch of {} prices to price table.", saved_count);
            let history_count = self.repository.save_price_history(&averages).await?;
            debug!("Saved batch of {} prices to history table.", history_count);
        }
        if !granular.is_empty() {
            let granular_count = self.repository.save_granular_prices(&granular).await?;
            debug!("Saved batch of {} granular price rows.", granular_count);
        }
        Ok(())
    }
}
