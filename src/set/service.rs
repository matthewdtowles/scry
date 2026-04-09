use crate::set::domain::{Set, SetPrice};
use crate::set::{mapper::SetMapper, repository::SetRepository};
use crate::{database::ConnectionPool, utils::http_client::HttpClient};
use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, warn};

pub struct SetService {
    client: Arc<HttpClient>,
    repository: SetRepository,
}

impl SetService {
    pub fn new(db: Arc<ConnectionPool>, http_client: Arc<HttpClient>) -> Self {
        Self {
            client: http_client,
            repository: SetRepository::new(db.clone()),
        }
    }

    pub async fn fetch_count(&self) -> Result<i64> {
        self.repository.count().await
    }

    pub async fn fetch_set_codes_with_prices(&self) -> Result<Vec<String>> {
        self.repository.fetch_set_codes_with_prices().await
    }

    pub async fn ingest_all(&self) -> Result<i64> {
        debug!("Starting MTG set ingestion");
        let raw_data: Value = self.client.fetch_all_sets().await?;
        debug!("Raw data fetched.");
        let mut to_save: Vec<Set> = Vec::new();
        if let Some(arr) = raw_data.get("data").and_then(|d| d.as_array()) {
            for set_obj in arr {
                if let Some(code) = set_obj.get("code").and_then(|v| v.as_str()) {
                    match SetMapper::map_mtg_json_to_set(set_obj) {
                        Ok(set) => {
                            if !set.should_filter() {
                                to_save.push(set);
                            }
                        }
                        Err(e) => warn!("Failed to map set {}: {}", code, e),
                    }
                }
            }
        }
        if to_save.is_empty() {
            debug!("No sets to save.");
            return Ok(0);
        }
        let count = self.repository.save_sets(&to_save).await?;
        debug!("Successfully ingested {} sets", count);
        Ok(count)
    }

    pub async fn update_set_prices(&self) -> Result<i64> {
        debug!("Starting update set prices (batched)");
        let codes = self.repository.fetch_set_codes_with_prices().await?;
        if codes.is_empty() {
            warn!("No set codes with prices");
            return Ok(0);
        }
        let batch_size = 200usize;
        let mut total_updated = 0i64;
        for chunk in codes.chunks(batch_size) {
            let set_prices = self.repository.calculate_set_prices(chunk).await?;
            if !set_prices.is_empty() {
                total_updated += self.save_set_prices(set_prices).await?;
            }
        }
        debug!("Successfully updated {} set prices", total_updated);
        Ok(total_updated)
    }

    pub async fn cleanup_sets(&self) -> Result<i64> {
        debug!("Starting cleanup for sets");
        let raw_data: Value = self.client.fetch_all_sets().await?;
        let mut total_deleted = 0i64;
        if let Some(arr) = raw_data.get("data").and_then(|d| d.as_array()) {
            for set_obj in arr {
                if let Some(code) = set_obj.get("code").and_then(|v| v.as_str()) {
                    let code = code.to_lowercase();
                    if let Ok(set) = SetMapper::map_mtg_json_to_set(set_obj) {
                        if set.should_filter() {
                            total_deleted += self.repository.delete_set_batch(&code).await?;
                        }
                    }
                }
            }
        }
        debug!("Total sets deleted: {}", total_deleted);
        Ok(total_deleted)
    }

    pub async fn delete_all(&self) -> Result<i64> {
        debug!("Deleting all sets.");
        self.repository.delete_all().await
    }

    pub async fn prune_empty_sets(&self) -> Result<i64> {
        debug!("Deleting sets that do not have any cards.");
        let empty_sets: Vec<Set> = self.repository.fetch_empty_sets().await?;
        debug!("Found {} empty sets", empty_sets.len());
        if empty_sets.is_empty() {
            return Ok(0);
        }
        let mut total_deleted = 0i64;
        for set in empty_sets {
            let code = set.code.to_lowercase();
            let n = self.repository.delete_set_batch(&code).await?;
            total_deleted += n;
        }
        debug!("Pruned {} rows for empty sets", total_deleted);
        Ok(total_deleted)
    }

    pub async fn prune_missing_prices(&self, missing_limit_pct: f64) -> Result<i64> {
        let printable_arg = 100.0 * missing_limit_pct;
        debug!(
            "Pruning sets with price data for less than {}% of its cards.",
            printable_arg
        );
        let sets_missing_prices = self
            .repository
            .fetch_missing_prices(missing_limit_pct)
            .await?;
        let mut total_deleted = 0i64;
        for set in sets_missing_prices {
            let code = set.code.to_lowercase();
            total_deleted += self.repository.delete_set_batch(&code).await?;
        }
        debug!(
            "Pruned {} rows with price data for less than {}% of its cards.",
            total_deleted, printable_arg
        );
        Ok(total_deleted)
    }

    pub async fn update_parent_codes(&self) -> Result<i64> {
        self.repository.update_parent_codes().await
    }

    pub async fn update_main_status(&self) -> Result<i64> {
        self.repository.update_is_main().await
    }

    pub async fn update_sizes(
        &self,
        base_sizes: Vec<(String, i64)>,
        total_sizes: Vec<(String, i64)>,
    ) -> Result<i64> {
        let updated = self
            .repository
            .update_sizes(&base_sizes, &total_sizes)
            .await?;
        Ok(updated)
    }

    pub async fn backfill_set_price_history(&self) -> Result<i64> {
        self.repository.backfill_set_price_history().await
    }

    pub async fn apply_set_price_history_retention(&self) -> Result<(i64, i64)> {
        let weekly = self
            .repository
            .apply_set_price_history_weekly_retention()
            .await?;
        let monthly = self
            .repository
            .apply_set_price_history_monthly_retention()
            .await?;
        Ok((weekly, monthly))
    }

    pub async fn update_set_price_change_weekly(&self) -> Result<i64> {
        self.repository.update_set_price_change_weekly().await
    }

    async fn save_set_prices(&self, set_prices: Vec<SetPrice>) -> Result<i64> {
        if set_prices.is_empty() {
            return Ok(0);
        }
        let mut total = 0i64;
        let batch_size = 200usize;
        for chunk in set_prices.chunks(batch_size) {
            total += self.repository.update_prices(chunk.to_vec()).await?;
        }
        // Also save to set_price_history
        for chunk in set_prices.chunks(batch_size) {
            self.repository
                .save_set_price_history(chunk.to_vec())
                .await?;
        }
        Ok(total)
    }
}
