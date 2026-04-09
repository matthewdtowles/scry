use crate::database::ConnectionPool;
use crate::sealed_product::event_processor::SealedProductEventProcessor;
use crate::sealed_product::price_event_processor::SealedProductPriceEventProcessor;
use crate::sealed_product::repository::SealedProductRepository;
use crate::utils::http_client::HttpClient;
use crate::utils::json_stream_parser::JsonStreamParser;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

pub struct SealedProductService {
    client: Arc<HttpClient>,
    repository: SealedProductRepository,
}

impl SealedProductService {
    const BATCH_SIZE: usize = 200;
    const CONCURRENCY: usize = 4;

    pub fn new(db: Arc<ConnectionPool>, http_client: Arc<HttpClient>) -> Self {
        Self {
            client: http_client,
            repository: SealedProductRepository::new(db),
        }
    }

    pub async fn fetch_count(&self) -> Result<i64> {
        self.repository.count().await
    }

    /// Ingest all sealed products by streaming AllPrintings.json.
    /// Extracts sealedProduct arrays from each set in the stream.
    pub async fn ingest_all(&self) -> Result<i64> {
        info!("Starting sealed product ingestion from AllPrintings stream");
        let byte_stream = self.client.all_cards_stream().await?;
        let sem = Arc::new(Semaphore::new(Self::CONCURRENCY));
        let event_processor = SealedProductEventProcessor::new(Self::BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let repo = self.repository.clone();
        let total = Arc::new(tokio::sync::Mutex::new(0i64));
        let total_for_closure = total.clone();

        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let repo = repo.clone();
                let sem = sem.clone();
                let total = total_for_closure.clone();
                Box::pin(async move {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    let _permit = sem.clone().acquire_owned().await;
                    let set_code = batch[0].set_code.clone();
                    debug!(
                        "Saving {} sealed products for set {}",
                        batch.len(),
                        set_code
                    );
                    match repo.save(&batch).await {
                        Ok(count) => {
                            let mut lock = total.lock().await;
                            *lock += count;
                        }
                        Err(e) => {
                            warn!(
                                "Failed to save sealed products for set {}: {}",
                                set_code, e
                            );
                        }
                    }
                    Ok(())
                })
            })
            .await?;

        let final_total = *total.lock().await;
        info!(
            "Sealed product ingestion complete: {} total saved",
            final_total
        );
        Ok(final_total)
    }

    /// Ingest sealed product prices by streaming AllPricesToday.json.
    /// Filters to only UUIDs that exist in the sealed_product table.
    pub async fn ingest_prices(&self) -> Result<i64> {
        info!("Starting sealed product price ingestion");
        let valid_uuids = self.repository.fetch_all_uuids().await?;
        info!(
            "Found {} sealed product UUIDs to match prices against",
            valid_uuids.len()
        );

        let byte_stream = self.client.all_today_prices_stream().await?;
        let event_processor = SealedProductPriceEventProcessor::new(Self::BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let repo = self.repository.clone();
        let valid_uuids = Arc::new(valid_uuids);
        let total = Arc::new(tokio::sync::Mutex::new(0i64));
        let total_for_closure = total.clone();

        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let repo = repo.clone();
                let valid_uuids = valid_uuids.clone();
                let total = total_for_closure.clone();
                Box::pin(async move {
                    // Filter to only sealed product UUIDs
                    let sealed_prices: Vec<_> = batch
                        .into_iter()
                        .filter(|p| valid_uuids.contains(&p.sealed_product_uuid))
                        .collect();

                    if sealed_prices.is_empty() {
                        return Ok(());
                    }

                    debug!("Saving {} sealed product prices", sealed_prices.len());

                    // Save current prices
                    match repo.save_prices(&sealed_prices).await {
                        Ok(count) => {
                            let mut lock = total.lock().await;
                            *lock += count;
                        }
                        Err(e) => {
                            warn!("Failed to save sealed product prices: {}", e);
                        }
                    }

                    // Save to price history
                    if let Err(e) = repo.save_price_history(&sealed_prices).await {
                        warn!("Failed to save sealed product price history: {}", e);
                    }

                    Ok(())
                })
            })
            .await?;

        let final_total = *total.lock().await;
        info!(
            "Sealed product price ingestion complete: {} prices saved",
            final_total
        );
        Ok(final_total)
    }
}
