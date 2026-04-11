use crate::database::ConnectionPool;
use crate::sealed_product::event_processor::SealedProductEventProcessor;
use crate::sealed_product::repository::SealedProductRepository;
use crate::utils::http_client::HttpClient;
use crate::utils::json_stream_parser::JsonStreamParser;
use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::{debug, info};

pub struct SealedProductService {
    client: Arc<HttpClient>,
    repository: SealedProductRepository,
}

impl SealedProductService {
    const BATCH_SIZE: usize = 200;

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
        let event_processor = SealedProductEventProcessor::new(Self::BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let repo = self.repository.clone();
        let total = Arc::new(tokio::sync::Mutex::new(0i64));
        let total_for_closure = total.clone();

        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let repo = repo.clone();
                let total = total_for_closure.clone();
                Box::pin(async move {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    let set_code = batch[0].set_code.clone();
                    debug!(
                        "Saving {} sealed products for set {}",
                        batch.len(),
                        set_code
                    );
                    let count = repo.save(&batch).await.with_context(|| {
                        format!("Failed to save sealed products for set {}", set_code)
                    })?;
                    let mut lock = total.lock().await;
                    *lock += count;
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
}
