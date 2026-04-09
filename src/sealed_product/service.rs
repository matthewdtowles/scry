use crate::database::ConnectionPool;
use crate::sealed_product::mapper::SealedProductMapper;
use crate::sealed_product::repository::SealedProductRepository;
use crate::utils::http_client::HttpClient;
use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, info, warn};

pub struct SealedProductService {
    client: Arc<HttpClient>,
    repository: SealedProductRepository,
}

impl SealedProductService {
    pub fn new(db: Arc<ConnectionPool>, http_client: Arc<HttpClient>) -> Self {
        Self {
            client: http_client,
            repository: SealedProductRepository::new(db),
        }
    }

    pub async fn fetch_count(&self) -> Result<i64> {
        self.repository.count().await
    }

    /// Ingest sealed products for a single set by fetching its full set data.
    pub async fn ingest_for_set(&self, set_code: &str) -> Result<i64> {
        debug!("Ingesting sealed products for set: {}", set_code);
        let raw_data: Value = self.client.fetch_set_cards(set_code).await?;
        let products =
            SealedProductMapper::map_mtg_json_to_sealed_products(&raw_data, set_code)?;

        if products.is_empty() {
            debug!("No sealed products found for set: {}", set_code);
            return Ok(0);
        }

        let count = self.repository.save(&products).await?;
        debug!(
            "Saved {} sealed products for set {}",
            count, set_code
        );
        Ok(count)
    }

    /// Ingest sealed products for all sets.
    /// Fetches the set list, then for each set fetches its detailed data
    /// which includes the sealedProduct array.
    pub async fn ingest_all(&self, set_codes: &[String]) -> Result<i64> {
        info!("Starting sealed product ingestion for {} sets", set_codes.len());
        let mut total = 0i64;

        for code in set_codes {
            match self.ingest_for_set(code).await {
                Ok(count) => total += count,
                Err(e) => warn!("Failed to ingest sealed products for set {}: {}", code, e),
            }
        }

        info!("Sealed product ingestion complete: {} total saved", total);
        Ok(total)
    }
}
