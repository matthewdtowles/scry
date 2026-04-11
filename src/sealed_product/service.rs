use crate::database::ConnectionPool;
use crate::sealed_product::domain::SealedProduct;
use crate::sealed_product::event_processor::SealedProductEventProcessor;
use crate::sealed_product::repository::SealedProductRepository;
use crate::set::repository::SetRepository;
use crate::utils::http_client::HttpClient;
use crate::utils::json_stream_parser::JsonStreamParser;
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

pub struct SealedProductService {
    client: Arc<HttpClient>,
    repository: SealedProductRepository,
    set_repository: SetRepository,
}

impl SealedProductService {
    const BATCH_SIZE: usize = 200;

    pub fn new(db: Arc<ConnectionPool>, http_client: Arc<HttpClient>) -> Self {
        Self {
            client: http_client,
            repository: SealedProductRepository::new(db.clone()),
            set_repository: SetRepository::new(db),
        }
    }

    pub async fn fetch_count(&self) -> Result<i64> {
        self.repository.count().await
    }

    /// Ingest all sealed products by streaming AllPrintings.json.
    /// Extracts sealedProduct arrays from each set in the stream.
    ///
    /// Products belonging to sets absent from the `set` table are silently
    /// dropped. This mirrors the set-ingestion filter (online-only, foreign-only,
    /// memorabilia) without duplicating the rule: whatever set ingestion accepted
    /// is exactly what sealed-product ingestion will write, so FK violations
    /// against `set(code)` are impossible by construction.
    pub async fn ingest_all(&self) -> Result<i64> {
        info!("Starting sealed product ingestion from AllPrintings stream");
        let valid_set_codes: Arc<HashSet<String>> = Arc::new(
            self.set_repository
                .fetch_all_set_codes()
                .await
                .context("Failed to load set codes for sealed-product filter")?
                .into_iter()
                .collect(),
        );
        debug!(
            "Loaded {} valid set codes for sealed-product filter",
            valid_set_codes.len()
        );

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
                let valid_set_codes = valid_set_codes.clone();
                Box::pin(async move {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    let set_code = batch[0].set_code.clone();
                    let incoming = batch.len();
                    let filtered = retain_valid_sets(batch, &valid_set_codes);
                    if filtered.is_empty() {
                        debug!(
                            "Skipping {} sealed products for excluded set {}",
                            incoming, set_code
                        );
                        return Ok(());
                    }
                    debug!(
                        "Saving {} sealed products for set {}",
                        filtered.len(),
                        set_code
                    );
                    // Per-batch failures are non-fatal: log and continue so one
                    // bad set can't abort the entire ingestion. Stream-level
                    // errors (network, JSON parse) still propagate via the
                    // parser itself.
                    match repo.save(&filtered).await {
                        Ok(count) => {
                            let mut lock = total.lock().await;
                            *lock += count;
                        }
                        Err(e) => {
                            warn!(
                                "Failed to save sealed products for set {}: {:#}",
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
}

/// Drop sealed products whose `set_code` is not in the allowed set.
///
/// The allowed set is sourced from the `set` table, which already reflects the
/// set-ingestion filter. Keeping this as a free function makes it trivially
/// unit-testable without HTTP/DB fixtures.
fn retain_valid_sets(
    batch: Vec<SealedProduct>,
    valid_set_codes: &HashSet<String>,
) -> Vec<SealedProduct> {
    batch
        .into_iter()
        .filter(|p| valid_set_codes.contains(&p.set_code))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn product(uuid: &str, set_code: &str) -> SealedProduct {
        SealedProduct {
            uuid: uuid.to_string(),
            name: format!("Product {}", uuid),
            set_code: set_code.to_string(),
            category: None,
            subtype: None,
            card_count: None,
            product_size: None,
            release_date: None,
            contents_summary: None,
            purchase_url_tcgplayer: None,
            tcgplayer_product_id: None,
        }
    }

    fn set_of(codes: &[&str]) -> HashSet<String> {
        codes.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn retain_valid_sets_keeps_all_when_every_set_is_valid() {
        let batch = vec![product("a", "woe"), product("b", "woe"), product("c", "blb")];
        let valid = set_of(&["woe", "blb"]);
        let kept = retain_valid_sets(batch, &valid);
        assert_eq!(kept.len(), 3);
    }

    #[test]
    fn retain_valid_sets_drops_products_for_excluded_sets() {
        // 30a is excluded (memorabilia); woe is valid.
        let batch = vec![
            product("a", "30a"),
            product("b", "woe"),
            product("c", "30a"),
            product("d", "woe"),
        ];
        let valid = set_of(&["woe"]);
        let kept = retain_valid_sets(batch, &valid);
        assert_eq!(kept.len(), 2);
        assert!(kept.iter().all(|p| p.set_code == "woe"));
        assert!(kept.iter().any(|p| p.uuid == "b"));
        assert!(kept.iter().any(|p| p.uuid == "d"));
    }

    #[test]
    fn retain_valid_sets_returns_empty_when_all_sets_excluded() {
        let batch = vec![product("a", "30a"), product("b", "30a")];
        let valid = set_of(&["woe", "blb"]);
        let kept = retain_valid_sets(batch, &valid);
        assert!(kept.is_empty());
    }

    #[test]
    fn retain_valid_sets_empty_batch_returns_empty() {
        let batch: Vec<SealedProduct> = Vec::new();
        let valid = set_of(&["woe"]);
        let kept = retain_valid_sets(batch, &valid);
        assert!(kept.is_empty());
    }

    #[test]
    fn retain_valid_sets_empty_valid_set_drops_everything() {
        let batch = vec![product("a", "woe"), product("b", "blb")];
        let valid: HashSet<String> = HashSet::new();
        let kept = retain_valid_sets(batch, &valid);
        assert!(kept.is_empty());
    }
}
