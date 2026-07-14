//! Set module ports (public contracts other modules depend on).

use crate::set::repository::SetRepository;
use anyhow::Result;
use async_trait::async_trait;

/// The set-code catalog. Sealed-product ingestion filters against it (only sets
/// already in the DB may carry sealed products) - it depends on this port
/// rather than reaching into `SetRepository` directly, so it stays testable.
#[async_trait]
pub trait SetCodesSource: Send + Sync {
    async fn fetch_all_set_codes(&self) -> Result<Vec<String>>;
}

#[async_trait]
impl SetCodesSource for SetRepository {
    async fn fetch_all_set_codes(&self) -> Result<Vec<String>> {
        SetRepository::fetch_all_set_codes(self).await
    }
}
