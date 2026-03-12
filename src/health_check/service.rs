use crate::database::ConnectionPool;
use crate::health_check::models::{BasicHealthStatus, DetailedHealthStatus};
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

pub struct HealthCheckService {
    db: Arc<ConnectionPool>,
}

impl HealthCheckService {
    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self { db }
    }

    pub async fn basic_check(&self) -> Result<BasicHealthStatus> {
        info!("Performing basic health check");
        let card_count = self.count_cards().await?;
        let price_count = self.count_prices().await?;
        let set_count = self.count_sets().await?;
        Ok(BasicHealthStatus {
            card_count,
            price_count,
            set_count,
        })
    }

    pub async fn detailed_check(&self) -> Result<DetailedHealthStatus> {
        info!("Performing detailed health check");
        let basic = self.basic_check().await?;
        let cards_with_prices = self.count_cards_with_prices().await?;
        let cards_without_prices = basic.card_count - cards_with_prices;
        Ok(DetailedHealthStatus {
            basic,
            cards_with_prices,
            cards_without_prices,
        })
    }

    async fn count_cards_with_prices(&self) -> Result<i64> {
        self.db
            .count("SELECT COUNT(DISTINCT card_id) FROM price")
            .await
    }

    async fn count_cards(&self) -> Result<i64> {
        self.db.count("SELECT COUNT(*) FROM card").await
    }

    async fn count_prices(&self) -> Result<i64> {
        self.db.count("SELECT COUNT(*) FROM price").await
    }

    async fn count_sets(&self) -> Result<i64> {
        self.db.count("SELECT COUNT(*) FROM set").await
    }
}
