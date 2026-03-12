use tracing::info;

#[derive(Debug)]
pub struct BasicHealthStatus {
    pub card_count: i64,
    pub price_count: i64,
    pub set_count: i64,
}

impl BasicHealthStatus {
    pub fn display(&self) {
        info!("=== SYSTEM HEALTH REPORT ===");
        info!("Cards in database: {}", self.card_count);
        info!("Current prices: {}", self.price_count);
        info!("Sets in database: {}", self.set_count);
        info!("=== END HEALTH REPORT ===");
    }
}

#[derive(Debug)]
pub struct DetailedHealthStatus {
    pub basic: BasicHealthStatus,
    pub cards_with_prices: i64,
    pub cards_without_prices: i64,
}

impl DetailedHealthStatus {
    pub fn display(&self) {
        self.basic.display();
        info!("=== DETAILED HEALTH CHECK ===");
        info!("Cards with prices: {}", self.cards_with_prices);
        info!("Cards without prices: {}", self.cards_without_prices);
        info!("=== END DETAILED REPORT ===");
    }
}