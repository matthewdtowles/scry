use crate::database::ConnectionPool;
use crate::portfolio::domain::PortfolioValueSnapshot;
use crate::portfolio::repository::PortfolioRepository;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

pub struct PortfolioService {
    repository: PortfolioRepository,
}

impl PortfolioService {
    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self {
            repository: PortfolioRepository::new(db),
        }
    }

    pub async fn snapshot_portfolio_values(&self) -> Result<i64> {
        debug!("Calculating portfolio values for all users");
        let summaries = self.repository.calculate_portfolio_summaries().await?;
        debug!("Found {} users with inventory", summaries.len());

        if summaries.is_empty() {
            return Ok(0);
        }

        let today = chrono::Local::now().date_naive();
        let mut snapshots: Vec<PortfolioValueSnapshot> = summaries
            .iter()
            .map(|s| PortfolioValueSnapshot {
                user_id: s.user_id,
                total_value: s.total_value,
                total_cost: None,
                total_cards: s.total_cards,
                date: today,
            })
            .collect();

        let total_costs = self.repository.calculate_holding_costs().await?;
        let cost_map: HashMap<i32, rust_decimal::Decimal> =
            total_costs.into_iter().collect();

        for snapshot in &mut snapshots {
            if let Some(cost) = cost_map.get(&snapshot.user_id) {
                snapshot.total_cost = Some(*cost);
            }
        }

        let saved = self.repository.save_snapshots(&snapshots).await?;
        debug!("Saved {} portfolio value snapshots", saved);
        Ok(saved)
    }

    pub async fn compute_portfolio_summaries(&self) -> Result<(i64, i64)> {
        debug!("Computing portfolio summaries for all users");

        let mut summaries = self.repository.calculate_portfolio_summaries().await?;
        debug!("Found {} users with inventory", summaries.len());

        if summaries.is_empty() {
            return Ok((0, 0));
        }

        // Skip users who already refreshed today (preserves FIFO precision from NestJS)
        let refreshed_today = self
            .repository
            .get_recently_refreshed_user_ids()
            .await?
            .into_iter()
            .collect::<HashSet<_>>();

        if !refreshed_today.is_empty() {
            debug!(
                "Skipping {} users who refreshed today",
                refreshed_today.len()
            );
            summaries.retain(|s| !refreshed_today.contains(&s.user_id));
        }

        if summaries.is_empty() {
            debug!("All users already refreshed today, nothing to compute");
            return Ok((0, 0));
        }

        // Compute per-card performance, then derive summary totals from it
        let mut card_performance = self.repository.calculate_card_performance().await?;
        debug!("Computed {} card performance rows", card_performance.len());

        // Exclude recently refreshed users from card performance too
        if !refreshed_today.is_empty() {
            card_performance.retain(|r| !refreshed_today.contains(&r.user_id));
        }

        // Aggregate total_cost and realized_gain per user from card performance
        let mut cost_by_user: HashMap<i32, rust_decimal::Decimal> = HashMap::new();
        let mut gain_by_user: HashMap<i32, rust_decimal::Decimal> = HashMap::new();
        for row in &card_performance {
            *cost_by_user.entry(row.user_id).or_default() += row.total_cost;
            *gain_by_user.entry(row.user_id).or_default() += row.realized_gain;
        }

        for summary in &mut summaries {
            if let Some(cost) = cost_by_user.get(&summary.user_id) {
                summary.total_cost = Some(*cost);
            }
            if let Some(gain) = gain_by_user.get(&summary.user_id) {
                summary.total_realized_gain = Some(*gain);
            }
        }

        let saved_summaries = self.repository.save_summaries(&summaries).await?;
        debug!("Saved {} portfolio summaries", saved_summaries);

        let saved_performance = self
            .repository
            .save_card_performance(&card_performance)
            .await?;
        debug!("Saved {} card performance rows", saved_performance);

        Ok((saved_summaries, saved_performance))
    }

    pub async fn apply_retention(&self) -> Result<(i64, i64)> {
        let weekly = self.repository.apply_weekly_retention().await?;
        let monthly = self.repository.apply_monthly_retention().await?;
        Ok((weekly, monthly))
    }

}
