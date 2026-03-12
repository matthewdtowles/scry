use crate::database::ConnectionPool;
use crate::portfolio::domain::{CardPerformanceRow, PortfolioSummaryRow, PortfolioValueSnapshot};
use anyhow::Result;
use sqlx::QueryBuilder;
use std::sync::Arc;

#[derive(Clone)]
pub struct PortfolioRepository {
    db: Arc<ConnectionPool>,
}

impl PortfolioRepository {
    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self { db }
    }

    /// Cost basis of currently held cards using average cost allocation.
    /// For each card: (total_buy_cost × remaining_qty / total_bought), summed per user.
    pub async fn calculate_holding_costs(&self) -> Result<Vec<(i32, rust_decimal::Decimal)>> {
        let qb = QueryBuilder::new(
            "WITH holdings AS (
                SELECT user_id, card_id, is_foil,
                    SUM(CASE WHEN type = 'BUY' THEN quantity ELSE 0 END) AS total_bought,
                    SUM(CASE WHEN type = 'BUY' THEN quantity * price_per_unit ELSE 0 END) AS total_buy_cost,
                    SUM(CASE WHEN type = 'SELL' THEN quantity ELSE 0 END) AS total_sold
                FROM \"transaction\"
                GROUP BY user_id, card_id, is_foil
                HAVING SUM(CASE WHEN type = 'BUY' THEN quantity ELSE 0 END) > 0
            )
            SELECT
                user_id,
                SUM(CASE WHEN total_bought > total_sold
                    THEN total_buy_cost * (total_bought - total_sold) / total_bought
                    ELSE 0 END
                )::numeric(12,2) AS total_cost
            FROM holdings
            GROUP BY user_id",
        );
        let rows: Vec<(i32, rust_decimal::Decimal)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows)
    }

    /// Find user IDs whose portfolio summary was already computed today
    /// (e.g. via on-demand refresh in the web app with FIFO precision).
    pub async fn get_recently_refreshed_user_ids(&self) -> Result<Vec<i32>> {
        let qb = QueryBuilder::new(
            "SELECT user_id FROM portfolio_summary WHERE computed_at::date = CURRENT_DATE",
        );
        let rows: Vec<(i32,)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    pub async fn save_snapshots(&self, snapshots: &[PortfolioValueSnapshot]) -> Result<i64> {
        if snapshots.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::new(
            "INSERT INTO portfolio_value_history (user_id, total_value, total_cost, total_cards, date)",
        );
        qb.push_values(snapshots, |mut b, s| {
            b.push_bind(s.user_id)
                .push_bind(&s.total_value)
                .push_bind(&s.total_cost)
                .push_bind(s.total_cards)
                .push_bind(s.date);
        });
        qb.push(
            " ON CONFLICT (user_id, date) DO UPDATE SET
                total_value = EXCLUDED.total_value,
                total_cost = COALESCE(EXCLUDED.total_cost, portfolio_value_history.total_cost),
                total_cards = EXCLUDED.total_cards",
        );
        self.db.execute_query_builder(qb).await
    }

    pub async fn apply_weekly_retention(&self) -> Result<i64> {
        self.db
            .count(
                "WITH deleted AS ( \
                    DELETE FROM portfolio_value_history \
                    WHERE date >= CURRENT_DATE - INTERVAL '28 days' \
                      AND date < CURRENT_DATE - INTERVAL '7 days' \
                      AND EXTRACT(DOW FROM date) NOT IN (1) \
                    RETURNING 1 \
                ) \
                SELECT COUNT(*) FROM deleted",
            )
            .await
    }

    pub async fn apply_monthly_retention(&self) -> Result<i64> {
        self.db
            .count(
                "WITH deleted AS ( \
                    DELETE FROM portfolio_value_history \
                    WHERE date < CURRENT_DATE - INTERVAL '28 days' \
                      AND EXTRACT(DAY FROM date) != 1 \
                    RETURNING 1 \
                ) \
                SELECT COUNT(*) FROM deleted",
            )
            .await
    }

    /// Compute portfolio summary data per user (total value, total cards, total quantity)
    pub async fn calculate_portfolio_summaries(&self) -> Result<Vec<PortfolioSummaryRow>> {
        let qb = QueryBuilder::new(
            "SELECT
                i.user_id,
                COALESCE(SUM(
                    i.quantity * CASE WHEN i.foil THEN COALESCE(p.foil, p.normal, 0)
                                      ELSE COALESCE(p.normal, p.foil, 0) END
                ), 0)::numeric(12,2) AS total_value,
                NULL::numeric(12,2) AS total_cost,
                NULL::numeric(12,2) AS total_realized_gain,
                COUNT(DISTINCT i.card_id)::int AS total_cards,
                COALESCE(SUM(i.quantity), 0)::int AS total_quantity
            FROM inventory i
            JOIN price p ON p.card_id = i.card_id
            GROUP BY i.user_id",
        );
        self.db.fetch_all_query_builder(qb).await
    }

    /// Compute per-card performance for all users who have transactions
    pub async fn calculate_card_performance(&self) -> Result<Vec<CardPerformanceRow>> {
        let qb = QueryBuilder::new(
            "WITH buy_costs AS (
                SELECT user_id, card_id, is_foil,
                    SUM(quantity) AS total_bought,
                    SUM(quantity * price_per_unit) AS total_buy_cost
                FROM \"transaction\"
                WHERE type = 'BUY'
                GROUP BY user_id, card_id, is_foil
            ),
            sell_totals AS (
                SELECT user_id, card_id, is_foil,
                    SUM(quantity) AS total_sold,
                    SUM(quantity * price_per_unit) AS total_sell_revenue
                FROM \"transaction\"
                WHERE type = 'SELL'
                GROUP BY user_id, card_id, is_foil
            ),
            holdings AS (
                SELECT
                    bc.user_id,
                    bc.card_id,
                    bc.is_foil,
                    GREATEST(bc.total_bought - COALESCE(st.total_sold, 0), 0)::int AS quantity,
                    bc.total_buy_cost,
                    bc.total_bought,
                    COALESCE(st.total_sold, 0) AS total_sold,
                    COALESCE(st.total_sell_revenue, 0) AS total_sell_revenue
                FROM buy_costs bc
                LEFT JOIN sell_totals st ON st.user_id = bc.user_id
                    AND st.card_id = bc.card_id AND st.is_foil = bc.is_foil
            )
            SELECT
                h.user_id,
                h.card_id,
                h.is_foil,
                h.quantity,
                CASE WHEN h.total_bought > 0
                    THEN (h.total_buy_cost * h.quantity / h.total_bought)::numeric(10,2)
                    ELSE 0 END AS total_cost,
                CASE WHEN h.quantity > 0 AND h.total_bought > 0
                    THEN (h.total_buy_cost / h.total_bought)::numeric(10,2)
                    ELSE 0 END AS average_cost,
                (h.quantity * CASE WHEN h.is_foil
                    THEN COALESCE(p.foil, p.normal, 0)
                    ELSE COALESCE(p.normal, p.foil, 0) END)::numeric(10,2) AS current_value,
                (h.quantity * (CASE WHEN h.is_foil
                    THEN COALESCE(p.foil, p.normal, 0)
                    ELSE COALESCE(p.normal, p.foil, 0) END
                    - CASE WHEN h.total_bought > 0 THEN h.total_buy_cost / h.total_bought ELSE 0 END
                ))::numeric(10,2) AS unrealized_gain,
                (h.total_sell_revenue - CASE WHEN h.total_bought > 0
                    THEN h.total_buy_cost * h.total_sold / h.total_bought
                    ELSE 0 END)::numeric(10,2) AS realized_gain,
                CASE WHEN h.quantity > 0 AND h.total_bought > 0 AND h.total_buy_cost > 0
                    THEN ((h.quantity * CASE WHEN h.is_foil
                        THEN COALESCE(p.foil, p.normal, 0)
                        ELSE COALESCE(p.normal, p.foil, 0) END
                        - h.total_buy_cost * h.quantity / h.total_bought)
                        / (h.total_buy_cost * h.quantity / h.total_bought) * 100)::numeric(8,2)
                    ELSE NULL END AS roi_percent
            FROM holdings h
            LEFT JOIN price p ON p.card_id = h.card_id
            WHERE h.total_bought > 0",
        );
        self.db.fetch_all_query_builder(qb).await
    }

    /// UPSERT portfolio summaries
    pub async fn save_summaries(&self, summaries: &[PortfolioSummaryRow]) -> Result<i64> {
        if summaries.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::new(
            "INSERT INTO portfolio_summary (user_id, total_value, total_cost, total_realized_gain, total_cards, total_quantity, computed_at, refreshes_today, last_refresh_date, computation_method)",
        );
        qb.push_values(summaries, |mut b, s| {
            b.push_bind(s.user_id)
                .push_bind(&s.total_value)
                .push_bind(&s.total_cost)
                .push_bind(&s.total_realized_gain)
                .push_bind(s.total_cards)
                .push_bind(s.total_quantity)
                .push("NOW()")
                .push("0")
                .push("CURRENT_DATE")
                .push_bind("average");
        });
        qb.push(
            " ON CONFLICT (user_id) DO UPDATE SET
                total_value = EXCLUDED.total_value,
                total_cost = EXCLUDED.total_cost,
                total_realized_gain = EXCLUDED.total_realized_gain,
                total_cards = EXCLUDED.total_cards,
                total_quantity = EXCLUDED.total_quantity,
                computed_at = NOW(),
                computation_method = 'average'",
        );
        self.db.execute_query_builder(qb).await
    }

    /// Replace all card performance rows for given users
    pub async fn save_card_performance(&self, rows: &[CardPerformanceRow]) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Delete existing rows for all users that appear in the new data
        let user_ids: Vec<i32> = rows.iter().map(|r| r.user_id).collect::<std::collections::HashSet<_>>().into_iter().collect();
        let mut delete_qb = QueryBuilder::new("DELETE FROM portfolio_card_performance WHERE user_id IN (");
        let mut separated = delete_qb.separated(", ");
        for uid in &user_ids {
            separated.push_bind(*uid);
        }
        separated.push_unseparated(")");
        self.db.execute_query_builder(delete_qb).await?;

        // Insert new rows
        let mut qb = QueryBuilder::new(
            "INSERT INTO portfolio_card_performance (user_id, card_id, is_foil, quantity, total_cost, average_cost, current_value, unrealized_gain, realized_gain, roi_percent, computed_at)",
        );
        qb.push_values(rows, |mut b, r| {
            b.push_bind(r.user_id)
                .push_bind(&r.card_id)
                .push_bind(r.is_foil)
                .push_bind(r.quantity)
                .push_bind(&r.total_cost)
                .push_bind(&r.average_cost)
                .push_bind(&r.current_value)
                .push_bind(&r.unrealized_gain)
                .push_bind(&r.realized_gain)
                .push_bind(&r.roi_percent)
                .push("NOW()");
        });
        self.db.execute_query_builder(qb).await
    }
}
