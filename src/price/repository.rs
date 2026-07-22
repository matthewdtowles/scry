use crate::database::ConnectionPool;
use crate::price::domain::{GranularPrice, Price};
use anyhow::Result;
use chrono::NaiveDate;
use sqlx::QueryBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

use rust_decimal::Decimal;

#[derive(Clone)]
pub struct PriceRepository {
    db: Arc<ConnectionPool>,
}

impl PriceRepository {
    const PRICE_TABLE: &str = "price";
    const PRICE_HISTORY_TABLE: &str = "price_history";
    const GRANULAR_PRICE_TABLE: &str = "granular_price";

    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self { db }
    }

    pub async fn price_count(&self) -> Result<i64> {
        self.count(Self::PRICE_TABLE).await
    }

    pub async fn price_history_count(&self) -> Result<i64> {
        self.count(Self::PRICE_HISTORY_TABLE).await
    }

    pub async fn fetch_all_card_ids(&self) -> Result<std::collections::HashSet<String>> {
        let query = "SELECT id FROM card";
        let query_builder = QueryBuilder::new(query);
        let rows: Vec<(String,)> = self.db.fetch_all_query_builder(query_builder).await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// scryfall_id -> card.id, for matching Card Kingdom's pricelist (keyed by
    /// scryfall_id) to our MTGJSON-uuid cards.
    pub async fn fetch_scryfall_card_id_map(
        &self,
    ) -> Result<std::collections::HashMap<String, String>> {
        let query = "SELECT scryfall_id, id FROM card WHERE scryfall_id IS NOT NULL";
        let query_builder = QueryBuilder::new(query);
        let rows: Vec<(String, String)> = self.db.fetch_all_query_builder(query_builder).await?;
        Ok(rows.into_iter().collect())
    }

    pub async fn fetch_price_dates(&self) -> Result<Vec<NaiveDate>> {
        let query = format!(
            "SELECT DISTINCT(date) FROM {} ORDER BY date DESC",
            Self::PRICE_TABLE
        );
        let query_builder = QueryBuilder::new(query);
        let rows: Vec<(NaiveDate,)> = self.db.fetch_all_query_builder(query_builder).await?;
        Ok(rows.into_iter().map(|(date,)| date).collect())
    }

    pub async fn save_prices(&self, prices: &[Price]) -> Result<i64> {
        self.save(prices, Self::PRICE_TABLE).await
    }

    pub async fn save_price_history(&self, prices: &[Price]) -> Result<i64> {
        self.save(prices, Self::PRICE_HISTORY_TABLE).await
    }

    /// Upsert the current per-vendor offer (one row per series, no date in the
    /// key). The date guard keeps each series monotonic: a stale ingest can't
    /// move a series backwards, and a vendor that doesn't quote today keeps its
    /// last-known price. price is overwritten (last writer wins — MTGJSON is
    /// ingested before any future CK-direct row).
    pub async fn save_granular_prices(&self, prices: &[GranularPrice]) -> Result<i64> {
        self.upsert_granular(
            prices,
            Self::GRANULAR_PRICE_TABLE,
            " ON CONFLICT (card_id, provider, price_type, finish, condition) \
              DO UPDATE SET price = EXCLUDED.price, date = EXCLUDED.date, \
              qty = EXCLUDED.qty \
              WHERE EXCLUDED.date >= granular_price.date",
        )
        .await
    }

    /// Shared batch UPSERT for the granular current-offer table; callers supply
    /// the table and the `ON CONFLICT` clause.
    async fn upsert_granular(
        &self,
        prices: &[GranularPrice],
        table: &str,
        conflict_clause: &str,
    ) -> Result<i64> {
        if prices.is_empty() {
            return Ok(0);
        }
        // Chunk so a large batch can't exceed Postgres's 65535 bind-param limit
        // (8 binds/row).
        const CHUNK: usize = 4000;
        let mut total = 0;
        for chunk in prices.chunks(CHUNK) {
            let mut query_builder = QueryBuilder::new(format!(
                "INSERT INTO {} (card_id, provider, price_type, finish, condition, date, price, qty) ",
                table
            ));
            query_builder.push_values(chunk, |mut b, price| {
                b.push_bind(&price.card_id)
                    .push_bind(&price.provider)
                    .push_bind(&price.price_type)
                    .push_bind(&price.finish)
                    .push_bind(&price.condition)
                    .push_bind(price.date)
                    .push_bind(price.price)
                    .push_bind(price.qty);
            });
            query_builder.push(conflict_clause);
            match self.db.execute_query_builder(query_builder).await {
                Ok(count) => total += count,
                Err(e) => {
                    error!("Database error: {:?}", e);
                    return Err(e);
                }
            }
        }
        Ok(total)
    }

    pub async fn delete_by_date(&self, date: NaiveDate) -> Result<i64> {
        let query = format!("DELETE FROM {} WHERE date = ", Self::PRICE_TABLE);
        let mut query_builder = QueryBuilder::new(query);
        query_builder.push_bind(date);
        self.db.execute_query_builder(query_builder).await
    }

    /// Delete every price row older than the most recent price date, in one
    /// statement (§5). No-op when the table is empty or has a single date.
    pub async fn delete_prices_before_latest(&self) -> Result<i64> {
        let query = format!(
            "DELETE FROM {t} WHERE date < (SELECT MAX(date) FROM {t})",
            t = Self::PRICE_TABLE
        );
        let query_builder = QueryBuilder::new(query);
        self.db.execute_query_builder(query_builder).await
    }

    pub async fn fetch_prices_for_card_ids(
        &self,
        card_ids: &[String],
    ) -> Result<HashMap<String, (Option<Decimal>, Option<Decimal>)>> {
        if card_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut qb = QueryBuilder::new(
            "SELECT p.card_id, p.normal, p.foil FROM price p WHERE p.card_id = ANY(",
        );
        qb.push_bind(card_ids);
        qb.push(")");
        let rows: Vec<(String, Option<Decimal>, Option<Decimal>)> =
            self.db.fetch_all_query_builder(qb).await?;
        let mut map = HashMap::with_capacity(rows.len());
        for (id, normal, foil) in rows {
            map.insert(id, (normal, foil));
        }
        Ok(map)
    }

    /// Used to help merge split foil and normal cards
    pub async fn update_price_foil_if_null(
        &self,
        card_id: &str,
        new_foil: &Decimal,
    ) -> Result<i64> {
        let mut qb = QueryBuilder::new("UPDATE price SET foil = ");
        qb.push_bind(new_foil);
        qb.push(" WHERE card_id = ");
        qb.push_bind(card_id);
        qb.push(" AND foil IS NULL");
        let n = self.db.execute_query_builder(qb).await?;
        Ok(n)
    }

    pub async fn insert_price_for_card(
        &self,
        card_id: &str,
        normal: Option<Decimal>,
        foil: Option<Decimal>,
    ) -> Result<i64> {
        let mut qb = QueryBuilder::new("INSERT INTO price (card_id, normal, foil, date) VALUES (");
        qb.push_bind(card_id)
            .push(", ")
            .push_bind(normal)
            .push(", ")
            .push_bind(foil)
            .push(", CURRENT_DATE)");
        qb.push(" ON CONFLICT (card_id, date) DO UPDATE SET normal = COALESCE(price.normal, EXCLUDED.normal), foil = COALESCE(price.foil, EXCLUDED.foil)");
        let n = self.db.execute_query_builder(qb).await?;
        Ok(n)
    }

    pub async fn price_history_size(&self) -> Result<String> {
        let qb = QueryBuilder::new(
            "SELECT pg_size_pretty(pg_total_relation_size('public.price_history'))",
        );
        let rows: Vec<(String,)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows.into_iter().next().map(|(s,)| s).unwrap_or_default())
    }

    pub async fn apply_weekly_retention(&self) -> Result<i64> {
        self.db.retain_weekly_tier(Self::PRICE_HISTORY_TABLE).await
    }

    pub async fn apply_monthly_retention(&self) -> Result<i64> {
        self.db.retain_monthly_tier(Self::PRICE_HISTORY_TABLE).await
    }

    pub async fn truncate_price_history(&self) -> Result<()> {
        self.db.execute_raw("TRUNCATE TABLE price_history").await
    }

    async fn save(&self, prices: &[Price], table: &str) -> Result<i64> {
        if prices.is_empty() {
            return Ok(0);
        }
        // Chunk so a large batch can't exceed Postgres's 65535 bind-param limit
        // (4 binds/row).
        const CHUNK: usize = 8000;
        let mut total = 0;
        for chunk in prices.chunks(CHUNK) {
            let query = format!("INSERT INTO {} (card_id, foil, normal, date) ", table);
            let mut query_builder = QueryBuilder::new(query);
            query_builder.push_values(chunk, |mut b, price| {
                b.push_bind(&price.card_id)
                    .push_bind(price.foil)
                    .push_bind(price.normal)
                    .push_bind(price.date);
            });
            query_builder.push(
                " ON CONFLICT (card_id, date) DO UPDATE SET
                foil = COALESCE(EXCLUDED.foil, ",
            );
            query_builder.push(table);
            query_builder.push(
                ".foil),
                normal = COALESCE(EXCLUDED.normal, ",
            );
            query_builder.push(table);
            query_builder.push(".normal)");
            match self.db.execute_query_builder(query_builder).await {
                Ok(count) => total += count,
                Err(e) => {
                    error!("Database error: {:?}", e);
                    return Err(e);
                }
            }
        }
        Ok(total)
    }

    pub async fn update_price_change_weekly(&self) -> Result<i64> {
        let qb = QueryBuilder::new(
            "UPDATE price p \
             SET normal_change_weekly = p.normal - ph.normal, \
                 foil_change_weekly = p.foil - ph.foil \
             FROM ( \
                 SELECT DISTINCT ON (card_id) card_id, normal, foil \
                 FROM price_history \
                 WHERE date <= CURRENT_DATE - INTERVAL '7 days' \
                 ORDER BY card_id, date DESC \
             ) ph \
             WHERE ph.card_id = p.card_id",
        );
        self.db.execute_query_builder(qb).await
    }

    async fn count(&self, table: &str) -> Result<i64> {
        let query = format!("SELECT COUNT(*) FROM {}", table);
        let count = self.db.count(query.as_str()).await?;
        Ok(count)
    }
}
