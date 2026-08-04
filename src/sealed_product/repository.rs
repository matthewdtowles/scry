use crate::database::ConnectionPool;
use crate::sealed_product::domain::SealedProduct;
use anyhow::Result;
use chrono::NaiveDate;
use sqlx::QueryBuilder;
use std::sync::Arc;
use tracing::warn;

#[derive(Clone)]
pub struct SealedProductRepository {
    db: Arc<ConnectionPool>,
}

impl SealedProductRepository {
    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self { db }
    }

    pub async fn count(&self) -> Result<i64> {
        let count = self.db.count("SELECT COUNT(*) FROM sealed_product").await?;
        Ok(count)
    }

    pub async fn save(&self, products: &[SealedProduct]) -> Result<i64> {
        if products.is_empty() {
            warn!("0 sealed products given, 0 saved.");
            return Ok(0);
        }

        let mut qb = QueryBuilder::new(
            "INSERT INTO sealed_product (
                uuid, name, set_code, category, subtype,
                card_count, product_size, release_date,
                contents_summary, tcgplayer_product_id
            )",
        );

        qb.push_values(products, |mut b, p| {
            b.push_bind(&p.uuid)
                .push_bind(&p.name)
                .push_bind(&p.set_code)
                .push_bind(&p.category)
                .push_bind(&p.subtype)
                .push_bind(p.card_count)
                .push_bind(p.product_size)
                .push_bind(p.release_date)
                .push_bind(&p.contents_summary)
                .push_bind(&p.tcgplayer_product_id);
        });

        qb.push(
            " ON CONFLICT (uuid) DO UPDATE SET
                name = EXCLUDED.name,
                set_code = EXCLUDED.set_code,
                category = EXCLUDED.category,
                subtype = EXCLUDED.subtype,
                card_count = EXCLUDED.card_count,
                product_size = EXCLUDED.product_size,
                release_date = EXCLUDED.release_date,
                contents_summary = EXCLUDED.contents_summary,
                tcgplayer_product_id = EXCLUDED.tcgplayer_product_id
            WHERE
                sealed_product.name IS DISTINCT FROM EXCLUDED.name OR
                sealed_product.set_code IS DISTINCT FROM EXCLUDED.set_code OR
                sealed_product.category IS DISTINCT FROM EXCLUDED.category OR
                sealed_product.subtype IS DISTINCT FROM EXCLUDED.subtype OR
                sealed_product.card_count IS DISTINCT FROM EXCLUDED.card_count OR
                sealed_product.product_size IS DISTINCT FROM EXCLUDED.product_size OR
                sealed_product.release_date IS DISTINCT FROM EXCLUDED.release_date OR
                sealed_product.contents_summary IS DISTINCT FROM EXCLUDED.contents_summary OR
                sealed_product.tcgplayer_product_id IS DISTINCT FROM EXCLUDED.tcgplayer_product_id",
        );

        self.db.execute_query_builder(qb).await
    }

    /// Stamp `last_seen` on the products this ingest run found in MTGJSON.
    /// Separate from [`Self::save`] for the same reason as the card stamp: that
    /// upsert skips rows whose data is unchanged, so a stamp folded into it
    /// would leave the unchanged majority looking unseen.
    pub async fn stamp_seen(&self, uuids: &[String], date: NaiveDate) -> Result<i64> {
        if uuids.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::new("UPDATE sealed_product SET last_seen = ");
        qb.push_bind(date);
        qb.push(" WHERE uuid = ANY(");
        qb.push_bind(uuids.to_vec());
        qb.push(")");
        self.db.execute_query_builder(qb).await
    }

    pub async fn count_seen_on(&self, date: NaiveDate) -> Result<i64> {
        let mut qb =
            QueryBuilder::new("SELECT COUNT(*)::bigint FROM sealed_product WHERE last_seen = ");
        qb.push_bind(date);
        let rows: Vec<(i64,)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows.into_iter().next().map(|(n,)| n).unwrap_or(0))
    }

    /// Delete products MTGJSON no longer lists. Gated by the caller exactly as
    /// the card sweep is - see [`crate::ingest::IngestLedger`].
    pub async fn delete_stale(&self, date: NaiveDate) -> Result<i64> {
        let mut qb =
            QueryBuilder::new("DELETE FROM sealed_product WHERE last_seen IS DISTINCT FROM ");
        qb.push_bind(date);
        self.db.execute_query_builder(qb).await
    }
}
