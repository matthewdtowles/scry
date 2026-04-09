use crate::database::ConnectionPool;
use crate::sealed_product::domain::{SealedProduct, SealedProductPrice};
use anyhow::Result;
use sqlx::QueryBuilder;
use std::collections::HashSet;
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
        let count = self
            .db
            .count("SELECT COUNT(*) FROM sealed_product")
            .await?;
        Ok(count)
    }

    pub async fn fetch_all_uuids(&self) -> Result<HashSet<String>> {
        let qb = QueryBuilder::new("SELECT uuid FROM sealed_product");
        let rows: Vec<(String,)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows.into_iter().map(|(uuid,)| uuid).collect())
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
                contents_summary, purchase_url_tcgplayer
            )",
        );

        qb.push_values(products, |mut b, p| {
            b.push_bind(&p.uuid)
                .push_bind(&p.name)
                .push_bind(&p.set_code)
                .push_bind(&p.category)
                .push_bind(&p.subtype)
                .push_bind(&p.card_count)
                .push_bind(&p.product_size)
                .push_bind(&p.release_date)
                .push_bind(&p.contents_summary)
                .push_bind(&p.purchase_url_tcgplayer);
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
                purchase_url_tcgplayer = EXCLUDED.purchase_url_tcgplayer
            WHERE
                sealed_product.name IS DISTINCT FROM EXCLUDED.name OR
                sealed_product.set_code IS DISTINCT FROM EXCLUDED.set_code OR
                sealed_product.category IS DISTINCT FROM EXCLUDED.category OR
                sealed_product.subtype IS DISTINCT FROM EXCLUDED.subtype OR
                sealed_product.card_count IS DISTINCT FROM EXCLUDED.card_count OR
                sealed_product.product_size IS DISTINCT FROM EXCLUDED.product_size OR
                sealed_product.release_date IS DISTINCT FROM EXCLUDED.release_date OR
                sealed_product.contents_summary IS DISTINCT FROM EXCLUDED.contents_summary OR
                sealed_product.purchase_url_tcgplayer IS DISTINCT FROM EXCLUDED.purchase_url_tcgplayer",
        );

        self.db.execute_query_builder(qb).await
    }

    pub async fn save_prices(&self, prices: &[SealedProductPrice]) -> Result<i64> {
        if prices.is_empty() {
            return Ok(0);
        }

        let mut qb = QueryBuilder::new(
            "INSERT INTO sealed_product_price (sealed_product_uuid, price, date)",
        );

        qb.push_values(prices, |mut b, p| {
            b.push_bind(&p.sealed_product_uuid)
                .push_bind(&p.price)
                .push_bind(&p.date);
        });

        qb.push(
            " ON CONFLICT (sealed_product_uuid) DO UPDATE SET
                price = EXCLUDED.price,
                date = EXCLUDED.date
            WHERE
                sealed_product_price.price IS DISTINCT FROM EXCLUDED.price OR
                sealed_product_price.date IS DISTINCT FROM EXCLUDED.date",
        );

        self.db.execute_query_builder(qb).await
    }

    pub async fn save_price_history(&self, prices: &[SealedProductPrice]) -> Result<i64> {
        if prices.is_empty() {
            return Ok(0);
        }

        let mut qb = QueryBuilder::new(
            "INSERT INTO sealed_product_price_history (sealed_product_uuid, price, date)",
        );

        qb.push_values(prices, |mut b, p| {
            b.push_bind(&p.sealed_product_uuid)
                .push_bind(&p.price)
                .push_bind(&p.date);
        });

        qb.push(
            " ON CONFLICT ON CONSTRAINT uq_sealed_product_price_history DO UPDATE SET
                price = COALESCE(EXCLUDED.price, sealed_product_price_history.price)",
        );

        self.db.execute_query_builder(qb).await
    }
}
