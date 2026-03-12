use crate::{
    database::ConnectionPool,
    set::domain::{Set, SetPrice},
};
use anyhow::{Ok, Result};
use sqlx::QueryBuilder;
use std::sync::Arc;
use tracing::warn;

#[derive(Clone)]
pub struct SetRepository {
    db: Arc<ConnectionPool>,
}

impl SetRepository {
    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self { db }
    }

    pub async fn count(&self) -> Result<i64> {
        let count = self.db.count("SELECT COUNT(*) FROM \"set\"").await?;
        Ok(count)
    }

    pub async fn fetch_empty_sets(&self) -> Result<Vec<Set>> {
        let qb = QueryBuilder::new(
            "SELECT * FROM \"set\" s
            WHERE NOT EXISTS (
                SELECT 1 FROM card c WHERE c.set_code = s.code
            ) ORDER BY s.code",
        );
        let result: Vec<Set> = self.db.fetch_all_query_builder(qb).await?;
        Ok(result)
    }

    pub async fn fetch_missing_prices(&self, threshold_pct: f64) -> Result<Vec<Set>> {
        let mut qb = QueryBuilder::new(
            "SELECT * FROM \"set\" s
            WHERE s.code IN (
                SELECT c.set_code 
                FROM card c
                LEFT JOIN price p ON p.card_id = c.id
                GROUP BY c.set_code
                HAVING (COUNT(DISTINCT p.card_id)::float / NULLIF(COUNT(*), 0)) < ",
        );
        qb.push_bind(threshold_pct);
        qb.push(")");
        let result: Vec<Set> = self.db.fetch_all_query_builder(qb).await?;
        Ok(result)
    }

    pub async fn save_sets(&self, sets: &[Set]) -> Result<i64> {
        if sets.is_empty() {
            warn!("0 sets given, 0 sets saved.");
            return Ok(0);
        }
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO \"set\" (
                code, base_size, block, keyrune_code,
                name, parent_code, release_date, type, is_main
            )",
        );
        query_builder.push_values(sets, |mut b, set| {
            b.push_bind(&set.code)
                .push_bind(&set.base_size)
                .push_bind(&set.block)
                .push_bind(&set.keyrune_code)
                .push_bind(&set.name)
                .push_bind(&set.parent_code)
                .push_bind(&set.release_date)
                .push_bind(&set.set_type)
                .push_bind(&set.is_main);
        });
        query_builder.push(
            " ON CONFLICT (code) DO UPDATE SET
            base_size = EXCLUDED.base_size,
            block = EXCLUDED.block,
            keyrune_code = EXCLUDED.keyrune_code,
            name = EXCLUDED.name,
            parent_code = EXCLUDED.parent_code,
            release_date = EXCLUDED.release_date,
            type = EXCLUDED.type,
            is_main = EXCLUDED.is_main
        WHERE
            set.base_size IS DISTINCT FROM EXCLUDED.base_size OR
            set.block IS DISTINCT FROM EXCLUDED.block OR
            set.keyrune_code IS DISTINCT FROM EXCLUDED.keyrune_code OR
            set.name IS DISTINCT FROM EXCLUDED.name OR
            set.parent_code IS DISTINCT FROM EXCLUDED.parent_code OR
            set.release_date IS DISTINCT FROM EXCLUDED.release_date OR
            set.type IS DISTINCT FROM EXCLUDED.type OR
            set.is_main IS DISTINCT FROM EXCLUDED.is_main",
        );
        self.db.execute_query_builder(query_builder).await
    }

    pub async fn fetch_set_codes_with_prices(&self) -> Result<Vec<String>> {
        let qb = QueryBuilder::new(
            "SELECT DISTINCT c.set_code FROM card c
             JOIN price p ON p.card_id = c.id",
        );
        let rows: Vec<(String,)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    pub async fn calculate_set_prices(&self, codes: &[String]) -> Result<Vec<SetPrice>> {
        if codes.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = QueryBuilder::new(
            "SELECT c.set_code AS set_code,
                COALESCE(SUM(COALESCE(p.normal, p.foil, 0)) FILTER (WHERE c.in_main), 0) AS base_price,
                COALESCE(SUM(COALESCE(p.normal, p.foil, 0)), 0) AS total_price,
                COALESCE(SUM(COALESCE(p.normal, 0) + COALESCE(p.foil, 0)) FILTER (WHERE c.in_main), 0) AS base_price_all,
                COALESCE(SUM(COALESCE(p.normal, 0) + COALESCE(p.foil, 0)), 0) AS total_price_all,
                MAX(p.date) AS date
            FROM card c
            JOIN price p ON p.card_id = c.id
            WHERE c.set_code IN ("
        );
        for (i, code) in codes.iter().enumerate() {
            if i > 0 {
                qb.push(",");
            }
            qb.push_bind(code);
        }
        qb.push(") GROUP BY c.set_code");
        let set_prices: Vec<SetPrice> = self.db.fetch_all_query_builder(qb).await?;
        Ok(set_prices)
    }

    pub async fn update_prices(&self, set_prices: Vec<SetPrice>) -> Result<i64> {
        if set_prices.is_empty() {
            warn!("0 set prices given, 0 prices saved.");
            return Ok(0);
        }
        let mut qb = QueryBuilder::new(
            "INSERT INTO set_price (set_code, base_price, total_price, base_price_all, total_price_all, date)",
        );
        qb.push_values(&set_prices, |mut b, sp| {
            b.push_bind(&sp.set_code)
                .push_bind(&sp.base_price)
                .push_bind(&sp.total_price)
                .push_bind(&sp.base_price_all)
                .push_bind(&sp.total_price_all)
                .push_bind(&sp.date);
        });
        qb.push(
            " ON CONFLICT (set_code) DO UPDATE SET
                base_price = COALESCE(EXCLUDED.base_price, set_price.base_price),
                total_price = COALESCE(EXCLUDED.total_price, set_price.total_price),
                base_price_all = COALESCE(EXCLUDED.base_price_all, set_price.base_price_all),
                total_price_all = COALESCE(EXCLUDED.total_price_all, set_price.total_price_all),
                date = EXCLUDED.date
            WHERE
                set_price.base_price IS DISTINCT FROM COALESCE(EXCLUDED.base_price, set_price.base_price) OR
                set_price.total_price IS DISTINCT FROM COALESCE(EXCLUDED.total_price, set_price.total_price) OR
                set_price.base_price_all IS DISTINCT FROM COALESCE(EXCLUDED.base_price_all, set_price.base_price_all) OR
                set_price.total_price_all IS DISTINCT FROM COALESCE(EXCLUDED.total_price_all, set_price.total_price_all) OR
                set_price.date IS DISTINCT FROM EXCLUDED.date",
        );
        self.db.execute_query_builder(qb).await
    }

    pub async fn save_set_price_history(&self, set_prices: Vec<SetPrice>) -> Result<i64> {
        if set_prices.is_empty() {
            warn!("0 set prices given, 0 set price history rows saved.");
            return Ok(0);
        }
        let mut qb = QueryBuilder::new(
            "INSERT INTO set_price_history (set_code, base_price, total_price, base_price_all, total_price_all, date)",
        );
        qb.push_values(&set_prices, |mut b, sp| {
            b.push_bind(&sp.set_code)
                .push_bind(&sp.base_price)
                .push_bind(&sp.total_price)
                .push_bind(&sp.base_price_all)
                .push_bind(&sp.total_price_all)
                .push_bind(&sp.date);
        });
        qb.push(
            " ON CONFLICT (set_code, date) DO UPDATE SET
                base_price = COALESCE(EXCLUDED.base_price, set_price_history.base_price),
                total_price = COALESCE(EXCLUDED.total_price, set_price_history.total_price),
                base_price_all = COALESCE(EXCLUDED.base_price_all, set_price_history.base_price_all),
                total_price_all = COALESCE(EXCLUDED.total_price_all, set_price_history.total_price_all)
            WHERE
                set_price_history.base_price IS DISTINCT FROM COALESCE(EXCLUDED.base_price, set_price_history.base_price) OR
                set_price_history.total_price IS DISTINCT FROM COALESCE(EXCLUDED.total_price, set_price_history.total_price) OR
                set_price_history.base_price_all IS DISTINCT FROM COALESCE(EXCLUDED.base_price_all, set_price_history.base_price_all) OR
                set_price_history.total_price_all IS DISTINCT FROM COALESCE(EXCLUDED.total_price_all, set_price_history.total_price_all)",
        );
        self.db.execute_query_builder(qb).await
    }

    pub async fn backfill_set_price_history(&self) -> Result<i64> {
        let qb = QueryBuilder::new(
            "INSERT INTO set_price_history (set_code, base_price, total_price, base_price_all, total_price_all, date)
            SELECT
                c.set_code,
                COALESCE(SUM(COALESCE(ph.normal, ph.foil, 0)) FILTER (WHERE c.in_main), 0),
                COALESCE(SUM(COALESCE(ph.normal, ph.foil, 0)), 0),
                COALESCE(SUM(COALESCE(ph.normal, 0) + COALESCE(ph.foil, 0)) FILTER (WHERE c.in_main), 0),
                COALESCE(SUM(COALESCE(ph.normal, 0) + COALESCE(ph.foil, 0)), 0),
                ph.date
            FROM card c
            JOIN price_history ph ON ph.card_id = c.id
            GROUP BY c.set_code, ph.date
            ON CONFLICT (set_code, date) DO UPDATE SET
                base_price = COALESCE(EXCLUDED.base_price, set_price_history.base_price),
                total_price = COALESCE(EXCLUDED.total_price, set_price_history.total_price),
                base_price_all = COALESCE(EXCLUDED.base_price_all, set_price_history.base_price_all),
                total_price_all = COALESCE(EXCLUDED.total_price_all, set_price_history.total_price_all)",
        );
        self.db.execute_query_builder(qb).await
    }

    pub async fn apply_set_price_history_weekly_retention(&self) -> Result<i64> {
        self.db
            .count(
                "WITH deleted AS ( \
                    DELETE FROM set_price_history \
                    WHERE date >= CURRENT_DATE - INTERVAL '28 days' \
                      AND date < CURRENT_DATE - INTERVAL '7 days' \
                      AND EXTRACT(DOW FROM date) NOT IN (1) \
                    RETURNING 1 \
                ) \
                SELECT COUNT(*) FROM deleted",
            )
            .await
    }

    pub async fn apply_set_price_history_monthly_retention(&self) -> Result<i64> {
        self.db
            .count(
                "WITH deleted AS ( \
                    DELETE FROM set_price_history \
                    WHERE date < CURRENT_DATE - INTERVAL '28 days' \
                      AND EXTRACT(DAY FROM date) != 1 \
                    RETURNING 1 \
                ) \
                SELECT COUNT(*) FROM deleted",
            )
            .await
    }

    pub async fn update_set_price_change_weekly(&self) -> Result<i64> {
        let qb = QueryBuilder::new(
            "UPDATE set_price sp \
             SET base_price_change_weekly = sp.base_price - sph.base_price, \
                 total_price_change_weekly = sp.total_price - sph.total_price, \
                 base_price_all_change_weekly = sp.base_price_all - sph.base_price_all, \
                 total_price_all_change_weekly = sp.total_price_all - sph.total_price_all \
             FROM ( \
                 SELECT DISTINCT ON (set_code) set_code, base_price, total_price, base_price_all, total_price_all \
                 FROM set_price_history \
                 WHERE date <= CURRENT_DATE - INTERVAL '7 days' \
                 ORDER BY set_code, date DESC \
             ) sph \
             WHERE sph.set_code = sp.set_code",
        );
        self.db.execute_query_builder(qb).await
    }

    pub async fn delete_all(&self) -> Result<i64> {
        let qb = QueryBuilder::new("DELETE FROM set CASCADE");
        self.db.execute_query_builder(qb).await
    }

    pub async fn delete_set_batch(&self, set_code: &str) -> Result<i64> {
        if set_code.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::new("WITH to_del AS (SELECT id FROM card WHERE set_code = ");
        qb.push_bind(set_code);
        qb.push(
            "),
            del_legalities AS (
                DELETE FROM legality WHERE card_id IN (SELECT id FROM to_del)
            ),
            del_prices AS (
                DELETE FROM price WHERE card_id IN (SELECT id FROM to_del)
            ),
            del_inventory AS (
                DELETE FROM inventory WHERE card_id IN (SELECT id FROM to_del)
            ),
            del_cards AS (
                DELETE FROM card WHERE id IN (SELECT id FROM to_del) RETURNING id
            )
            DELETE FROM \"set\" WHERE code = ",
        );
        qb.push_bind(set_code);
        let deleted = self.db.execute_query_builder(qb).await?;
        Ok(deleted)
    }

    pub async fn update_parent_codes(&self) -> Result<i64> {
        // Step 1: Set parent_code where a set's name matches the block name
        let qb_name_match = QueryBuilder::new(
            "UPDATE \"set\" s
            SET parent_code = p.code
            FROM \"set\" p
            WHERE s.parent_code IS NULL
              AND s.block IS NOT NULL
              AND p.name = s.block
              AND p.code != s.code
              AND s.parent_code IS DISTINCT FROM p.code",
        );
        let name_matched = self.db.execute_query_builder(qb_name_match).await?;

        // Step 2: For blocks where no set name matches the block name,
        // use the earliest is_main set as the parent
        let qb_earliest = QueryBuilder::new(
            "UPDATE \"set\" s
            SET parent_code = first.code
            FROM (
                SELECT DISTINCT ON (b.block) b.code, b.block
                FROM \"set\" b
                WHERE b.block IS NOT NULL
                  AND b.is_main = true
                  AND NOT EXISTS (
                      SELECT 1 FROM \"set\" n WHERE n.name = b.block
                  )
                ORDER BY b.block, b.release_date ASC, b.code ASC
            ) first
            WHERE s.block = first.block
              AND s.code != first.code
              AND s.parent_code IS NULL
              AND s.parent_code IS DISTINCT FROM first.code",
        );
        let earliest_matched = self.db.execute_query_builder(qb_earliest).await?;

        // Step 3: Normalize parent_codes within each block so all sets
        // point to the same canonical parent (earliest main set).
        // Fixes cases where Scryfall sets parent_code to a non-first
        // set in the block (e.g., AER promos → AER instead of KLD).
        let qb_normalize = QueryBuilder::new(
            "UPDATE \"set\" s
            SET parent_code = first.code
            FROM (
                SELECT DISTINCT ON (b.block) b.code, b.block
                FROM \"set\" b
                WHERE b.block IS NOT NULL
                  AND b.is_main = true
                ORDER BY b.block, b.release_date ASC, b.code ASC
            ) first
            WHERE s.block = first.block
              AND s.code != first.code
              AND s.parent_code IS DISTINCT FROM first.code",
        );
        let normalized = self.db.execute_query_builder(qb_normalize).await?;

        Ok(name_matched + earliest_matched + normalized)
    }

    pub async fn update_is_main(&self) -> Result<i64> {
        let qb = QueryBuilder::new(
            "UPDATE \"set\" SET is_main = (base_size > 0 AND type != 'masterpiece')
            WHERE is_main IS DISTINCT FROM (base_size > 0 AND type != 'masterpiece')",
        );
        self.db.execute_query_builder(qb).await
    }

    pub async fn update_sizes(
        &self,
        base_sizes: &[(String, i64)],
        total_sizes: &[(String, i64)],
    ) -> Result<i64> {
        let mut total_updated = 0i64;
        let update_stmt = |col: &str| {
            format!(
                ") UPDATE \"set\" s SET {col} = v.size FROM vals v
                WHERE s.code = v.code AND (s.{col} IS DISTINCT FROM v.size)"
            )
        };
        if !base_sizes.is_empty() {
            let mut qb = QueryBuilder::new("WITH vals(code, size) AS (");
            qb.push_values(base_sizes, |mut b, pair| {
                b.push_bind(&pair.0).push_bind(&pair.1);
            });
            qb.push(&update_stmt("base_size"));
            total_updated += self.db.execute_query_builder(qb).await?;
        }
        if !total_sizes.is_empty() {
            let mut qb = QueryBuilder::new("WITH vals(code, size) AS (");
            qb.push_values(total_sizes, |mut b, pair| {
                b.push_bind(&pair.0).push_bind(&pair.1);
            });
            qb.push(&update_stmt("total_size"));
            total_updated += self.db.execute_query_builder(qb).await?;
        }
        Ok(total_updated)
    }
}
