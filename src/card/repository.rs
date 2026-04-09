use crate::{card::domain::Card, database::ConnectionPool};
use anyhow::Result;
use sqlx::QueryBuilder;
use std::sync::Arc;
use tracing::{debug, error, warn};

#[derive(Clone)]
pub struct CardRepository {
    db: Arc<ConnectionPool>,
}

impl CardRepository {
    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self { db }
    }

    pub async fn fetch_unpriced_ids(&self, candidate_ids: &[String]) -> Result<Vec<String>> {
        if candidate_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = QueryBuilder::new(
            "SELECT c.id FROM card c
            LEFT JOIN price p ON p.card_id = c.id
            WHERE p.id IS NULL and c.id = ANY(",
        );
        qb.push_bind(candidate_ids);
        qb.push(")");
        let rows: Vec<(String,)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    pub async fn fetch_non_ascii_numbers_in_set(&self, set_code: &str) -> Result<Vec<Card>> {
        let mut qb = QueryBuilder::new("SELECT * FROM card WHERE set_code = ");
        qb.push_bind(set_code);
        qb.push(" AND number ~ '[^\\x00-\\x7F]' ORDER BY sort_number");
        let rows: Vec<Card> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows)
    }

    pub async fn fetch_ascii_cards_by_set_and_names(
        &self,
        set_code: &str,
        names: &[String],
    ) -> Result<Vec<Card>> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = QueryBuilder::new("SELECT * FROM card WHERE set_code = ");
        qb.push_bind(set_code);
        qb.push(" AND name = ANY(");
        qb.push_bind(names);
        qb.push(") AND number !~ '[^\\x00-\\x7F]' ORDER BY name, sort_number");
        let rows: Vec<Card> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows)
    }

    pub async fn fetch_in_main_cards_for_set_types(
        &self,
        set_types: &[&str],
    ) -> Result<Vec<Card>> {
        if set_types.is_empty() {
            return Ok(Vec::new());
        }
        let mut qb = QueryBuilder::new(
            "SELECT c.* FROM card c
            JOIN \"set\" s ON s.code = c.set_code
            WHERE c.in_main = true AND s.type = ANY(",
        );
        qb.push_bind(
            set_types
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
        );
        qb.push(") ORDER BY c.set_code, c.sort_number");
        let rows: Vec<Card> = self.db.fetch_all_query_builder(qb).await?;
        debug!(
            "Found {} in_main cards for non-main set types.",
            rows.len()
        );
        Ok(rows)
    }

    pub async fn fetch_misclassified_as_in_main(&self) -> Result<Vec<Card>> {
        let qb = QueryBuilder::new(
            "WITH nums AS (
                SELECT *, number::int AS n
                FROM card
                WHERE number ~ '^\\d+$'
            ),
            min_non_main AS (
                SELECT set_code, 
                    CASE 
                        WHEN MIN(n)= 0 THEN MIN(n) FILTER (WHERE n> 0) 
                        ELSE MIN(n) 
                    END AS min_n
                FROM nums
                WHERE in_main = false \
                AND n IS NOT NULL
                GROUP BY set_code 
                HAVING CASE 
                    WHEN MIN(n) = 0 THEN MIN(n) FILTER (WHERE n > 0) 
                    ELSE MIN(n) 
                END IS NOT NULL
            )
            SELECT *
            FROM nums n
            JOIN min_non_main m USING (set_code)
            WHERE n.n > m.min_n
            AND n.in_main = true
            ORDER BY n.set_code, n.n",
        );
        let rows: Vec<Card> = self.db.fetch_all_query_builder(qb).await?;
        debug!(
            "Found {} cards misclassified as being in the main set.",
            rows.len()
        );
        Ok(rows)
    }

    pub async fn save_cards(&self, cards: &[Card]) -> Result<i64> {
        if cards.is_empty() {
            warn!("0 cards given, 0 cards saved.");
            return Ok(0);
        }
        debug!("Saving {} cards", cards.len());
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO card (
                id, artist, flavor_name, has_foil, has_non_foil, img_src,
                in_main, is_alternative, is_reserved, mana_cost, name,
                number, oracle_text, purchase_url_tcgplayer,
                purchase_url_tcgplayer_etched, rarity, set_code,
                sort_number, type, layout
            )",
        );
        query_builder.push_values(cards, |mut b, card| {
            b.push_bind(&card.id)
                .push_bind(&card.artist)
                .push_bind(&card.flavor_name)
                .push_bind(&card.has_foil)
                .push_bind(&card.has_non_foil)
                .push_bind(&card.img_src)
                .push_bind(&card.in_main)
                .push_bind(&card.is_alternative)
                .push_bind(&card.is_reserved)
                .push_bind(&card.mana_cost)
                .push_bind(&card.name)
                .push_bind(&card.number)
                .push_bind(&card.oracle_text)
                .push_bind(&card.purchase_url_tcgplayer)
                .push_bind(&card.purchase_url_tcgplayer_etched)
                .push_bind(&card.rarity)
                .push_bind(&card.set_code)
                .push_bind(&card.sort_number)
                .push_bind(&card.type_line)
                .push_bind(&card.layout);
        });
        query_builder.push(
            " ON CONFLICT (id) DO UPDATE SET
            artist = EXCLUDED.artist,
            flavor_name = EXCLUDED.flavor_name,
            has_foil = EXCLUDED.has_foil,
            has_non_foil = EXCLUDED.has_non_foil,
            img_src = EXCLUDED.img_src,
            in_main = EXCLUDED.in_main,
            is_alternative = EXCLUDED.is_alternative,
            is_reserved = EXCLUDED.is_reserved,
            mana_cost = EXCLUDED.mana_cost,
            name = EXCLUDED.name,
            number = EXCLUDED.number,
            oracle_text = EXCLUDED.oracle_text,
            purchase_url_tcgplayer = EXCLUDED.purchase_url_tcgplayer,
            purchase_url_tcgplayer_etched = EXCLUDED.purchase_url_tcgplayer_etched,
            rarity = EXCLUDED.rarity,
            set_code = EXCLUDED.set_code,
            sort_number = EXCLUDED.sort_number,
            type = EXCLUDED.type,
            layout = EXCLUDED.layout
        WHERE
            card.artist IS DISTINCT FROM EXCLUDED.artist OR
            card.flavor_name IS DISTINCT FROM EXCLUDED.flavor_name OR
            card.has_foil IS DISTINCT FROM EXCLUDED.has_foil OR
            card.has_non_foil IS DISTINCT FROM EXCLUDED.has_non_foil OR
            card.img_src IS DISTINCT FROM EXCLUDED.img_src OR
            card.in_main IS DISTINCT FROM EXCLUDED.in_main OR
            card.is_alternative IS DISTINCT FROM EXCLUDED.is_alternative OR
            card.is_reserved IS DISTINCT FROM EXCLUDED.is_reserved OR
            card.mana_cost IS DISTINCT FROM EXCLUDED.mana_cost OR
            card.name IS DISTINCT FROM EXCLUDED.name OR
            card.number IS DISTINCT FROM EXCLUDED.number OR
            card.oracle_text IS DISTINCT FROM EXCLUDED.oracle_text OR
            card.purchase_url_tcgplayer IS DISTINCT FROM EXCLUDED.purchase_url_tcgplayer OR
            card.purchase_url_tcgplayer_etched IS DISTINCT FROM EXCLUDED.purchase_url_tcgplayer_etched OR
            card.rarity IS DISTINCT FROM EXCLUDED.rarity OR
            card.set_code IS DISTINCT FROM EXCLUDED.set_code OR
            card.sort_number IS DISTINCT FROM EXCLUDED.sort_number OR
            card.type IS DISTINCT FROM EXCLUDED.type OR
            card.layout IS DISTINCT FROM EXCLUDED.layout",
        );
        match self.db.execute_query_builder(query_builder).await {
            Ok(rows_affected) => Ok(rows_affected),
            Err(e) => {
                error!("Failed to save {} cards: {}", cards.len(), e);
                Err(e)
            }
        }
    }

    pub async fn save_legalities(&self, cards: &[Card]) -> Result<i64> {
        let card_ids: Vec<String> = cards.iter().map(|c| c.id.clone()).collect();
        if card_ids.is_empty() {
            return Ok(0);
        }
        let legalities: Vec<_> = cards.iter().flat_map(|c| c.legalities.clone()).collect();
        if legalities.is_empty() {
            let mut delete_qb = QueryBuilder::new("DELETE FROM legality WHERE card_id = ANY(");
            delete_qb.push_bind(card_ids);
            delete_qb.push(")");
            return self.db.execute_query_builder(delete_qb).await;
        }
        let mut delete_qb = QueryBuilder::new("DELETE FROM legality WHERE card_id = ANY(");
        delete_qb.push_bind(card_ids.clone());
        delete_qb.push(")");
        self.db.execute_query_builder(delete_qb).await?;
        let mut ids: Vec<String> = Vec::with_capacity(legalities.len());
        let mut formats: Vec<String> = Vec::with_capacity(legalities.len());
        let mut statuses: Vec<String> = Vec::with_capacity(legalities.len());
        for l in &legalities {
            ids.push(l.card_id.clone());
            formats.push(l.format.to_string());
            statuses.push(l.status.to_string());
        }
        let mut qb = QueryBuilder::new(
            "INSERT INTO legality (card_id, format, status) \
             SELECT u.card_id, u.format::format_enum, u.status::legality_status_enum \
             FROM UNNEST(",
        );
        qb.push_bind(ids);
        qb.push(", ");
        qb.push_bind(formats);
        qb.push(", ");
        qb.push_bind(statuses);
        qb.push(") AS u(card_id, format, status) JOIN card c ON c.id = u.card_id");
        let inserted = self.db.execute_query_builder(qb).await?;
        Ok(inserted)
    }

    pub async fn count(&self) -> Result<u64> {
        let count = self.db.count("SELECT COUNT(*) FROM card").await?;
        Ok(count as u64)
    }

    pub async fn count_for_sets(&self, main_only: bool) -> Result<Vec<(String, i64)>> {
        let mut qb = QueryBuilder::new("SELECT set_code, COUNT(*)::bigint AS total FROM card");
        if main_only {
            qb.push(" WHERE in_main = true");
        }
        qb.push(" GROUP BY set_code");
        let rows: Vec<(String, i64)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows)
    }

    pub async fn legality_count(&self) -> Result<u64> {
        let count = self.db.count("SELECT COUNT(*) FROM legality").await?;
        Ok(count as u64)
    }

    pub async fn set_exists(&self, code: &str) -> Result<bool> {
        if code.is_empty() {
            return Ok(false);
        }
        let exists = self.db.row_exists("set", "code", code).await?;
        Ok(exists)
    }

    pub async fn delete_all(&self) -> Result<i64> {
        self.delete_table(String::from("legality")).await?;
        let cards_deleted = self.delete_table(String::from("card")).await?;
        debug!("{} cards deleted.", cards_deleted);
        Ok(cards_deleted)
    }

    pub async fn delete_cards_batch(&self, ids: &[String], batch_size: i64) -> Result<i64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut total_deleted = 0i64;
        for chunk in ids.chunks(batch_size as usize) {
            let mut qb = QueryBuilder::new("DELETE FROM legality WHERE card_id = ANY(");
            qb.push_bind(chunk.to_vec());
            qb.push(")");
            self.db.execute_query_builder(qb).await?;
            let mut qb = QueryBuilder::new("DELETE FROM price WHERE card_id = ANY(");
            qb.push_bind(chunk.to_vec());
            qb.push(")");
            self.db.execute_query_builder(qb).await?;
            let mut qb = QueryBuilder::new("DELETE FROM inventory WHERE card_id = ANY(");
            qb.push_bind(chunk.to_vec());
            qb.push(")");
            self.db.execute_query_builder(qb).await?;
            let mut qb = QueryBuilder::new("DELETE FROM card WHERE id = ANY(");
            qb.push_bind(chunk.to_vec());
            qb.push(")");
            total_deleted += self.db.execute_query_builder(qb).await?;
        }
        debug!("Deleted {} cards for {} card ids", total_deleted, ids.len());
        Ok(total_deleted)
    }

    async fn delete_table(&self, table: String) -> Result<i64> {
        let qb = QueryBuilder::new(format!("DELETE FROM {} CASCADE", table));
        let total_deleted = self.db.execute_query_builder(qb).await?;
        debug!("{} {} entities deleted.", total_deleted, table);
        Ok(total_deleted)
    }
}
