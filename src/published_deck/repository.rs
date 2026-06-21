use crate::database::ConnectionPool;
use crate::published_deck::domain::{RawDeck, ResolvedCard};
use anyhow::Result;
use chrono::NaiveDate;
use sqlx::QueryBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

#[derive(Clone)]
pub struct PublishedDeckRepository {
    db: Arc<ConnectionPool>,
}

impl PublishedDeckRepository {
    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self { db }
    }

    pub async fn count(&self) -> Result<i64> {
        self.db.count("SELECT COUNT(*) FROM published_deck").await
    }

    /// Resolve lowercased card names to a representative printing id (cheapest
    /// priced, falling back to any). One query; returns name_key -> card_id.
    pub async fn resolve_card_ids(
        &self,
        lower_names: &[String],
    ) -> Result<HashMap<String, String>> {
        if lower_names.is_empty() {
            return Ok(HashMap::new());
        }
        let mut qb = QueryBuilder::new(
            "SELECT DISTINCT ON (lower(c.name)) lower(c.name) AS name_key, c.id \
             FROM card c \
             LEFT JOIN price p ON p.card_id = c.id \
                 AND p.date = (SELECT MAX(date) FROM price WHERE card_id = c.id) \
             WHERE lower(c.name) = ANY(",
        );
        qb.push_bind(lower_names);
        qb.push(") ORDER BY lower(c.name), COALESCE(p.normal, p.foil) ASC NULLS LAST, c.id");

        let rows: Vec<(String, String)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows.into_iter().collect())
    }

    /// Upsert a deck (dedup on source + source_uri) and replace its card rows.
    pub async fn save_deck(&self, deck: &RawDeck, cards: &[ResolvedCard]) -> Result<()> {
        let mut qb = QueryBuilder::new(
            "INSERT INTO published_deck \
             (source, source_uri, tournament_name, tournament_date, format, player, result, updated_at) \
             VALUES (",
        );
        let mut sep = qb.separated(", ");
        sep.push_bind(&deck.source);
        sep.push_bind(&deck.source_uri);
        sep.push_bind(&deck.tournament_name);
        sep.push_bind(deck.tournament_date);
        sep.push_bind(&deck.format);
        sep.push_bind(&deck.player);
        sep.push_bind(&deck.result);
        qb.push(", NOW()) ON CONFLICT (source, source_uri) DO UPDATE SET \
             tournament_name = EXCLUDED.tournament_name, \
             tournament_date = EXCLUDED.tournament_date, \
             format = EXCLUDED.format, \
             player = EXCLUDED.player, \
             result = EXCLUDED.result, \
             updated_at = NOW() \
             RETURNING id");

        let rows: Vec<(i32,)> = self.db.fetch_all_query_builder(qb).await?;
        let Some((deck_id,)) = rows.into_iter().next() else {
            warn!("published_deck upsert returned no id for {}", deck.source_uri);
            return Ok(());
        };

        // Replace children so re-ingesting a changed deck stays consistent.
        let mut del = QueryBuilder::new("DELETE FROM published_deck_card WHERE published_deck_id = ");
        del.push_bind(deck_id);
        self.db.execute_query_builder(del).await?;

        if !cards.is_empty() {
            let mut ins = QueryBuilder::new(
                "INSERT INTO published_deck_card (published_deck_id, card_id, quantity, is_sideboard) ",
            );
            ins.push_values(cards, |mut b, c| {
                b.push_bind(deck_id)
                    .push_bind(&c.card_id)
                    .push_bind(c.quantity)
                    .push_bind(c.is_sideboard);
            });
            self.db.execute_query_builder(ins).await?;
        }
        Ok(())
    }

    /// Drop decks whose tournament date is older than the cutoff. Returns the
    /// number removed.
    pub async fn prune_older_than(&self, cutoff: NaiveDate) -> Result<i64> {
        let mut qb = QueryBuilder::new(
            "WITH deleted AS (DELETE FROM published_deck WHERE tournament_date < ",
        );
        qb.push_bind(cutoff);
        qb.push(" RETURNING 1) SELECT COUNT(*) FROM deleted");
        let rows: Vec<(i64,)> = self.db.fetch_all_query_builder(qb).await?;
        Ok(rows.into_iter().next().map(|(n,)| n).unwrap_or(0))
    }
}
