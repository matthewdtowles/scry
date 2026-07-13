use crate::database::ConnectionPool;
use crate::published_deck::domain::{RawDeck, ResolvedCard};
use crate::published_deck::repository::PublishedDeckRepository;
use crate::published_deck::source::{DecklistSource, FbettegaSource};
use crate::utils::clock;
use anyhow::Result;
use chrono::Duration;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{info, warn};

/// Keep published decks for this many days; older tournaments are pruned.
const RETENTION_DAYS: i64 = 90;

pub struct PublishedDeckService {
    repository: PublishedDeckRepository,
    sources: Vec<Box<dyn DecklistSource>>,
}

impl PublishedDeckService {
    pub fn new(db: Arc<ConnectionPool>) -> Self {
        Self {
            repository: PublishedDeckRepository::new(db),
            sources: vec![Box::new(FbettegaSource::new())],
        }
    }

    pub async fn fetch_count(&self) -> Result<i64> {
        self.repository.count().await
    }

    /// Ingest tournament decks published in the last `days` days from every
    /// configured source, then prune anything past the retention window.
    pub async fn ingest(&self, days: i64) -> Result<()> {
        let mut saved = 0_i64;
        let mut skipped_unresolved = 0_i64;

        for source in &self.sources {
            let name = source.source_name();
            info!("published-deck ingest: fetching from {name} (last {days} days)");
            let decks = match source.fetch_recent(days).await {
                Ok(d) => d,
                Err(e) => {
                    warn!("published-deck source {name} failed: {e}");
                    continue;
                }
            };
            info!("published-deck ingest: {name} returned {} decks", decks.len());

            let resolved = self.resolve_names(&decks).await?;

            for deck in &decks {
                let (cards, unresolved) = Self::resolve_deck(deck, &resolved);
                skipped_unresolved += unresolved as i64;
                if let Err(e) = self.repository.save_deck(deck, &cards).await {
                    warn!("failed to save deck {}: {e}", deck.source_uri);
                    continue;
                }
                saved += 1;
            }
        }

        let cutoff = clock::today() - Duration::days(RETENTION_DAYS);
        let pruned = self.repository.prune_older_than(cutoff).await?;

        info!(
            "published-deck ingest complete: saved={saved}, unresolved_cards={skipped_unresolved}, pruned={pruned}"
        );
        Ok(())
    }

    /// One batched name->id lookup across every card in every deck.
    async fn resolve_names(&self, decks: &[RawDeck]) -> Result<HashMap<String, String>> {
        let mut names: HashSet<String> = HashSet::new();
        for deck in decks {
            for line in &deck.lines {
                names.insert(line.card_name.to_lowercase());
            }
        }
        let names: Vec<String> = names.into_iter().collect();
        self.repository.resolve_card_ids(&names).await
    }

    /// Map a deck's lines to resolved card rows, aggregating by (card_id, board)
    /// so the same representative printing never collides. Returns the resolved
    /// rows plus the count of lines whose name could not be resolved.
    fn resolve_deck(deck: &RawDeck, resolved: &HashMap<String, String>) -> (Vec<ResolvedCard>, usize) {
        let mut agg: HashMap<(String, bool), i32> = HashMap::new();
        let mut unresolved = 0;
        for line in &deck.lines {
            match resolved.get(&line.card_name.to_lowercase()) {
                Some(card_id) => {
                    *agg.entry((card_id.clone(), line.is_sideboard)).or_insert(0) += line.count;
                }
                None => unresolved += 1,
            }
        }
        let cards = agg
            .into_iter()
            .map(|((card_id, is_sideboard), quantity)| ResolvedCard {
                card_id,
                quantity,
                is_sideboard,
            })
            .collect();
        (cards, unresolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::published_deck::domain::DeckLine;

    fn line(name: &str, count: i32, side: bool) -> DeckLine {
        DeckLine { card_name: name.to_string(), count, is_sideboard: side }
    }

    #[test]
    fn resolves_names_aggregates_by_board_and_counts_unresolved() {
        let deck = RawDeck {
            source: "fbettega".into(),
            source_uri: "uri".into(),
            tournament_name: None,
            tournament_date: None,
            format: None,
            player: None,
            result: None,
            lines: vec![
                line("Lightning Bolt", 2, false),
                line("Lightning Bolt", 2, false),
                line("Lightning Bolt", 1, true),
                line("Unknown Card", 4, false),
            ],
        };
        let mut resolved = HashMap::new();
        resolved.insert("lightning bolt".to_string(), "bolt-id".to_string());

        let (cards, unresolved) = PublishedDeckService::resolve_deck(&deck, &resolved);

        assert_eq!(unresolved, 1);
        let main = cards.iter().find(|c| !c.is_sideboard).unwrap();
        let side = cards.iter().find(|c| c.is_sideboard).unwrap();
        assert_eq!(main.card_id, "bolt-id");
        assert_eq!(main.quantity, 4); // two main lines summed
        assert_eq!(side.quantity, 1);
        assert_eq!(cards.len(), 2);
    }
}
