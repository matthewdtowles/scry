use crate::{
    card::{
        domain::{Card, MainSetClassifier},
        event_processor::CardEventProcessor,
        mapper::CardMapper,
        ports::{CardDataSource, CardRepositoryPort},
        repository::CardRepository,
    },
    database::ConnectionPool,
    price::service::PriceService,
    utils::{HttpClient, JsonStreamParser},
};
use anyhow::Result;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::{debug, warn};

pub struct CardService {
    data_source: Arc<dyn CardDataSource>,
    repository: Arc<dyn CardRepositoryPort>,
}

impl CardService {
    pub(crate) const BATCH_SIZE: usize = 500;

    pub fn new(db: Arc<ConnectionPool>, http_client: Arc<HttpClient>) -> Self {
        Self::with_ports(http_client, Arc::new(CardRepository::new(db)))
    }

    /// Construct from explicit ports; used by tests to inject fakes (a canned
    /// data source + an in-memory repository) instead of live HTTP + Postgres.
    pub fn with_ports(
        data_source: Arc<dyn CardDataSource>,
        repository: Arc<dyn CardRepositoryPort>,
    ) -> Self {
        Self {
            data_source,
            repository,
        }
    }

    /// The card persistence port (cheap `Arc` clone), for the single-pass
    /// card+sealed ingest orchestrated in [`crate::cli::ingest_pipeline`].
    pub(crate) fn repository(&self) -> Arc<dyn CardRepositoryPort> {
        self.repository.clone()
    }

    /// The `AllPrintings.json` byte stream, for the single-pass ingest above.
    pub(crate) async fn all_cards_stream(&self) -> Result<crate::card::ports::ByteStream> {
        self.data_source.all_cards_stream().await
    }

    pub async fn fetch_count(&self) -> Result<u64> {
        self.repository.count().await
    }

    pub async fn count_per_all_sets(&self, main_only: bool) -> Result<Vec<(String, i64)>> {
        self.repository.count_for_sets(main_only).await
    }

    pub async fn fetch_legality_count(&self) -> Result<u64> {
        self.repository.legality_count().await
    }

    pub async fn ingest_set_cards(&self, set_code: &str) -> Result<i64> {
        debug!("Starting card ingestion for set: {}", set_code);
        let raw_data: Value = self.data_source.fetch_set_cards(set_code).await?;
        let parsed = CardMapper::map_to_cards(raw_data)?;
        if parsed.is_empty() {
            warn!("No cards found for set: {}", set_code);
            return Ok(0);
        }
        let final_cards = Self::merge_and_filter_cards(parsed);
        if final_cards.is_empty() {
            return Ok(0);
        }
        let count = self.repository.save_cards(&final_cards).await?;
        self.repository.save_legalities(&final_cards).await?;
        // Conditional upsert, so `count` is rows changed, not cards seen.
        debug!("Cards ingest for set {}: {} rows changed", set_code, count);
        Ok(count)
    }

    pub async fn ingest_all(&self) -> Result<()> {
        debug!("Start ingestion of all cards");
        let byte_stream = self.data_source.all_cards_stream().await?;
        debug!("Received byte stream for all cards");
        let event_processor = CardEventProcessor::new(Self::BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let repo = self.repository.clone();
        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let repo = repo.clone();
                Box::pin(async move { Self::save_card_batch(&repo, batch).await })
            })
            .await?;
        Ok(())
    }

    /// Persist one parsed card batch. A batch is a whole set
    /// (flush-on-set-boundary, so the split-card merge sees both faces): skip it
    /// if the set isn't in the DB yet, merge/filter, then save cards +
    /// legalities in bind-parameter-safe chunks. The stream parser hands batches
    /// to us one at a time, so this runs sequentially. Shared by
    /// [`Self::ingest_all`] and the single-pass ingest in
    /// [`crate::cli::ingest_pipeline`].
    pub(crate) async fn save_card_batch(
        repo: &Arc<dyn CardRepositoryPort>,
        batch: Vec<Card>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let set_code = batch[0].set_code.clone();
        if !repo.set_exists(&set_code).await? {
            warn!("Skipping cards for missing set {}", set_code);
            return Ok(());
        }
        let batch = Self::merge_and_filter_cards(batch);
        for chunk in batch.chunks(Self::BATCH_SIZE) {
            repo.save_cards(chunk).await?;
            repo.save_legalities(chunk).await?;
        }
        Ok(())
    }

    /// Wipe the entire MTG catalog for a full re-ingest (`ingest -r`).
    pub async fn reset_all_data(&self) -> Result<()> {
        debug!("Resetting all MTG catalog data.");
        self.repository.reset_all_data().await
    }

    pub async fn cleanup_cards(&self, batch_size: i64) -> Result<u64> {
        debug!("Starting streaming cleanup");
        let byte_stream = self.data_source.all_cards_stream().await?;
        let event_processor = CardEventProcessor::new(Self::BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let repo = self.repository.clone();
        let total = Arc::new(Mutex::new(0u64));
        let total_for_closure = total.clone();
        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let repo = repo.clone();
                let total = total_for_closure.clone();
                Box::pin(async move {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    let mut ids_to_delete: Vec<String> = Vec::new();
                    for c in batch.iter() {
                        if c.should_filter() {
                            ids_to_delete.push(c.id.clone());
                        }
                    }
                    if ids_to_delete.is_empty() {
                        return Ok(());
                    }
                    let deleted = repo.delete_cards_batch(&ids_to_delete, batch_size).await?;
                    let mut lock = total.lock().await;
                    *lock += deleted as u64;
                    Ok(())
                })
            })
            .await?;
        let final_total = *total.lock().await;
        debug!(
            "Streaming cleanup complete; total affected: {}",
            final_total
        );
        Ok(final_total)
    }

    /// Delete foreign (non-English) cards that have no price row. Fully
    /// DB-driven via the persisted `language` column, so it works the same
    /// whether run inside the ingest pipeline or as a standalone
    /// `post-ingest-prune` invocation.
    pub async fn prune_foreign_unpriced(&self) -> Result<i64> {
        let ids_to_delete = self.repository.fetch_foreign_unpriced_ids().await?;
        if ids_to_delete.is_empty() {
            debug!("Found 0 unpriced foreign cards to delete.");
            return Ok(0);
        }
        debug!(
            "Found {} unpriced foreign cards to delete.",
            ids_to_delete.len()
        );
        self.repository
            .delete_cards_batch(&ids_to_delete, Self::BATCH_SIZE as i64)
            .await
    }

    /// Pricing-aware dedup: `price_service` is passed in by the ingest pipeline
    /// (the application layer that owns both services) rather than held as a
    /// field, so `CardService` doesn't depend on the price module to construct.
    pub async fn prune_duplicate_foils(&self, price_service: &PriceService) -> Result<i64> {
        let dup_foil_sets: Vec<&str> = vec![
            "40k", "7ed", "8ed", "9ed", "10e", "frf", "ons", "shm", "stx", "thb", "unh",
        ];
        let mut total_deleted = 0i64;
        for set_code in dup_foil_sets {
            let non_ascii_cards = self
                .repository
                .fetch_non_ascii_numbers_in_set(set_code)
                .await?;
            if non_ascii_cards.is_empty() {
                continue;
            }
            let names: Vec<String> = non_ascii_cards.iter().map(|c| c.name.clone()).collect();
            let ascii_cards = self
                .repository
                .fetch_ascii_cards_by_set_and_names(set_code, &names)
                .await?;
            let mut ascii_by_name: HashMap<&str, &Card> = HashMap::new();
            for ac in &ascii_cards {
                ascii_by_name.entry(ac.name.as_str()).or_insert(ac);
            }
            let mut price_ids: Vec<String> = Vec::new();
            for c in &non_ascii_cards {
                price_ids.push(c.id.clone());
                if let Some(a) = ascii_by_name.get(c.name.as_str()) {
                    price_ids.push(a.id.clone());
                }
            }
            price_ids.sort();
            price_ids.dedup();
            let prices = price_service.fetch_prices_for_card_ids(&price_ids).await?;
            for non_ascii in non_ascii_cards {
                if let Some(ascii) = ascii_by_name.get(non_ascii.name.as_str()) {
                    if non_ascii.has_foil {
                        let mut ascii_clone = (*ascii).clone();
                        if ascii_clone.enable_foil_from(&non_ascii) {
                            let _ = self.repository.save_cards(&[ascii_clone]).await?;
                        }
                    }
                    let non_price = prices.get(&non_ascii.id);
                    let ascii_price = prices.get(&ascii.id);
                    if let Some((_, Some(src_foil))) = non_price {
                        match ascii_price {
                            Some((_, None)) => {
                                let _ = price_service
                                    .update_price_foil_if_null(&ascii.id, src_foil)
                                    .await?;
                            }
                            None => {
                                let normal_opt = non_price.and_then(|p| p.0);
                                let foil_opt = Some(*src_foil);
                                let _ = price_service
                                    .insert_price_for_card(&ascii.id, normal_opt, foil_opt)
                                    .await?;
                            }
                            _ => {}
                        }
                    }
                    let deleted = self
                        .repository
                        .delete_cards_batch(
                            std::slice::from_ref(&non_ascii.id),
                            Self::BATCH_SIZE as i64,
                        )
                        .await?;
                    total_deleted += deleted;
                }
            }
        }
        Ok(total_deleted)
    }

    pub async fn reclassify_non_main_set_types(&self) -> Result<i64> {
        debug!("Reclassify cards in non-main set types.");
        let set_types = MainSetClassifier::non_main_set_types();
        let mut cards = self
            .repository
            .fetch_in_main_cards_for_set_types(set_types)
            .await?;
        cards.iter_mut().for_each(Card::mark_as_non_main);
        let total = self.save_cards_batched(&cards).await?;
        debug!("Reclassified {} cards from non-main set types.", total);
        Ok(total)
    }

    pub async fn fix_main_classification(&self) -> Result<i64> {
        debug!("Fix main set classification for all cards.");
        let mut cards = self.repository.fetch_misclassified_as_in_main().await?;
        cards.iter_mut().for_each(Card::mark_as_non_main);
        let total = self.save_cards_batched(&cards).await?;
        debug!("Moved {} cards from main set.", total);
        Ok(total)
    }

    async fn save_cards_batched(&self, cards: &[Card]) -> Result<i64> {
        if cards.is_empty() {
            return Ok(0);
        }
        let mut total = 0i64;
        for chunk in cards.chunks(Self::BATCH_SIZE) {
            total += self.repository.save_cards(chunk).await?;
        }
        Ok(total)
    }

    pub(crate) fn merge_and_filter_cards(mut cards: Vec<Card>) -> Vec<Card> {
        let mut id_index: HashMap<String, usize> = HashMap::new();
        for (i, c) in cards.iter().enumerate() {
            id_index.insert(c.id.clone(), i);
        }
        let mut keep_mask = vec![true; cards.len()];
        let mut mana_cost_updates: Vec<(usize, Option<String>)> = Vec::new();
        for i in 0..cards.len() {
            if cards[i].should_filter() {
                keep_mask[i] = false;
                continue;
            }
            if cards[i].is_split_card() {
                if let Some(ref other_ids) = cards[i].other_face_ids {
                    for oid in other_ids.iter() {
                        if let Some(&j) = id_index.get(oid) {
                            let merged = cards[i].merge_mana_costs(cards[j].mana_cost.as_deref());
                            mana_cost_updates.push((i, merged));
                            keep_mask[j] = false;
                        }
                    }
                }
            }
        }
        for (idx, new_cost) in mana_cost_updates {
            cards[idx].mana_cost = new_cost;
        }
        cards
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| keep_mask[*idx])
            .map(|(_, c)| c)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::card::domain::CardRarity;
    use crate::card::ports::ByteStream;
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;

    /// A one-set `AllPrintings.json` fragment with a single card.
    const SAMPLE_ALL_PRINTINGS: &str = r#"{
      "data": {
        "TST": {
          "name": "Test Set",
          "type": "expansion",
          "cards": [
            {
              "uuid": "card-uuid-1",
              "name": "Test Card",
              "setCode": "TST",
              "number": "1",
              "type": "Creature",
              "rarity": "common",
              "identifiers": {"scryfallId": "scry-abc-1"}
            }
          ]
        }
      }
    }"#;

    /// Feeds a canned byte stream instead of hitting MTGJSON.
    struct FakeDataSource(&'static str);

    #[async_trait]
    impl CardDataSource for FakeDataSource {
        async fn all_cards_stream(&self) -> Result<ByteStream> {
            let bytes = bytes::Bytes::from(self.0);
            Ok(Box::pin(futures::stream::once(async move {
                Ok::<_, reqwest::Error>(bytes)
            })))
        }
        async fn fetch_set_cards(&self, _set_code: &str) -> Result<Value> {
            unimplemented!("not exercised by these tests")
        }
    }

    /// Records the ids handed to `save_cards`; other methods are unused here.
    #[derive(Default)]
    struct SpyRepo {
        saved: StdMutex<Vec<String>>,
    }

    #[async_trait]
    impl CardRepositoryPort for SpyRepo {
        async fn set_exists(&self, _code: &str) -> Result<bool> {
            Ok(true)
        }
        async fn save_cards(&self, cards: &[Card]) -> Result<i64> {
            let mut saved = self.saved.lock().unwrap();
            for c in cards {
                saved.push(c.id.clone());
            }
            Ok(cards.len() as i64)
        }
        async fn save_legalities(&self, _cards: &[Card]) -> Result<i64> {
            Ok(0)
        }
        async fn count(&self) -> Result<u64> {
            unimplemented!()
        }
        async fn count_for_sets(&self, _main_only: bool) -> Result<Vec<(String, i64)>> {
            unimplemented!()
        }
        async fn legality_count(&self) -> Result<u64> {
            unimplemented!()
        }
        async fn fetch_foreign_unpriced_ids(&self) -> Result<Vec<String>> {
            unimplemented!()
        }
        async fn delete_cards_batch(&self, _ids: &[String], _batch_size: i64) -> Result<i64> {
            unimplemented!()
        }
        async fn fetch_non_ascii_numbers_in_set(&self, _set_code: &str) -> Result<Vec<Card>> {
            unimplemented!()
        }
        async fn fetch_ascii_cards_by_set_and_names(
            &self,
            _set_code: &str,
            _names: &[String],
        ) -> Result<Vec<Card>> {
            unimplemented!()
        }
        async fn fetch_in_main_cards_for_set_types(
            &self,
            _set_types: &[&str],
        ) -> Result<Vec<Card>> {
            unimplemented!()
        }
        async fn fetch_misclassified_as_in_main(&self) -> Result<Vec<Card>> {
            unimplemented!()
        }
        async fn reset_all_data(&self) -> Result<()> {
            unimplemented!()
        }
    }

    /// The port refactor's payoff: `ingest_all` streams + parses + persists with
    /// no live HTTP or Postgres - a fake data source and a spy repository.
    #[tokio::test]
    async fn ingest_all_saves_parsed_cards_through_ports() {
        let repo = Arc::new(SpyRepo::default());
        let service =
            CardService::with_ports(Arc::new(FakeDataSource(SAMPLE_ALL_PRINTINGS)), repo.clone());

        service.ingest_all().await.unwrap();

        let saved = repo.saved.lock().unwrap();
        assert_eq!(saved.as_slice(), &["card-uuid-1".to_string()]);
    }

    fn create_test_card(id: &str) -> Card {
        Card {
            artist: Some("Artist".to_string()),
            flavor_name: None,
            has_foil: true,
            has_non_foil: true,
            id: id.to_string(),
            in_main: true,
            is_alternative: false,
            is_reserved: false,
            colors: Some(vec!["U".to_string()]),
            is_online_only: false,
            is_oversized: false,
            language: "English".to_string(),
            layout: "normal".to_string(),
            legalities: vec![],
            mana_cost: Some("{2}{U}".to_string()),
            name: "Test Card".to_string(),
            number: "1".to_string(),
            oracle_text: Some("Test text".to_string()),
            other_face_ids: None,
            tcgplayer_product_id: None,
            tcgplayer_etched_product_id: None,
            rarity: CardRarity::Rare,
            scryfall_id: Some(format!("scryfall-{}", id)),
            set_code: "tst".to_string(),
            side: None,
            sort_number: "000001".to_string(),
            type_line: "Creature — Test".to_string(),
        }
    }

    #[test]
    fn test_merge_and_filter_removes_online_only() {
        let mut card = create_test_card("c1");
        card.is_online_only = true;
        let normal = create_test_card("c2");
        let result = CardService::merge_and_filter_cards(vec![card, normal]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "c2");
    }

    #[test]
    fn test_merge_and_filter_removes_side_b() {
        let card_a = create_test_card("c1");
        let mut card_b = create_test_card("c2");
        card_b.side = Some("b".to_string());
        let result = CardService::merge_and_filter_cards(vec![card_a, card_b]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "c1");
    }

    #[test]
    fn test_merge_and_filter_merges_split_card_mana_costs() {
        let mut card_a = create_test_card("split-a");
        card_a.layout = "split".to_string();
        card_a.mana_cost = Some("{1}{R}".to_string());
        card_a.other_face_ids = Some(vec!["split-b".to_string()]);
        card_a.side = None;

        // Side "b" card — won't be filtered by should_filter (side=None to keep it in the list),
        // but will be removed by the split merge logic (keep_mask[j] = false)
        let mut card_b = create_test_card("split-b");
        card_b.layout = "normal".to_string(); // not a split card itself
        card_b.mana_cost = Some("{2}{G}".to_string());
        card_b.other_face_ids = None;
        card_b.side = None;

        let result = CardService::merge_and_filter_cards(vec![card_a, card_b]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "split-a");
        assert_eq!(result[0].mana_cost, Some("{1}{R} // {2}{G}".to_string()));
    }
}
