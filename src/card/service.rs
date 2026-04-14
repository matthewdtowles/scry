use crate::{
    card::{
        domain::{Card, MainSetClassifier},
        event_processor::CardEventProcessor,
        mapper::CardMapper,
        repository::CardRepository,
    },
    database::ConnectionPool,
    price::service::PriceService,
    utils::{HttpClient, JsonStreamParser},
};
use anyhow::{Context, Result};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, warn};

pub struct CardService {
    client: Arc<HttpClient>,
    repository: CardRepository,
    foreign_cards: Arc<Mutex<Vec<String>>>,
    price_service: Arc<PriceService>,
}

impl CardService {
    const BATCH_SIZE: usize = 500;
    const CONCURRENCY: usize = 6;

    pub fn new(
        db: Arc<ConnectionPool>,
        http_client: Arc<HttpClient>,
        price_service: Arc<PriceService>,
    ) -> Self {
        Self {
            client: http_client,
            repository: CardRepository::new(db),
            foreign_cards: Arc::new(Mutex::new(Vec::new())),
            price_service,
        }
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
        let raw_data: Value = self.client.fetch_set_cards(&set_code).await?;
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
        let _ = self.repository.save_legalities(&final_cards).await?;
        debug!("Successfully ingested {} cards for set {}", count, set_code);
        Ok(count)
    }

    pub async fn ingest_all(&self) -> Result<()> {
        debug!("Start ingestion of all cards");
        {
            let mut lock = self.foreign_cards.lock().await;
            lock.clear();
        }
        let byte_stream = self.client.all_cards_stream().await?;
        debug!("Received byte stream for all cards");
        let existing_set_cache: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let sem = Arc::new(Semaphore::new(Self::CONCURRENCY));
        let event_processor = CardEventProcessor::new(Self::BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let repo = self.repository.clone();
        let foreign_cards = self.foreign_cards.clone();
        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let repo = repo.clone();
                let sem = sem.clone();
                let cache = existing_set_cache.clone();
                let foreign_cards = foreign_cards.clone();
                Box::pin(async move {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    let permit = sem
                        .clone()
                        .acquire_owned()
                        .await
                        .context("semaphore closed while acquiring card ingest permit")?;
                    let mut batch_owned = batch.to_vec();
                    let handle = tokio::spawn(async move {
                        let _permit_guard = permit;
                        let set_code = batch_owned[0].set_code.clone();
                        {
                            let cache_lock = cache.lock().await;
                            if !cache_lock.contains(&set_code) {
                                drop(cache_lock);
                                match repo.set_exists(&set_code).await {
                                    Ok(true) => {
                                        let mut cache_lock = cache.lock().await;
                                        cache_lock.insert(set_code.clone());
                                    }
                                    Ok(false) => {
                                        warn!("Skipping cards for missing set {}", set_code);
                                        return Ok::<(), anyhow::Error>(());
                                    }
                                    Err(e) => return Err::<(), anyhow::Error>(e),
                                }
                            }
                        }
                        batch_owned = CardService::merge_and_filter_cards(batch_owned);
                        let mut foreign_ids = Vec::new();
                        for c in &batch_owned {
                            if c.is_foreign() {
                                foreign_ids.push(c.id.clone());
                            }
                        }
                        if !foreign_ids.is_empty() {
                            let mut lock = foreign_cards.lock().await;
                            lock.extend(foreign_ids);
                        }
                        if !batch_owned.is_empty() {
                            repo.save_cards(&batch_owned).await?;
                            repo.save_legalities(&batch_owned).await?;
                        }
                        Ok::<(), anyhow::Error>(())
                    });
                    match handle.await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(join_err) => Err(anyhow::anyhow!("Task join error: {}", join_err)),
                    }
                })
            })
            .await?;
        Ok(())
    }

    pub async fn delete_all(&self) -> Result<i64> {
        debug!("Deleting all prices.");
        self.repository.delete_all().await
    }

    pub async fn cleanup_cards(&self, batch_size: i64) -> Result<u64> {
        debug!("Starting streaming cleanup");
        let byte_stream = self.client.all_cards_stream().await?;
        let sem = Arc::new(Semaphore::new(Self::CONCURRENCY));
        let event_processor = CardEventProcessor::new(Self::BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let repo = self.repository.clone();
        let total = Arc::new(tokio::sync::Mutex::new(0u64));
        let total_for_closure = total.clone();
        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let repo = repo.clone();
                let sem = sem.clone();
                let total = total_for_closure.clone();
                Box::pin(async move {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    let _permit = sem
                        .clone()
                        .acquire_owned()
                        .await
                        .context("semaphore closed while acquiring card cleanup permit")?;
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

    pub async fn prune_foreign_unpriced(&self) -> Result<i64> {
        let card_candidates = {
            let lock = self.foreign_cards.lock().await;
            lock.clone()
        };
        if card_candidates.is_empty() {
            debug!("No foreign card candidates found to prune.");
            return Ok(0);
        }
        debug!(
            "Checking {} foreign card candidates for pricing.",
            card_candidates.len()
        );
        let ids_to_delete = self.repository.fetch_unpriced_ids(&card_candidates).await?;
        if ids_to_delete.is_empty() {
            debug!("Found 0 unpriced foreign cards to delete.");
            return Ok(0);
        }
        debug!(
            "Found {} unpriced foreign cards to delete.",
            ids_to_delete.len()
        );
        let total_deleted = self
            .repository
            .delete_cards_batch(&ids_to_delete, Self::BATCH_SIZE as i64)
            .await?;
        let mut lock = self.foreign_cards.lock().await;
        lock.clear();
        Ok(total_deleted)
    }

    pub async fn prune_duplicate_foils(&self) -> Result<i64> {
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
            let prices = self
                .price_service
                .fetch_prices_for_card_ids(&price_ids)
                .await?;
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
                                let _ = self
                                    .price_service
                                    .update_price_foil_if_null(&ascii.id, src_foil)
                                    .await?;
                            }
                            None => {
                                let normal_opt = non_price.and_then(|p| p.0.clone());
                                let foil_opt = Some(src_foil.clone());
                                let _ = self
                                    .price_service
                                    .insert_price_for_card(&ascii.id, normal_opt, foil_opt)
                                    .await?;
                            }
                            _ => {}
                        }
                    }
                    let deleted = self
                        .repository
                        .delete_cards_batch(&[non_ascii.id.clone()], Self::BATCH_SIZE as i64)
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

    fn create_test_card(id: &str) -> Card {
        Card {
            artist: Some("Artist".to_string()),
            flavor_name: None,
            has_foil: true,
            has_non_foil: true,
            id: id.to_string(),
            img_src: "a/b/test.jpg".to_string(),
            in_main: true,
            is_alternative: false,
            is_reserved: false,
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
        assert_eq!(
            result[0].mana_cost,
            Some("{1}{R} // {2}{G}".to_string())
        );
    }
}
