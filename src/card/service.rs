use crate::{
    card::{
        domain::{Card, MainSetClassifier},
        event_processor::CardEventProcessor,
        mapper::CardMapper,
        ports::{CardDataSource, CardRepositoryPort},
        repository::CardRepository,
    },
    database::ConnectionPool,
    ingest::IngestLedger,
    price::service::PriceService,
    utils::{clock, HttpClient, JsonStreamParser},
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

    /// Sets that ship non-ASCII-numbered foil variants (starred/etched promos)
    /// alongside an ASCII-numbered sibling of the same name. `prune_duplicate_foils`
    /// folds each variant into its sibling. Policy list, kept here rather than
    /// inline in the method body.
    const DUP_FOIL_SETS: &[&str] = &[
        "40k", "7ed", "8ed", "9ed", "10e", "frf", "ons", "shm", "stx", "thb", "unh",
    ];

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
        // Merge over the whole set before chunking: a split card's two faces
        // must both be present for the cross-face mana-cost merge, exactly as
        // the streaming path flushes one batch per set for that reason.
        let final_cards = Self::merge_and_filter_cards(parsed);
        let Some(first) = final_cards.first() else {
            return Ok(0);
        };
        // The mapper lowercases `setCode`, so this matches the `set` table
        // regardless of how the code was typed on the command line - unlike the
        // raw argument. Sets the ingest filter excludes (online-only,
        // foreign-only, memorabilia) are absent here, and inserting against one
        // would fail on card.set_code's foreign key; skip as the streaming path
        // does instead.
        // Named apart from the `set_code` argument rather than shadowing it:
        // the two can differ in case, which is the whole reason this exists, so
        // logging both under one name would be misleading.
        let mapped_set_code = first.set_code.clone();
        if !self.repository.set_exists(&mapped_set_code).await? {
            warn!("Skipping cards for missing set {}", mapped_set_code);
            return Ok(0);
        }
        // Chunked for the same reason `save_card_batch` chunks: `save_cards`
        // binds 22 parameters per card against Postgres's 65535-parameter
        // ceiling, so one statement caps out near 2978 cards and sets like PLST
        // (5045) exceed it outright (#69).
        let today = clock::today();
        let mut count = 0i64;
        for chunk in final_cards.chunks(Self::BATCH_SIZE) {
            count += self.repository.save_cards(chunk).await?;
            self.repository.save_legalities(chunk).await?;
            // Stamp for the same reason the streaming path does, so every card
            // persisted from the feed carries a `last_seen`. No ledger here: a
            // single-set ingest can never prove catalog coverage, so it never
            // enables the sweep - the next full ingest is what does.
            let ids: Vec<String> = chunk.iter().map(|c| c.id.clone()).collect();
            self.repository.stamp_cards_seen(&ids, today).await?;
        }
        // Conditional upsert, so `count` is rows changed, not cards seen.
        debug!(
            "Cards ingest for set {}: {} rows changed",
            mapped_set_code, count
        );
        Ok(count)
    }

    pub async fn ingest_all(&self, ledger: Arc<IngestLedger>) -> Result<()> {
        debug!("Start ingestion of all cards");
        let byte_stream = self.data_source.all_cards_stream().await?;
        debug!("Received byte stream for all cards");
        let event_processor = CardEventProcessor::new(Self::BATCH_SIZE);
        let mut json_stream_parser = JsonStreamParser::new(event_processor);
        let repo = self.repository.clone();
        json_stream_parser
            .parse_stream(byte_stream, move |batch| {
                let repo = repo.clone();
                let ledger = ledger.clone();
                Box::pin(async move { Self::save_card_batch(&repo, batch, &ledger).await })
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
    ///
    /// Every persisted row is also stamped as seen in this run's feed, and the
    /// set is recorded in `ledger`, so post-ingest prune can tell a card
    /// MTGJSON dropped from a card the stream simply never reached. A set
    /// skipped for not existing in the DB is deliberately not recorded - the
    /// stream reached it, but nothing here owns its rows.
    pub(crate) async fn save_card_batch(
        repo: &Arc<dyn CardRepositoryPort>,
        batch: Vec<Card>,
        ledger: &IngestLedger,
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
        let mut stamped = 0i64;
        for chunk in batch.chunks(Self::BATCH_SIZE) {
            repo.save_cards(chunk).await?;
            repo.save_legalities(chunk).await?;
            let ids: Vec<String> = chunk.iter().map(|c| c.id.clone()).collect();
            stamped += repo.stamp_cards_seen(&ids, ledger.date()).await?;
        }
        ledger.record_cards(&set_code, stamped);
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
        let mut total_deleted = 0i64;
        for set_code in Self::DUP_FOIL_SETS {
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

            // Accumulate the per-set writes and flush them in one round trip each
            // rather than per card. Keyed by id so a sibling matched by several
            // variants is saved once (and never violates ON CONFLICT).
            let mut foil_updates: HashMap<String, Card> = HashMap::new();
            let mut ids_to_delete: Vec<String> = Vec::new();
            for non_ascii in non_ascii_cards {
                if let Some(ascii) = ascii_by_name.get(non_ascii.name.as_str()) {
                    if non_ascii.has_foil {
                        let mut ascii_clone = (*ascii).clone();
                        if ascii_clone.enable_foil_from(&non_ascii) {
                            foil_updates.insert(ascii_clone.id.clone(), ascii_clone);
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
                    ids_to_delete.push(non_ascii.id.clone());
                }
            }

            if !foil_updates.is_empty() {
                let cards: Vec<Card> = foil_updates.into_values().collect();
                self.repository.save_cards(&cards).await?;
            }
            if !ids_to_delete.is_empty() {
                total_deleted += self
                    .repository
                    .delete_cards_batch(&ids_to_delete, Self::BATCH_SIZE as i64)
                    .await?;
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
    use chrono::NaiveDate;
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

    /// A single-set MTGJSON payload (the `<SET>.json` shape `ingest_set_cards`
    /// fetches) carrying `count` mappable cards.
    fn set_cards_payload(count: usize) -> Value {
        let cards: Vec<Value> = (0..count)
            .map(|i| {
                serde_json::json!({
                    "uuid": format!("big-uuid-{i}"),
                    "name": format!("Big Card {i}"),
                    "setCode": "BIG",
                    "number": (i + 1).to_string(),
                    "type": "Creature",
                    "rarity": "common",
                    "identifiers": {"scryfallId": format!("scry-big-{i}")}
                })
            })
            .collect();
        serde_json::json!({"data": {"type": "expansion", "cards": cards}})
    }

    /// Feeds a canned byte stream instead of hitting MTGJSON. `set_cards` backs
    /// the `ingest -k <set>` path.
    struct FakeDataSource(&'static str, Option<Value>);

    impl FakeDataSource {
        fn streaming(all_printings: &'static str) -> Self {
            Self(all_printings, None)
        }
        fn set_cards(payload: Value) -> Self {
            Self("", Some(payload))
        }
    }

    #[async_trait]
    impl CardDataSource for FakeDataSource {
        async fn all_cards_stream(&self) -> Result<ByteStream> {
            let bytes = bytes::Bytes::from(self.0);
            Ok(Box::pin(futures::stream::once(async move {
                Ok::<_, reqwest::Error>(bytes)
            })))
        }
        async fn fetch_set_cards(&self, _set_code: &str) -> Result<Value> {
            Ok(self
                .1
                .clone()
                .expect("this fake was built for the streaming path"))
        }
    }

    /// Records the ids handed to `save_cards` and to `stamp_cards_seen`, plus
    /// the size of each call so chunking is observable; other methods are
    /// unused here.
    #[derive(Default)]
    struct SpyRepo {
        saved: StdMutex<Vec<String>>,
        stamped: StdMutex<Vec<(String, NaiveDate)>>,
        save_sizes: StdMutex<Vec<usize>>,
        stamp_sizes: StdMutex<Vec<usize>>,
        set_missing: bool,
    }

    #[async_trait]
    impl CardRepositoryPort for SpyRepo {
        async fn set_exists(&self, _code: &str) -> Result<bool> {
            Ok(!self.set_missing)
        }
        async fn save_cards(&self, cards: &[Card]) -> Result<i64> {
            let mut saved = self.saved.lock().unwrap();
            for c in cards {
                saved.push(c.id.clone());
            }
            self.save_sizes.lock().unwrap().push(cards.len());
            Ok(cards.len() as i64)
        }
        async fn save_legalities(&self, _cards: &[Card]) -> Result<()> {
            Ok(())
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
        async fn stamp_cards_seen(&self, ids: &[String], date: NaiveDate) -> Result<i64> {
            let mut stamped = self.stamped.lock().unwrap();
            for id in ids {
                stamped.push((id.clone(), date));
            }
            self.stamp_sizes.lock().unwrap().push(ids.len());
            Ok(ids.len() as i64)
        }
        async fn count_seen_on(&self, _date: NaiveDate) -> Result<i64> {
            unimplemented!()
        }
        async fn fetch_set_codes_with_cards(&self) -> Result<Vec<String>> {
            unimplemented!()
        }
        async fn delete_stale_cards(&self, _date: NaiveDate) -> Result<i64> {
            unimplemented!()
        }
    }

    fn run_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, 4).unwrap()
    }

    /// The port refactor's payoff: `ingest_all` streams + parses + persists with
    /// no live HTTP or Postgres - a fake data source and a spy repository.
    #[tokio::test]
    async fn ingest_all_saves_parsed_cards_through_ports() {
        let repo = Arc::new(SpyRepo::default());
        let service = CardService::with_ports(
            Arc::new(FakeDataSource::streaming(SAMPLE_ALL_PRINTINGS)),
            repo.clone(),
        );

        service
            .ingest_all(Arc::new(IngestLedger::new(run_date())))
            .await
            .unwrap();

        let saved = repo.saved.lock().unwrap();
        assert_eq!(saved.as_slice(), &["card-uuid-1".to_string()]);
    }

    /// Every card persisted from the feed is stamped with the run's date, and
    /// the run's set coverage is recorded - the two facts the stale-row sweep
    /// (scry#67) depends on. Without the stamp, the sweep would read the whole
    /// catalog as unseen and delete it.
    #[tokio::test]
    async fn ingest_all_stamps_saved_cards_and_records_set_coverage() {
        let repo = Arc::new(SpyRepo::default());
        let ledger = Arc::new(IngestLedger::new(run_date()));
        let service = CardService::with_ports(
            Arc::new(FakeDataSource::streaming(SAMPLE_ALL_PRINTINGS)),
            repo.clone(),
        );

        service.ingest_all(ledger.clone()).await.unwrap();

        let stamped = repo.stamped.lock().unwrap();
        assert_eq!(
            stamped.as_slice(),
            &[("card-uuid-1".to_string(), run_date())]
        );
        // One card stamped, in the one set the fixture covers: the gate opens.
        assert_eq!(ledger.card_sweep_block(&["tst".to_string()], 1), None);
    }

    /// A set that still holds cards but delivered no batch means the stream did
    /// not reach it. The row count alone cannot see this - it agrees perfectly
    /// with itself - so set coverage has to be what blocks the sweep.
    #[tokio::test]
    async fn truncated_stream_blocks_the_sweep_despite_matching_counts() {
        let repo = Arc::new(SpyRepo::default());
        let ledger = Arc::new(IngestLedger::new(run_date()));
        let service = CardService::with_ports(
            Arc::new(FakeDataSource::streaming(SAMPLE_ALL_PRINTINGS)),
            repo.clone(),
        );

        service.ingest_all(ledger.clone()).await.unwrap();

        let block = ledger
            .card_sweep_block(&["tst".to_string(), "sos".to_string()], 1)
            .expect("a set with cards but no batch must block the sweep");
        assert!(
            block.contains("sos"),
            "block reason should name it: {block}"
        );
    }

    /// `ingest -k <set>` used to hand the whole set to `save_cards` in one
    /// statement. That binds 22 parameters per card against Postgres's 65535
    /// ceiling, so it caps out near 2978 cards and PLST (5045) exceeds it
    /// outright (#69). Every write in the loop must stay within BATCH_SIZE.
    #[tokio::test]
    async fn ingest_set_cards_chunks_writes_under_the_bind_parameter_ceiling() {
        let card_count = CardService::BATCH_SIZE * 2 + 37;
        let repo = Arc::new(SpyRepo::default());
        let service = CardService::with_ports(
            Arc::new(FakeDataSource::set_cards(set_cards_payload(card_count))),
            repo.clone(),
        );

        let changed = service.ingest_set_cards("big").await.unwrap();

        assert_eq!(changed, card_count as i64, "every card is still persisted");
        let save_sizes = repo.save_sizes.lock().unwrap().clone();
        let stamp_sizes = repo.stamp_sizes.lock().unwrap().clone();
        assert_eq!(
            save_sizes,
            vec![CardService::BATCH_SIZE, CardService::BATCH_SIZE, 37]
        );
        assert_eq!(
            stamp_sizes, save_sizes,
            "the stamp follows the same chunking as the save it accompanies"
        );
        assert_eq!(repo.saved.lock().unwrap().len(), card_count);
    }

    /// The streaming path skips a set the ingest filter excluded rather than
    /// inserting against a `set` row that does not exist (`save_card_batch`).
    /// `ingest -k prm` took no such guard and would have failed on
    /// `card.set_code`'s foreign key instead (#69).
    #[tokio::test]
    async fn ingest_set_cards_skips_a_set_missing_from_the_db() {
        let repo = Arc::new(SpyRepo {
            set_missing: true,
            ..SpyRepo::default()
        });
        let service = CardService::with_ports(
            Arc::new(FakeDataSource::set_cards(set_cards_payload(3))),
            repo.clone(),
        );

        assert_eq!(service.ingest_set_cards("big").await.unwrap(), 0);
        assert!(
            repo.saved.lock().unwrap().is_empty(),
            "nothing may be written for a set the filter excluded"
        );
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
