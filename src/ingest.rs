//! Single-pass ingest of cards + sealed products.
//!
//! Card ingestion and sealed-product ingestion both stream the same MTGJSON
//! `AllPrintings.json` file, just extracting different sub-trees of each set
//! object (`cards[]` vs `sealedProduct[]`). Run separately that means the large
//! file is downloaded and tokenized twice. The tee processor here drives both
//! extractors over a single stream instead.

use crate::card::domain::Card;
use crate::card::event_processor::CardEventProcessor;
use crate::sealed_product::domain::SealedProduct;
use crate::sealed_product::event_processor::SealedProductEventProcessor;
use crate::utils::json_stream_parser::JsonEventProcessor;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use chrono::NaiveDate;
use std::collections::HashSet;
use std::sync::Mutex;

/// What one ingest run actually saw and wrote, so the stale-row sweep in
/// post-ingest prune can prove the run covered the whole catalog before it
/// deletes anything.
///
/// MTGJSON re-issues cards under a new uuid when it corrects preview-season
/// data. Because `save_cards` upserts on that uuid, the superseded row is never
/// touched again and lives in the DB forever - scry#67, where a spoiler-era
/// "The Very Hungry Archaic" sat at SOS #168 long after MTGJSON had replaced it
/// with Wildgrowth Archaic. Absence from the feed is the only evidence such a
/// row is dead, so the sweep deletes whatever this run did not stamp.
///
/// That makes an incomplete run catastrophic, and it takes two independent
/// checks to rule one out, because neither is sufficient alone:
///
/// * **Set coverage** - every set holding cards in the DB must have delivered a
///   batch. This is what catches a truncated stream, and the row count cannot:
///   a stream that dies half way stamps N rows and then finds exactly N rows
///   carrying today's date, so the counts agree perfectly while half the
///   catalog went unseen.
/// * **Row count** - rows stamped must equal rows carrying today's date. This
///   catches a batch that failed to persist after the stream itself completed,
///   which set coverage would miss.
///
/// The run's date is fixed once, at construction, rather than read per batch:
/// a full ingest takes long enough to cross UTC midnight, and a run that
/// stamped some sets with day N and the rest with day N+1 would sweep away
/// everything it wrote before the boundary.
pub struct IngestLedger {
    date: NaiveDate,
    state: Mutex<LedgerState>,
}

#[derive(Default)]
struct LedgerState {
    card_sets: HashSet<String>,
    cards_stamped: Option<i64>,
    sealed_stamped: Option<i64>,
    sealed_save_failed: bool,
}

impl IngestLedger {
    pub fn new(date: NaiveDate) -> Self {
        Self {
            date,
            state: Mutex::new(LedgerState::default()),
        }
    }

    /// The single date every stamp in this run carries.
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    /// Record that `set_code` delivered a card batch, `stamped` rows of which
    /// were marked seen. A set whose batch is emptied by filtering still counts
    /// as covered with `stamped` 0 - the stream did reach it, so whatever rows
    /// it still has in the DB are genuinely stale.
    pub fn record_cards(&self, set_code: &str, stamped: i64) {
        let mut state = self.state.lock().unwrap();
        state.card_sets.insert(set_code.to_string());
        *state.cards_stamped.get_or_insert(0) += stamped;
    }

    pub fn record_sealed(&self, stamped: i64) {
        let mut state = self.state.lock().unwrap();
        *state.sealed_stamped.get_or_insert(0) += stamped;
    }

    /// A sealed-product batch failed to persist. Sealed ingest logs and carries
    /// on rather than aborting the stream, so unlike the card path a failure
    /// leaves no other trace: the products go unstamped, the row count agrees
    /// with itself, and the sweep would delete perfectly live products. This is
    /// the only signal, so it has to block the sealed sweep outright.
    pub fn record_sealed_failure(&self) {
        self.state.lock().unwrap().sealed_save_failed = true;
    }

    /// Why the card sweep must not run, or `None` if every check passed.
    ///
    /// `db_set_codes` is every set that currently holds cards; `rows_dated_today`
    /// is how many card rows carry this run's date.
    pub fn card_sweep_block(
        &self,
        db_set_codes: &[String],
        rows_dated_today: i64,
    ) -> Option<String> {
        let state = self.state.lock().unwrap();
        let Some(stamped) = state.cards_stamped else {
            return Some("no card ingest ran in this process".to_string());
        };
        let mut missing: Vec<&str> = db_set_codes
            .iter()
            .filter(|code| !state.card_sets.contains(*code))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            // A set that vanishes from MTGJSON entirely blocks the sweep until
            // someone looks at it. That is the intended outcome: a whole set
            // disappearing warrants human eyes, not a silent mass delete.
            return Some(format!(
                "{} set(s) hold cards but delivered no batch this run: {}",
                missing.len(),
                missing.join(", ")
            ));
        }
        if stamped != rows_dated_today {
            return Some(format!(
                "stamped {stamped} card rows but {rows_dated_today} carry today's date"
            ));
        }
        None
    }

    /// Why the sealed sweep must not run, or `None` if it may. Callers must
    /// already have cleared [`Self::card_sweep_block`]: sealed products ride
    /// the same `AllPrintings.json` stream, and set coverage over the cards is
    /// what proves that stream ran to completion.
    pub fn sealed_sweep_block(&self, rows_dated_today: i64) -> Option<String> {
        let state = self.state.lock().unwrap();
        let Some(stamped) = state.sealed_stamped else {
            return Some("no sealed-product ingest ran in this process".to_string());
        };
        if state.sealed_save_failed {
            return Some("at least one sealed-product batch failed to save".to_string());
        }
        if stamped != rows_dated_today {
            return Some(format!(
                "stamped {stamped} sealed rows but {rows_dated_today} carry today's date"
            ));
        }
        None
    }
}

/// One record extracted from a single pass over `AllPrintings.json`.
///
/// `Card` is much larger than `Sealed`, but boxing it to even the variants out
/// would add a heap allocation per card across the whole catalog on the ingest
/// hot path - the padding in a bounded batch Vec is the cheaper trade.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum IngestRecord {
    Card(Card),
    Sealed(SealedProduct),
}

/// Drives the card and sealed-product extractors over one shared byte stream.
///
/// Each sub-processor keeps its own depth counter and skip state, so every
/// event is simply forwarded to both: the one whose sub-tree the event belongs
/// to acts on it, the other ignores it (the card extractor never enters
/// `sealedProduct`, and the sealed extractor skips the `cards` array wholesale).
/// At most one of them flushes a batch on any given event, so the combined
/// batch is homogeneous in practice; the consumer splits by variant regardless.
pub struct CardSealedEventProcessor {
    card: CardEventProcessor,
    sealed: SealedProductEventProcessor,
}

impl CardSealedEventProcessor {
    pub fn new(card_batch_size: usize, sealed_batch_size: usize) -> Self {
        Self {
            card: CardEventProcessor::new(card_batch_size),
            sealed: SealedProductEventProcessor::new(sealed_batch_size),
        }
    }
}

impl JsonEventProcessor<IngestRecord> for CardSealedEventProcessor {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        // JsonEvent is Copy; both read the current token non-destructively.
        let cards = self.card.process_event(event, parser).await?;
        let sealed = self.sealed.process_event(event, parser).await?;
        Ok(cards + sealed)
    }

    fn take_batch(&mut self) -> Vec<IngestRecord> {
        let mut out: Vec<IngestRecord> = Vec::new();
        out.extend(self.card.take_batch().into_iter().map(IngestRecord::Card));
        out.extend(
            self.sealed
                .take_batch()
                .into_iter()
                .map(IngestRecord::Sealed),
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::json_stream_parser::JsonStreamParser;
    use futures::stream;

    /// One set carrying both a `cards` array and a `sealedProduct` array, at the
    /// real MTGJSON depths (root / data / set / {cards|sealedProduct} / object).
    const SAMPLE: &str = r#"{
      "meta": {"date": "2024-01-15", "version": "5.2.0"},
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
          ],
          "sealedProduct": [
            {
              "uuid": "sealed-uuid-1",
              "name": "Test Booster Box",
              "category": "booster_box"
            }
          ]
        }
      }
    }"#;

    async fn run_tee(json: &str) -> Vec<IngestRecord> {
        let processor = CardSealedEventProcessor::new(8, 8);
        let mut parser = JsonStreamParser::new(processor);
        let collected = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = collected.clone();
        let bytes = bytes::Bytes::from(json.to_string());
        let byte_stream = stream::once(async move { Ok::<_, reqwest::Error>(bytes) });
        parser
            .parse_stream(byte_stream, move |batch| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock().unwrap().extend(batch);
                    Ok(())
                })
            })
            .await
            .unwrap();
        std::sync::Arc::try_unwrap(collected)
            .unwrap()
            .into_inner()
            .unwrap()
    }

    // The realistic multi-set fixture through the tee: every card batch and
    // sealed batch arrives, and neither extractor disturbs the other even when
    // one is mid-skip or mid-collection while the other is routing.
    #[tokio::test]
    async fn fixture_single_pass_extracts_all_cards_and_sealed() {
        let fixture = include_str!("../tests/fixtures/all_printings_sample.json");
        let records = run_tee(fixture).await;

        let card_count = records
            .iter()
            .filter(|r| matches!(r, IngestRecord::Card(_)))
            .count();
        let sealed_count = records
            .iter()
            .filter(|r| matches!(r, IngestRecord::Sealed(_)))
            .count();

        assert_eq!(card_count, 6, "ESC 2 + SPL 3 + BAD 1");
        assert_eq!(sealed_count, 2, "ESC's two valid sealed products");
    }

    #[tokio::test]
    async fn single_pass_extracts_both_cards_and_sealed() {
        let records = run_tee(SAMPLE).await;

        let cards: Vec<&Card> = records
            .iter()
            .filter_map(|r| match r {
                IngestRecord::Card(c) => Some(c),
                IngestRecord::Sealed(_) => None,
            })
            .collect();
        let sealed: Vec<&SealedProduct> = records
            .iter()
            .filter_map(|r| match r {
                IngestRecord::Sealed(s) => Some(s),
                IngestRecord::Card(_) => None,
            })
            .collect();

        assert_eq!(cards.len(), 1, "single pass should extract the one card");
        assert_eq!(cards[0].id, "card-uuid-1");
        assert_eq!(
            sealed.len(),
            1,
            "single pass should extract the one product"
        );
        assert_eq!(sealed[0].uuid, "sealed-uuid-1");
        assert_eq!(sealed[0].set_code, "tst");
    }
}
