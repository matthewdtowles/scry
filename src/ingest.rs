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
