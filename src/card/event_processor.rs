use crate::card::{domain::Card, mapper::CardMapper};
use crate::utils::json_stream_parser::JsonEventProcessor;
use crate::utils::subtree_collector::{DocumentCursor, SubtreeCollector};
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use tracing::{debug, info, warn};

/// Routing shell over the AllPrintings stream: tracks which set it is in,
/// finds each set's `cards` array, and hands every card object's subtree to a
/// [`SubtreeCollector`]. Document layout (depths):
/// root(1) / data(2) / set object(3) / cards array(4) / card object(5).
///
/// Collected card subtrees are held in `pending_cards` and only mapped to
/// domain when the set *object* closes: MTGJSON's set keys are alphabetical,
/// so the set's `type` field arrives after its `cards` array, and mapping
/// needs that type for `in_main` classification.
pub struct CardEventProcessor {
    batch: Vec<Card>,
    collector: Option<SubtreeCollector>,
    current_set_code: Option<String>,
    current_set_type: Option<String>,
    cursor: DocumentCursor,
    expecting_cards_array: bool,
    expecting_set_type: bool,
    in_cards_array: bool,
    pending_cards: Vec<serde_json::Value>,
}

impl JsonEventProcessor<Card> for CardEventProcessor {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        if self.cursor.consume_if_skipping(event) {
            return Ok(0);
        }

        // While a card subtree is being collected, every event belongs to it.
        if let Some(collector) = self.collector.as_mut() {
            self.cursor.observe(event);
            if let Some(card_json) = collector.push_event(event, parser)? {
                self.collector = None;
                // Defer mapping until the set object closes (EndObject below):
                // the set's `type` may not have streamed by yet.
                self.pending_cards.push(card_json);
            }
            return Ok(0);
        }

        match event {
            JsonEvent::StartObject => {
                self.cursor.enter();
                if self.cursor.depth() == 2 {
                    // Entering the `data` object: reset set state.
                    self.current_set_code = None;
                    self.current_set_type = None;
                    self.expecting_cards_array = false;
                    self.expecting_set_type = false;
                }
                if self.in_cards_array && self.cursor.depth() == 5 {
                    // A card object begins: collect its whole subtree.
                    let mut collector = SubtreeCollector::new();
                    collector.push_event(event, parser)?;
                    self.collector = Some(collector);
                }
                Ok(0)
            }
            JsonEvent::EndObject => {
                // A set object closes only after all of its fields — including
                // `type`, which follows `cards` in MTGJSON's alphabetical key
                // order — so this is the earliest point the set's cards can be
                // mapped with the correct set type. Flushing here (rather than
                // mid-set at a fixed count) also keeps a split card's two faces
                // in one batch, so the cross-face mana-cost merge (done per
                // delivered batch) sees the pair. The per-set type state is
                // consumed here, so it cannot leak into the next set.
                let flushed = if self.cursor.depth() == 3 {
                    self.map_pending_cards();
                    self.expecting_set_type = false;
                    self.batch.len()
                } else {
                    0
                };
                self.cursor.exit();
                Ok(flushed)
            }
            JsonEvent::StartArray => {
                self.cursor.enter();
                if self.cursor.depth() == 4 && self.expecting_cards_array {
                    self.in_cards_array = true;
                    self.expecting_cards_array = false;
                    if let Some(code) = self.current_set_code.as_deref() {
                        info!("Processing cards for set: {}", code);
                    }
                }
                Ok(0)
            }
            JsonEvent::EndArray => {
                if self.in_cards_array && self.cursor.depth() == 4 {
                    self.in_cards_array = false;
                }
                self.cursor.exit();
                Ok(0)
            }
            JsonEvent::FieldName => self.handle_field_name(parser),
            JsonEvent::ValueString if self.expecting_set_type => {
                self.current_set_type = Some(parser.current_str().unwrap_or_default().to_string());
                self.expecting_set_type = false;
                Ok(0)
            }
            _ => Ok(0),
        }
    }

    fn take_batch(&mut self) -> Vec<Card> {
        std::mem::take(&mut self.batch)
    }
}

impl CardEventProcessor {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch: Vec::with_capacity(batch_size),
            collector: None,
            current_set_code: None,
            current_set_type: None,
            cursor: DocumentCursor::new(),
            expecting_cards_array: false,
            expecting_set_type: false,
            in_cards_array: false,
            pending_cards: Vec::new(),
        }
    }

    /// Map the closing set's collected card subtrees into the batch, consuming
    /// the set's type so it cannot leak into the next set.
    fn map_pending_cards(&mut self) {
        let set_type = self.current_set_type.take().unwrap_or_default();
        for card_json in std::mem::take(&mut self.pending_cards) {
            match CardMapper::map_json_to_card(&card_json, &set_type) {
                Ok(card) => self.batch.push(card),
                Err(e) => {
                    if let Some(code) = &self.current_set_code {
                        warn!("Failed to parse {} card: {}", code, e);
                    }
                }
            }
        }
    }

    fn handle_field_name<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        let field_name = parser.current_str().unwrap_or_default();
        if self.cursor.depth() == 2 {
            debug!("ENTERING SET: '{}'", field_name);
            self.current_set_code = Some(String::from(field_name));
            self.expecting_cards_array = false;
            return Ok(0);
        }

        match field_name {
            "meta" if self.cursor.depth() == 1 => self.cursor.skip_value(),
            "cards" if self.cursor.depth() == 3 => self.expecting_cards_array = true,
            "type" if self.cursor.depth() == 3 => self.expecting_set_type = true,
            _ => {
                // Defensive: skip unknown subtrees seen before any set key
                // (unreachable with today's MTGJSON shape, kept from the old
                // state machine).
                if !self.in_cards_array
                    && !["name", "cards"].contains(&field_name)
                    && self.cursor.depth() >= 3
                    && self.current_set_code.is_none()
                {
                    self.cursor.skip_value();
                }
            }
        }
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive the processor over a JSON document and return the size of each
    /// batch it flushes.
    async fn batch_sizes(json: &str, batch_size: usize) -> Vec<usize> {
        crate::utils::json_stream_parser::test_support::collect_batches(
            CardEventProcessor::new(batch_size),
            json,
        )
        .await
        .iter()
        .map(Vec::len)
        .collect()
    }

    /// Realistic AllPrintings-shaped document: alphabetical set keys (so `type`
    /// follows `cards`, as in the real feed), a set-level `booster` blob,
    /// escaping edge cases, split faces, an unmappable card, and an empty set.
    const FIXTURE: &str = include_str!("../../tests/fixtures/all_printings_sample.json");

    async fn fixture_batches() -> Vec<Vec<Card>> {
        crate::utils::json_stream_parser::test_support::collect_batches(
            CardEventProcessor::new(500),
            FIXTURE,
        )
        .await
    }

    // One batch per non-empty set: ESC (2 cards), SPL (3), BAD (1 of 2 — the
    // scryfallId-less card is warn-and-skipped), MTY (empty, no batch).
    #[tokio::test]
    async fn fixture_flushes_one_batch_per_nonempty_set() {
        let sizes: Vec<usize> = fixture_batches().await.iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![2, 3, 1]);
    }

    // Strings must survive the stream → subtree → domain trip byte-for-byte:
    // quotes, backslashes, \n, \t, a raw control char (U+0001), and multi-byte
    // UTF-8 split across feeder chunk boundaries.
    #[tokio::test]
    async fn fixture_preserves_escaped_strings_and_nested_values() {
        let batches = fixture_batches().await;
        let card = batches[0]
            .iter()
            .find(|c| c.id == "esc-card-0001")
            .expect("torture card should be extracted");

        assert_eq!(card.name, "Say \"Cheese!\"");
        assert_eq!(
            card.oracle_text.as_deref(),
            Some(
                "Choose one —\n• Say \"Cheese!\" deals 2 damage.\n• Target player discards a card named C:\\WINDOWS\\config.sys.\tGood luck.\u{1}"
            )
        );
        assert_eq!(card.artist.as_deref(), Some("Éowyn Õzturk"));
        assert_eq!(card.mana_cost.as_deref(), Some("{1}{r}"));
        assert_eq!(card.colors, Some(vec!["R".to_string(), "W".to_string()]));
        assert_eq!(card.rarity, crate::card::domain::CardRarity::Rare);
        assert_eq!(card.scryfall_id.as_deref(), Some("esc-scry-0001"));
        assert_eq!(card.tcgplayer_product_id.as_deref(), Some("500001"));
        assert_eq!(card.legalities.len(), 2);
        assert!(card.has_foil);
        assert!(card.has_non_foil);
        assert_eq!(card.set_code, "esc");

        let german = batches[0]
            .iter()
            .find(|c| c.id == "esc-card-0002")
            .expect("second card should be extracted");
        assert_eq!(
            german.oracle_text.as_deref(),
            Some("Zerstöre das Ziel.\u{7} (Klingel!)")
        );
        assert_eq!(german.language, "German");
        assert!(german.has_foil);
        assert!(!german.has_non_foil);
    }

    // Both faces of a split card arrive in the same (whole-set) batch, keeping
    // the raw combined name, so the downstream cross-face merge can pair them.
    #[tokio::test]
    async fn fixture_split_faces_stay_in_one_batch() {
        let batches = fixture_batches().await;
        let spl = &batches[1];
        assert_eq!(spl.len(), 3);
        let faces: Vec<&Card> = spl.iter().filter(|c| c.name == "Fire // Ice").collect();
        assert_eq!(faces.len(), 2);
        let sides: Vec<Option<&str>> = faces.iter().map(|c| c.side.as_deref()).collect();
        assert!(sides.contains(&Some("a")) && sides.contains(&Some("b")));
    }

    // A card the mapper can't handle (missing scryfallId) is skipped with a
    // warning; the rest of its set still comes through.
    #[tokio::test]
    async fn fixture_unmappable_card_is_skipped_not_fatal() {
        let batches = fixture_batches().await;
        let bad = &batches[2];
        assert_eq!(bad.len(), 1);
        assert_eq!(bad[0].name, "Survivor");
    }

    // A set larger than the batch size must still be delivered as one batch, so
    // the cross-face split-card merge (applied per delivered batch downstream)
    // sees both faces. The old code flushed mid-set at batch_size, giving [2, 1]
    // and splitting a face pair straddling that boundary.
    #[tokio::test]
    async fn flushes_whole_set_as_one_batch() {
        let json = r#"{
            "meta": {"date": "2024-01-15"},
            "data": {
                "TST": {
                    "name": "Test Set",
                    "type": "expansion",
                    "cards": [
                        {"uuid":"a","name":"A","setCode":"TST","number":"1","type":"Instant","rarity":"common","identifiers":{"scryfallId":"s-a"}},
                        {"uuid":"b","name":"B","setCode":"TST","number":"2","type":"Instant","rarity":"common","identifiers":{"scryfallId":"s-b"}},
                        {"uuid":"c","name":"C","setCode":"TST","number":"3","type":"Instant","rarity":"common","identifiers":{"scryfallId":"s-c"}}
                    ]
                }
            }
        }"#;

        assert_eq!(batch_sizes(json, 2).await, vec![3]);
    }

    // MTGJSON set keys are alphabetical, so `type` follows `cards` — the set's
    // type is only known after its cards have streamed by. Each card must be
    // classified with its OWN set's type: without deferred mapping, AAA's card
    // saw no type at all and BBB's card saw AAA's leaked "expansion",
    // inverting both `in_main` results (neither card has `boosterTypes`, so
    // the classifier's set-type fallback decides).
    #[tokio::test]
    async fn set_type_following_cards_applies_to_own_set_not_next() {
        let json = r#"{
            "data": {
                "AAA": {
                    "cards": [
                        {"uuid":"a1","name":"A1","setCode":"AAA","number":"1","type":"Creature","rarity":"common","borderColor":"black","finishes":["nonfoil"],"identifiers":{"scryfallId":"s-a1"}}
                    ],
                    "name": "First",
                    "type": "expansion"
                },
                "BBB": {
                    "cards": [
                        {"uuid":"b1","name":"B1","setCode":"BBB","number":"1","type":"Creature","rarity":"common","borderColor":"black","finishes":["nonfoil"],"identifiers":{"scryfallId":"s-b1"}}
                    ],
                    "name": "Second",
                    "type": "commander"
                }
            }
        }"#;

        let batches = crate::utils::json_stream_parser::test_support::collect_batches(
            CardEventProcessor::new(500),
            json,
        )
        .await;
        assert_eq!(batches.len(), 2);
        assert!(
            batches[0][0].in_main,
            "expansion set's card should be in_main via the intrinsic fallback"
        );
        assert!(
            !batches[1][0].in_main,
            "commander set's card must not inherit the previous set's type"
        );
    }
}
