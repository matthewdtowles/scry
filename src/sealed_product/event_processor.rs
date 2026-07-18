use crate::sealed_product::{domain::SealedProduct, mapper::SealedProductMapper};
use crate::utils::json_stream_parser::JsonEventProcessor;
use crate::utils::subtree_collector::{DocumentCursor, SubtreeCollector};
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use tracing::{debug, warn};

/// Routing shell over the AllPrintings stream: tracks which set it is in,
/// finds each set's `sealedProduct` array (skipping every other set-level
/// subtree, including the large `cards` array), and hands every product
/// object's subtree to a [`SubtreeCollector`]. Document layout (depths):
/// root(1) / data(2) / set object(3) / sealedProduct array(4) / product(5).
pub struct SealedProductEventProcessor {
    batch: Vec<SealedProduct>,
    batch_size: usize,
    collector: Option<SubtreeCollector>,
    current_set_code: Option<String>,
    cursor: DocumentCursor,
    expecting_sealed_array: bool,
    in_sealed_array: bool,
}

impl JsonEventProcessor<SealedProduct> for SealedProductEventProcessor {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        if self.cursor.consume_if_skipping(event) {
            return Ok(0);
        }

        // While a product subtree is being collected, every event belongs to it.
        if let Some(collector) = self.collector.as_mut() {
            self.cursor.observe(event);
            if let Some(product_json) = collector.push_event(event, parser)? {
                self.collector = None;
                if let Some(set_code) = &self.current_set_code {
                    match SealedProductMapper::map_single_item(&product_json, set_code) {
                        Ok(Some(product)) => {
                            self.batch.push(product);
                            // Unlike cards (whole-set batches for the face
                            // merge), sealed products can flush mid-set.
                            if self.batch.len() >= self.batch_size {
                                return Ok(self.batch.len());
                            }
                        }
                        Ok(None) => {} // filtered out (online-only)
                        Err(e) => {
                            warn!("Failed to parse sealed product for {}: {}", set_code, e);
                        }
                    }
                }
            }
            return Ok(0);
        }

        match event {
            JsonEvent::StartObject => {
                self.cursor.enter();
                if self.cursor.depth() == 2 {
                    // Entering the `data` object: reset set state.
                    self.current_set_code = None;
                    self.expecting_sealed_array = false;
                }
                if self.in_sealed_array && self.cursor.depth() == 5 {
                    // A product object begins: collect its whole subtree.
                    let mut collector = SubtreeCollector::new();
                    collector.push_event(event, parser)?;
                    self.collector = Some(collector);
                }
                Ok(0)
            }
            JsonEvent::EndObject => {
                self.cursor.exit();
                Ok(0)
            }
            JsonEvent::StartArray => {
                self.cursor.enter();
                if self.cursor.depth() == 4 && self.expecting_sealed_array {
                    self.in_sealed_array = true;
                    self.expecting_sealed_array = false;
                    if let Some(code) = &self.current_set_code {
                        debug!("Processing sealed products for set: {}", code);
                    }
                }
                Ok(0)
            }
            JsonEvent::EndArray => {
                // Flush the remainder at the end of each set's array.
                let flushed = if self.in_sealed_array && self.cursor.depth() == 4 {
                    self.in_sealed_array = false;
                    self.batch.len()
                } else {
                    0
                };
                self.cursor.exit();
                Ok(flushed)
            }
            JsonEvent::FieldName => self.handle_field_name(parser),
            _ => Ok(0),
        }
    }

    fn take_batch(&mut self) -> Vec<SealedProduct> {
        std::mem::take(&mut self.batch)
    }
}

impl SealedProductEventProcessor {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch: Vec::with_capacity(batch_size),
            batch_size,
            collector: None,
            current_set_code: None,
            cursor: DocumentCursor::new(),
            expecting_sealed_array: false,
            in_sealed_array: false,
        }
    }

    fn handle_field_name<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        let field_name = parser.current_str().unwrap_or_default();

        // depth 2 = set code key (e.g., "woe", "blb")
        if self.cursor.depth() == 2 {
            self.current_set_code = Some(String::from(field_name));
            self.expecting_sealed_array = false;
            return Ok(0);
        }

        match field_name {
            "meta" if self.cursor.depth() == 1 => self.cursor.skip_value(),
            "sealedProduct" if self.cursor.depth() == 3 => self.expecting_sealed_array = true,
            _ => {
                // Skip every other set-level subtree wholesale.
                if !self.in_sealed_array
                    && self.cursor.depth() >= 3
                    && field_name != "sealedProduct"
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
    use crate::utils::json_stream_parser::test_support::collect_batches;

    const FIXTURE: &str = include_str!("../../tests/fixtures/all_printings_sample.json");

    // ESC carries four sealed products: two valid, one online-only (MTGO —
    // filtered), one without a name (unmappable — warn-and-skipped). The other
    // sets have no sealedProduct array, so no further batches.
    #[tokio::test]
    async fn fixture_extracts_valid_products_and_filters_the_rest() {
        let batches = collect_batches(SealedProductEventProcessor::new(500), FIXTURE).await;
        assert_eq!(batches.len(), 1, "only ESC has sealed products");
        let products = &batches[0];
        assert_eq!(products.len(), 2);

        let boxp = &products[0];
        assert_eq!(boxp.uuid, "esc-sealed-0001");
        assert_eq!(boxp.name, "Escape Draft Booster Box — \"Collector's\" Cut");
        assert_eq!(boxp.set_code, "esc");
        assert_eq!(boxp.category.as_deref(), Some("booster_box"));
        assert_eq!(boxp.subtype.as_deref(), Some("draft"));
        assert_eq!(boxp.card_count, Some(540));
        assert_eq!(boxp.product_size, Some(36));
        assert_eq!(boxp.tcgplayer_product_id.as_deref(), Some("600001"));
        assert_eq!(
            boxp.contents_summary.as_deref(),
            Some("36x ESC default, Escape Spindown — \"Lucky\" Edition")
        );

        let bundle = &products[1];
        assert_eq!(bundle.uuid, "esc-sealed-0002");
        assert_eq!(bundle.contents_summary.as_deref(), Some("Escape Starter"));
    }

    // Unlike cards, sealed products flush mid-set once batch_size is reached.
    #[tokio::test]
    async fn fixture_flushes_mid_set_at_batch_size() {
        let batches = collect_batches(SealedProductEventProcessor::new(1), FIXTURE).await;
        let sizes: Vec<usize> = batches.iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![1, 1]);
    }
}
