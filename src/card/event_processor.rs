use crate::card::{domain::Card, mapper::CardMapper};
use crate::utils::json_stream_parser::JsonEventProcessor;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use tracing::{debug, info, warn};

pub struct CardEventProcessor {
    batch: Vec<Card>,
    card_object_depth: usize,
    current_card_json: String,
    current_set_code: Option<String>,
    current_set_type: Option<String>,
    expecting_cards_array: bool,
    expecting_set_type: bool,
    in_card_object: bool,
    in_cards_array: bool,
    in_set_object: bool,
    is_skipping_value: bool,
    json_depth: usize,
    skip_depth: usize,
}

impl JsonEventProcessor<Card> for CardEventProcessor {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>, // Do not remove
    ) -> Result<usize> {
        if self.is_skipping_value {
            match event {
                JsonEvent::StartObject | JsonEvent::StartArray => {
                    self.json_depth += 1;
                }
                JsonEvent::EndObject | JsonEvent::EndArray => {
                    self.json_depth -= 1;
                    if self.json_depth <= self.skip_depth {
                        self.is_skipping_value = false;
                    }
                }
                _ => {
                    // Scalar value (string, int, float, bool, null) — the
                    // skipped value is consumed, so stop skipping. Without this
                    // a scalar-valued skipped field would wedge the processor
                    // in skip mode forever (matches SealedProductEventProcessor).
                    if self.json_depth <= self.skip_depth {
                        self.is_skipping_value = false;
                    }
                }
            }
            return Ok(0);
        }
        match event {
            JsonEvent::StartObject => {
                self.json_depth += 1;
                self.handle_start_object()
            }
            JsonEvent::EndObject => {
                let result = self.handle_end_object().await;
                self.json_depth -= 1;
                result
            }
            JsonEvent::StartArray => {
                self.json_depth += 1;
                self.handle_start_array()
            }
            JsonEvent::EndArray => {
                let result = self.handle_end_array();
                self.json_depth -= 1;
                result
            }
            JsonEvent::FieldName => self.handle_field_name(parser),
            JsonEvent::ValueString => {
                let value = parser.current_str().unwrap_or_default();
                self.handle_string_value(value)
            }
            JsonEvent::ValueInt => {
                let value = parser.current_int::<i64>()?.to_string();
                self.handle_number_value(&value)
            }
            JsonEvent::ValueFloat => {
                let value = parser.current_float()?.to_string();
                self.handle_number_value(&value)
            }
            JsonEvent::ValueTrue => self.handle_boolean_value(true),
            JsonEvent::ValueFalse => self.handle_boolean_value(false),
            JsonEvent::ValueNull => self.handle_null_value(),
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
            card_object_depth: 0,
            current_card_json: String::new(),
            current_set_code: None,
            current_set_type: None,
            expecting_cards_array: false,
            expecting_set_type: false,
            in_card_object: false,
            in_cards_array: false,
            in_set_object: false,
            is_skipping_value: false,
            json_depth: 0,
            skip_depth: 0,
        }
    }

    fn handle_start_object(&mut self) -> Result<usize> {
        // Critical: Reset ALL set state immediately when entering a new set object
        if self.json_depth == 2 {
            self.current_set_code = None;
            self.current_set_type = None;
            self.expecting_cards_array = false;
            self.expecting_set_type = false;
            self.in_set_object = true;
        }
        // Handle card objects within the cards array
        if self.in_cards_array && self.json_depth == 5 && !self.in_card_object {
            self.in_card_object = true;
            self.card_object_depth = self.json_depth;
            self.current_card_json.clear();
            self.current_card_json.push('{');
        } else if self.in_card_object {
            // Starting a nested object within a card
            let last_char = self.current_card_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_card_json.push(',');
            }
            self.current_card_json.push('{');
        }
        Ok(0)
    }

    async fn handle_end_object(&mut self) -> Result<usize> {
        if self.in_card_object {
            self.current_card_json.push('}');
            // Only process when we're ending the top-level card object
            if self.json_depth == self.card_object_depth {
                self.in_card_object = false;
                let card_result = self.parse_card_from_json(&self.current_card_json);
                match card_result {
                    Ok(card) => {
                        // Accumulate the whole set and flush only at the end of
                        // its `cards` array (handle_end_array). Flushing mid-set
                        // at a fixed count would split a card's two faces across
                        // batches, so the cross-face mana-cost merge (done per
                        // delivered batch) would silently miss the pair.
                        self.batch.push(card);
                    }
                    Err(e) => {
                        if let Some(code) = &self.current_set_code {
                            warn!("Failed to parse {} card: {}", code, e);
                        }
                    }
                }
                self.current_card_json.clear();
            }
        }
        Ok(0)
    }

    fn handle_start_array(&mut self) -> Result<usize> {
        if self.json_depth == 4 && self.expecting_cards_array {
            self.in_cards_array = true;
            self.expecting_cards_array = false;
            if self.current_set_code.is_some() {
                let code = self.current_set_code.as_deref().unwrap();
                info!("Processing cards for set: {}", code);
            }
        } else if self.in_card_object {
            let last_char = self.current_card_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_card_json.push(',');
            }
            self.current_card_json.push('[');
        }
        Ok(0)
    }

    fn handle_end_array(&mut self) -> Result<usize> {
        if self.in_cards_array && self.json_depth == 4 {
            self.in_cards_array = false;
            // return batch len to trigger json_stream_parser to call on_batch fn per set
            if !self.batch.is_empty() {
                return Ok(self.batch.len());
            }
        } else if self.in_card_object {
            self.current_card_json.push(']');
        }
        Ok(0)
    }

    fn handle_field_name<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        let field_name = parser.current_str().unwrap_or_default();
        if self.json_depth == 2 {
            debug!("ENTERING SET: '{}'", field_name);
            // Reset all state for new set
            self.current_set_code = Some(String::from(field_name));
            self.expecting_cards_array = false;
            return Ok(0);
        }

        match field_name {
            "meta" if self.json_depth == 1 => {
                self.is_skipping_value = true;
                self.skip_depth = self.json_depth;
            }
            "cards" if self.json_depth == 3 => {
                self.expecting_cards_array = true;
            }
            "type" if self.json_depth == 3 && !self.in_card_object => {
                self.expecting_set_type = true;
            }
            _ if self.in_card_object => {
                if !self.current_card_json.ends_with('{') {
                    self.current_card_json.push(',');
                }
                self.current_card_json.push('"');
                self.current_card_json.push_str(field_name);
                self.current_card_json.push('"');
                self.current_card_json.push(':');
            }
            _ => {
                // Don't skip fields within the current set
                if !self.in_cards_array
                    && !["name", "cards"].contains(&field_name)
                    && self.json_depth >= 3
                    && self.current_set_code.is_none()
                {
                    self.is_skipping_value = true;
                    self.skip_depth = self.json_depth;
                }
            }
        }
        Ok(0)
    }

    fn handle_string_value(&mut self, value: &str) -> Result<usize> {
        if self.expecting_set_type {
            self.current_set_type = Some(String::from(value));
            self.expecting_set_type = false;
            return Ok(0);
        }
        // Handle card object values
        if self.in_card_object {
            // Check if we need a comma
            let last_char = self.current_card_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_card_json.push(',');
            }

            self.current_card_json.push('"');
            // Proper JSON string escaping
            for ch in value.chars() {
                match ch {
                    '"' => self.current_card_json.push_str("\\\""),
                    '\\' => self.current_card_json.push_str("\\\\"),
                    '\n' => self.current_card_json.push_str("\\n"),
                    '\r' => self.current_card_json.push_str("\\r"),
                    '\t' => self.current_card_json.push_str("\\t"),
                    '\u{08}' => self.current_card_json.push_str("\\b"),
                    '\u{0C}' => self.current_card_json.push_str("\\f"),
                    c if c.is_control() => {
                        self.current_card_json
                            .push_str(&format!("\\u{:04x}", c as u32));
                    }
                    c => self.current_card_json.push(c),
                }
            }
            self.current_card_json.push('"');
        }
        Ok(0)
    }

    fn handle_number_value(&mut self, value: &str) -> Result<usize> {
        if self.in_card_object {
            // Check if we need a comma
            let last_char = self.current_card_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_card_json.push(',');
            }
            self.current_card_json.push_str(value);
        }
        Ok(0)
    }

    fn handle_boolean_value(&mut self, value: bool) -> Result<usize> {
        if self.in_card_object {
            // Check if we need a comma
            let last_char = self.current_card_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_card_json.push(',');
            }
            self.current_card_json
                .push_str(if value { "true" } else { "false" });
        }
        Ok(0)
    }

    fn handle_null_value(&mut self) -> Result<usize> {
        if self.in_card_object {
            // Check if we need a comma
            let last_char = self.current_card_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_card_json.push(',');
            }
            self.current_card_json.push_str("null");
        }
        Ok(0)
    }

    fn parse_card_from_json(&self, json: &str) -> Result<Card> {
        let value: serde_json::Value = serde_json::from_str(json)?;
        let set_type = self.current_set_type.as_deref().unwrap_or("");
        CardMapper::map_json_to_card(&value, set_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    fn empty_parser() -> JsonParser<AsyncBufReaderJsonFeeder<&'static [u8]>> {
        JsonParser::new(AsyncBufReaderJsonFeeder::new(BufReader::new(&b""[..])))
    }

    // A skipped field whose value is a scalar must end skip mode. Otherwise the
    // processor stays in skip mode forever and silently drops the rest of the
    // document. Aligns CardEventProcessor with SealedProductEventProcessor.
    #[tokio::test]
    async fn skip_mode_ends_on_scalar_value() {
        let mut processor = CardEventProcessor::new(10);
        // Simulate having just started skipping a field's value at depth 3.
        processor.is_skipping_value = true;
        processor.skip_depth = 3;
        processor.json_depth = 3;

        let parser = empty_parser();
        processor
            .process_event(JsonEvent::ValueString, &parser)
            .await
            .unwrap();

        assert!(
            !processor.is_skipping_value,
            "a scalar skipped value should end skip mode, not wedge it"
        );
    }

    /// Drive the processor over a JSON document and return the size of each
    /// batch it flushes.
    async fn batch_sizes(json: &str, batch_size: usize) -> Vec<usize> {
        use crate::utils::json_stream_parser::JsonStreamParser;
        use futures::stream;
        use std::sync::{Arc, Mutex};

        let mut parser = JsonStreamParser::new(CardEventProcessor::new(batch_size));
        let sizes = Arc::new(Mutex::new(Vec::new()));
        let sink = sizes.clone();
        let bytes = bytes::Bytes::from(json.to_string());
        let byte_stream = stream::once(async move { Ok::<_, std::io::Error>(bytes) });
        parser
            .parse_stream(byte_stream, move |batch| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock().unwrap().push(batch.len());
                    Ok(())
                })
            })
            .await
            .unwrap();
        Arc::try_unwrap(sizes).unwrap().into_inner().unwrap()
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
}
