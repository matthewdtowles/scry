use crate::sealed_product::{domain::SealedProduct, mapper::SealedProductMapper};
use crate::utils::json_stream_parser::JsonEventProcessor;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use tracing::{debug, warn};

pub struct SealedProductEventProcessor {
    batch: Vec<SealedProduct>,
    batch_size: usize,
    current_set_code: Option<String>,
    expecting_sealed_array: bool,
    in_sealed_array: bool,
    in_sealed_object: bool,
    sealed_object_depth: usize,
    current_sealed_json: String,
    is_skipping_value: bool,
    json_depth: usize,
    skip_depth: usize,
}

impl JsonEventProcessor<SealedProduct> for SealedProductEventProcessor {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
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
                    // Scalar value (string, int, float, bool, null) —
                    // stop skipping since the value is consumed
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
                let result = self.handle_end_object();
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
                self.handle_string_value(&value)
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

    fn take_batch(&mut self) -> Vec<SealedProduct> {
        std::mem::take(&mut self.batch)
    }
}

impl SealedProductEventProcessor {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch: Vec::with_capacity(batch_size),
            batch_size,
            current_set_code: None,
            expecting_sealed_array: false,
            in_sealed_array: false,
            in_sealed_object: false,
            sealed_object_depth: 0,
            current_sealed_json: String::new(),
            is_skipping_value: false,
            json_depth: 0,
            skip_depth: 0,
        }
    }

    fn handle_start_object(&mut self) -> Result<usize> {
        if self.json_depth == 2 {
            self.current_set_code = None;
            self.expecting_sealed_array = false;
        }
        if self.in_sealed_array && !self.in_sealed_object && self.json_depth == 5 {
            self.in_sealed_object = true;
            self.sealed_object_depth = self.json_depth;
            self.current_sealed_json.clear();
            self.current_sealed_json.push('{');
        } else if self.in_sealed_object {
            let last_char = self.current_sealed_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_sealed_json.push(',');
            }
            self.current_sealed_json.push('{');
        }
        Ok(0)
    }

    fn handle_end_object(&mut self) -> Result<usize> {
        if self.in_sealed_object {
            self.current_sealed_json.push('}');
            if self.json_depth == self.sealed_object_depth {
                self.in_sealed_object = false;
                if let Some(set_code) = &self.current_set_code {
                    match self.parse_sealed_product(&self.current_sealed_json, set_code) {
                        Ok(Some(product)) => {
                            self.batch.push(product);
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
                self.current_sealed_json.clear();
            }
        }
        Ok(0)
    }

    fn handle_start_array(&mut self) -> Result<usize> {
        if self.json_depth == 4 && self.expecting_sealed_array {
            self.in_sealed_array = true;
            self.expecting_sealed_array = false;
            if let Some(code) = &self.current_set_code {
                debug!("Processing sealed products for set: {}", code);
            }
        } else if self.in_sealed_object {
            let last_char = self.current_sealed_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_sealed_json.push(',');
            }
            self.current_sealed_json.push('[');
        }
        Ok(0)
    }

    fn handle_end_array(&mut self) -> Result<usize> {
        if self.in_sealed_array && self.json_depth == 4 {
            self.in_sealed_array = false;
            if !self.batch.is_empty() {
                return Ok(self.batch.len());
            }
        } else if self.in_sealed_object {
            self.current_sealed_json.push(']');
        }
        Ok(0)
    }

    fn handle_field_name<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        let field_name = parser.current_str().unwrap_or_default();

        // depth 2 = set code key (e.g., "woe", "blb")
        if self.json_depth == 2 {
            self.current_set_code = Some(String::from(field_name));
            self.expecting_sealed_array = false;
            return Ok(0);
        }

        match field_name {
            "meta" if self.json_depth == 1 => {
                self.is_skipping_value = true;
                self.skip_depth = self.json_depth;
            }
            "sealedProduct" if self.json_depth == 3 => {
                self.expecting_sealed_array = true;
            }
            _ if self.in_sealed_object => {
                if !self.current_sealed_json.ends_with('{') {
                    self.current_sealed_json.push(',');
                }
                self.current_sealed_json.push('"');
                self.current_sealed_json.push_str(&field_name);
                self.current_sealed_json.push('"');
                self.current_sealed_json.push(':');
            }
            _ => {
                // Skip non-sealedProduct fields at set level
                if !self.in_sealed_array
                    && self.json_depth >= 3
                    && field_name != "sealedProduct"
                {
                    self.is_skipping_value = true;
                    self.skip_depth = self.json_depth;
                }
            }
        }
        Ok(0)
    }

    fn handle_string_value(&mut self, value: &str) -> Result<usize> {
        if self.in_sealed_object {
            let last_char = self.current_sealed_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_sealed_json.push(',');
            }
            self.current_sealed_json.push('"');
            for ch in value.chars() {
                match ch {
                    '"' => self.current_sealed_json.push_str("\\\""),
                    '\\' => self.current_sealed_json.push_str("\\\\"),
                    '\n' => self.current_sealed_json.push_str("\\n"),
                    '\r' => self.current_sealed_json.push_str("\\r"),
                    '\t' => self.current_sealed_json.push_str("\\t"),
                    '\u{08}' => self.current_sealed_json.push_str("\\b"),
                    '\u{0C}' => self.current_sealed_json.push_str("\\f"),
                    c if c.is_control() => {
                        self.current_sealed_json
                            .push_str(&format!("\\u{:04x}", c as u32));
                    }
                    c => self.current_sealed_json.push(c),
                }
            }
            self.current_sealed_json.push('"');
        }
        Ok(0)
    }

    fn handle_number_value(&mut self, value: &str) -> Result<usize> {
        if self.in_sealed_object {
            let last_char = self.current_sealed_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_sealed_json.push(',');
            }
            self.current_sealed_json.push_str(value);
        }
        Ok(0)
    }

    fn handle_boolean_value(&mut self, value: bool) -> Result<usize> {
        if self.in_sealed_object {
            let last_char = self.current_sealed_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_sealed_json.push(',');
            }
            self.current_sealed_json
                .push_str(if value { "true" } else { "false" });
        }
        Ok(0)
    }

    fn handle_null_value(&mut self) -> Result<usize> {
        if self.in_sealed_object {
            let last_char = self.current_sealed_json.chars().last();
            if !matches!(last_char, Some('{') | Some('[') | Some(':') | Some(',')) {
                self.current_sealed_json.push(',');
            }
            self.current_sealed_json.push_str("null");
        }
        Ok(0)
    }

    fn parse_sealed_product(
        &self,
        json: &str,
        set_code: &str,
    ) -> Result<Option<SealedProduct>> {
        let value: serde_json::Value = serde_json::from_str(json)?;
        SealedProductMapper::map_single_item(&value, set_code)
    }
}
