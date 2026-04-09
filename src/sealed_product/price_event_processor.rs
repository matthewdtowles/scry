use crate::sealed_product::domain::SealedProductPrice;
use crate::utils::json_stream_parser::JsonEventProcessor;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use rust_decimal::Decimal;
use std::str::FromStr;

/// Allowed price providers (same as card price ingestion).
const ALLOWED_PROVIDERS: &[&str] = &["tcgplayer", "cardkingdom", "cardsphere"];

/// Streams AllPricesToday.json and extracts prices for sealed product UUIDs.
///
/// AllPricesToday structure:
/// ```json
/// { "data": { "<uuid>": { "paper": { "<provider>": { "retail": { "normal": { "<date>": price } } } } } } }
/// ```
///
/// Sealed products only have "normal" prices (no foil/etched).
/// Multiple providers are averaged.
pub struct SealedProductPriceEventProcessor {
    batch: Vec<SealedProductPrice>,
    batch_size: usize,
    current_uuid: Option<String>,
    in_data_object: bool,
    json_depth: usize,
    path: Vec<String>,
    // Accumulator for averaging across providers
    price_sum: f64,
    price_count: u32,
    price_date: Option<String>,
}

impl JsonEventProcessor<SealedProductPrice> for SealedProductPriceEventProcessor {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        match event {
            JsonEvent::StartObject => self.handle_start_object(),
            JsonEvent::EndObject => self.handle_end_object(),
            JsonEvent::FieldName => {
                let field_name = parser.current_str().unwrap_or_default();
                self.handle_field_name(String::from(field_name))
            }
            JsonEvent::ValueString => {
                let value = parser.current_str().unwrap_or_default();
                self.handle_value(String::from(value))
            }
            JsonEvent::ValueInt => {
                let value = parser.current_int().unwrap_or(0).to_string();
                self.handle_value(value)
            }
            JsonEvent::ValueFloat => {
                let value = parser.current_float().unwrap_or(0.0).to_string();
                self.handle_value(value)
            }
            JsonEvent::ValueTrue => self.handle_value("true".to_string()),
            JsonEvent::ValueFalse => self.handle_value("false".to_string()),
            JsonEvent::ValueNull => self.handle_value("null".to_string()),
            JsonEvent::StartArray => {
                self.json_depth += 1;
                Ok(0)
            }
            JsonEvent::EndArray => {
                self.json_depth -= 1;
                Ok(0)
            }
            _ => Ok(0),
        }
    }

    fn take_batch(&mut self) -> Vec<SealedProductPrice> {
        std::mem::take(&mut self.batch)
    }
}

impl SealedProductPriceEventProcessor {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch: Vec::with_capacity(batch_size),
            batch_size,
            current_uuid: None,
            in_data_object: false,
            json_depth: 0,
            path: Vec::new(),
            price_sum: 0.0,
            price_count: 0,
            price_date: None,
        }
    }

    fn handle_start_object(&mut self) -> Result<usize> {
        self.json_depth += 1;
        Ok(0)
    }

    fn handle_end_object(&mut self) -> Result<usize> {
        // depth 3 = end of a UUID entry
        if self.in_data_object && self.json_depth == 3 {
            if let Some(uuid) = self.current_uuid.take() {
                if self.price_count > 0 {
                    let avg = self.price_sum / self.price_count as f64;
                    if let (Ok(price), Some(date_str)) = (
                        Decimal::from_str(&format!("{:.2}", avg)),
                        self.price_date.take(),
                    ) {
                        if let Ok(date) =
                            chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                        {
                            self.batch.push(SealedProductPrice {
                                sealed_product_uuid: uuid,
                                price,
                                date,
                            });
                        }
                    }
                }
            }
            self.price_sum = 0.0;
            self.price_count = 0;
            self.price_date = None;

            let processed = if self.batch.len() >= self.batch_size {
                self.batch.len()
            } else {
                0
            };
            self.json_depth -= 1;
            self.path.pop();
            return Ok(processed);
        }
        if self.in_data_object && self.json_depth == 2 {
            self.in_data_object = false;
            self.path.pop();
        }
        self.json_depth -= 1;
        self.path.pop();
        Ok(0)
    }

    fn handle_field_name(&mut self, field_name: String) -> Result<usize> {
        if self.json_depth == 1 && field_name == "data" {
            self.in_data_object = true;
        } else if self.in_data_object && self.json_depth == 2 {
            self.current_uuid = Some(field_name.clone());
            self.price_sum = 0.0;
            self.price_count = 0;
            self.price_date = None;
        }
        self.path.push(field_name);
        Ok(0)
    }

    fn handle_value(&mut self, value: String) -> Result<usize> {
        // Path: data > uuid > paper > provider > retail > normal > date
        // Indices: 0      1      2       3         4       5       6
        let at_price_value = self.path.len() == 7
            && self.path[0] == "data"
            && self.path[2] == "paper"
            && self.path[4] == "retail"
            && self.path[5] == "normal";

        if at_price_value {
            let provider = &self.path[3];
            if ALLOWED_PROVIDERS.contains(&provider.as_str()) {
                if let Ok(price) = value.parse::<f64>() {
                    self.price_sum += price;
                    self.price_count += 1;
                    self.price_date = Some(self.path[6].clone());
                }
            }
        }
        self.path.pop();
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_processor() {
        let processor = SealedProductPriceEventProcessor::new(100);
        assert_eq!(processor.batch.capacity(), 100);
        assert!(processor.current_uuid.is_none());
    }
}
