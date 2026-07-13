use crate::price::domain::{CardPrices, Price, PriceAccumulator};
use crate::price::AVERAGE_PROVIDERS;
use crate::utils::json_stream_parser::JsonEventProcessor;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use std::collections::HashMap;

/// Event processor for AllPrices.json (historical multi-date data).
///
/// Per card, emits a `CardPrices` bundle with one averaged retail `Price` per
/// date (the `price_history` behavior). The averages use `AVERAGE_PROVIDERS`
/// (retail only). The granular store is CK-direct-only now (ROADMAP 10.10), so
/// this processor no longer captures granular rows.
pub struct HistoricalPriceEventProcessor {
    /// Map of date string -> PriceAccumulator for the current card
    accumulators: HashMap<String, PriceAccumulator>,
    batch: Vec<CardPrices>,
    batch_size: usize,
    current_card_uuid: Option<String>,
    in_data_object: bool,
    json_depth: usize,
    path: Vec<String>,
}

impl JsonEventProcessor<CardPrices> for HistoricalPriceEventProcessor {
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
                let value = parser.current_int::<i64>()?.to_string();
                self.handle_value(value)
            }
            JsonEvent::ValueFloat => {
                let value = parser.current_float()?.to_string();
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

    fn take_batch(&mut self) -> Vec<CardPrices> {
        std::mem::take(&mut self.batch)
    }
}

impl HistoricalPriceEventProcessor {
    pub fn new(batch_size: usize) -> Self {
        Self {
            accumulators: HashMap::new(),
            batch: Vec::with_capacity(batch_size),
            batch_size,
            current_card_uuid: None,
            in_data_object: false,
            json_depth: 0,
            path: Vec::new(),
        }
    }

    fn handle_start_object(&mut self) -> Result<usize> {
        self.json_depth += 1;
        Ok(0)
    }

    fn handle_end_object(&mut self) -> Result<usize> {
        // Card object ends: depth 3 -> 2
        if self.in_data_object && self.json_depth == 3 {
            if let Some(card_uuid) = self.current_card_uuid.take() {
                let accumulators = std::mem::take(&mut self.accumulators);
                let mut averages: Vec<Price> = Vec::with_capacity(accumulators.len());
                for (date_str, mut acc) in accumulators {
                    acc.set_date(date_str);
                    if let Ok(price) = acc.into_price(card_uuid.clone()) {
                        averages.push(price);
                    }
                }
                if !averages.is_empty() {
                    self.batch.push(CardPrices { averages });
                }
            }
            self.accumulators.clear();
            let processed = if self.batch.len() >= self.batch_size {
                self.batch.len()
            } else {
                0
            };
            self.json_depth -= 1;
            self.path.pop();
            return Ok(processed);
        }
        if self.in_price_object() {
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
        } else if self.in_price_object() {
            self.current_card_uuid = Some(field_name.clone());
            self.accumulators.clear();
        }
        self.path.push(field_name);
        Ok(0)
    }

    fn handle_value(&mut self, value: String) -> Result<usize> {
        if self.current_card_uuid.is_some() && self.at_price_value() {
            let provider = self.path[3].clone();
            let price_type = self.path[4].clone();
            let finish = self.path[5].clone();
            let date_str = self.path[6].clone();

            // Derived per-date averages: original providers, retail only.
            if price_type == "retail" && AVERAGE_PROVIDERS.contains(&provider.as_str()) {
                if let Ok(price) = value.parse::<f64>() {
                    let acc = self
                        .accumulators
                        .entry(date_str)
                        .or_insert_with(PriceAccumulator::new);
                    if finish == "foil" || finish == "etched" {
                        acc.add_foil(price);
                    } else if finish == "normal" {
                        acc.add_normal(price);
                    }
                }
            }
        }
        self.path.pop();
        Ok(0)
    }

    /// path = data / uuid / paper / provider / {retail|buylist} / finish / date
    fn at_price_value(&self) -> bool {
        self.path.len() == 7
            && self.path[0] == "data"
            && self.path[2] == "paper"
            && (self.path[4] == "retail" || self.path[4] == "buylist")
            && (self.path[5] == "normal" || self.path[5] == "foil" || self.path[5] == "etched")
    }

    fn in_price_object(&self) -> bool {
        self.in_data_object && self.json_depth == 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actson::tokio::AsyncBufReaderJsonFeeder;
    use actson::JsonParser;
    use chrono::NaiveDate;
    use tokio::io::BufReader;

    /// Parse a JSON string through the processor and return all emitted bundles.
    async fn parse_json(json: &str) -> Vec<CardPrices> {
        let reader = BufReader::new(json.as_bytes());
        let feeder = AsyncBufReaderJsonFeeder::new(reader);
        let mut parser = JsonParser::new(feeder);
        let mut processor = HistoricalPriceEventProcessor::new(500);
        let mut all_cards = Vec::new();

        loop {
            let event = match parser.next_event() {
                Ok(Some(actson::JsonEvent::NeedMoreInput)) => {
                    match parser.feeder.fill_buf().await {
                        Ok(()) => continue,
                        Err(_) => break,
                    }
                }
                Ok(Some(event)) => event,
                Ok(None) => {
                    all_cards.extend(processor.take_batch());
                    break;
                }
                Err(_) => break,
            };
            let count = processor.process_event(event, &parser).await.unwrap();
            if count > 0 {
                all_cards.extend(processor.take_batch());
            }
        }
        all_cards
    }

    /// Flatten the derived averaged prices across all emitted cards.
    fn averages(cards: &[CardPrices]) -> Vec<Price> {
        cards.iter().flat_map(|c| c.averages.clone()).collect()
    }

    #[tokio::test]
    async fn test_multi_date_parsing() {
        let json = r#"{
            "data": {
                "card-uuid-1": {
                    "paper": {
                        "tcgplayer": {
                            "retail": {
                                "normal": {
                                    "2024-01-01": 1.50,
                                    "2024-01-02": 2.00,
                                    "2024-01-03": 2.50
                                }
                            }
                        }
                    }
                }
            }
        }"#;

        let prices = averages(&parse_json(json).await);
        assert_eq!(prices.len(), 3, "Should emit 3 prices for 3 dates");

        let mut dates: Vec<NaiveDate> = prices.iter().map(|p| p.date).collect();
        dates.sort();
        assert_eq!(dates[0], NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(dates[1], NaiveDate::from_ymd_opt(2024, 1, 2).unwrap());
        assert_eq!(dates[2], NaiveDate::from_ymd_opt(2024, 1, 3).unwrap());

        for p in &prices {
            assert_eq!(p.card_id, "card-uuid-1");
        }
    }

    #[tokio::test]
    async fn test_multi_provider_averaging() {
        let json = r#"{
            "data": {
                "card-uuid-2": {
                    "paper": {
                        "tcgplayer": {
                            "retail": { "normal": { "2024-06-15": 10.00 } }
                        },
                        "cardkingdom": {
                            "retail": { "normal": { "2024-06-15": 12.00 } }
                        }
                    }
                }
            }
        }"#;

        let prices = averages(&parse_json(json).await);
        assert_eq!(prices.len(), 1, "Should emit 1 price (one date)");
        let price = &prices[0];
        assert_eq!(price.card_id, "card-uuid-2");
        assert_eq!(price.date, NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
        // Average of 10.00 and 12.00 = 11.00
        assert_eq!(price.normal.unwrap().to_string(), "11");
    }

    #[tokio::test]
    async fn test_batch_threshold() {
        // Two cards with 2 and 3 dates. Batch size = 1 card -> each card flushes
        // its bundle, so two emissions totaling 5 averaged prices.
        let json = r#"{
            "data": {
                "card-uuid-a": {
                    "paper": {
                        "tcgplayer": {
                            "retail": { "normal": { "2024-01-01": 1.0, "2024-01-02": 2.0 } }
                        }
                    }
                },
                "card-uuid-b": {
                    "paper": {
                        "tcgplayer": {
                            "retail": { "normal": { "2024-01-03": 3.0, "2024-01-04": 4.0, "2024-01-05": 5.0 } }
                        }
                    }
                }
            }
        }"#;

        let reader = BufReader::new(json.as_bytes());
        let feeder = AsyncBufReaderJsonFeeder::new(reader);
        let mut parser = JsonParser::new(feeder);
        let mut processor = HistoricalPriceEventProcessor::new(1);
        let mut batch_count = 0;
        let mut total_prices = 0;

        loop {
            let event = match parser.next_event() {
                Ok(Some(actson::JsonEvent::NeedMoreInput)) => {
                    match parser.feeder.fill_buf().await {
                        Ok(()) => continue,
                        Err(_) => break,
                    }
                }
                Ok(Some(event)) => event,
                Ok(None) => {
                    let remaining = processor.take_batch();
                    if !remaining.is_empty() {
                        batch_count += 1;
                        total_prices += averages(&remaining).len();
                    }
                    break;
                }
                Err(_) => break,
            };
            let count = processor.process_event(event, &parser).await.unwrap();
            if count > 0 {
                let batch = processor.take_batch();
                total_prices += averages(&batch).len();
                batch_count += 1;
            }
        }
        assert_eq!(total_prices, 5, "Should emit 5 total averaged prices");
        assert_eq!(batch_count, 2, "One flush per card at batch size 1");
    }

    #[tokio::test]
    async fn test_filtered_provider_ignored() {
        let json = r#"{
            "data": {
                "card-uuid-4": {
                    "paper": {
                        "unknownprovider": {
                            "retail": { "normal": { "2024-01-01": 99.99 } }
                        }
                    }
                }
            }
        }"#;

        let cards = parse_json(json).await;
        assert!(cards.is_empty(), "Unknown provider yields no averages");
    }

    #[tokio::test]
    async fn test_foil_and_normal() {
        let json = r#"{
            "data": {
                "card-uuid-5": {
                    "paper": {
                        "tcgplayer": {
                            "retail": {
                                "normal": { "2024-03-01": 5.00 },
                                "foil": { "2024-03-01": 15.00 }
                            }
                        }
                    }
                }
            }
        }"#;

        let prices = averages(&parse_json(json).await);
        assert_eq!(prices.len(), 1);
        let price = &prices[0];
        assert_eq!(price.normal.unwrap().to_string(), "5");
        assert_eq!(price.foil.unwrap().to_string(), "15");
    }
}
