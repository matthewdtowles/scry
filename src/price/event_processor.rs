use crate::price::domain::{CardPrices, Price, PriceAccumulator};
use crate::price::AVERAGE_PROVIDERS;
use crate::utils::json_stream_parser::JsonEventProcessor;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use std::collections::HashMap;

/// Streams an MTGJSON price file and emits, per card, derived averaged retail
/// prices. The average is retail-only across `AVERAGE_PROVIDERS`; buylist and
/// per-provider retail are no longer captured here — the granular store is
/// CK-direct-only (ROADMAP 10.10).
///
/// One processor serves both price files; the only difference is how prices
/// accumulate per card ([`Accumulation`]):
/// - [`Self::new`] — `AllPricesToday.json`: one running average (single date),
///   for the `price` table.
/// - [`Self::new_historical`] — `AllPrices.json`: one average per date, for
///   the `price_history` backfill.
pub struct PriceEventProcessor {
    accumulation: Accumulation,
    batch: Vec<CardPrices>,
    batch_size: usize,
    current_card_uuid: Option<String>,
    in_data_object: bool,
    json_depth: usize,
    path: Vec<String>,
}

/// How prices accumulate within one card object.
enum Accumulation {
    /// One running average; the date is set as values arrive.
    SingleDate(Option<PriceAccumulator>),
    /// One running average per date string.
    PerDate(HashMap<String, PriceAccumulator>),
}

impl Accumulation {
    fn start_card(&mut self) {
        match self {
            Accumulation::SingleDate(acc) => *acc = Some(PriceAccumulator::new()),
            Accumulation::PerDate(map) => map.clear(),
        }
    }

    fn record(&mut self, finish: &str, price: f64, date_str: String) {
        match self {
            Accumulation::SingleDate(Some(acc)) => {
                if finish == "foil" || finish == "etched" {
                    acc.add_foil(price);
                    acc.set_date(date_str);
                } else if finish == "normal" {
                    acc.add_normal(price);
                    acc.set_date(date_str);
                }
            }
            Accumulation::SingleDate(None) => {}
            Accumulation::PerDate(map) => {
                let acc = map.entry(date_str).or_default();
                if finish == "foil" || finish == "etched" {
                    acc.add_foil(price);
                } else if finish == "normal" {
                    acc.add_normal(price);
                }
            }
        }
    }

    /// Drain the card's accumulated averages into `Price` rows.
    fn finish_card(&mut self, card_uuid: String) -> Vec<Price> {
        match self {
            Accumulation::SingleDate(acc) => acc
                .take()
                .and_then(|a| a.into_price(card_uuid).ok())
                .into_iter()
                .collect(),
            Accumulation::PerDate(map) => {
                let accumulators = std::mem::take(map);
                let mut averages = Vec::with_capacity(accumulators.len());
                for (date_str, mut acc) in accumulators {
                    acc.set_date(date_str);
                    if let Ok(price) = acc.into_price(card_uuid.clone()) {
                        averages.push(price);
                    }
                }
                averages
            }
        }
    }
}

impl JsonEventProcessor<CardPrices> for PriceEventProcessor {
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

impl PriceEventProcessor {
    /// Processor for `AllPricesToday.json`: one averaged retail price per card.
    pub fn new(batch_size: usize) -> Self {
        Self::with_accumulation(batch_size, Accumulation::SingleDate(None))
    }

    /// Processor for `AllPrices.json` (historical): one averaged retail price
    /// per card per date.
    pub fn new_historical(batch_size: usize) -> Self {
        Self::with_accumulation(batch_size, Accumulation::PerDate(HashMap::new()))
    }

    fn with_accumulation(batch_size: usize, accumulation: Accumulation) -> Self {
        Self {
            accumulation,
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
                let averages = self.accumulation.finish_card(card_uuid);
                if !averages.is_empty() {
                    self.batch.push(CardPrices { averages });
                }
            }
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
            self.accumulation.start_card();
        }
        self.path.push(field_name);
        Ok(0)
    }

    fn handle_value(&mut self, value: String) -> Result<usize> {
        if self.current_card_uuid.is_some() && self.at_price_value() {
            let provider = &self.path[3];
            let price_type = &self.path[4];
            let finish = self.path[5].clone();
            let date_str = self.path[6].clone();

            // Derived averages: original providers, retail only, foil+etched
            // fold into foil.
            if price_type == "retail" && AVERAGE_PROVIDERS.contains(&provider.as_str()) {
                if let Ok(price) = value.parse::<f64>() {
                    self.accumulation.record(&finish, price, date_str);
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
    use crate::utils::json_stream_parser::test_support::collect_batches;
    use chrono::NaiveDate;
    use rust_decimal::Decimal;

    /// Drive a processor over a JSON document and collect all emitted cards.
    async fn run(processor: PriceEventProcessor, json: &str) -> Vec<CardPrices> {
        collect_batches(processor, json)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Flatten the derived averaged prices across all emitted cards.
    fn averages(cards: &[CardPrices]) -> Vec<Price> {
        cards.iter().flat_map(|c| c.averages.clone()).collect()
    }

    fn sample_json() -> &'static str {
        r#"{
          "meta": {"date": "2024-01-15", "version": "5.2.0"},
          "data": {
            "card-uuid-1": {
              "paper": {
                "tcgplayer": {
                  "retail": {
                    "normal": {"2024-01-15": 5.00},
                    "foil": {"2024-01-15": 10.00}
                  }
                },
                "cardkingdom": {
                  "retail": {"normal": {"2024-01-15": 5.50}},
                  "buylist": {"normal": {"2024-01-15": 3.50}}
                },
                "cardsphere": {
                  "buylist": {"normal": {"2024-01-15": 3.25}}
                }
              }
            }
          }
        }"#
    }

    #[tokio::test]
    async fn derived_average_is_retail_only_unchanged() {
        let cards = run(PriceEventProcessor::new(1), sample_json()).await;
        assert_eq!(cards[0].averages.len(), 1);
        let avg = cards[0]
            .averages
            .first()
            .expect("expected an average price");
        // normal retail = avg(5.00, 5.50) = 5.25; foil retail = 10.00
        assert_eq!(avg.normal, Some(Decimal::new(525, 2)));
        assert_eq!(avg.foil, Some(Decimal::from(10)));
        // buylist must not influence the average
    }

    #[tokio::test]
    async fn average_uses_only_average_providers() {
        let json = r#"{
          "data": {
            "card-uuid-1": {
              "paper": {
                "tcgplayer": {"retail": {"normal": {"2024-01-15": 4.00}}},
                "manapool": {"retail": {"normal": {"2024-01-15": 6.00}}},
                "cardmarket": {"retail": {"normal": {"2024-01-15": 99.00}}}
              }
            }
          }
        }"#;
        let cards = run(PriceEventProcessor::new(1), json).await;
        // average uses only AVERAGE_PROVIDERS (tcgplayer here) = 4.00, not manapool/cardmarket
        let avg = cards[0].averages.first().expect("expected an average");
        assert_eq!(avg.normal, Some(Decimal::from(4)));
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

        let prices = averages(&run(PriceEventProcessor::new_historical(500), json).await);
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

        let prices = averages(&run(PriceEventProcessor::new_historical(500), json).await);
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

        let batches = collect_batches(PriceEventProcessor::new_historical(1), json).await;
        let total_prices: usize = batches.iter().map(|b| averages(b).len()).sum();
        assert_eq!(total_prices, 5, "Should emit 5 total averaged prices");
        assert_eq!(batches.len(), 2, "One flush per card at batch size 1");
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

        let cards = run(PriceEventProcessor::new_historical(500), json).await;
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

        let prices = averages(&run(PriceEventProcessor::new_historical(500), json).await);
        assert_eq!(prices.len(), 1);
        let price = &prices[0];
        assert_eq!(price.normal.unwrap().to_string(), "5");
        assert_eq!(price.foil.unwrap().to_string(), "15");
    }
}
