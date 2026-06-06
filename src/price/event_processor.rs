use crate::price::domain::{CardPrices, GranularPrice, Price, PriceAccumulator};
use crate::price::{AVERAGE_PROVIDERS, GRANULAR_PROVIDERS};
use crate::utils::json_stream_parser::JsonEventProcessor;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use chrono::NaiveDate;
use rust_decimal::Decimal;

/// Streams MTGJSON `AllPricesToday` and, per card, emits:
/// - one `GranularPrice` row per (provider, retail|buylist, finish, date), and
/// - the derived averaged retail price (`PriceAccumulator`) for the `price` table.
///
/// The average is retail-only across `AVERAGE_PROVIDERS`, exactly as before, so
/// the existing `price`/`price_history` tables are unchanged while the granular
/// store fills. Buylist and per-provider retail are captured only in granular.
pub struct PriceEventProcessor {
    accumulator: Option<PriceAccumulator>,
    current_granular: Vec<GranularPrice>,
    batch: Vec<CardPrices>,
    batch_size: usize,
    current_card_uuid: Option<String>,
    in_data_object: bool,
    json_depth: usize,
    path: Vec<String>,
}

impl JsonEventProcessor<CardPrices> for PriceEventProcessor {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>, // Do not remove
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
    pub fn new(batch_size: usize) -> Self {
        Self {
            accumulator: None,
            current_granular: Vec::new(),
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
        if self.in_data_object && self.json_depth == 3 {
            if let Some(card_uuid) = self.current_card_uuid.take() {
                let granular = std::mem::take(&mut self.current_granular);
                let averages: Vec<Price> = self
                    .accumulator
                    .take()
                    .and_then(|acc| acc.into_price(card_uuid).ok())
                    .into_iter()
                    .collect();
                if !averages.is_empty() || !granular.is_empty() {
                    self.batch.push(CardPrices { averages, granular });
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
            self.accumulator = Some(PriceAccumulator::new());
            self.current_granular = Vec::new();
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

            // Granular capture: broad provider set, retail + buylist.
            if GRANULAR_PROVIDERS.contains(&provider.as_str()) {
                self.record_granular(&provider, &price_type, &finish, &date_str, &value);
            }

            // Derived average: original providers, retail only, foil+etched fold
            // into foil, so the `price` table is identical to the pre-granular
            // behavior.
            if price_type == "retail" && AVERAGE_PROVIDERS.contains(&provider.as_str()) {
                if let (Some(acc), Ok(price)) = (self.accumulator.as_mut(), value.parse::<f64>()) {
                    if finish == "foil" || finish == "etched" {
                        acc.add_foil(price);
                        acc.set_date(date_str);
                    } else if finish == "normal" {
                        acc.add_normal(price);
                        acc.set_date(date_str);
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

    fn record_granular(
        &mut self,
        provider: &str,
        price_type: &str,
        finish: &str,
        date_str: &str,
        value: &str,
    ) {
        let (Some(card_uuid), Ok(price), Ok(date)) = (
            self.current_card_uuid.clone(),
            value.parse::<Decimal>(),
            NaiveDate::parse_from_str(date_str, "%Y-%m-%d"),
        ) else {
            return;
        };
        if let Ok(gp) = GranularPrice::new(
            card_uuid,
            provider.to_string(),
            price_type.to_string(),
            finish.to_string(),
            GranularPrice::DEFAULT_CONDITION.to_string(),
            date,
            price,
            None,
        ) {
            self.current_granular.push(gp);
        }
    }

    fn in_price_object(&self) -> bool {
        self.in_data_object && self.json_depth == 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::json_stream_parser::JsonStreamParser;
    use futures::stream;

    /// Drive the processor over a JSON document and collect all emitted cards.
    async fn run(json: &str) -> Vec<CardPrices> {
        let processor = PriceEventProcessor::new(1);
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
    async fn emits_granular_for_retail_and_buylist_all_providers() {
        let cards = run(sample_json()).await;
        assert_eq!(cards.len(), 1);
        let g = &cards[0].granular;
        // tcgplayer retail normal+foil, cardkingdom retail normal + buylist normal,
        // cardsphere buylist normal = 5 rows
        assert_eq!(g.len(), 5);

        let buylist: Vec<_> = g.iter().filter(|r| r.price_type == "buylist").collect();
        assert_eq!(buylist.len(), 2);
        assert!(buylist
            .iter()
            .all(|r| r.condition == "NM" && r.finish == "normal"));
        assert!(buylist
            .iter()
            .any(|r| r.provider == "cardkingdom" && r.price == Decimal::new(350, 2)));
        assert!(buylist
            .iter()
            .any(|r| r.provider == "cardsphere" && r.price == Decimal::new(325, 2)));
    }

    #[tokio::test]
    async fn derived_average_is_retail_only_unchanged() {
        let cards = run(sample_json()).await;
        assert_eq!(cards[0].averages.len(), 1);
        let avg = cards[0].averages.first().expect("expected an average price");
        // normal retail = avg(5.00, 5.50) = 5.25; foil retail = 10.00
        assert_eq!(avg.normal, Some(Decimal::new(525, 2)));
        assert_eq!(avg.foil, Some(Decimal::from(10)));
        // buylist must not influence the average
    }

    #[tokio::test]
    async fn manapool_captured_in_granular_cardmarket_ignored_neither_in_average() {
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
        let cards = run(json).await;
        let g = &cards[0].granular;
        // manapool captured, cardmarket not
        assert!(g.iter().any(|r| r.provider == "manapool" && r.price == Decimal::from(6)));
        assert!(g.iter().all(|r| r.provider != "cardmarket"));
        // average uses only AVERAGE_PROVIDERS (tcgplayer here) = 4.00, not manapool/cardmarket
        let avg = cards[0].averages.first().expect("expected an average");
        assert_eq!(avg.normal, Some(Decimal::from(4)));
    }
}
