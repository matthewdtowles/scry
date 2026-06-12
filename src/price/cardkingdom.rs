use crate::price::domain::GranularPrice;
use crate::utils::json_stream_parser::JsonEventProcessor;
use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;

pub(crate) const CK_PROVIDER: &str = "cardkingdom";
pub(crate) const BUYLIST_PRICE_TYPE: &str = "buylist";

/// One product from Card Kingdom's direct pricelist
/// (`api.cardkingdom.com/api/v2/pricelist`), reduced to the fields the
/// buylist ingest needs. Parsed raw; offer policy lives in
/// `granular_from_ck_products`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CkProduct {
    pub scryfall_id: Option<String>,
    pub is_foil: bool,
    pub price_buy: Option<Decimal>,
    pub qty_buying: Option<i32>,
}

/// Map a batch of CK products to granular buylist rows.
///
/// Only real offers are emitted: `price_buy > 0` AND `qty_buying > 0`.
/// CK publishes a theoretical buy price for nearly everything while actually
/// buying far less; MTGJSON's buylist already encodes "CK is buying" by row
/// presence, so emitting qty-0 products here would add offers the indicative
/// feed deliberately drops. The CK-direct row overwrites the indicative
/// MTGJSON row on the shared key (CK-direct ingests after MTGJSON), carrying
/// the live qty.
///
/// Returns the rows plus the count of real offers that matched no card
/// (scryfall_id unknown to us -- e.g. sets we exclude).
pub(crate) fn granular_from_ck_products(
    products: Vec<CkProduct>,
    scryfall_to_card_id: &HashMap<String, String>,
    date: NaiveDate,
) -> (Vec<GranularPrice>, u64) {
    // Keyed by (card_id, finish): distinct CK products can collapse onto one
    // series (etched products report is_foil=true and share the regular foil's
    // scryfall_id; variations share one too). A multi-row upsert with duplicate
    // keys is a Postgres error ("cannot affect row a second time"), so dedupe
    // here, keeping the best offer for the series.
    let mut by_series: HashMap<(String, &'static str), GranularPrice> = HashMap::new();
    let mut unmatched: u64 = 0;
    for product in products {
        let (Some(price_buy), Some(qty)) = (product.price_buy, product.qty_buying) else {
            continue;
        };
        if price_buy <= Decimal::ZERO || qty <= 0 {
            continue;
        }
        let Some(scryfall_id) = product.scryfall_id.as_deref() else {
            continue;
        };
        let Some(card_id) = scryfall_to_card_id.get(scryfall_id) else {
            unmatched += 1;
            continue;
        };
        let finish = if product.is_foil { "foil" } else { "normal" };
        if let Ok(mut gp) = GranularPrice::new(
            card_id.clone(),
            CK_PROVIDER.to_string(),
            BUYLIST_PRICE_TYPE.to_string(),
            finish.to_string(),
            GranularPrice::DEFAULT_CONDITION.to_string(),
            date,
            price_buy,
        ) {
            gp.qty = Some(qty);
            match by_series.entry((card_id.clone(), finish)) {
                std::collections::hash_map::Entry::Occupied(mut existing) => {
                    if gp.price > existing.get().price {
                        existing.insert(gp);
                    }
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(gp);
                }
            }
        }
    }
    (by_series.into_values().collect(), unmatched)
}

/// Streams CK's pricelist: `{ "meta": {...}, "data": [ {product}, ... ] }`.
/// Products are flat objects at depth 3 (root object 1, data array 2); the
/// nested `condition_values` object (depth 4) is skipped. ~147k products /
/// ~62MB, so this must stream like the MTGJSON processors.
pub struct CkPricelistEventProcessor {
    batch: Vec<CkProduct>,
    batch_size: usize,
    current: CkProduct,
    current_field: Option<String>,
    in_data_array: bool,
    json_depth: usize,
}

impl JsonEventProcessor<CkProduct> for CkPricelistEventProcessor {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize> {
        match event {
            JsonEvent::StartObject | JsonEvent::StartArray => {
                self.json_depth += 1;
                Ok(0)
            }
            JsonEvent::EndObject => self.handle_end_object(),
            JsonEvent::EndArray => {
                if self.json_depth == 2 {
                    self.in_data_array = false;
                }
                self.json_depth -= 1;
                Ok(0)
            }
            JsonEvent::FieldName => {
                let field_name = parser.current_str().unwrap_or_default();
                if self.json_depth == 1 && field_name == "data" {
                    self.in_data_array = true;
                } else if self.at_product_level() {
                    self.current_field = Some(String::from(field_name));
                }
                Ok(0)
            }
            JsonEvent::ValueString => {
                let value = parser.current_str().unwrap_or_default();
                self.handle_value(String::from(value));
                Ok(0)
            }
            JsonEvent::ValueInt => {
                let value = parser.current_int::<i64>()?.to_string();
                self.handle_value(value);
                Ok(0)
            }
            JsonEvent::ValueFloat => {
                let value = parser.current_float()?.to_string();
                self.handle_value(value);
                Ok(0)
            }
            JsonEvent::ValueTrue => {
                self.handle_value("true".to_string());
                Ok(0)
            }
            JsonEvent::ValueFalse => {
                self.handle_value("false".to_string());
                Ok(0)
            }
            _ => Ok(0),
        }
    }

    fn take_batch(&mut self) -> Vec<CkProduct> {
        std::mem::take(&mut self.batch)
    }
}

impl CkPricelistEventProcessor {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch: Vec::with_capacity(batch_size),
            batch_size,
            current: CkProduct::default(),
            current_field: None,
            in_data_array: false,
            json_depth: 0,
        }
    }

    /// Product objects sit at depth 3: root object (1) / data array (2) /
    /// product (3). `condition_values` contents are at depth 4 and ignored.
    fn at_product_level(&self) -> bool {
        self.in_data_array && self.json_depth == 3
    }

    fn handle_end_object(&mut self) -> Result<usize> {
        if self.at_product_level() {
            let product = std::mem::take(&mut self.current);
            self.batch.push(product);
            self.current_field = None;
            self.json_depth -= 1;
            return Ok(if self.batch.len() >= self.batch_size {
                self.batch.len()
            } else {
                0
            });
        }
        self.json_depth -= 1;
        Ok(0)
    }

    fn handle_value(&mut self, value: String) {
        if !self.at_product_level() {
            return;
        }
        match self.current_field.as_deref() {
            Some("scryfall_id") => {
                if !value.is_empty() && value != "null" {
                    self.current.scryfall_id = Some(value);
                }
            }
            Some("is_foil") => self.current.is_foil = value == "true",
            Some("price_buy") => self.current.price_buy = value.parse::<Decimal>().ok(),
            Some("qty_buying") => self.current.qty_buying = value.parse::<i32>().ok(),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::json_stream_parser::JsonStreamParser;
    use futures::stream;

    /// Drive the processor over a JSON document and collect all parsed products.
    async fn run(json: &str) -> Vec<CkProduct> {
        let processor = CkPricelistEventProcessor::new(1);
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

    /// Real shape: meta object, string booleans/prices, int quantities, and a
    /// nested condition_values object whose fields must not bleed into the
    /// product.
    fn sample_json() -> &'static str {
        r#"{
          "meta": {"created_at": "2026-06-10 05:05:57", "base_url": "https://www.cardkingdom.com/"},
          "data": [
            {"id": 10000, "sku": "4ED-117", "scryfall_id": "a363bc91-8278-448e-9d5c-564e4b51eb62",
             "url": "mtg/4th-edition/abomination", "name": "Abomination", "variation": "",
             "edition": "4th Edition", "is_foil": "false",
             "price_retail": "0.39", "qty_retail": 12,
             "price_buy": "0.02", "qty_buying": 0,
             "condition_values": {"nm_price": "0.39", "nm_qty": 2, "ex_price": "0.31", "ex_qty": 8}},
            {"id": 20001, "sku": "MH3-100", "scryfall_id": "e5cfaefb-764c-4c56-bdb3-5f0375168597",
             "url": "mtg/mh3/example", "name": "Example Foil", "variation": "",
             "edition": "Modern Horizons 3", "is_foil": "true",
             "price_retail": "10.00", "qty_retail": 3,
             "price_buy": "4.50", "qty_buying": 16,
             "condition_values": {"nm_price": "10.00", "nm_qty": 3}}
          ]
        }"#
    }

    #[tokio::test]
    async fn parses_products_with_buylist_fields() {
        let products = run(sample_json()).await;
        assert_eq!(products.len(), 2);

        let first = &products[0];
        assert_eq!(
            first.scryfall_id.as_deref(),
            Some("a363bc91-8278-448e-9d5c-564e4b51eb62")
        );
        assert!(!first.is_foil);
        assert_eq!(first.price_buy, Some(Decimal::new(2, 2)));
        assert_eq!(first.qty_buying, Some(0));

        let second = &products[1];
        assert!(second.is_foil);
        assert_eq!(second.price_buy, Some(Decimal::new(450, 2)));
        assert_eq!(second.qty_buying, Some(16));
    }

    #[tokio::test]
    async fn condition_values_do_not_bleed_into_product() {
        // nm_price 0.39 / nm_qty 2 must not overwrite price_buy / qty_buying.
        let products = run(sample_json()).await;
        assert_eq!(products[0].price_buy, Some(Decimal::new(2, 2)));
        assert_eq!(products[0].qty_buying, Some(0));
    }

    #[tokio::test]
    async fn meta_object_is_ignored() {
        let products = run(r#"{"meta": {"created_at": "x"}, "data": []}"#).await;
        assert!(products.is_empty());
    }

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 10).unwrap()
    }

    fn map() -> HashMap<String, String> {
        HashMap::from([
            (
                "a363bc91-8278-448e-9d5c-564e4b51eb62".to_string(),
                "card-1".to_string(),
            ),
            (
                "e5cfaefb-764c-4c56-bdb3-5f0375168597".to_string(),
                "card-2".to_string(),
            ),
        ])
    }

    #[tokio::test]
    async fn maps_only_real_offers_to_granular_rows() {
        let products = run(sample_json()).await;
        let (rows, unmatched) = granular_from_ck_products(products, &map(), date());

        // Abomination has qty_buying 0 -> not a real offer; only the foil row lands.
        assert_eq!(rows.len(), 1);
        assert_eq!(unmatched, 0);
        let row = &rows[0];
        assert_eq!(row.card_id, "card-2");
        assert_eq!(row.provider, "cardkingdom");
        assert_eq!(row.price_type, "buylist");
        assert_eq!(row.finish, "foil");
        assert_eq!(row.condition, "NM");
        assert_eq!(row.price, Decimal::new(450, 2));
        assert_eq!(row.qty, Some(16));
        assert_eq!(row.date, date());
    }

    #[test]
    fn unknown_scryfall_id_is_tallied_not_emitted() {
        let product = CkProduct {
            scryfall_id: Some("unknown-id".to_string()),
            is_foil: false,
            price_buy: Some(Decimal::ONE),
            qty_buying: Some(4),
        };
        let (rows, unmatched) = granular_from_ck_products(vec![product], &map(), date());
        assert!(rows.is_empty());
        assert_eq!(unmatched, 1);
    }

    #[test]
    fn dedupes_same_series_keeping_best_offer() {
        // Real-feed case: an etched product reports is_foil=true and shares its
        // scryfall_id with the regular foil, so two CK products collapse onto
        // one (card, finish) series. Emitting both in one batch makes Postgres
        // reject the multi-row upsert ("cannot affect row a second time").
        let cheaper = CkProduct {
            scryfall_id: Some("a363bc91-8278-448e-9d5c-564e4b51eb62".to_string()),
            is_foil: true,
            price_buy: Some(Decimal::new(200, 2)),
            qty_buying: Some(8),
        };
        let better = CkProduct {
            scryfall_id: Some("a363bc91-8278-448e-9d5c-564e4b51eb62".to_string()),
            is_foil: true,
            price_buy: Some(Decimal::new(350, 2)),
            qty_buying: Some(2),
        };
        let (rows, unmatched) =
            granular_from_ck_products(vec![cheaper, better], &map(), date());
        assert_eq!(unmatched, 0);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].price, Decimal::new(350, 2));
        assert_eq!(rows[0].qty, Some(2));
    }

    #[test]
    fn zero_price_or_missing_scryfall_id_skipped_silently() {
        let zero_price = CkProduct {
            scryfall_id: Some("a363bc91-8278-448e-9d5c-564e4b51eb62".to_string()),
            is_foil: false,
            price_buy: Some(Decimal::ZERO),
            qty_buying: Some(5),
        };
        let no_scryfall = CkProduct {
            scryfall_id: None,
            is_foil: false,
            price_buy: Some(Decimal::ONE),
            qty_buying: Some(5),
        };
        let (rows, unmatched) =
            granular_from_ck_products(vec![zero_price, no_scryfall], &map(), date());
        assert!(rows.is_empty());
        assert_eq!(unmatched, 0);
    }
}
