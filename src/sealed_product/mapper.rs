use crate::{sealed_product::domain::SealedProduct, utils::json};
use anyhow::Result;
use serde_json::Value;

pub struct SealedProductMapper;

impl SealedProductMapper {
    pub fn map_mtg_json_to_sealed_products(
        set_data: &Value,
        set_code: &str,
    ) -> Result<Vec<SealedProduct>> {
        let sealed_arr = match set_data
            .get("data")
            .and_then(|d| d.get("sealedProduct"))
            .and_then(|sp| sp.as_array())
        {
            Some(arr) => arr,
            None => return Ok(Vec::new()),
        };

        let mut products = Vec::new();
        for item in sealed_arr {
            match Self::map_single(item, set_code) {
                Ok(product) => {
                    if !product.is_online_only() {
                        products.push(product);
                    }
                }
                Err(_) => continue,
            }
        }
        Ok(products)
    }

    fn map_single(item: &Value, set_code: &str) -> Result<SealedProduct> {
        let uuid = json::extract_string(item, "uuid")?;
        let name = json::extract_string(item, "name")?;
        let category = json::extract_optional_string(item, "category");
        let subtype = json::extract_optional_string(item, "subtype");
        let card_count = item.get("cardCount").and_then(|v| v.as_i64()).map(|v| v as i32);
        let product_size = item.get("productSize").and_then(|v| v.as_i64()).map(|v| v as i32);
        let release_date = json::extract_optional_date(item, "releaseDate");

        let purchase_url_tcgplayer = item
            .get("purchaseUrls")
            .and_then(|urls| urls.get("tcgplayer"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let contents_summary = item
            .get("contents")
            .map(|c| Self::flatten_contents(c));

        Ok(SealedProduct {
            uuid,
            name,
            set_code: set_code.to_lowercase(),
            category,
            subtype,
            card_count,
            product_size,
            release_date,
            contents_summary,
            purchase_url_tcgplayer,
        })
    }

    /// Flatten MTGJSON contents object into a human-readable display string.
    ///
    /// Example outputs:
    /// - "36x Draft Booster Pack"
    /// - "9x Play Booster Pack, Thundertrap Trainer (Foil), Bloomburrow Spindown"
    fn flatten_contents(contents: &Value) -> String {
        let mut parts: Vec<String> = Vec::new();

        // sealed - inner sealed products
        if let Some(sealed) = contents.get("sealed").and_then(|v| v.as_array()) {
            for item in sealed {
                let count = item.get("count").and_then(|v| v.as_i64()).unwrap_or(1);
                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown");
                if count > 1 {
                    parts.push(format!("{}x {}", count, name));
                } else {
                    parts.push(name.to_string());
                }
            }
        }

        // pack - booster pack configs
        if let Some(packs) = contents.get("pack").and_then(|v| v.as_array()) {
            for item in packs {
                let count = item.get("count").and_then(|v| v.as_i64()).unwrap_or(1);
                let code = item.get("code").and_then(|v| v.as_str()).unwrap_or("pack");
                let set = item.get("set").and_then(|v| v.as_str()).unwrap_or("");
                let name = if !set.is_empty() {
                    format!("{} {}", set.to_uppercase(), code)
                } else {
                    code.to_string()
                };
                if count > 1 {
                    parts.push(format!("{}x {}", count, name));
                } else {
                    parts.push(name);
                }
            }
        }

        // card - specific promo/bonus cards
        if let Some(cards) = contents.get("card").and_then(|v| v.as_array()) {
            for item in cards {
                let count = item.get("count").and_then(|v| v.as_i64()).unwrap_or(1);
                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown Card");
                let foil = item.get("foil").and_then(|v| v.as_bool()).unwrap_or(false);
                let display = if foil {
                    format!("{} (Foil)", name)
                } else {
                    name.to_string()
                };
                if count > 1 {
                    parts.push(format!("{}x {}", count, display));
                } else {
                    parts.push(display);
                }
            }
        }

        // deck - pre-constructed decks
        if let Some(decks) = contents.get("deck").and_then(|v| v.as_array()) {
            for item in decks {
                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown Deck");
                parts.push(name.to_string());
            }
        }

        // other - non-card items
        if let Some(others) = contents.get("other").and_then(|v| v.as_array()) {
            for item in others {
                let count = item.get("count").and_then(|v| v.as_i64()).unwrap_or(1);
                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown");
                if count > 1 {
                    parts.push(format!("{}x {}", count, name));
                } else {
                    parts.push(name.to_string());
                }
            }
        }

        parts.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_map_single_product() {
        let set_data = json!({
            "data": {
                "sealedProduct": [{
                    "uuid": "abc-123",
                    "name": "Draft Booster Box",
                    "category": "booster_box",
                    "subtype": "draft",
                    "cardCount": 540,
                    "productSize": 36,
                    "releaseDate": "2024-08-02",
                    "purchaseUrls": { "tcgplayer": "https://tcg.com/123" },
                    "identifiers": {},
                    "contents": {
                        "sealed": [{ "count": 36, "name": "Draft Booster Pack", "set": "BLB", "uuid": "def-456" }]
                    }
                }]
            }
        });

        let products = SealedProductMapper::map_mtg_json_to_sealed_products(&set_data, "BLB").unwrap();
        assert_eq!(products.len(), 1);
        let p = &products[0];
        assert_eq!(p.uuid, "abc-123");
        assert_eq!(p.name, "Draft Booster Box");
        assert_eq!(p.set_code, "blb");
        assert_eq!(p.category.as_deref(), Some("booster_box"));
        assert_eq!(p.subtype.as_deref(), Some("draft"));
        assert_eq!(p.card_count, Some(540));
        assert_eq!(p.product_size, Some(36));
        assert_eq!(p.purchase_url_tcgplayer.as_deref(), Some("https://tcg.com/123"));
        assert_eq!(p.contents_summary.as_deref(), Some("36x Draft Booster Pack"));
    }

    #[test]
    fn test_filters_online_only() {
        let set_data = json!({
            "data": {
                "sealedProduct": [
                    { "uuid": "a", "name": "Draft Booster Box", "identifiers": {}, "purchaseUrls": {} },
                    { "uuid": "b", "name": "Bloomburrow MTGO Redemption", "identifiers": {}, "purchaseUrls": {} },
                    { "uuid": "c", "name": "Arena Starter Kit", "identifiers": {}, "purchaseUrls": {} }
                ]
            }
        });
        let products = SealedProductMapper::map_mtg_json_to_sealed_products(&set_data, "BLB").unwrap();
        assert_eq!(products.len(), 1);
        assert_eq!(products[0].name, "Draft Booster Box");
    }

    #[test]
    fn test_flatten_contents_mixed() {
        let contents = json!({
            "sealed": [{ "count": 9, "name": "Play Booster Pack" }],
            "card": [{ "name": "Thundertrap Trainer", "foil": true }],
            "other": [
                { "name": "Bloomburrow Spindown" },
                { "count": 2, "name": "Reference cards" }
            ]
        });
        let result = SealedProductMapper::flatten_contents(&contents);
        assert_eq!(result, "9x Play Booster Pack, Thundertrap Trainer (Foil), Bloomburrow Spindown, 2x Reference cards");
    }

    #[test]
    fn test_flatten_contents_deck() {
        let contents = json!({
            "deck": [{ "name": "Eldrazi Unbound" }],
            "sealed": [{ "count": 1, "name": "Commander Masters Collector Booster Sample Pack" }]
        });
        let result = SealedProductMapper::flatten_contents(&contents);
        assert_eq!(result, "Commander Masters Collector Booster Sample Pack, Eldrazi Unbound");
    }

    #[test]
    fn test_no_sealed_product_field() {
        let set_data = json!({ "data": {} });
        let products = SealedProductMapper::map_mtg_json_to_sealed_products(&set_data, "BLB").unwrap();
        assert!(products.is_empty());
    }
}
