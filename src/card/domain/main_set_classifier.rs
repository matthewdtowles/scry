use serde_json::Value;

/// Business rules to determine if a card belongs in the main set.
pub struct MainSetClassifier;

/// Set types where all cards are considered non-main (bonus/supplemental).
const NON_MAIN_SET_TYPES: &[&str] = &["masters"];

impl MainSetClassifier {
    /// Returns the list of set types where all cards are non-main.
    pub fn non_main_set_types() -> &'static [&'static str] {
        NON_MAIN_SET_TYPES
    }

    /// Determine if a card should be classified as part of the main set.
    ///
    /// Returns false if:
    /// - Card has non-canonical promo types
    /// - Card is not in default booster packs
    /// - Card number is non-ASCII (except for set "arn")
    pub fn is_main_set_card(card_data: &Value) -> bool {
        if let Some(promo_types) = card_data.get("promoTypes").and_then(|v| v.as_array()) {
            if !Self::has_canonical_promo_types(promo_types) {
                return false;
            }
        }
        if let Some(booster_types) = card_data.get("boosterTypes").and_then(|v| v.as_array()) {
            if !Self::is_in_default_booster(booster_types) {
                return false;
            }
        } else {
            return false;
        }
        // Check number format (ASCII only, except Arabian Nights)
        let set_code = card_data
            .get("setCode")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        if let Some(number) = card_data.get("number").and_then(|v| v.as_str()) {
            if !number.is_ascii() && set_code != "arn" {
                return false;
            }
        } else {
            return false;
        }
        true
    }

    /// Check if all promo types are considered canonical (part of main set).
    ///
    /// Canonical promo types include official release promos, starter decks,
    /// welcome decks, etc. Non-canonical types like judge promos, buy-a-box,
    /// etc. are excluded from main sets.
    fn has_canonical_promo_types(promo_types: &[Value]) -> bool {
        const CANONICAL_PROMOS: &[&str] = &[
            "beginnerbox",
            "draftweekend",
            "ffi",
            "ffii",
            "ffiii",
            "ffiv",
            "ffix",
            "ffv",
            "ffvi",
            "ffvii",
            "ffviii",
            "ffx",
            "ffxi",
            "ffxii",
            "ffxiii",
            "ffxiv",
            "ffxv",
            "ffxvi",
            "intropack",
            "league",
            "openhouse",
            "playtest",
            "release",
            "startercollection",
            "starterdeck",
            "themepack",
            "universesbeyond",
            "upsidedown",
            "welcome",
        ];
        promo_types.iter().all(|promo| {
            promo
                .as_str()
                .map(|s| CANONICAL_PROMOS.contains(&s))
                .unwrap_or(false)
        })
    }

    /// Check if the card appears in default booster packs.
    fn is_in_default_booster(booster_types: &[Value]) -> bool {
        booster_types.iter().any(|v| v.as_str() == Some("default"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_classify_standard_card() {
        let card = json!({
            "setCode": "BRO",
            "number": "123",
            "boosterTypes": ["default"]
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(result);
    }

    #[test]
    fn test_classify_no_boosters() {
        let card = json!({
            "setCode": "BRO",
            "number": "123"
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(!result);
    }

    #[test]
    fn test_classify_non_default_booster() {
        let card = json!({
            "setCode": "BRO",
            "number": "123",
            "boosterTypes": ["arena"]
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(!result);
    }

    #[test]
    fn test_classify_canonical_promo() {
        let card = json!({
            "setCode": "UNH",
            "number": "1",
            "boosterTypes": ["default"],
            "promoTypes": ["upsidedown"]
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(result);
    }

    #[test]
    fn test_classify_non_canonical_promo() {
        let card = json!({
            "setCode": "BRO",
            "number": "123",
            "boosterTypes": ["default"],
            "promoTypes": ["buyabox"]
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(!result);
    }

    #[test]
    fn test_classify_playtest_promo_canonical() {
        let card = json!({
            "setCode": "UNH",
            "number": "88",
            "boosterTypes": ["default"],
            "promoTypes": ["playtest"]
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(result);
    }

    #[test]
    fn test_classify_non_ascii_number() {
        let card = json!({
            "setCode": "BRO",
            "number": "232†",
            "boosterTypes": ["default"]
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(!result);
    }

    #[test]
    fn test_classify_non_ascii_number_arabian_nights() {
        let card = json!({
            "setCode": "arn",
            "number": "Ⅸ",
            "boosterTypes": ["default"]
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(result);
    }

    #[test]
    fn test_classify_missing_number() {
        let card = json!({
            "setCode": "BRO",
            "boosterTypes": ["default"]
        });
        let result = MainSetClassifier::is_main_set_card(&card);
        assert!(!result);
    }

    #[test]
    fn test_has_canonical_promo_types_all_valid() {
        let promos = vec![json!("release"), json!("starterdeck")];
        let result = MainSetClassifier::has_canonical_promo_types(&promos);
        assert!(result);
    }

    #[test]
    fn test_has_canonical_promo_types_one_invalid() {
        let promos = vec![json!("release"), json!("buyabox")];
        let result = MainSetClassifier::has_canonical_promo_types(&promos);
        assert!(!result);
    }

    #[test]
    fn test_is_in_default_booster_true() {
        let boosters = vec![json!("default"), json!("arena")];
        let result = MainSetClassifier::is_in_default_booster(&boosters);
        assert!(result);
    }

    #[test]
    fn test_is_in_default_booster_false() {
        let boosters = vec![json!("arena"), json!("collector")];
        let result = MainSetClassifier::is_in_default_booster(&boosters);
        assert!(!result);
    }
}
