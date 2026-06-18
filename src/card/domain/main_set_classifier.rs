use serde_json::Value;

/// Business rules to determine if a card belongs in the main set.
pub struct MainSetClassifier;

/// Set types where all cards are considered non-main (bonus/supplemental).
const NON_MAIN_SET_TYPES: &[&str] = &["masters"];

/// Set types that ship cards in default booster packs. Used as the
/// allowlist for the intrinsic-signals fallback when MTGJSON has not
/// populated `boosterTypes` yet (common for newly-released sets).
///
/// Set types NOT in this list (commander, duel_deck, from_the_vault, etc.)
/// have cards that never appear in default boosters - so missing
/// `boosterTypes` correctly means "not in main" and the fallback is skipped.
const BOOSTER_BEARING_SET_TYPES: &[&str] =
    &["expansion", "core", "draft_innovation", "masters", "funny"];

/// Frame effects that indicate a variant or special-treatment printing
/// (extended art, showcase, full art, inverted, etched). Cards with any
/// of these are excluded from the main set numbering.
const SPECIAL_FRAME_EFFECTS: &[&str] =
    &["extendedart", "showcase", "fullart", "inverted", "etched"];

/// Border colors that count as "main set" appearance. Borderless / silver /
/// gold borders mark special treatments and are excluded.
const ALLOWED_BORDER_COLORS: &[&str] = &["black", "white"];

impl MainSetClassifier {
    /// Returns the list of set types where all cards are non-main.
    pub fn non_main_set_types() -> &'static [&'static str] {
        NON_MAIN_SET_TYPES
    }

    /// Determine if a card should be classified as part of the main set.
    ///
    /// When MTGJSON has populated `boosterTypes`, that field is authoritative:
    /// presence of `"default"` means in-main.
    ///
    /// When `boosterTypes` is absent (common on new sets - MTGJSON lags),
    /// the classifier falls back to intrinsic per-card signals (borderColor,
    /// frameEffects, availability), but only for set types that are supposed
    /// to ship cards in boosters in the first place. Precon-only set types
    /// (commander, duel_deck, etc.) stay at in_main=false when boosterTypes
    /// is absent, because their cards genuinely don't go in packs.
    pub fn is_main_set_card(card_data: &Value, set_type: &str) -> bool {
        if let Some(promo_types) = card_data.get("promoTypes").and_then(|v| v.as_array()) {
            if !Self::has_canonical_promo_types(promo_types) {
                return false;
            }
        }
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
        if let Some(booster_types) = card_data.get("boosterTypes").and_then(|v| v.as_array()) {
            return Self::is_in_default_booster(booster_types);
        }
        if !BOOSTER_BEARING_SET_TYPES.contains(&set_type) {
            return false;
        }
        Self::passes_intrinsic_signals(card_data)
    }

    /// Fallback check using per-card fields that MTGJSON populates on
    /// release day (borderColor, frameEffects, availability).
    fn passes_intrinsic_signals(card_data: &Value) -> bool {
        if let Some(border) = card_data.get("borderColor").and_then(|v| v.as_str()) {
            if !ALLOWED_BORDER_COLORS.contains(&border) {
                return false;
            }
        }
        if let Some(frame_effects) = card_data.get("frameEffects").and_then(|v| v.as_array()) {
            let has_special = frame_effects.iter().any(|fe| {
                fe.as_str()
                    .map(|s| SPECIAL_FRAME_EFFECTS.contains(&s))
                    .unwrap_or(false)
            });
            if has_special {
                return false;
            }
        }
        if let Some(availability) = card_data.get("availability").and_then(|v| v.as_array()) {
            let has_paper = availability.iter().any(|v| v.as_str() == Some("paper"));
            if !availability.is_empty() && !has_paper {
                return false;
            }
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
        assert!(MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_no_boosters_expansion_falls_back_to_intrinsic() {
        // The SOS case: boosterTypes missing, but it's an expansion with
        // plain black border and no special frame effects -> in_main.
        let card = json!({
            "setCode": "SOS",
            "number": "123",
            "borderColor": "black"
        });
        assert!(MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_no_boosters_commander_stays_out() {
        // Commander decks never ship boosters - missing boosterTypes
        // should keep the card out of in_main regardless of border.
        let card = json!({
            "setCode": "MOC",
            "number": "1",
            "borderColor": "black"
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "commander"));
    }

    #[test]
    fn test_classify_no_boosters_borderless_excluded() {
        let card = json!({
            "setCode": "SOS",
            "number": "282",
            "borderColor": "borderless"
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_no_boosters_extended_art_excluded() {
        let card = json!({
            "setCode": "SOS",
            "number": "300",
            "borderColor": "black",
            "frameEffects": ["extendedart"]
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_no_boosters_showcase_excluded() {
        let card = json!({
            "setCode": "SOS",
            "number": "350",
            "borderColor": "black",
            "frameEffects": ["showcase"]
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_no_boosters_arena_only_excluded() {
        let card = json!({
            "setCode": "SOS",
            "number": "5",
            "borderColor": "black",
            "availability": ["arena"]
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_no_boosters_legendary_frame_ok() {
        // `legendary` and `enchantment` are normal frame variants, not
        // special treatments - they should NOT exclude a card.
        let card = json!({
            "setCode": "SOS",
            "number": "5",
            "borderColor": "black",
            "frameEffects": ["legendary"]
        });
        assert!(MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_no_boosters_masters_uses_fallback() {
        let card = json!({
            "setCode": "UMA",
            "number": "1",
            "borderColor": "black"
        });
        assert!(MainSetClassifier::is_main_set_card(&card, "masters"));
    }

    #[test]
    fn test_classify_no_boosters_duel_deck_stays_out() {
        let card = json!({
            "setCode": "DDP",
            "number": "1",
            "borderColor": "black"
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "duel_deck"));
    }

    #[test]
    fn test_classify_no_boosters_from_the_vault_stays_out() {
        let card = json!({
            "setCode": "V09",
            "number": "1",
            "borderColor": "black"
        });
        assert!(!MainSetClassifier::is_main_set_card(
            &card,
            "from_the_vault"
        ));
    }

    #[test]
    fn test_classify_non_default_booster() {
        let card = json!({
            "setCode": "BRO",
            "number": "123",
            "boosterTypes": ["arena"]
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_canonical_promo() {
        let card = json!({
            "setCode": "UNH",
            "number": "1",
            "boosterTypes": ["default"],
            "promoTypes": ["upsidedown"]
        });
        assert!(MainSetClassifier::is_main_set_card(&card, "funny"));
    }

    #[test]
    fn test_classify_non_canonical_promo() {
        let card = json!({
            "setCode": "BRO",
            "number": "123",
            "boosterTypes": ["default"],
            "promoTypes": ["buyabox"]
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_playtest_promo_canonical() {
        let card = json!({
            "setCode": "UNH",
            "number": "88",
            "boosterTypes": ["default"],
            "promoTypes": ["playtest"]
        });
        assert!(MainSetClassifier::is_main_set_card(&card, "funny"));
    }

    #[test]
    fn test_classify_non_ascii_number() {
        let card = json!({
            "setCode": "BRO",
            "number": "232†",
            "boosterTypes": ["default"]
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_non_ascii_number_arabian_nights() {
        let card = json!({
            "setCode": "arn",
            "number": "Ⅸ",
            "boosterTypes": ["default"]
        });
        assert!(MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_classify_missing_number() {
        let card = json!({
            "setCode": "BRO",
            "boosterTypes": ["default"]
        });
        assert!(!MainSetClassifier::is_main_set_card(&card, "expansion"));
    }

    #[test]
    fn test_has_canonical_promo_types_all_valid() {
        let promos = vec![json!("release"), json!("starterdeck")];
        assert!(MainSetClassifier::has_canonical_promo_types(&promos));
    }

    #[test]
    fn test_has_canonical_promo_types_one_invalid() {
        let promos = vec![json!("release"), json!("buyabox")];
        assert!(!MainSetClassifier::has_canonical_promo_types(&promos));
    }

    #[test]
    fn test_is_in_default_booster_true() {
        let boosters = vec![json!("default"), json!("arena")];
        assert!(MainSetClassifier::is_in_default_booster(&boosters));
    }

    #[test]
    fn test_is_in_default_booster_false() {
        let boosters = vec![json!("arena"), json!("collector")];
        assert!(!MainSetClassifier::is_in_default_booster(&boosters));
    }
}
