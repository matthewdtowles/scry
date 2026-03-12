use crate::card::domain::{
    Card, CardRarity, Format, Legality, LegalityStatus, MainSetClassifier,
};
use crate::utils::json;
use anyhow::Result;
use serde_json::Value;

pub struct CardMapper;

impl CardMapper {
    pub fn map_to_cards(set_data: Value) -> Result<Vec<Card>> {
        let cards_array = set_data
            .get("data")
            .and_then(|d| d.get("cards"))
            .and_then(|c| c.as_array())
            .ok_or_else(|| anyhow::anyhow!("Invalid MTG JSON set structure"))?;

        cards_array
            .iter()
            .filter(|c| {
                !c.get("isOnlineOnly")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            })
            .map(|card_data| Self::map_json_to_card(card_data))
            .collect()
    }

    pub fn map_json_to_card(card_data: &Value) -> Result<Card> {
        let id = json::extract_string(card_data, "uuid")?;
        let raw_name = json::extract_string(card_data, "name")?;
        let raw_face_name = json::extract_optional_string(card_data, "faceName");
        let set_code = json::extract_string(card_data, "setCode")?.to_lowercase();
        let number_str = json::extract_string(card_data, "number")?;
        let type_line = json::extract_string(card_data, "type")?;
        let rarity_str = json::extract_string(card_data, "rarity")?;
        let rarity = rarity_str
            .parse::<CardRarity>()
            .unwrap_or(CardRarity::Common);

        let raw_mana_cost = json::extract_optional_string(card_data, "manaCost");
        let mana_cost = Card::normalize_mana_cost(raw_mana_cost);

        let oracle_text = json::extract_optional_string(card_data, "text");
        let artist = json::extract_optional_string(card_data, "artist");
        let has_foil = Self::has_foil(card_data);
        let has_non_foil = Self::has_non_foil(card_data) || !has_foil;
        let is_alternative = card_data
            .get("isAlternative")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let is_reserved = card_data
            .get("isReserved")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let scryfall_id = card_data
            .get("identifiers")
            .and_then(|i| i.get("scryfallId"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing scryfallId"))?;
        let img_src = Card::build_scryfall_image_path(scryfall_id)?;

        let in_main = MainSetClassifier::is_main_set_card(card_data);
        let layout = card_data
            .get("layout")
            .and_then(|v| v.as_str())
            .unwrap_or("normal")
            .to_string();

        let name = if layout == "aftermath" || layout == "split" {
            raw_name
        } else {
            raw_face_name.unwrap_or(raw_name)
        };

        let legalities = card_data
            .get("legalities")
            .map(|l| Self::extract_legalities(l, &id))
            .transpose()?
            .unwrap_or_default();

        let side = card_data
            .get("side")
            .and_then(|v| v.as_str())
            .map(String::from);
        let other_face_ids = card_data
            .get("otherFaceIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            });

        let is_online_only = card_data
            .get("isOnlineOnly")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let is_oversized = card_data
            .get("isOversized")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let language = card_data
            .get("language")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let sort_number = Card::compute_sort_number(&number_str, in_main);

        Ok(Card {
            artist,
            has_foil,
            has_non_foil,
            id,
            img_src,
            in_main,
            is_alternative,
            is_online_only,
            is_oversized,
            is_reserved,
            language,
            layout,
            legalities,
            mana_cost,
            name,
            number: number_str,
            oracle_text,
            other_face_ids,
            rarity,
            set_code,
            side,
            sort_number,
            type_line,
        })
    }

    fn get_finishes(card_data: &Value) -> Option<Vec<&str>> {
        card_data
            .get("finishes")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|f| f.as_str()).collect())
    }

    fn has_foil(card_data: &Value) -> bool {
        Self::get_finishes(card_data)
            .map(|f| f.contains(&"foil") || f.contains(&"etched"))
            .unwrap_or_else(|| {
                card_data
                    .get("hasFoil")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            })
    }

    fn has_non_foil(card_data: &Value) -> bool {
        Self::get_finishes(card_data)
            .map(|f| f.contains(&"nonfoil"))
            .unwrap_or_else(|| {
                card_data
                    .get("hasNonFoil")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            })
    }

    fn extract_legalities(legalities_dto: &Value, card_id: &str) -> Result<Vec<Legality>> {
        let Some(obj) = legalities_dto.as_object() else {
            return Ok(Vec::new());
        };

        Ok(obj
            .iter()
            .filter_map(|(format_str, status_str)| {
                let format = format_str.parse::<Format>().ok()?;
                let status = status_str.as_str()?.parse::<LegalityStatus>().ok()?;
                Legality::new_if_relevant(card_id.to_string(), format, status)
            })
            .collect())
    }
}
