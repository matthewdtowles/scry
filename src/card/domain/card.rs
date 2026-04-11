use super::{CardRarity, Legality};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow, Serialize, Deserialize)]
pub struct Card {
    pub artist: Option<String>,
    pub flavor_name: Option<String>,
    pub has_foil: bool,
    pub has_non_foil: bool,
    pub id: String,
    pub img_src: String,
    pub in_main: bool,
    pub is_alternative: bool,
    pub is_reserved: bool,
    pub layout: String,
    pub mana_cost: Option<String>,
    pub name: String,
    pub number: String,
    pub oracle_text: Option<String>,
    pub purchase_url_tcgplayer: Option<String>,
    pub purchase_url_tcgplayer_etched: Option<String>,
    pub rarity: CardRarity,
    pub set_code: String,
    pub sort_number: String,

    #[sqlx(skip)]
    pub is_online_only: bool,

    #[sqlx(skip)]
    pub is_oversized: bool,

    #[sqlx(skip)]
    pub language: String,

    #[sqlx(skip)]
    pub legalities: Vec<Legality>,

    #[sqlx(skip)]
    pub other_face_ids: Option<Vec<String>>,

    #[sqlx(skip)]
    pub side: Option<String>,

    #[sqlx(rename = "type")]
    pub type_line: String,
}

impl Card {
    /// Should this card be filtered out of inclusion in DB?
    pub fn should_filter(&self) -> bool {
        if self.is_online_only || self.is_oversized {
            return true;
        }
        if let Some(side) = self.side.as_deref() {
            return side != "a";
        }
        false
    }

    /// Mark this card as not belonging to the main (base) set.
    ///
    /// Sets `in_main` to false and recomputes `sort_number` accordingly
    /// (non-main cards get a `~` prefix for sorting after main cards).
    pub fn mark_as_non_main(&mut self) {
        self.in_main = false;
        self.sort_number = Self::compute_sort_number(&self.number, self.in_main);
    }

    /// Is this a split or aftermath card requiring mana cost merging?
    pub fn is_split_card(&self) -> bool {
        matches!(self.layout.as_str(), "split" | "aftermath")
    }

    /// Is this a foreign (non-English) printing?
    pub fn is_foreign(&self) -> bool {
        !self.language.is_empty() && self.language != "English"
    }

    /// Enable foil availability from another card if not already set
    ///
    /// Used when consolidating duplicate foil entries.
    pub fn enable_foil_from(&mut self, source: &Card) -> bool {
        if source.has_foil && !self.has_foil {
            self.has_foil = true;
            true
        } else {
            false
        }
    }

    /// Merge another card's mana cost into this one (for split cards)
    ///
    /// Used when consolidating split card faces like "Fire // Ice".
    /// Returns new merged cost without mutating the card.
    pub fn merge_mana_costs(&self, other_cost: Option<&str>) -> Option<String> {
        match (self.mana_cost.as_deref(), other_cost) {
            (Some(base), Some(other)) => Some(format!("{} // {}", base, other)),
            (Some(base), None) => Some(base.to_string()),
            (None, Some(other)) => Some(other.to_string()),
            (None, None) => None,
        }
    }

    /// Normalize mana cost from MTG JSON format
    ///
    /// Converts to lowercase and removes hybrid mana slashes.
    /// - `{2/W}` becomes `{2w}`
    /// - `{W/G/P}` becomes `{wgp}`
    /// - `{U} // {R}` preserved (split card separator)
    pub fn normalize_mana_cost(raw: Option<String>) -> Option<String> {
        raw.map(|cost| {
            let mut result = String::new();
            let mut chars = cost.chars().peekable();
            let mut in_brace = false;

            while let Some(ch) = chars.next() {
                match ch {
                    '{' => {
                        in_brace = true;
                        result.push(ch);
                    }
                    '}' => {
                        in_brace = false;
                        result.push(ch);
                    }
                    '/' if in_brace => continue, // Skip slashes inside braces
                    '/' if chars.peek() == Some(&'/') => {
                        result.push_str("//");
                        chars.next();
                    }
                    _ => result.push(ch.to_ascii_lowercase()),
                }
            }
            result
        })
    }

    /// Compute sortable card number
    ///
    /// Pads numbers for proper string sorting:
    /// - "1" → "000001"
    /// - "2-3" → "2-0003"
    /// - "232†" → "~000232†" (non-ASCII gets ~ prefix)
    /// - Non-main set cards get additional ~ prefix
    pub fn compute_sort_number(number: &str, in_main: bool) -> String {
        let s = number.trim();
        let prefix = Self::sort_prefix(number, in_main);

        // Handle hyphenated numbers (e.g., "2-3")
        if let Some(idx) = s.find('-') {
            let (left, right) = s.split_at(idx);
            let right = &right[1..];
            let digits_end = right
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(right.len());
            let (right_digits, right_rest) = right.split_at(digits_end);
            let padded_right = if right_digits.is_empty() {
                right.to_string()
            } else {
                format!("{:0>4}{}", right_digits, right_rest)
            };
            return format!("{}{}-{}", prefix, left, padded_right);
        }

        // Handle pure numeric numbers
        if s.starts_with(|c: char| c.is_ascii_digit()) {
            let digits_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
            let (digits, rest) = s.split_at(digits_end);
            return format!("{}{:0>6}{}", prefix, digits, rest);
        }

        // Handle mixed alphanumeric
        if let Some(idx) = s.find(|c: char| c.is_ascii_digit()) {
            let (letters, digits_and_rest) = s.split_at(idx);
            let digits_end = digits_and_rest
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(digits_and_rest.len());
            let (digits, rest) = digits_and_rest.split_at(digits_end);
            return format!("{}{}{:0>4}{}", prefix, letters, digits, rest);
        }

        format!("{}{}", prefix, s)
    }

    /// Build Scryfall image path from Scryfall UUID
    ///
    /// Encodes Scryfall's directory structure: `{first}/{second}/{uuid}.jpg`
    pub fn build_scryfall_image_path(scryfall_id: &str) -> Result<String> {
        if scryfall_id.len() < 2 {
            return Err(anyhow::anyhow!("ScryfallId too short"));
        }
        let first = scryfall_id.chars().next().unwrap();
        let second = scryfall_id.chars().nth(1).unwrap();
        Ok(format!("{}/{}/{}.jpg", first, second, scryfall_id))
    }

    fn sort_prefix(number: &str, in_main: bool) -> String {
        let mut prefix = String::new();
        if !in_main {
            prefix.push('~');
        }
        if !number.is_ascii() {
            prefix.push('~');
        }
        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_card() -> Card {
        Card {
            artist: Some("Artist Name".to_string()),
            flavor_name: None,
            has_foil: true,
            has_non_foil: true,
            id: "test-id".to_string(),
            img_src: "a/b/test.jpg".to_string(),
            in_main: true,
            is_alternative: false,
            is_reserved: false,
            is_online_only: false,
            is_oversized: false,
            language: "English".to_string(),
            layout: "normal".to_string(),
            legalities: vec![],
            mana_cost: Some("{2}{U}".to_string()),
            name: "Test Card".to_string(),
            number: "123".to_string(),
            oracle_text: Some("Test text".to_string()),
            other_face_ids: None,
            purchase_url_tcgplayer: None,
            purchase_url_tcgplayer_etched: None,
            rarity: CardRarity::Rare,
            set_code: "tst".to_string(),
            side: None,
            sort_number: "000123".to_string(),
            type_line: "Creature — Test".to_string(),
        }
    }

    #[test]
    fn test_should_filter() {
        let mut card = create_test_card();
        assert!(!card.should_filter());

        card.is_online_only = true;
        assert!(card.should_filter());
    }

    #[test]
    fn test_is_foreign() {
        let mut card = create_test_card();
        assert!(!card.is_foreign());

        card.language = "Japanese".to_string();
        assert!(card.is_foreign());
    }

    #[test]
    fn test_enable_foil_from() {
        let mut target = create_test_card();
        target.has_foil = false;

        let source = create_test_card();
        assert!(source.has_foil);

        let changed = target.enable_foil_from(&source);
        assert!(changed); // Should return true when changed
        assert!(target.has_foil);

        // Test idempotency - calling again doesn't break and returns false
        let changed = target.enable_foil_from(&source);
        assert!(!changed); // Should return false when no change
        assert!(target.has_foil);
    }

    #[test]
    fn test_enable_foil_from_no_change() {
        let mut target = create_test_card();
        target.has_foil = true;

        let source = create_test_card();

        let changed = target.enable_foil_from(&source);
        assert!(!changed); // Should return false - was already true
        assert!(target.has_foil);
    }

    #[test]
    fn test_enable_foil_from_source_no_foil() {
        let mut target = create_test_card();
        target.has_foil = false;

        let mut source = create_test_card();
        source.has_foil = false;

        let changed = target.enable_foil_from(&source);
        assert!(!changed); // Should return false - source has no foil
        assert!(!target.has_foil);
    }

    #[test]
    fn test_merge_mana_costs() {
        let card = create_test_card();
        let result = card.merge_mana_costs(Some("{R}{G}"));
        assert_eq!(result, Some("{2}{U} // {R}{G}".to_string()));
    }

    #[test]
    fn test_normalize_mana_cost() {
        let result = Card::normalize_mana_cost(Some("{2/W}{W/G/P} // {U/R}".to_string()));
        assert_eq!(result, Some("{2w}{wgp} // {ur}".to_string()));
    }

    #[test]
    fn test_compute_sort_number() {
        assert_eq!(Card::compute_sort_number("123", true), "000123");
        assert_eq!(Card::compute_sort_number("232†", true), "~000232†");
        assert_eq!(Card::compute_sort_number("2-3", true), "2-0003");
        assert_eq!(Card::compute_sort_number("232†", false), "~~000232†");
    }

    #[test]
    fn test_build_scryfall_image_path_valid() {
        let result = Card::build_scryfall_image_path("abcdef12-3456-7890-abcd-ef1234567890");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "a/b/abcdef12-3456-7890-abcd-ef1234567890.jpg"
        );
    }

    #[test]
    fn test_build_scryfall_image_path_two_chars() {
        let result = Card::build_scryfall_image_path("ab");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "a/b/ab.jpg");
    }

    #[test]
    fn test_build_scryfall_image_path_one_char_fails() {
        let result = Card::build_scryfall_image_path("a");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "ScryfallId too short");
    }
}
