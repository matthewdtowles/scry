use chrono::NaiveDate;

/// A single card line within a published decklist (name + count, one board).
#[derive(Debug, Clone)]
pub struct DeckLine {
    pub card_name: String,
    pub count: i32,
    pub is_sideboard: bool,
}

/// A published tournament deck as produced by a source adapter, before card
/// names are resolved to ids. `source_uri` is the dedup key within a source.
#[derive(Debug, Clone)]
pub struct RawDeck {
    pub source: String,
    pub source_uri: String,
    pub tournament_name: Option<String>,
    pub tournament_date: Option<NaiveDate>,
    pub format: Option<String>,
    pub player: Option<String>,
    pub result: Option<String>,
    pub lines: Vec<DeckLine>,
}

/// A resolved card entry ready to persist (name resolved to a representative
/// printing's id). Quantities are pre-aggregated per (card_id, board).
#[derive(Debug, Clone)]
pub struct ResolvedCard {
    pub card_id: String,
    pub quantity: i32,
    pub is_sideboard: bool,
}
