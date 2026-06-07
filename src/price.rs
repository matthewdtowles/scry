pub mod domain;
pub mod repository;
pub mod service;
mod event_processor;
mod historical_event_processor;

pub use service::PriceService;

/// Providers whose retail prices feed the derived averaged `price` value. Kept
/// to the original three (all USD) so the `price`/`price_history` tables stay
/// byte-identical to the pre-granular behavior.
pub(crate) const AVERAGE_PROVIDERS: &[&str] = &["tcgplayer", "cardkingdom", "cardsphere"];

/// Providers captured as granular rows (retail + buylist). Superset of
/// `AVERAGE_PROVIDERS` plus Mana Pool (USD). Cardmarket is intentionally
/// excluded: it is EUR and `granular_price` has no currency column yet, so its
/// prices can't be stored unambiguously — revisit when currency is modeled.
pub(crate) const GRANULAR_PROVIDERS: &[&str] =
    &["tcgplayer", "cardkingdom", "cardsphere", "manapool"];