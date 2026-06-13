pub mod domain;
pub mod repository;
pub mod service;
mod cardkingdom;
mod event_processor;
mod historical_event_processor;

pub use service::PriceService;

/// Providers whose retail prices feed the derived averaged `price` value (all
/// USD). Cardsphere was dropped (ROADMAP 6.9): it is absent from the MTGJSON
/// feed entirely, so it never contributed a retail value - the average is
/// byte-identical with or without it.
pub(crate) const AVERAGE_PROVIDERS: &[&str] = &["tcgplayer", "cardkingdom"];

/// Providers captured as granular rows (retail + buylist), all USD: the ones
/// with usable data (tcgplayer, cardkingdom) plus Mana Pool. Cardmarket is
/// excluded as EUR: `granular_price` has no currency column yet, so its prices
/// can't be stored unambiguously - revisit when currency is modeled.
pub(crate) const GRANULAR_PROVIDERS: &[&str] = &["tcgplayer", "cardkingdom", "manapool"];