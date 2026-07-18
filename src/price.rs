mod cardkingdom;
pub mod domain;
mod event_processor;
pub mod repository;
pub mod service;
mod write_timings;

pub use service::PriceService;

/// Providers whose retail prices feed the derived averaged `price` value (all
/// USD). Cardsphere was dropped (ROADMAP 6.9): it is absent from the MTGJSON
/// feed entirely, so it never contributed a retail value - the average is
/// byte-identical with or without it.
pub(crate) const AVERAGE_PROVIDERS: &[&str] = &["tcgplayer", "cardkingdom"];
