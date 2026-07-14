use crate::published_deck::domain::{DeckLine, RawDeck};
use crate::utils::clock;
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration as StdDuration;
use tracing::{debug, warn};

/// Port for a published-decklist feed. Adapters fetch recent tournament decks
/// from an external source and normalize them to [`RawDeck`].
#[async_trait]
pub trait DecklistSource: Send + Sync {
    fn source_name(&self) -> &'static str;

    /// Decks published within the last `days` days (best-effort; a flaky feed
    /// should log and return what it has rather than failing the whole run).
    async fn fetch_recent(&self, days: i64) -> Result<Vec<RawDeck>>;
}

// --- fbettega/MTG_decklistcache adapter --------------------------------------

const REPO: &str = "fbettega/MTG_decklistcache";
// Source folders inside Tournaments/. MTGO + Melee cover the competitive meta;
// the others are additive.
const FEEDS: &[&str] = &["MTGO", "MTGmelee", "Topdeck", "Manatrader", "CardsRealm"];

/// Reads JSON tournament caches from the fbettega GitHub repo. Files are listed
/// via the GitHub contents API (one call per source/day) and fetched from the
/// raw CDN (no API rate limit). Set `GITHUB_TOKEN` to raise the API limit.
pub struct FbettegaSource {
    client: Client,
    token: Option<String>,
}

#[derive(Deserialize)]
struct GhContent {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    download_url: Option<String>,
}

#[derive(Deserialize)]
struct CacheItem {
    #[serde(rename = "Tournament")]
    tournament: CacheTournament,
    #[serde(rename = "Decks", default)]
    decks: Vec<CacheDeck>,
}

#[derive(Deserialize)]
struct CacheTournament {
    #[serde(rename = "Name")]
    name: Option<String>,
    #[serde(rename = "Date")]
    date: Option<String>,
    #[serde(rename = "Formats")]
    formats: Option<String>,
}

#[derive(Deserialize)]
struct CacheDeck {
    #[serde(rename = "Player")]
    player: Option<String>,
    #[serde(rename = "Result")]
    result: Option<String>,
    #[serde(rename = "AnchorUri")]
    anchor_uri: Option<String>,
    #[serde(rename = "Mainboard", default)]
    mainboard: Vec<CacheCard>,
    #[serde(rename = "Sideboard", default)]
    sideboard: Vec<CacheCard>,
}

#[derive(Deserialize)]
struct CacheCard {
    #[serde(rename = "CardName")]
    card_name: String,
    #[serde(rename = "Count")]
    count: i32,
}

impl Default for FbettegaSource {
    fn default() -> Self {
        Self::new()
    }
}

impl FbettegaSource {
    pub fn new() -> Self {
        let client = Client::builder()
            .user_agent("scry-iwantmymtg")
            .connect_timeout(StdDuration::from_secs(30))
            .read_timeout(StdDuration::from_secs(60))
            .build()
            .expect("failed to build fbettega HTTP client");
        Self {
            client,
            token: std::env::var("GITHUB_TOKEN").ok().filter(|t| !t.is_empty()),
        }
    }

    /// List a directory via the GitHub contents API. Returns an empty list for
    /// a missing directory (a day with no tournaments for that source).
    async fn list_dir(&self, path: &str) -> Result<Vec<GhContent>> {
        let url = format!("https://api.github.com/repos/{REPO}/contents/{path}");
        let mut req = self.client.get(&url);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.with_context(|| format!("GET {url}"))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        let resp = resp
            .error_for_status()
            .with_context(|| format!("listing {path}"))?;
        match resp.json::<Vec<GhContent>>().await {
            Ok(items) => Ok(items),
            // A shape change / unexpected payload shouldn't fail the whole run,
            // but log it so silent empties are debuggable.
            Err(e) => {
                warn!("fbettega: failed to parse listing for {path}: {e}");
                Ok(Vec::new())
            }
        }
    }

    async fn fetch_file(&self, url: &str) -> Result<CacheItem> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()?;
        Ok(resp.json::<CacheItem>().await?)
    }

    fn to_raw_decks(item: CacheItem) -> Vec<RawDeck> {
        let tname = item.tournament.name.clone();
        let tdate = item.tournament.date.as_deref().and_then(|d| {
            // Take the first 10 chars (YYYY-MM-DD) without byte-slicing, which
            // would panic if a malformed value had a multi-byte char at byte 10.
            let prefix: String = d.chars().take(10).collect();
            NaiveDate::parse_from_str(&prefix, "%Y-%m-%d").ok()
        });
        let format = item
            .tournament
            .formats
            .as_deref()
            .map(|f| f.trim().to_lowercase())
            .filter(|f| !f.is_empty());

        item.decks
            .into_iter()
            .filter_map(|deck| {
                let source_uri = deck.anchor_uri?;
                let mut lines: Vec<DeckLine> = Vec::new();
                for c in deck.mainboard {
                    lines.push(DeckLine {
                        card_name: c.card_name,
                        count: c.count,
                        is_sideboard: false,
                    });
                }
                for c in deck.sideboard {
                    lines.push(DeckLine {
                        card_name: c.card_name,
                        count: c.count,
                        is_sideboard: true,
                    });
                }
                if lines.is_empty() {
                    return None;
                }
                Some(RawDeck {
                    source: "fbettega".to_string(),
                    source_uri,
                    tournament_name: tname.clone(),
                    tournament_date: tdate,
                    format: format.clone(),
                    player: deck.player,
                    result: deck.result,
                    lines,
                })
            })
            .collect()
    }
}

#[async_trait]
impl DecklistSource for FbettegaSource {
    fn source_name(&self) -> &'static str {
        "fbettega"
    }

    async fn fetch_recent(&self, days: i64) -> Result<Vec<RawDeck>> {
        let today = clock::today();
        let mut decks: Vec<RawDeck> = Vec::new();

        for offset in 0..days.max(1) {
            let day = today - Duration::days(offset);
            for feed in FEEDS {
                let path = format!(
                    "Tournaments/{feed}/{:04}/{:02}/{:02}",
                    day.year(),
                    day.month(),
                    day.day(),
                );
                let files = match self.list_dir(&path).await {
                    Ok(f) => f,
                    Err(e) => {
                        warn!("fbettega: failed to list {path}: {e}");
                        continue;
                    }
                };
                for file in files {
                    if file.kind != "file" || !file.name.ends_with(".json") {
                        continue;
                    }
                    let Some(url) = file.download_url else {
                        continue;
                    };
                    match self.fetch_file(&url).await {
                        Ok(item) => decks.extend(Self::to_raw_decks(item)),
                        Err(e) => warn!("fbettega: failed to parse {}: {e}", file.name),
                    }
                }
            }
        }

        debug!("fbettega: fetched {} decks over {days} days", decks.len());
        Ok(decks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
        "Tournament": { "Name": "Modern Challenge 32", "Date": "2026-06-14", "Uri": "https://x", "Formats": "Modern" },
        "Decks": [
            {
                "Player": "reidq7",
                "Result": "1st Place",
                "AnchorUri": "https://x#deck_reidq7",
                "Mainboard": [ { "Count": 4, "CardName": "Forest" }, { "Count": 3, "CardName": "Green Sun's Zenith" } ],
                "Sideboard": [ { "Count": 2, "CardName": "Boseiju, Who Endures" } ]
            },
            { "Player": "nobody", "Mainboard": [], "Sideboard": [] }
        ]
    }"#;

    #[test]
    fn parses_a_cache_item_into_raw_decks() {
        let item: CacheItem = serde_json::from_str(SAMPLE).unwrap();
        let decks = FbettegaSource::to_raw_decks(item);

        // The empty deck (no anchor + no cards) is dropped.
        assert_eq!(decks.len(), 1);
        let deck = &decks[0];
        assert_eq!(deck.source, "fbettega");
        assert_eq!(deck.source_uri, "https://x#deck_reidq7");
        assert_eq!(deck.format.as_deref(), Some("modern"));
        assert_eq!(deck.tournament_date, NaiveDate::from_ymd_opt(2026, 6, 14));
        assert_eq!(deck.lines.len(), 3);
        assert!(deck
            .lines
            .iter()
            .any(|l| l.card_name == "Boseiju, Who Endures" && l.is_sideboard));
    }
}
