use anyhow::Result;
use bytes::Bytes;
use chrono::NaiveDate;
use futures::Stream;
use reqwest::Client;
use serde::de::DeserializeOwned;
use std::time::Duration;
use tracing::{debug, info};

#[derive(Clone)]
pub struct HttpClient {
    client: Client,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    const BASE_INGESTION_URL: &str = "https://mtgjson.com/api/v5/";
    const ALL_CARDS_URL: &str = "AllPrintings.json";
    const SET_LIST_URL: &str = "SetList.json";
    const TODAY_PRICES_URL: &str = "AllPricesToday.json";
    const ALL_PRICES_URL: &str = "AllPrices.json";
    const CK_PRICELIST_URL: &str = "https://api.cardkingdom.com/api/v2/pricelist";

    pub fn new() -> Self {
        // A bare `Client::new()` has no timeouts, so a stalled CDN connection
        // (or a body stream that goes silent mid-download) hangs forever with no
        // log. `connect_timeout` caps the handshake; `read_timeout` is a
        // per-read inactivity timeout that fails a stalled stream instead of a
        // total deadline, so a legitimately long download is not cut off.
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(60))
            .build()
            .expect("failed to build HTTP client");
        Self { client }
    }

    pub async fn all_cards_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::ALL_CARDS_URL);
        info!("Stream all cards from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn all_today_prices_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::TODAY_PRICES_URL);
        info!("Stream all prices from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn all_prices_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::ALL_PRICES_URL);
        info!("Stream all historical prices from: {}", url);
        self.fetch_json_bytes_stream(url.as_str()).await
    }

    pub async fn cardkingdom_pricelist_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        info!(
            "Stream Card Kingdom pricelist from: {}",
            Self::CK_PRICELIST_URL
        );
        self.fetch_json_bytes_stream(Self::CK_PRICELIST_URL).await
    }

    /// The build date carried by `AllPricesToday.json` itself, read without
    /// downloading it.
    ///
    /// `meta` is the first key in the file, so a 200-byte Range request answers
    /// "is there new price data?" for ~0.0004% of the 53MB body.
    ///
    /// This reads the date out of *the file we would actually ingest*, which is
    /// the only one that decides what we end up storing. `Meta.json` is a
    /// sibling endpoint and can disagree: on 2026-08-28 a download at 08:03
    /// returned the previous day's prices, so gating on anything other than
    /// this file risks fetching 53MB to discover we already had it. The
    /// `Last-Modified` header is likewise not a signal - it read 06:08 that
    /// morning for content that was not yet being served.
    pub async fn published_price_build_date(&self) -> Result<NaiveDate> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::TODAY_PRICES_URL);
        let response = self
            .client
            .get(&url)
            .header(reqwest::header::RANGE, "bytes=0-255")
            .send()
            .await?
            .error_for_status()?;
        // Check the status before touching the body. A server that ignores the
        // Range header answers 200 with the whole file, and `.bytes()` would
        // then quietly pull 53MB - hourly, that is over a gigabyte a day to
        // answer a yes/no question, and it would keep working, so nothing would
        // ever surface it. Refuse instead: the caller treats an error as
        // "cannot tell" and skips the run.
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            let size = response
                .content_length()
                .map_or_else(|| "unknown".to_string(), |n| format!("{n}"));
            return Err(anyhow::anyhow!(
                "{url} ignored the Range header: expected 206 Partial Content, got {}. \
                 Refusing to read the {size}-byte body for a date check.",
                response.status()
            ));
        }
        let head = response.bytes().await?;
        let head = String::from_utf8_lossy(&head);
        // Deliberately not a JSON parse: the slice is a truncated document by
        // construction, so no parser can accept it. The shape is fixed and
        // upstream-controlled - `{"meta":{"date":"YYYY-MM-DD",...`.
        let date = head
            .split("\"date\":\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .ok_or_else(|| anyhow::anyhow!("no meta.date in the first bytes of {url}: {head:?}"))?;
        Ok(date.parse::<NaiveDate>()?)
    }

    pub async fn fetch_set_cards<T>(&self, set_code: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}{}.json", Self::BASE_INGESTION_URL, set_code);
        self.fetch_json(url.as_str()).await
    }

    pub async fn fetch_all_sets<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::SET_LIST_URL);
        self.fetch_json(url.as_str()).await
    }

    async fn fetch_json_bytes_stream(
        &self,
        url: &str,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>> {
        debug!("Fetch JSON Bytes Stream.");
        let response = self.client.get(url).send().await?.error_for_status()?;
        debug!("Received response from: {}", url);
        let byte_stream = response.bytes_stream();
        debug!("Returning response byte stream.");
        Ok(byte_stream)
    }

    async fn fetch_json<T>(&self, url: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let response = self.client.get(url).send().await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "HTTP request failed: {}",
                response.status()
            ));
        }
        Ok(response.json::<T>().await?)
    }
}
