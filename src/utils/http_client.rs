use anyhow::Result;
use bytes::Bytes;
use chrono::NaiveDate;
use futures::Stream;
use reqwest::Client;
use serde::de::DeserializeOwned;
use std::time::Duration;
use tracing::{debug, info};

/// `Meta.json`: `{"meta":{...},"data":{"date":"2026-08-28","version":"..."}}`.
#[derive(serde::Deserialize)]
struct MetaEnvelope {
    data: MetaBody,
}

#[derive(serde::Deserialize)]
struct MetaBody {
    date: NaiveDate,
}

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
    const META_URL: &str = "Meta.json";

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

    /// The build date MTGJSON says it has currently published.
    ///
    /// `Meta.json` is ~113 bytes and is the only authoritative answer to "what
    /// does upstream have right now". Everything else is inference: the
    /// `Last-Modified` header on the bulk files is NOT when their new content
    /// starts being served - on 2026-08-28 it read 06:08 UTC while a download
    /// at 08:03 still returned the previous day's build - and a hardcoded
    /// publish hour has now been wrong twice.
    pub async fn fetch_published_build_date(&self) -> Result<NaiveDate> {
        let url = format!("{}{}", Self::BASE_INGESTION_URL, Self::META_URL);
        let meta: MetaEnvelope = self.fetch_json(url.as_str()).await?;
        Ok(meta.data.date)
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
