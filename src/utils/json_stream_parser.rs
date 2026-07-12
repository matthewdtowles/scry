use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use std::marker::PhantomData;
use tokio::io::BufReader;
use tokio_util::io::StreamReader;
use tracing::{debug, error, warn};

const BUF_READER_SIZE: usize = 64 * 1024;

pub(crate) struct JsonStreamParser<T, P>
where
    P: JsonEventProcessor<T>,
{
    event_processor: P,
    _phantom: PhantomData<T>,
}

pub(crate) trait JsonEventProcessor<T> {
    async fn process_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<usize>;

    fn take_batch(&mut self) -> Vec<T>;
}

/// The outcome of pulling one event from the underlying parser.
enum NextEvent {
    /// A JSON event ready to be processed.
    Event(JsonEvent),
    /// End of the JSON document.
    End,
    /// A recoverable parser (tokenizer) error. The stream tolerates a bounded
    /// number of these before aborting.
    ParserError(String),
}

impl<T, P> JsonStreamParser<T, P>
where
    P: JsonEventProcessor<T>,
{
    pub fn new(event_processor: P) -> Self {
        Self {
            event_processor,
            _phantom: PhantomData,
        }
    }

    pub async fn parse_stream<'a, S, E, F>(&mut self, byte_stream: S, mut on_batch: F) -> Result<()>
    where
        S: futures::Stream<Item = Result<Bytes, E>>,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
        F: FnMut(Vec<T>) -> futures::future::BoxFuture<'a, Result<()>>,
    {
        let stream_reader =
            StreamReader::new(byte_stream.map(|result| {
                result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            }));
        let mut pinned_stream_reader = Box::pin(stream_reader);
        let buf_reader = BufReader::with_capacity(BUF_READER_SIZE, pinned_stream_reader.as_mut());
        let feeder = AsyncBufReaderJsonFeeder::new(buf_reader);
        let mut parser = JsonParser::new(feeder);
        let mut error_count = 0;
        loop {
            let next_event = self.get_next_event(&mut parser).await?;
            let should_continue = self
                .handle_parse_event(next_event, &parser, &mut on_batch, &mut error_count)
                .await?;
            if !should_continue {
                return Ok(());
            }
        }
    }

    /// Pull the next event from the parser, filling the feeder as needed.
    ///
    /// Fills until the parser produces a real event or reaches end of input,
    /// rather than a fixed number of attempts. A feeder IO error (e.g. the
    /// connection dropping mid-download) is propagated as a hard failure. The
    /// old code swallowed it, ran on to EOF, and then aborted with a misleading
    /// "parser error" that hid the real network failure.
    async fn get_next_event<R: tokio::io::AsyncRead + Unpin>(
        &self,
        parser: &mut JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<NextEvent> {
        let mut event_result = parser.next_event();
        while let Ok(Some(JsonEvent::NeedMoreInput)) = event_result {
            parser
                .feeder
                .fill_buf()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read from stream: {}", e))?;
            event_result = parser.next_event();
        }

        match event_result {
            Ok(Some(event)) => Ok(NextEvent::Event(event)),
            Ok(None) => Ok(NextEvent::End),
            Err(e) => Ok(NextEvent::ParserError(format!("Parser error: {}", e))),
        }
    }

    async fn handle_parse_event<'a, R, F>(
        &mut self,
        next_event: NextEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
        on_batch: &mut F,
        error_count: &mut usize,
    ) -> Result<bool>
    where
        R: tokio::io::AsyncRead + Unpin,
        F: FnMut(Vec<T>) -> futures::future::BoxFuture<'a, Result<()>>,
    {
        match next_event {
            NextEvent::Event(event) => {
                *error_count = 0;
                let processed_count = self.event_processor.process_event(event, parser).await?;
                if processed_count > 0 {
                    let batch = self.event_processor.take_batch();
                    if !batch.is_empty() {
                        on_batch(batch).await?;
                    }
                }
                Ok(true)
            }
            NextEvent::End => {
                let remaining = self.event_processor.take_batch();
                if !remaining.is_empty() {
                    debug!("Processing final batch of {} length", remaining.len());
                    on_batch(remaining).await?;
                }
                Ok(false)
            }
            NextEvent::ParserError(error_msg) => {
                warn!("JSON parser error: {}", error_msg);
                *error_count += 1;
                if *error_count > 10 {
                    error!("Parser error limit (10) exceeded. Aborting stream.");
                    return Err(anyhow::anyhow!(
                        "JSON streaming parse failed: {}",
                        error_msg
                    ));
                }
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::io;
    use std::time::Duration;

    /// Minimal processor that ignores every event and never emits a batch.
    struct NoopProcessor;

    impl JsonEventProcessor<()> for NoopProcessor {
        async fn process_event<R: tokio::io::AsyncRead + Unpin>(
            &mut self,
            _event: JsonEvent,
            _parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
        ) -> Result<usize> {
            Ok(0)
        }

        fn take_batch(&mut self) -> Vec<()> {
            Vec::new()
        }
    }

    // A mid-stream IO failure (e.g. the connection dropping during a large
    // download) must surface as the actual read error. The old code swallowed
    // the feeder IO error, ran on to EOF, and reported a misleading parser
    // error instead — so assert on the message, not just that it failed.
    // Wrapped in a timeout as insurance against a busy-loop regression.
    #[tokio::test]
    async fn surfaces_stream_io_failure() {
        let byte_stream = stream::iter(vec![
            Ok(Bytes::from_static(b"{\"data\": {")),
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection dropped mid-download",
            )),
        ]);
        let mut parser = JsonStreamParser::new(NoopProcessor);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            parser.parse_stream(byte_stream, |_batch| Box::pin(async { Ok(()) })),
        )
        .await
        .expect("parse_stream hung on a failing stream (busy-loop regression)");

        let err = result.expect_err("expected a hard error when the stream fails mid-download");
        let msg = format!("{err}");
        assert!(
            msg.contains("Failed to read from stream"),
            "error should report the underlying read failure, got: {msg}"
        );
    }

    // A well-formed document delivered across multiple chunks still parses to
    // completion after the fill-loop change.
    #[tokio::test]
    async fn completes_on_well_formed_chunked_stream() {
        let byte_stream = stream::iter(vec![
            Ok::<_, io::Error>(Bytes::from_static(b"[1, 2,")),
            Ok(Bytes::from_static(b" 3]")),
        ]);
        let mut parser = JsonStreamParser::new(NoopProcessor);

        let result = parser
            .parse_stream(byte_stream, |_batch| Box::pin(async { Ok(()) }))
            .await;

        assert!(
            result.is_ok(),
            "well-formed JSON should parse cleanly: {result:?}"
        );
    }
}
