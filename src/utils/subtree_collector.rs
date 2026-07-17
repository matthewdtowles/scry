//! Shared building blocks for the streaming-JSON event processors.
//!
//! [`SubtreeCollector`] turns a run of actson events into a
//! `serde_json::Value`; [`DocumentCursor`] tracks document depth and skips
//! unwanted subtrees. Together they replace the hand-rolled per-processor
//! state machines whose duplicated escaping/comma/skip logic had already
//! diverged once (codebase analysis §1.8).

use actson::tokio::AsyncBufReaderJsonFeeder;
use actson::{JsonEvent, JsonParser};
use anyhow::{anyhow, Result};
use serde_json::{Map, Number, Value};

/// Tracks the current nesting depth of a streamed JSON document, plus a
/// "skip this field's value" mode that consumes events until the document
/// returns to the depth where the skip began.
pub(crate) struct DocumentCursor {
    depth: usize,
    skip_until_depth: Option<usize>,
}

impl DocumentCursor {
    pub(crate) fn new() -> Self {
        Self {
            depth: 0,
            skip_until_depth: None,
        }
    }

    pub(crate) fn depth(&self) -> usize {
        self.depth
    }

    /// Record entering a container. Call on `StartObject`/`StartArray`
    /// *before* routing on depth, so routing sees the just-opened depth.
    pub(crate) fn enter(&mut self) {
        self.depth += 1;
    }

    /// Record leaving a container. Call on `EndObject`/`EndArray` *after*
    /// routing on depth, so routing sees the depth of the closing container.
    pub(crate) fn exit(&mut self) {
        self.depth -= 1;
    }

    /// Keep depth in sync for an event that needs no routing (e.g. while a
    /// subtree is being collected).
    pub(crate) fn observe(&mut self, event: JsonEvent) {
        match event {
            JsonEvent::StartObject | JsonEvent::StartArray => self.depth += 1,
            JsonEvent::EndObject | JsonEvent::EndArray => self.depth -= 1,
            _ => {}
        }
    }

    /// Skip the value of the field just seen: subsequent events are consumed
    /// by [`Self::consume_if_skipping`] until the document returns to the
    /// current depth.
    pub(crate) fn skip_value(&mut self) {
        self.skip_until_depth = Some(self.depth);
    }

    /// While skipping, consume `event` (keeping depth in sync) and return
    /// true. Skip mode ends once the skipped value has been fully consumed —
    /// a scalar at the skip depth, or its container closing back down to it.
    /// The scalar case matters: without it a scalar-valued skipped field
    /// would wedge the processor in skip mode forever.
    pub(crate) fn consume_if_skipping(&mut self, event: JsonEvent) -> bool {
        let Some(limit) = self.skip_until_depth else {
            return false;
        };
        match event {
            JsonEvent::StartObject | JsonEvent::StartArray => self.depth += 1,
            JsonEvent::EndObject | JsonEvent::EndArray => {
                self.depth -= 1;
                if self.depth <= limit {
                    self.skip_until_depth = None;
                }
            }
            _ => {
                if self.depth <= limit {
                    self.skip_until_depth = None;
                }
            }
        }
        true
    }
}

/// Builds a `serde_json::Value` from a run of streaming JSON events.
///
/// A processor creates one when it decides to capture the subtree rooted at
/// the event it is currently looking at, then feeds that event and every
/// following one to [`Self::push_event`] until it returns the finished value.
/// Building the `Value` directly replaces the old "re-serialize to a `String`,
/// then `serde_json::from_str`" round trip and its hand-rolled escaping and
/// comma-insertion logic.
pub(crate) struct SubtreeCollector {
    stack: Vec<Frame>,
}

enum Frame {
    Object {
        map: Map<String, Value>,
        pending_key: Option<String>,
    },
    Array(Vec<Value>),
}

impl Frame {
    fn into_value(self) -> Value {
        match self {
            Frame::Object { map, .. } => Value::Object(map),
            Frame::Array(items) => Value::Array(items),
        }
    }
}

impl SubtreeCollector {
    pub(crate) fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// Feed one event. Returns the completed subtree when its root closes.
    pub(crate) fn push_event<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        event: JsonEvent,
        parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
    ) -> Result<Option<Value>> {
        match event {
            JsonEvent::StartObject => {
                self.stack.push(Frame::Object {
                    map: Map::new(),
                    pending_key: None,
                });
                Ok(None)
            }
            JsonEvent::StartArray => {
                self.stack.push(Frame::Array(Vec::new()));
                Ok(None)
            }
            JsonEvent::EndObject | JsonEvent::EndArray => {
                let frame = self
                    .stack
                    .pop()
                    .ok_or_else(|| anyhow!("unbalanced end event in JSON subtree"))?;
                self.insert(frame.into_value())
            }
            JsonEvent::FieldName => {
                let key = parser.current_str().unwrap_or_default().to_string();
                match self.stack.last_mut() {
                    Some(Frame::Object { pending_key, .. }) => {
                        *pending_key = Some(key);
                        Ok(None)
                    }
                    _ => Err(anyhow!("field name outside an object in JSON subtree")),
                }
            }
            JsonEvent::ValueString => {
                let s = parser.current_str().unwrap_or_default().to_string();
                self.insert(Value::String(s))
            }
            JsonEvent::ValueInt => self.insert(Value::Number(parser.current_int::<i64>()?.into())),
            JsonEvent::ValueFloat => {
                let f = parser.current_float()?;
                let n = Number::from_f64(f)
                    .ok_or_else(|| anyhow!("non-finite number in JSON stream"))?;
                self.insert(Value::Number(n))
            }
            JsonEvent::ValueTrue => self.insert(Value::Bool(true)),
            JsonEvent::ValueFalse => self.insert(Value::Bool(false)),
            JsonEvent::ValueNull => self.insert(Value::Null),
            // NeedMoreInput is handled by the stream driver, never forwarded.
            _ => Ok(None),
        }
    }

    /// Attach a completed value to the enclosing container, or yield it if it
    /// is the root of the collected subtree.
    fn insert(&mut self, value: Value) -> Result<Option<Value>> {
        match self.stack.last_mut() {
            Some(Frame::Object { map, pending_key }) => {
                let key = pending_key
                    .take()
                    .ok_or_else(|| anyhow!("value without a field name in JSON subtree"))?;
                map.insert(key, value);
                Ok(None)
            }
            Some(Frame::Array(items)) => {
                items.push(value);
                Ok(None)
            }
            None => Ok(Some(value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::json_stream_parser::{test_support::collect_batches, JsonEventProcessor};

    /// Test processor that collects the document's root value.
    struct RootCollector {
        collector: SubtreeCollector,
        out: Vec<Value>,
    }

    impl JsonEventProcessor<Value> for RootCollector {
        async fn process_event<R: tokio::io::AsyncRead + Unpin>(
            &mut self,
            event: JsonEvent,
            parser: &JsonParser<AsyncBufReaderJsonFeeder<R>>,
        ) -> Result<usize> {
            if let Some(value) = self.collector.push_event(event, parser)? {
                self.out.push(value);
                return Ok(1);
            }
            Ok(0)
        }

        fn take_batch(&mut self) -> Vec<Value> {
            std::mem::take(&mut self.out)
        }
    }

    // The collector must agree with serde_json's own parse of the same
    // document: nesting, escapes, unicode, numbers, booleans, nulls.
    #[tokio::test]
    async fn collected_value_matches_serde_json_parse() {
        let doc = r#"{
            "name": "Say \"Cheese!\"",
            "text": "line one\n\ttab — bullet • and a bell\u0007",
            "path": "C:\\WINDOWS\\config.sys",
            "int": 12345,
            "negative": -7,
            "float": 2.5,
            "flag": true,
            "off": false,
            "nothing": null,
            "empty_list": [],
            "empty_obj": {},
            "nested": {"list": [1, {"deep": ["a", 2.25, false, null]}]}
        }"#;

        let batches = collect_batches(
            RootCollector {
                collector: SubtreeCollector::new(),
                out: Vec::new(),
            },
            doc,
        )
        .await;

        let expected: Value = serde_json::from_str(doc).unwrap();
        assert_eq!(batches, vec![vec![expected]]);
    }

    // A skipped field whose value is a scalar must end skip mode immediately;
    // otherwise the cursor stays skipping forever and the rest of the
    // document is silently dropped (the §1.8 divergence, now in one place).
    #[test]
    fn skip_ends_on_scalar_value() {
        let mut cursor = DocumentCursor::new();
        cursor.enter();
        cursor.enter();
        cursor.enter();
        cursor.skip_value();

        assert!(cursor.consume_if_skipping(JsonEvent::ValueString));
        assert!(
            !cursor.consume_if_skipping(JsonEvent::ValueString),
            "a scalar skipped value should end skip mode, not wedge it"
        );
        assert_eq!(cursor.depth(), 3);
    }

    // Skipping a container value consumes everything inside it (including
    // field names and scalars at deeper depths) and ends when it closes.
    #[test]
    fn skip_consumes_whole_container() {
        let mut cursor = DocumentCursor::new();
        cursor.enter(); // depth 1: inside the root object
        cursor.skip_value();

        for event in [
            JsonEvent::StartObject,
            JsonEvent::FieldName,
            JsonEvent::ValueString,
            JsonEvent::StartArray,
            JsonEvent::ValueInt,
            JsonEvent::EndArray,
            JsonEvent::EndObject,
        ] {
            assert!(cursor.consume_if_skipping(event), "still inside the skip");
        }
        assert!(
            !cursor.consume_if_skipping(JsonEvent::FieldName),
            "skip should end when the container closes"
        );
        assert_eq!(cursor.depth(), 1);
    }
}
