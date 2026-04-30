//! JSON codec для слота. Stateless, но реализует Codec через `&mut self`
//! ради единого интерфейса.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::proto::{Codec, DecodedEvent, Kind};
use crate::ring::RingConfig;

#[derive(Debug, Serialize)]
struct EventOut<'a> {
    seq: u64,
    timestamp: u64,
    source: &'a str,
    kind: Kind,
    payload: &'a str,
}

/// Borrowed deserialize. На payload без escape’ов serde-json
/// не аллоцирует — `&str` указывает в исходный буфер.
#[derive(Debug, Deserialize)]
struct EventIn<'a> {
    seq: u64,
    timestamp: u64,
    #[serde(borrow)]
    source: &'a str,
    kind: Kind,
    #[serde(borrow)]
    payload: &'a str,
}

pub struct JsonCodec;

impl Codec for JsonCodec {
    const NAME: &'static str = "json";

    fn new(_: RingConfig) -> Self {
        JsonCodec
    }

    fn encode(
        &mut self,
        seq: u64,
        ts: u64,
        source: &str,
        kind: Kind,
        payload: &[u8],
        dst: &mut Vec<u8>,
    ) -> Result<()> {
        let pl = std::str::from_utf8(payload)?;
        let ev = EventOut {
            seq,
            timestamp: ts,
            source,
            kind,
            payload: pl,
        };
        serde_json::to_writer(dst, &ev)?;
        Ok(())
    }

    fn decode_into(&mut self, raw: &[u8], out: &mut DecodedEvent) -> Result<()> {
        let ev: EventIn<'_> = serde_json::from_slice(raw)?;
        out.seq = ev.seq;
        out.timestamp = ev.timestamp;
        out.source.clear();
        out.source.push_str(ev.source);
        out.kind = ev.kind;
        out.payload.clear();
        out.payload.extend_from_slice(ev.payload.as_bytes());
        Ok(())
    }
}
