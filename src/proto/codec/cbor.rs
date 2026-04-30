//! CBOR (RFC 8949) codec через `ciborium`.
//!
//! Для bytes используем `serde_bytes`, как рекомендуют доки ciborium —
//! иначе serde-derive обрабатывает `Vec<u8>` как `seq[u8]`, что не
//! совпадает с CBOR major type 2 (byte string) и валится на больших
//! payload’ах.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteBuf, Bytes};

use crate::proto::{Codec, DecodedEvent, Kind};
use crate::ring::RingConfig;

#[derive(Debug, Serialize)]
struct EventOut<'a> {
    seq: u64,
    timestamp: u64,
    source: &'a str,
    kind: Kind,
    payload: &'a Bytes,
}

#[derive(Debug, Deserialize)]
struct EventIn {
    seq: u64,
    timestamp: u64,
    source: String,
    kind: Kind,
    payload: ByteBuf,
}

pub struct CborCodec;

impl Codec for CborCodec {
    const NAME: &'static str = "cbor";

    fn new(_: RingConfig) -> Self {
        CborCodec
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
        let ev = EventOut {
            seq,
            timestamp: ts,
            source,
            kind,
            payload: Bytes::new(payload),
        };
        ciborium::ser::into_writer(&ev, dst)?;
        Ok(())
    }

    fn decode_into(&mut self, raw: &[u8], out: &mut DecodedEvent) -> Result<()> {
        let ev: EventIn = ciborium::de::from_reader(raw)?;
        out.seq = ev.seq;
        out.timestamp = ev.timestamp;
        out.source.clear();
        out.source.push_str(&ev.source);
        out.kind = ev.kind;
        out.payload.clear();
        out.payload.extend_from_slice(&ev.payload);
        Ok(())
    }
}
