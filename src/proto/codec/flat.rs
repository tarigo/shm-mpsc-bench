//! FlatBuffers codec без codegen — пишем напрямую через
//! `FlatBufferBuilder` + читаем по offset’ам vtable вручную.
//!
//! Эквивалент схемы:
//! ```fbs
//! table Event {
//!   seq:       uint64;   // slot 0 (vt-offset 4)
//!   timestamp: uint64;   // slot 1 (vt-offset 6)
//!   source:    string;   // slot 2 (vt-offset 8)
//!   kind:      ubyte;    // slot 3 (vt-offset 10)
//!   payload:   [ubyte];  // slot 4 (vt-offset 12)
//! }
//! root_type Event;
//! ```
//!
//! `FlatBufferBuilder` переиспользуется через `reset()` — без аллокаций
//! на горячем пути.

use anyhow::{anyhow, Result};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Table, Vector, WIPOffset};

use crate::proto::{Codec, DecodedEvent, Kind};
use crate::ring::RingConfig;

const SEQ: flatbuffers::VOffsetT = 4;
const TS: flatbuffers::VOffsetT = 6;
const SOURCE: flatbuffers::VOffsetT = 8;
const KIND: flatbuffers::VOffsetT = 10;
const PAYLOAD: flatbuffers::VOffsetT = 12;

pub struct FlatCodec {
    fbb: FlatBufferBuilder<'static>,
}

impl Codec for FlatCodec {
    const NAME: &'static str = "flat";

    fn new(cfg: RingConfig) -> Self {
        Self {
            fbb: FlatBufferBuilder::with_capacity(cfg.slot_size + 256),
        }
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
        self.fbb.reset();
        let source_off = self.fbb.create_string(source);
        let payload_off = self.fbb.create_vector(payload);

        let table_start = self.fbb.start_table();
        self.fbb.push_slot::<u64>(SEQ, seq, 0);
        self.fbb.push_slot::<u64>(TS, ts, 0);
        self.fbb
            .push_slot_always::<WIPOffset<&str>>(SOURCE, source_off);
        self.fbb.push_slot::<u8>(KIND, kind_to_u8(kind), 0);
        self.fbb
            .push_slot_always::<WIPOffset<Vector<'_, u8>>>(PAYLOAD, payload_off);
        let table_off = self.fbb.end_table(table_start);
        self.fbb.finish_minimal(table_off);
        dst.extend_from_slice(self.fbb.finished_data());
        Ok(())
    }

    fn decode_into(&mut self, raw: &[u8], out: &mut DecodedEvent) -> Result<()> {
        if raw.len() < 4 {
            return Err(anyhow!("flatbuf: buffer too short"));
        }
        let root_off = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
        // SAFETY: `raw` — это выход FlatBufferBuilder::finish, корректно
        // по построению. flatbuffers::Table::get unsafe потому, что доверяет
        // расположению, а не валидирует.
        unsafe {
            let tab = Table::new(raw, root_off);
            out.seq = tab.get::<u64>(SEQ, Some(0)).unwrap_or(0);
            out.timestamp = tab.get::<u64>(TS, Some(0)).unwrap_or(0);
            let source = tab
                .get::<ForwardsUOffset<&str>>(SOURCE, Some(""))
                .unwrap_or("");
            out.source.clear();
            out.source.push_str(source);
            out.kind = kind_from_u8(tab.get::<u8>(KIND, Some(0)).unwrap_or(0));
            let payload = tab
                .get::<ForwardsUOffset<Vector<u8>>>(PAYLOAD, None)
                .ok_or_else(|| anyhow!("flatbuf: missing payload"))?;
            out.payload.clear();
            out.payload.extend_from_slice(payload.bytes());
        }
        Ok(())
    }
}

fn kind_to_u8(k: Kind) -> u8 {
    match k {
        Kind::Info => 0,
        Kind::Warn => 1,
        Kind::Error => 2,
        Kind::Metric => 3,
    }
}

fn kind_from_u8(v: u8) -> Kind {
    match v {
        1 => Kind::Warn,
        2 => Kind::Error,
        3 => Kind::Metric,
        _ => Kind::Info,
    }
}
