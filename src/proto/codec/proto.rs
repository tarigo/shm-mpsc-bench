//! Protobuf codec через `prost`. Schema — `proto/event.proto`,
//! сгенерирована `prost-build` в `OUT_DIR/shm_mpsc_bench.rs`.

use anyhow::Result;
use prost::Message;

use crate::event_proto::event::Kind as PbKind;
use crate::event_proto::Event as PbEvent;
use crate::proto::{Codec, DecodedEvent, Kind};
use crate::ring::RingConfig;

pub struct ProtoCodec {
    ev: PbEvent,
}

impl Codec for ProtoCodec {
    const NAME: &'static str = "proto";

    fn new(_: RingConfig) -> Self {
        Self {
            ev: PbEvent::default(),
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
        // Reuse одного и того же `PbEvent`: clear-and-fill вместо new()
        // на каждое событие. `String`/`Vec<u8>` поля — переиспользуем
        // через clear()+extend, чтобы не раз за разом аллоцировать.
        self.ev.seq = seq;
        self.ev.timestamp = ts;
        self.ev.source.clear();
        self.ev.source.push_str(source);
        self.ev.kind = kind_to_pb(kind) as i32;
        self.ev.payload.clear();
        self.ev.payload.extend_from_slice(payload);
        self.ev.encode(dst)?;
        Ok(())
    }

    fn decode_into(&mut self, raw: &[u8], out: &mut DecodedEvent) -> Result<()> {
        // Декодируем поверх собственного `self.ev`, чтобы переиспользовать
        // его буферы. `merge` в prost поддерживается, но проще `decode`,
        // и пусть аллокатор переиспользует за счёт `String/Vec` Default+clear.
        self.ev = PbEvent::decode(raw)?;
        out.seq = self.ev.seq;
        out.timestamp = self.ev.timestamp;
        out.source.clear();
        out.source.push_str(&self.ev.source);
        out.kind = kind_from_pb_i32(self.ev.kind);
        out.payload.clear();
        out.payload.extend_from_slice(&self.ev.payload);
        Ok(())
    }
}

fn kind_to_pb(k: Kind) -> PbKind {
    match k {
        Kind::Info => PbKind::Info,
        Kind::Warn => PbKind::Warn,
        Kind::Error => PbKind::Error,
        Kind::Metric => PbKind::Metric,
    }
}

fn kind_from_pb_i32(k: i32) -> Kind {
    match PbKind::try_from(k).unwrap_or(PbKind::Info) {
        PbKind::Info => Kind::Info,
        PbKind::Warn => Kind::Warn,
        PbKind::Error => Kind::Error,
        PbKind::Metric => Kind::Metric,
    }
}
