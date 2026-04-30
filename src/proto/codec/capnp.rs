//! Cap'n Proto codec для слота. Builder поверх `ScratchSpaceHeapAllocator`,
//! который занимает память один раз и переиспользует её на каждом encode.

use anyhow::Result;
use capnp::message::{Builder, ReaderOptions, ScratchSpaceHeapAllocator};

use crate::control_capnp::event;
use crate::proto::{Codec, DecodedEvent, Kind};
use crate::ring::RingConfig;

pub struct CapnpCodec {
    /// Reusable scratch (в байтах, выровнен на 8) для capnp Builder.
    scratch: Vec<u8>,
}

impl Codec for CapnpCodec {
    const NAME: &'static str = "capnp";

    fn new(cfg: RingConfig) -> Self {
        // Слот + запас 512 байт под framing. Округляем вверх до 8 (Word).
        let bytes = (cfg.slot_size + 512 + 7) & !7usize;
        Self {
            scratch: vec![0u8; bytes],
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
        let alloc = ScratchSpaceHeapAllocator::new(self.scratch.as_mut_slice());
        let mut msg = Builder::new(alloc);
        let mut ev = msg.init_root::<event::Builder>();
        ev.set_seq(seq);
        ev.set_timestamp(ts);
        ev.set_source(source);
        ev.set_kind(kind_to_capnp(kind));
        ev.set_payload(payload);
        capnp::serialize::write_message(dst, &msg)?;
        Ok(())
    }

    fn decode_into(&mut self, raw: &[u8], out: &mut DecodedEvent) -> Result<()> {
        let mut cursor = raw;
        let msg = capnp::serialize::read_message_from_flat_slice(
            &mut cursor,
            ReaderOptions::new(),
        )?;
        let ev = msg.get_root::<event::Reader>()?;
        out.seq = ev.get_seq();
        out.timestamp = ev.get_timestamp();
        out.source.clear();
        out.source
            .push_str(ev.get_source().and_then(|t| t.to_str().map_err(Into::into))?);
        out.kind = kind_from_capnp(ev.get_kind().unwrap_or(event::Kind::Info));
        out.payload.clear();
        out.payload.extend_from_slice(ev.get_payload()?);
        Ok(())
    }
}

fn kind_to_capnp(k: Kind) -> event::Kind {
    match k {
        Kind::Info => event::Kind::Info,
        Kind::Warn => event::Kind::Warn,
        Kind::Error => event::Kind::Error,
        Kind::Metric => event::Kind::Metric,
    }
}

fn kind_from_capnp(k: event::Kind) -> Kind {
    match k {
        event::Kind::Info => Kind::Info,
        event::Kind::Warn => Kind::Warn,
        event::Kind::Error => Kind::Error,
        event::Kind::Metric => Kind::Metric,
    }
}
