use anyhow::Result;
use clap::Parser;
use shm_mpsc_bench::proto::codec::{
    avro::AvroCodec, capnp::CapnpCodec, cbor::CborCodec, flat::FlatCodec, json::JsonCodec,
    proto::ProtoCodec,
};
use shm_mpsc_bench::proto::control::{capnp_rpc::CapnpServer, jsonrpc::JsonServer};
use shm_mpsc_bench::proto::{CodecKind, ControlPlane};
use shm_mpsc_bench::ring::{presets, RingConfig};
use shm_mpsc_bench::server::{self, Handles};

#[derive(Parser)]
#[command(about = "shm-mpsc server")]
struct Args {
    #[arg(long, value_enum, default_value_t = ControlPlane::Jsonrpc)]
    control: ControlPlane,
    #[arg(long, value_enum, default_value_t = CodecKind::Json)]
    codec: CodecKind,
    #[arg(long, value_enum, default_value_t = SizePreset::Small)]
    size: SizePreset,
    #[arg(long)]
    slots: Option<usize>,
    #[arg(long, value_name = "BYTES")]
    slot_size: Option<usize>,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum SizePreset {
    Small,
    Medium,
    Large,
    Huge,
}

impl SizePreset {
    fn to_cfg(self) -> RingConfig {
        match self {
            SizePreset::Small => presets::SMALL,
            SizePreset::Medium => presets::MEDIUM,
            SizePreset::Large => presets::LARGE,
            SizePreset::Huge => presets::HUGE,
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut cfg = args.size.to_cfg();
    if let Some(s) = args.slots {
        cfg.num_slots = s;
    }
    if let Some(s) = args.slot_size {
        cfg.slot_size = s;
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let h = Handles::default();
        match (args.control, args.codec) {
            (ControlPlane::Capnp, CodecKind::Capnp) => server::run::<CapnpServer, CapnpCodec>(h, cfg).await,
            (ControlPlane::Capnp, CodecKind::Json) => server::run::<CapnpServer, JsonCodec>(h, cfg).await,
            (ControlPlane::Capnp, CodecKind::Cbor) => server::run::<CapnpServer, CborCodec>(h, cfg).await,
            (ControlPlane::Capnp, CodecKind::Proto) => server::run::<CapnpServer, ProtoCodec>(h, cfg).await,
            (ControlPlane::Capnp, CodecKind::Flat) => server::run::<CapnpServer, FlatCodec>(h, cfg).await,
            (ControlPlane::Capnp, CodecKind::Avro) => server::run::<CapnpServer, AvroCodec>(h, cfg).await,
            (ControlPlane::Jsonrpc, CodecKind::Capnp) => server::run::<JsonServer, CapnpCodec>(h, cfg).await,
            (ControlPlane::Jsonrpc, CodecKind::Json) => server::run::<JsonServer, JsonCodec>(h, cfg).await,
            (ControlPlane::Jsonrpc, CodecKind::Cbor) => server::run::<JsonServer, CborCodec>(h, cfg).await,
            (ControlPlane::Jsonrpc, CodecKind::Proto) => server::run::<JsonServer, ProtoCodec>(h, cfg).await,
            (ControlPlane::Jsonrpc, CodecKind::Flat) => server::run::<JsonServer, FlatCodec>(h, cfg).await,
            (ControlPlane::Jsonrpc, CodecKind::Avro) => server::run::<JsonServer, AvroCodec>(h, cfg).await,
        }
    })
}
