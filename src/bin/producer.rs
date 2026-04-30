use anyhow::Result;
use clap::Parser;
use shm_mpsc_bench::producer;
use shm_mpsc_bench::proto::codec::{
    avro::AvroCodec, capnp::CapnpCodec, cbor::CborCodec, flat::FlatCodec, json::JsonCodec,
    proto::ProtoCodec,
};
use shm_mpsc_bench::proto::control::{capnp_rpc::CapnpClient, jsonrpc::JsonClient};
use shm_mpsc_bench::proto::{CodecKind, ControlPlane};

#[derive(Parser)]
#[command(about = "shm-mpsc producer")]
struct Args {
    #[arg(long, value_enum, default_value_t = ControlPlane::Jsonrpc)]
    control: ControlPlane,
    #[arg(long, value_enum, default_value_t = CodecKind::Json)]
    codec: CodecKind,
    #[arg(long, default_value = "producer-1")]
    name: String,
    #[arg(long, default_value_t = 100_000)]
    count: u64,
    #[arg(long, default_value_t = 64)]
    payload_size: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let n = args.count;
        let p = args.payload_size;
        let name = args.name;
        match (args.control, args.codec) {
            (ControlPlane::Capnp, CodecKind::Capnp) => producer::run::<CapnpClient, CapnpCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Capnp, CodecKind::Json) => producer::run::<CapnpClient, JsonCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Capnp, CodecKind::Cbor) => producer::run::<CapnpClient, CborCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Capnp, CodecKind::Proto) => producer::run::<CapnpClient, ProtoCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Capnp, CodecKind::Flat) => producer::run::<CapnpClient, FlatCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Capnp, CodecKind::Avro) => producer::run::<CapnpClient, AvroCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Jsonrpc, CodecKind::Capnp) => producer::run::<JsonClient, CapnpCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Jsonrpc, CodecKind::Json) => producer::run::<JsonClient, JsonCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Jsonrpc, CodecKind::Cbor) => producer::run::<JsonClient, CborCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Jsonrpc, CodecKind::Proto) => producer::run::<JsonClient, ProtoCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Jsonrpc, CodecKind::Flat) => producer::run::<JsonClient, FlatCodec>(name, n, p).await.map(|_| ()),
            (ControlPlane::Jsonrpc, CodecKind::Avro) => producer::run::<JsonClient, AvroCodec>(name, n, p).await.map(|_| ()),
        }
    })
}
