//! Trait-based абстракция кодека слота и протокола control plane.
//!
//! `Codec` — сериализация одного события в shm-слот (stateful: владеет
//! reusable-буферами, конструируется через `Codec::new(cfg)`).
//!
//! `Client` / `Server` — control plane (handshake `attach`,
//! fire-and-forget `posted`).

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::thread::Thread;
use tokio::net::UnixStream;

use crate::ring::RingConfig;
use crate::stats::ControlStats;

pub mod codec;
pub mod control;

#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum ControlPlane {
    Capnp,
    Jsonrpc,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum CodecKind {
    Capnp,
    Json,
    Cbor,
    Proto,
    Flat,
    Avro,
}

impl CodecKind {
    pub const ALL: [Self; 6] = [
        Self::Capnp,
        Self::Json,
        Self::Cbor,
        Self::Proto,
        Self::Flat,
        Self::Avro,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::Capnp => "capnp",
            Self::Json => "json",
            Self::Cbor => "cbor",
            Self::Proto => "proto",
            Self::Flat => "flat",
            Self::Avro => "avro",
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Info,
    Warn,
    Error,
    Metric,
}

impl Default for Kind {
    fn default() -> Self {
        Kind::Info
    }
}

pub struct AttachInfo {
    pub slots: u32,
    pub slot_size: u32,
}

#[derive(Default)]
pub struct DecodedEvent {
    pub seq: u64,
    pub timestamp: u64,
    pub source: String,
    pub kind: Kind,
    pub payload: Vec<u8>,
}

/// Stateful кодек одного события: владеет reusable буферами,
/// конструируется один раз с известной геометрией кольца.
pub trait Codec: 'static {
    const NAME: &'static str;
    fn new(cfg: RingConfig) -> Self
    where
        Self: Sized;
    fn encode(
        &mut self,
        seq: u64,
        ts: u64,
        source: &str,
        kind: Kind,
        payload: &[u8],
        dst: &mut Vec<u8>,
    ) -> Result<()>;
    fn decode_into(&mut self, raw: &[u8], out: &mut DecodedEvent) -> Result<()>;
}

/// Клиент control plane.
#[async_trait(?Send)]
pub trait Client: Sized {
    async fn attach(stream: UnixStream, name: &str) -> Result<(Self, AttachInfo)>;
    fn signal_posted(&self, seq: u64);
    async fn shutdown(self) -> Result<()>;
}

/// Серверная сторона одной установленной сессии control plane.
#[async_trait(?Send)]
pub trait Server {
    async fn serve(
        stream: UnixStream,
        consumer: Arc<Thread>,
        stats: Arc<ControlStats>,
        cfg: RingConfig,
    ) -> Result<()>;
}
