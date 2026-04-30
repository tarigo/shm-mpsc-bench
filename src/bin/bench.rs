//! Бенч: для каждой пары (size × codec) поднимает сервер на JSON-RPC
//! control plane, гоняет N продюсеров, считает метрики, по `--out`
//! пишет JSON-отчёт для последующей отрисовки.

use anyhow::Result;
use clap::Parser;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::*;
use std::time::{Duration, Instant};

use shm_mpsc_bench::producer;
use shm_mpsc_bench::proto::codec::{
    avro::AvroCodec, capnp::CapnpCodec, cbor::CborCodec, flat::FlatCodec, json::JsonCodec,
    proto::ProtoCodec,
};
use shm_mpsc_bench::proto::control::jsonrpc::{JsonClient, JsonServer};
use shm_mpsc_bench::proto::{Codec, CodecKind};
use shm_mpsc_bench::ring::{presets, RingConfig};
use shm_mpsc_bench::server::{self, Handles};
use shm_mpsc_bench::stats::{print_table, BenchReport};
use shm_mpsc_bench::SOCK_PATH;

#[derive(Parser)]
#[command(about = "End-to-end benchmark across payload sizes and codecs")]
struct Args {
    #[arg(long, default_value_t = 4)]
    producers: usize,
    /// Какие сценарии гонять. По умолчанию все четыре.
    #[arg(long, value_enum, num_args = 1.., default_values_t = ALL_SIZES)]
    sizes: Vec<SizeName>,
    /// Какие кодеки гонять. По умолчанию все шесть.
    #[arg(long, value_enum, num_args = 1.., default_values_t = CodecKind::ALL)]
    codecs: Vec<CodecKind>,
    /// Куда положить JSON с подробными результатами (для plot.py).
    #[arg(long)]
    out: Option<PathBuf>,
    /// Пропустить warm-up прогон (ускоряет, но шумит).
    #[arg(long, default_value_t = false)]
    no_warmup: bool,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
enum SizeName {
    Small,
    Medium,
    Large,
    Huge,
}

const ALL_SIZES: [SizeName; 4] = [
    SizeName::Small,
    SizeName::Medium,
    SizeName::Large,
    SizeName::Huge,
];

#[derive(Clone, Copy, Debug)]
struct Scenario {
    name: &'static str,
    cfg: RingConfig,
    payload_size: usize,
    events_per_producer: u64,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "small",
        cfg: presets::SMALL,
        payload_size: 64,
        events_per_producer: 500_000,
    },
    Scenario {
        name: "medium",
        cfg: presets::MEDIUM,
        payload_size: 1024,
        events_per_producer: 200_000,
    },
    Scenario {
        name: "large",
        cfg: presets::LARGE,
        payload_size: 16 * 1024,
        events_per_producer: 50_000,
    },
    Scenario {
        name: "huge",
        cfg: presets::HUGE,
        payload_size: 1024 * 1024,
        events_per_producer: 1_500,
    },
];

fn pick_scenario(s: SizeName) -> &'static Scenario {
    let key = match s {
        SizeName::Small => "small",
        SizeName::Medium => "medium",
        SizeName::Large => "large",
        SizeName::Huge => "huge",
    };
    SCENARIOS.iter().find(|sc| sc.name == key).unwrap()
}

#[derive(Debug, Serialize)]
struct ReportRow {
    codec: &'static str,
    scenario: &'static str,
    producers: usize,
    events: u64,
    events_per_producer: u64,
    wall_secs: f64,
    events_per_sec: f64,
    wire_mb_per_sec: f64,
    payload_mb_per_sec: f64,
    avg_slot_bytes: f64,
    avg_payload_bytes: f64,
    framing_overhead_pct: f64,
    posted_calls: u64,
    posted_per_1k: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut rows: Vec<ReportRow> = Vec::new();
    let mut reports: Vec<BenchReport> = Vec::new();

    for size in &args.sizes {
        let sc = pick_scenario(*size);
        eprintln!(
            ">>> scenario={} cfg=(slots={}, slot_size={}B) payload={}B events/producer={}",
            sc.name, sc.cfg.num_slots, sc.cfg.slot_size, sc.payload_size, sc.events_per_producer
        );

        for codec in &args.codecs {
            let codec = *codec;
            if !args.no_warmup {
                let warmup_events = (sc.events_per_producer / 10).max(200);
                eprintln!("    warmup {} ({} ev/p)", codec.name(), warmup_events);
                run_one(codec, sc, args.producers, warmup_events)?;
            }
            eprintln!("    measure {}", codec.name());
            let r = run_one(codec, sc, args.producers, sc.events_per_producer)?;
            r.print();
            rows.push(report_row(codec, sc, args.producers, &r));
            reports.push(r);
        }
    }

    print_table(&reports);

    if let Some(path) = args.out {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let body = serde_json::to_string_pretty(&rows)?;
        std::fs::write(&path, body)?;
        eprintln!("\nresults written to {}", path.display());
    }
    Ok(())
}

fn report_row(
    codec: CodecKind,
    sc: &Scenario,
    producers: usize,
    r: &BenchReport,
) -> ReportRow {
    let secs = r.elapsed.as_secs_f64().max(1e-9);
    let avg_slot = r.wire_bytes as f64 / r.events.max(1) as f64;
    let avg_payload = r.payload_bytes as f64 / r.events.max(1) as f64;
    let overhead_pct = if r.wire_bytes > 0 {
        (1.0 - r.payload_bytes as f64 / r.wire_bytes as f64) * 100.0
    } else {
        0.0
    };
    ReportRow {
        codec: codec.name(),
        scenario: sc.name,
        producers,
        events: r.events,
        events_per_producer: r.events_per_producer,
        wall_secs: secs,
        events_per_sec: r.events as f64 / secs,
        wire_mb_per_sec: r.wire_bytes as f64 / (1024.0 * 1024.0) / secs,
        payload_mb_per_sec: r.payload_bytes as f64 / (1024.0 * 1024.0) / secs,
        avg_slot_bytes: avg_slot,
        avg_payload_bytes: avg_payload,
        framing_overhead_pct: overhead_pct,
        posted_calls: r.posted_calls,
        posted_per_1k: r.posted_calls as f64 / r.events.max(1) as f64 * 1000.0,
    }
}

fn run_one(
    codec: CodecKind,
    sc: &'static Scenario,
    producers: usize,
    events: u64,
) -> Result<BenchReport> {
    let _ = std::fs::remove_file(SOCK_PATH);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        run_inner(codec, sc, producers, events).await
    })
}

async fn run_inner(
    codec: CodecKind,
    sc: &'static Scenario,
    producers: usize,
    events: u64,
) -> Result<BenchReport> {
    let handles = Handles::default();
    let consumer_stats = handles.consumer_stats.clone();
    let control_stats = handles.control_stats.clone();
    let stop = handles.stop.clone();

    // Поднять сервер с нужным кодеком; control plane всегда JSON-RPC.
    let server_task: tokio::task::JoinHandle<Result<()>> = match codec {
        CodecKind::Capnp => spawn_server::<CapnpCodec>(handles, sc.cfg),
        CodecKind::Json => spawn_server::<JsonCodec>(handles, sc.cfg),
        CodecKind::Cbor => spawn_server::<CborCodec>(handles, sc.cfg),
        CodecKind::Proto => spawn_server::<ProtoCodec>(handles, sc.cfg),
        CodecKind::Flat => spawn_server::<FlatCodec>(handles, sc.cfg),
        CodecKind::Avro => spawn_server::<AvroCodec>(handles, sc.cfg),
    };

    wait_for_sock(SOCK_PATH).await?;
    let target = (producers as u64) * events;

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(producers);
    for i in 0..producers {
        let name = format!("p{i}");
        let p = sc.payload_size;
        match codec {
            CodecKind::Capnp => tasks.push(spawn_producer::<CapnpCodec>(name, events, p)),
            CodecKind::Json => tasks.push(spawn_producer::<JsonCodec>(name, events, p)),
            CodecKind::Cbor => tasks.push(spawn_producer::<CborCodec>(name, events, p)),
            CodecKind::Proto => tasks.push(spawn_producer::<ProtoCodec>(name, events, p)),
            CodecKind::Flat => tasks.push(spawn_producer::<FlatCodec>(name, events, p)),
            CodecKind::Avro => tasks.push(spawn_producer::<AvroCodec>(name, events, p)),
        }
    }
    for t in tasks {
        t.await??;
    }

    while consumer_stats.events.load(Relaxed) < target {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let elapsed = started.elapsed();

    stop.store(true, Relaxed);
    server_task.abort();
    let _ = server_task.await;
    tokio::time::sleep(Duration::from_millis(80)).await;

    Ok(BenchReport {
        proto: label(codec, sc.name),
        producers,
        events_per_producer: events,
        elapsed,
        events: consumer_stats.events.load(Relaxed),
        wire_bytes: consumer_stats.wire_bytes.load(Relaxed),
        payload_bytes: consumer_stats.payload_bytes.load(Relaxed),
        posted_calls: control_stats.posted_calls.load(Relaxed),
    })
}

fn spawn_server<C: Codec>(h: Handles, cfg: RingConfig) -> tokio::task::JoinHandle<Result<()>> {
    tokio::task::spawn_local(async move { server::run::<JsonServer, C>(h, cfg).await })
}

fn spawn_producer<C: Codec>(
    name: String,
    events: u64,
    payload_size: usize,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::task::spawn_local(async move {
        producer::run::<JsonClient, C>(name, events, payload_size)
            .await
            .map(|_| ())
    })
}

async fn wait_for_sock(path: &str) -> Result<()> {
    let p = Path::new(path);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !p.exists() {
        if Instant::now() > deadline {
            anyhow::bail!("socket {path} did not appear in 5s");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

fn label(codec: CodecKind, scenario: &str) -> &'static str {
    match (codec, scenario) {
        (CodecKind::Capnp, "small") => "capnp/small",
        (CodecKind::Capnp, "medium") => "capnp/medium",
        (CodecKind::Capnp, "large") => "capnp/large",
        (CodecKind::Capnp, "huge") => "capnp/huge",
        (CodecKind::Json, "small") => "json/small",
        (CodecKind::Json, "medium") => "json/medium",
        (CodecKind::Json, "large") => "json/large",
        (CodecKind::Json, "huge") => "json/huge",
        (CodecKind::Cbor, "small") => "cbor/small",
        (CodecKind::Cbor, "medium") => "cbor/medium",
        (CodecKind::Cbor, "large") => "cbor/large",
        (CodecKind::Cbor, "huge") => "cbor/huge",
        (CodecKind::Proto, "small") => "proto/small",
        (CodecKind::Proto, "medium") => "proto/medium",
        (CodecKind::Proto, "large") => "proto/large",
        (CodecKind::Proto, "huge") => "proto/huge",
        (CodecKind::Flat, "small") => "flat/small",
        (CodecKind::Flat, "medium") => "flat/medium",
        (CodecKind::Flat, "large") => "flat/large",
        (CodecKind::Flat, "huge") => "flat/huge",
        (CodecKind::Avro, "small") => "avro/small",
        (CodecKind::Avro, "medium") => "avro/medium",
        (CodecKind::Avro, "large") => "avro/large",
        (CodecKind::Avro, "huge") => "avro/huge",
        _ => "?",
    }
}
