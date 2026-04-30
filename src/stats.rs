//! Атомарные счётчики и форматтер отчёта бенча.

use std::sync::atomic::AtomicU64;
use std::time::Duration;

#[derive(Default)]
pub struct ConsumerStats {
    pub events: AtomicU64,
    /// Байты, прочитанные из слотов (включая framing протокола).
    pub wire_bytes: AtomicU64,
    /// Байты полезной нагрузки после декодирования (поле `payload`).
    pub payload_bytes: AtomicU64,
}

#[derive(Default)]
pub struct ControlStats {
    /// Сколько раз продюсеры дернули сигнал `posted` через control plane.
    pub posted_calls: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct ProducerStats {
    pub n: u64,
    pub wire_bytes: u64,
    pub payload_bytes: u64,
    pub elapsed: Duration,
}

#[derive(Debug, Clone)]
pub struct BenchReport {
    pub proto: &'static str,
    pub producers: usize,
    pub events_per_producer: u64,
    pub elapsed: Duration,
    pub events: u64,
    pub wire_bytes: u64,
    pub payload_bytes: u64,
    pub posted_calls: u64,
}

impl BenchReport {
    pub fn print(&self) {
        let secs = self.elapsed.as_secs_f64().max(1e-9);
        let events_per_sec = self.events as f64 / secs;
        let wire_mb = self.wire_bytes as f64 / (1024.0 * 1024.0);
        let payload_mb = self.payload_bytes as f64 / (1024.0 * 1024.0);
        let avg_wire = self.wire_bytes as f64 / self.events.max(1) as f64;
        let avg_payload = self.payload_bytes as f64 / self.events.max(1) as f64;
        let overhead_pct = if self.wire_bytes > 0 {
            (1.0 - self.payload_bytes as f64 / self.wire_bytes as f64) * 100.0
        } else {
            0.0
        };
        let posted_per_1k = self.posted_calls as f64 / self.events.max(1) as f64 * 1000.0;
        println!();
        println!("{:=^60}", format!(" {} ", self.proto));
        println!("producers              : {}", self.producers);
        println!("events / producer      : {}", self.events_per_producer);
        println!("events total           : {}", self.events);
        println!("wall time              : {:.3} s", secs);
        println!("throughput             : {:>10.0} events/s", events_per_sec);
        println!("wire bandwidth         : {:>10.2} MB/s", wire_mb / secs);
        println!("payload bandwidth      : {:>10.2} MB/s", payload_mb / secs);
        println!("avg slot bytes         : {:>10.1} B", avg_wire);
        println!("avg payload bytes      : {:>10.1} B", avg_payload);
        println!("framing overhead       : {:>10.1} %", overhead_pct);
        println!(
            "posted calls           : {} ({:.2} per 1k events)",
            self.posted_calls, posted_per_1k
        );
    }
}

pub fn print_table(reports: &[BenchReport]) {
    println!();
    println!("{:=^110}", " summary ");
    println!(
        "{:<18} {:>10} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "proto/scenario",
        "events",
        "wall(s)",
        "evt/s",
        "wire MB/s",
        "pl MB/s",
        "slot B",
        "overhead",
        "post/1k"
    );
    for r in reports {
        let secs = r.elapsed.as_secs_f64().max(1e-9);
        let events_per_sec = r.events as f64 / secs;
        let wire_mb = r.wire_bytes as f64 / (1024.0 * 1024.0) / secs;
        let pl_mb = r.payload_bytes as f64 / (1024.0 * 1024.0) / secs;
        let avg_slot = r.wire_bytes as f64 / r.events.max(1) as f64;
        let overhead_pct = if r.wire_bytes > 0 {
            (1.0 - r.payload_bytes as f64 / r.wire_bytes as f64) * 100.0
        } else {
            0.0
        };
        let posted_per_1k = r.posted_calls as f64 / r.events.max(1) as f64 * 1000.0;
        println!(
            "{:<18} {:>10} {:>10.3} {:>12.0} {:>10.2} {:>10.2} {:>10.0} {:>9.1}% {:>10.2}",
            r.proto,
            r.events,
            secs,
            events_per_sec,
            wire_mb,
            pl_mb,
            avg_slot,
            overhead_pct,
            posted_per_1k
        );
    }
}
