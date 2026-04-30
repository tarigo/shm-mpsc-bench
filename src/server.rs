//! Generic server scaffold: общий accept-цикл + consumer-поток на shared memory.

use anyhow::Result;
use std::os::fd::AsFd;
use std::sync::atomic::Ordering::*;
use std::sync::Arc;
use std::thread::Thread;
use std::time::Duration;
use tokio::net::UnixListener;

use crate::proto::{Codec, DecodedEvent, Server};
use crate::ring::{atom, init, try_pop, RingConfig, STATE_PARKED, STATE_RUNNING};
use crate::shm;
use crate::stats::{ConsumerStats, ControlStats};
use crate::SOCK_PATH;

pub struct Handles {
    pub consumer_stats: Arc<ConsumerStats>,
    pub control_stats: Arc<ControlStats>,
    pub stop: Arc<std::sync::atomic::AtomicBool>,
}

impl Default for Handles {
    fn default() -> Self {
        Self {
            consumer_stats: Arc::new(ConsumerStats::default()),
            control_stats: Arc::new(ControlStats::default()),
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

pub async fn run<S: Server, C: Codec>(handles: Handles, cfg: RingConfig) -> Result<()> {
    let _ = std::fs::remove_file(SOCK_PATH);
    let listener = UnixListener::bind(SOCK_PATH)?;
    eprintln!(
        "[server/{}] listening on {SOCK_PATH}; ring slots={} slot_size={}",
        C::NAME,
        cfg.num_slots,
        cfg.slot_size
    );

    let total = cfg.total();
    let fd = shm::create_memfd(total)?;
    let base_ptr = unsafe { shm::map(fd.as_fd(), total)? };
    unsafe { init(base_ptr, cfg) };
    let base_addr = base_ptr as usize;

    let cs = handles.consumer_stats.clone();
    let stop = handles.stop.clone();
    let consumer_handle =
        std::thread::spawn(move || consumer_loop::<C>(base_addr, cfg, cs, stop));
    let consumer_thread: Arc<Thread> = Arc::new(consumer_handle.thread().clone());

    loop {
        let (stream, _) = listener.accept().await?;
        shm::send_fd(&stream, fd.as_fd(), b"SHM").await?;
        let consumer = consumer_thread.clone();
        let stats = handles.control_stats.clone();
        tokio::task::spawn_local(async move {
            if let Err(e) = S::serve(stream, consumer, stats, cfg).await {
                eprintln!("[server] conn end: {e}");
            }
        });
    }
}

fn consumer_loop<C: Codec>(
    base_addr: usize,
    cfg: RingConfig,
    stats: Arc<ConsumerStats>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    let base = base_addr as *mut u8;
    let state = unsafe { atom(base, 128) };
    let mut buf = Vec::with_capacity(cfg.payload_capacity());
    let mut decoded = DecodedEvent::default();
    let mut codec = C::new(cfg);
    loop {
        if stop.load(Relaxed) {
            return;
        }
        let mut drained = 0u32;
        while let Some(_seq) = unsafe { try_pop(base, cfg, &mut buf) } {
            drained += 1;
            stats.events.fetch_add(1, Relaxed);
            stats.wire_bytes.fetch_add(buf.len() as u64, Relaxed);
            if codec.decode_into(&buf, &mut decoded).is_ok() {
                stats
                    .payload_bytes
                    .fetch_add(decoded.payload.len() as u64, Relaxed);
            }
        }
        if drained == 0 {
            state.store(STATE_PARKED, Release);
            if let Some(_seq) = unsafe { try_pop(base, cfg, &mut buf) } {
                state.store(STATE_RUNNING, Release);
                stats.events.fetch_add(1, Relaxed);
                stats.wire_bytes.fetch_add(buf.len() as u64, Relaxed);
                if codec.decode_into(&buf, &mut decoded).is_ok() {
                    stats
                        .payload_bytes
                        .fetch_add(decoded.payload.len() as u64, Relaxed);
                }
                continue;
            }
            std::thread::park_timeout(Duration::from_millis(50));
            state.store(STATE_RUNNING, Release);
        }
    }
}
