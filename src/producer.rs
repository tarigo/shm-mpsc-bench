//! Generic продюсер: connect → recv_fd → handshake → hot loop → shutdown.

use anyhow::{bail, ensure, Result};
use std::os::fd::AsFd;
use std::sync::atomic::Ordering::*;
use std::time::{Duration, Instant};
use tokio::net::UnixStream;

use crate::proto::{Client, Codec, Kind};
use crate::ring::{atom, push, PushErr, RingConfig, STATE_PARKED, STATE_RUNNING};
use crate::shm;
use crate::stats::ProducerStats;
use crate::{now_ns, SOCK_PATH};

/// Сгенерировать синтетический payload, выглядящий как base64-кодированный
/// бинарный блоб: символы из base64-алфавита плюс `=`-padding на хвосте.
fn gen_base64_blob(n: usize) -> Vec<u8> {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = vec![0u8; n];
    // Простой LCG, чтобы не тянуть `rand`. Стабильность последовательности
    // здесь несущественна, важно только распределение по алфавиту.
    let mut s: u64 = 0xdeadbeef_cafebabe;
    for byte in out.iter_mut() {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *byte = ALPHABET[((s >> 32) as usize) & 63];
    }
    if n >= 4 {
        let pad = (4 - n % 4) % 4;
        for i in 0..pad {
            out[n - 1 - i] = b'=';
        }
    }
    out
}

/// Запустить одного продюсера.
/// `payload_size` — размер байтового payload'а (`b'x' * payload_size`).
pub async fn run<CL: Client, C: Codec>(
    name: String,
    n: u64,
    payload_size: usize,
) -> Result<ProducerStats> {
    let stream = UnixStream::connect(SOCK_PATH).await?;
    let (marker, fd) = shm::recv_fd(&stream).await?;
    ensure!(&marker == b"SHM", "unexpected marker: {marker:?}");

    // Размер сегмента берём из fstat — геометрию узнаем после attach.
    let total = shm::file_size(fd.as_fd())?;
    let base_ptr = unsafe { shm::map(fd.as_fd(), total)? };

    let (client, info) = CL::attach(stream, &name).await?;
    let cfg = RingConfig {
        num_slots: info.slots as usize,
        slot_size: info.slot_size as usize,
    };
    eprintln!(
        "[{name}] attached: slots={} slot_size={} (codec={}, payload={}B)",
        cfg.num_slots,
        cfg.slot_size,
        C::NAME,
        payload_size
    );

    // Payload — фиксированный синтетический base64-блоб, аллоцируется один раз
    // и переиспользуется на каждое событие.
    let payload: Vec<u8> = gen_base64_blob(payload_size);
    let state = unsafe { atom(base_ptr, 128) };
    let mut scratch: Vec<u8> = Vec::with_capacity(cfg.slot_size);
    let mut codec = C::new(cfg);
    let started = Instant::now();
    let mut wire_bytes: u64 = 0;
    let mut payload_bytes: u64 = 0;

    for i in 0..n {
        scratch.clear();
        codec.encode(i, now_ns(), &name, Kind::Metric, &payload, &mut scratch)?;
        wire_bytes += scratch.len() as u64;
        payload_bytes += payload.len() as u64;

        loop {
            match unsafe { push(base_ptr, cfg, &scratch) } {
                Ok(seq) => {
                    if state
                        .compare_exchange(STATE_PARKED, STATE_RUNNING, AcqRel, Relaxed)
                        .is_ok()
                    {
                        client.signal_posted(seq);
                    }
                    break;
                }
                Err(PushErr::Full) => {
                    tokio::time::sleep(Duration::from_micros(50)).await;
                }
                Err(PushErr::TooBig) => {
                    bail!(
                        "payload too big: {} > slot capacity {}",
                        scratch.len(),
                        cfg.payload_capacity() - 4
                    )
                }
            }
        }
    }

    let elapsed = started.elapsed();
    eprintln!(
        "[{name}] done: {n} events in {:.2?} ({:.0} events/s)",
        elapsed,
        n as f64 / elapsed.as_secs_f64()
    );
    client.shutdown().await?;
    Ok(ProducerStats {
        n,
        wire_bytes,
        payload_bytes,
        elapsed,
    })
}
