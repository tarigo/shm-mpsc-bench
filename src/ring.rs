//! Vyukov-style MPSC ring buffer in shared memory, runtime-configurable.
//!
//! Геометрия (`num_slots`, `slot_size`) задаётся через `RingConfig` и
//! выбирается под сценарий. Многие продюсеры конкурируют за глобальный
//! `tail` через CAS, каждый слот несёт свой `seq` для синхронизации с
//! единственным консьюмером.

use std::sync::atomic::{AtomicU64, Ordering::*};

/// Заголовок сегмента (фиксированный):
///   [  0.. 64) tail            — резервирование продюсерами (CAS)
///   [ 64..128) head            — позиция консьюмера (только он пишет)
///   [128..192) consumer_state  — STATE_RUNNING / STATE_PARKED
/// Каждое поле — на отдельной кэш-линии, чтобы не было false sharing.
pub const HDR: usize = 192;

/// Заголовок слота:
///   [ 0.. 8) seq  — Vyukov sequence number
///   [ 8..12) len  — длина полезной нагрузки в байтах
///   [12..16) pad
pub const SLOT_HDR: usize = 16;

pub const STATE_RUNNING: u64 = 0;
pub const STATE_PARKED: u64 = 1;

/// Параметры геометрии кольца — известны и серверу, и продюсеру (после attach).
#[derive(Clone, Copy, Debug)]
pub struct RingConfig {
    /// Количество слотов; ОБЯЗАТЕЛЬНО степень двойки.
    pub num_slots: usize,
    /// Размер слота в байтах. Включает SLOT_HDR.
    pub slot_size: usize,
}

impl RingConfig {
    pub const fn total(&self) -> usize {
        HDR + self.slot_size * self.num_slots
    }
    pub const fn payload_capacity(&self) -> usize {
        self.slot_size - SLOT_HDR
    }
    pub const fn mask(&self) -> u64 {
        (self.num_slots as u64) - 1
    }
    pub fn check(&self) {
        assert!(
            self.num_slots.is_power_of_two(),
            "num_slots must be a power of two"
        );
        assert!(
            self.slot_size > SLOT_HDR + 4,
            "slot_size too small for SLOT_HDR + length"
        );
    }
}

#[inline]
pub unsafe fn atom(base: *mut u8, off: usize) -> &'static AtomicU64 {
    &*(base.add(off) as *const AtomicU64)
}

#[inline]
pub unsafe fn slot_ptr(base: *mut u8, cfg: RingConfig, idx: u64) -> *mut u8 {
    base.add(HDR + ((idx & cfg.mask()) as usize) * cfg.slot_size)
}

/// Однократная инициализация сегмента создателем (сервер).
/// Вызывается ДО того, как кто-либо подключится.
pub unsafe fn init(base: *mut u8, cfg: RingConfig) {
    cfg.check();
    atom(base, 0).store(0, Relaxed);
    atom(base, 64).store(0, Relaxed);
    atom(base, 128).store(STATE_RUNNING, Relaxed);
    for i in 0..cfg.num_slots as u64 {
        atom(slot_ptr(base, cfg, i), 0).store(i, Relaxed);
    }
    std::sync::atomic::fence(SeqCst);
}

#[derive(Debug)]
pub enum PushErr {
    Full,
    TooBig,
}

/// Записать `data` в кольцо. Безопасен для вызова из множества потоков и процессов.
///
/// # Safety
/// `base` должен указывать на корректно инициализированный shm-сегмент,
/// `cfg` — соответствовать тому, с которым был вызван `init`.
pub unsafe fn push(
    base: *mut u8,
    cfg: RingConfig,
    data: &[u8],
) -> Result<u64, PushErr> {
    if data.len() > cfg.payload_capacity() - 4 {
        return Err(PushErr::TooBig);
    }
    let tail = atom(base, 0);
    loop {
        let pos = tail.load(Relaxed);
        let s = slot_ptr(base, cfg, pos);
        let seq = atom(s, 0).load(Acquire);
        let diff = seq as i64 - pos as i64;
        if diff == 0 {
            if tail
                .compare_exchange_weak(pos, pos + 1, Relaxed, Relaxed)
                .is_ok()
            {
                *(s.add(8) as *mut u32) = data.len() as u32;
                std::ptr::copy_nonoverlapping(data.as_ptr(), s.add(SLOT_HDR), data.len());
                atom(s, 0).store(pos + 1, Release);
                return Ok(pos);
            }
        } else if diff < 0 {
            return Err(PushErr::Full);
        } else {
            std::hint::spin_loop();
        }
    }
}

/// Прочитать одно сообщение, если оно есть. Только для единственного консьюмера.
///
/// # Safety
/// `base` должен указывать на корректно инициализированный shm-сегмент,
/// `cfg` — соответствовать тому, с которым был вызван `init`,
/// и эта функция должна вызываться из одного потока.
pub unsafe fn try_pop(
    base: *mut u8,
    cfg: RingConfig,
    out: &mut Vec<u8>,
) -> Option<u64> {
    let head_a = atom(base, 64);
    let head = head_a.load(Relaxed);
    let s = slot_ptr(base, cfg, head);
    let seq = atom(s, 0).load(Acquire);
    if seq != head + 1 {
        return None;
    }
    let len = *(s.add(8) as *const u32) as usize;
    out.clear();
    out.extend_from_slice(std::slice::from_raw_parts(s.add(SLOT_HDR), len));
    atom(s, 0).store(head + cfg.num_slots as u64, Release);
    head_a.store(head + 1, Release);
    Some(head)
}

/// Пресеты под сценарии бенча.
pub mod presets {
    use super::RingConfig;

    pub const SMALL: RingConfig = RingConfig {
        num_slots: 1024,
        slot_size: 512,
    };
    pub const MEDIUM: RingConfig = RingConfig {
        num_slots: 512,
        slot_size: 4096,
    };
    pub const LARGE: RingConfig = RingConfig {
        num_slots: 256,
        slot_size: 32 * 1024,
    };
    pub const HUGE: RingConfig = RingConfig {
        num_slots: 32,
        slot_size: 2 * 1024 * 1024,
    };
}
