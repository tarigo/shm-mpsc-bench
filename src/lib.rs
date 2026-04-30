pub mod producer;
pub mod proto;
pub mod ring;
pub mod server;
pub mod shm;
pub mod stats;

#[allow(dead_code, clippy::all)]
pub mod control_capnp {
    include!(concat!(env!("OUT_DIR"), "/control_capnp.rs"));
}

#[allow(dead_code, clippy::all)]
pub mod event_proto {
    include!(concat!(env!("OUT_DIR"), "/shm_mpsc_bench.rs"));
}

pub const SOCK_PATH: &str = "/tmp/shm-mpsc-bench.sock";

pub fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
