# shm-mpsc-bench

A six-codec study on the same data-plane: a Vyukov MPSC ring living in a
shared `memfd`, fed by many producers and drained by a single consumer.
Around that ring sits a *control plane* — a tiny RPC over a Unix socket
that does two things only:

1. **`attach(name)`** — handshake right after the consumer hands the
   producer the shared `memfd` over `SCM_RIGHTS`.
2. **`posted(seq)`** — fire-and-forget wake-up notification, sent
   **only** when the producer wins a CAS that flips the consumer's
   state from `PARKED` to `RUNNING`.

The bench compares slot codecs on the same plumbing:

* **`capnp`** — Cap'n Proto schema, builder over `ScratchSpaceHeapAllocator`.
* **`json`** — UTF-8 JSON via `serde_json` with borrowed deserialize.
* **`cbor`** — CBOR (RFC 8949) via `ciborium` + `serde_bytes`.
* **`proto`** — Protobuf via `prost` (`protoc` bundled at build time).
* **`flat`** — FlatBuffers via the official `flatbuffers` crate (no
  codegen — direct builder API + manual vtable read).
* **`avro`** — Apache Avro via `apache-avro` (schema-driven, dynamic `Value`).

Two `Client`/`Server` control-plane impls are kept (`capnp-rpc` and
`json-rpc`), but the bench fixes the control plane to JSON-RPC so the
codec dimension is the only variable.

---

## Architecture

```
       producer-A ─┐
       producer-B ─┤
       producer-C ─┼─ Unix socket (control plane: attach / posted)
       producer-D ─┘            │
              │                 │
              ▼ SCM_RIGHTS       ▼
        ┌──────────────┐   ┌─────────────┐
        │ shared memfd │   │ consumer    │
        │  ┌──────────┐│   │ thread      │
        │  │ MPSC ring├┼──►│ try_pop()   │
        │  └──────────┘│   │ codec.decode│
        └──────────────┘   └─────────────┘
```

The server creates a `memfd`, mmaps it, initializes the ring, and runs
the only consumer. On each `accept` it sends the fd to the client over
`SCM_RIGHTS` (out-of-band, before any protocol bytes), then runs the
control plane on the same socket. Producers `fstat` the received fd to
mmap the segment without knowing the ring geometry, then `attach` to
learn `num_slots` / `slot_size` and start pushing.

Wake-up uses one bit `consumer_state ∈ {RUNNING, PARKED}`. A producer
fires `posted` only after winning `CAS(PARKED → RUNNING)`. Under load
the socket is almost idle.

---

## Structure

```
src/
├── lib.rs            — re-exports + SOCK_PATH + now_ns()
├── ring.rs           — Vyukov MPSC: push / try_pop / init  (runtime cfg)
├── shm.rs            — memfd + mmap + async SCM_RIGHTS
├── stats.rs          — atomic counters + report formatter
├── server.rs         — generic accept loop + consumer thread
├── producer.rs       — generic hot loop
├── proto/
│   ├── mod.rs        — Codec, Client, Server traits + Kind, AttachInfo
│   ├── codec/{capnp,json,cbor,proto,flat,avro}.rs
│   └── control/{capnp_rpc,jsonrpc}.rs
└── bin/
    ├── server.rs     — CLI: --control, --codec, --size
    ├── producer.rs   — CLI: --control, --codec, --name, --count, --payload-size
    └── bench.rs      — runs all sizes × codecs and writes JSON for plot.py
proto/event.proto     — protobuf schema (compiled by build.rs via prost-build)
capnp/control.capnp   — capnp schema (compiled by build.rs via capnpc)
scripts/run.sh        — build + bench + plot pipeline
scripts/plot.py       — matplotlib renderer
```

`Codec` is stateful: `Codec::new(cfg)` is called once per producer /
consumer; the codec owns reusable scratch (capnp `ScratchSpaceHeapAllocator`,
flatbuffers `FlatBufferBuilder::reset`, prost `String/Vec` reuse, etc).

`Client` / `Server` are stateless type-tags carrying the control-plane
behaviour; nothing prevents pairing any codec with any control plane.

### Async SCM_RIGHTS

Tokio sockets are non-blocking, so calling `nix::sendmsg` / `recvmsg`
directly on the raw fd will return `EAGAIN` long before the peer is
ready. `shm.rs` wraps the fd-passing in `UnixStream::async_io(Interest::
WRITABLE | READABLE, …)`: on `EAGAIN` we surface
`io::ErrorKind::WouldBlock` and Tokio reschedules us.

---

## Running

```bash
cargo build --release

# Standalone:
./target/release/server   --codec proto --size medium    # in one terminal
./target/release/producer --codec proto --name p0 --count 200000 --payload-size 1024

# Full bench + plots:
./scripts/run.sh
# or:  PRODUCERS=8 SIZES="medium large" ./scripts/run.sh

# Or run only the bench (no plots):
./target/release/bench --producers 4 --out out/bench.json
```

`scripts/run.sh` builds in release mode, runs the bench, writes
`out/bench.json`, and renders five PNGs (`throughput.png`,
`bandwidth.png`, `overhead.png`, `slot_bytes.png`, `summary.png`).
Python with `matplotlib` is the only extra dependency.

To regenerate numbers + plots on your hardware: run `./scripts/run.sh`,
then commit `out/`. The README tables below are produced from
`out/bench.json`.

---

## Benchmark results

Host: Intel Core i7-12700H (20 threads), Linux 6.19, Rust nightly
(2026-03-25), `release` profile with `lto = "thin"`,
`codegen-units = 1`. Single-process bench, `current_thread`
runtime + `LocalSet`. 4 producers per scenario.

| size   | payload | ring slots | slot bytes | events/producer |
|--------|---------|-----------:|-----------:|----------------:|
| small  | 64 B    | 1024       | 512        | 500 000         |
| medium | 1 KiB   | 512        | 4 096      | 200 000         |
| large  | 16 KiB  | 256        | 32 768     | 50 000          |
| huge   | 1 MiB   | 32         | 2 097 152  | 1 500           |

The huge-tier payload is a fixed synthetic base64 blob (1 MiB of
base64-alphabet bytes), allocated once per producer and reused.

![bench summary](out/summary.png)

### Throughput (events/s)

![throughput](out/throughput.png)


| codec  |     small |    medium |     large |    huge |
|--------|----------:|----------:|----------:|--------:|
| capnp  |   807 206 |   396 624 |   161 516 |   3 362 |
| json   |   777 402 |   347 605 |    99 383 |   2 279 |
| cbor   |   811 311 |   406 848 |   182 611 |   6 102 |
| proto  |   826 583 |   408 731 |   170 475 |   4 974 |
| flat   |   821 513 |   408 693 |   173 402 |   4 612 |
| avro   |   495 543 |   301 552 |   142 097 |   4 990 |

### Wire bandwidth (MB/s, consumer-side)

![wire bandwidth](out/bandwidth.png)


| codec  |  small |   medium |    large |     huge |
|--------|-------:|---------:|---------:|---------:|
| capnp  |  98.54 |  411.54  | 2 533.54 | 3 361.97 |
| json   | 113.27 |  368.78  | 1 561.18 | 2 279.04 |
| cbor   |  96.51 |  421.11  | 2 863.75 | 6 102.78 |
| proto  |  67.77 |  408.08  | 2 667.51 | 4 974.45 |
| flat   | 106.55 |  427.18  | 2 721.31 | 4 612.00 |
| avro   |  38.74 |  299.65  | 2 222.82 | 4 990.31 |

### Average slot size (bytes)

![slot bytes](out/slot_bytes.png)


| codec  | small | medium |   large |     huge |
|--------|------:|-------:|--------:|---------:|
| capnp  |  128  | 1 088  | 16 448  | 1 048 640 |
| json   |  153  | 1 112  | 16 472  | 1 048 662 |
| cbor   |  125  | 1 085  | 16 444  | 1 048 638 |
| proto  |   86  | 1 047  | 16 408  | 1 048 599 |
| flat   |  136  | 1 096  | 16 456  | 1 048 648 |
| avro   |   82  | 1 042  | 16 403  | 1 048 595 |

### Framing overhead = 1 − payload / wire

![framing overhead](out/overhead.png)


| codec  |  small | medium |  large |  huge |
|--------|-------:|-------:|-------:|------:|
| capnp  | 50.0 % |  5.9 % |  0.4 % | 0.0 % |
| json   | 58.1 % |  8.0 % |  0.5 % | 0.0 % |
| cbor   | 48.7 % |  5.7 % |  0.4 % | 0.0 % |
| proto  | 25.6 % |  2.2 % |  0.1 % | 0.0 % |
| flat   | 52.9 % |  6.6 % |  0.4 % | 0.0 % |
| avro   | 21.9 % |  1.7 % |  0.1 % | 0.0 % |

All five plots above (`throughput.png`, `bandwidth.png`,
`slot_bytes.png`, `overhead.png`, `summary.png`) are produced by
`scripts/plot.py` from `out/bench.json`.

### Reading the numbers

* **Tiny payloads pay fixed framing.** At 64 B, framing dominates
  everything. Avro and Protobuf are smallest on the wire (82 / 86 B
  per slot, 22–26 % overhead) thanks to varint field tags and
  zero-padding. JSON is the largest (153 B, 58 %) — every key is
  spelled out. Cap'n Proto's 8-byte alignment costs it the size game
  but its codec is fast enough to lead on events/s.
* **Avro is small but slow.** It's schema-driven and dynamic — every
  encode round-trips through `Value::Record(Vec<(String, Value)>)`,
  which doubles the path. The wire wins, the latency loses.
* **Big payloads = `memcpy` race.** Once payload >> framing, throughput
  is set by how fast the codec can move bytes. CBOR with `serde_bytes`,
  Protobuf, and FlatBuffers all use a raw length-prefix and run at
  4–6 GB/s on the consumer thread. JSON has to walk every byte to
  validate quotes/escapes both on encode and decode and tops out at
  ~2.3 GB/s.
* **`posted/1k events`** rises from ~1 (steady-state, consumer
  always RUNNING) to 15+ on huge, where one consumer can't keep up
  with four producers and parks more often. The CAS-on-state design
  means we never wake an already-running consumer — the cost only
  shows up where it has to.

### Allocation hygiene

A few non-obvious allocations were chased down before measurement.
The codec instance is created once per producer / consumer and owns
reusable scratch buffers; on the hot path nothing should allocate.

* `CapnpCodec` holds a `Vec<u8>` scratch and constructs a fresh
  `ScratchSpaceHeapAllocator` on each `encode` — one allocation at
  startup, not per event.
* `JsonCodec` decode uses `EventIn<'a>` with `#[serde(borrow)] &'a str`
  for `source` / `payload`; for our payload (no escape sequences)
  serde-json borrows straight out of the slot.
* `JsonClient`'s writer task owns its serialization buffer and
  reuses it via `serde_json::to_writer`.
* `ProtoCodec` reuses its `prost::Message` instance across events;
  `String::clear() + push_str` and `Vec::clear() + extend_from_slice`
  preserve capacity instead of reallocating.
* `FlatCodec` owns a `FlatBufferBuilder` and resets it between
  encodes — `reset()` rolls the builder's offset back to zero
  without touching the underlying buffer.
* `CborCodec` uses `serde_bytes::Bytes` / `ByteBuf` so ciborium
  encodes/decodes as CBOR major type 2 (byte string) with a single
  copy, instead of serde-derive's default `Vec<u8>`-as-sequence-of-u8
  path that breaks on payloads above ~1 KiB.
* `AvroCodec` is the honest exception: `apache-avro` always allocates
  a `Vec<(String, Value)>` per event because that's its API. That's
  baked into the `avro/*` numbers.

---

## What is intentionally simple

* One shm segment for all producers. For isolation, give each
  producer its own segment — MPSC degenerates into N × SPSC, which
  is faster and easier to reason about.
* No `F_ADD_SEALS`. In production seal `SHRINK | GROW` to make the
  size immutable.
* No watchdog on a stalled slot. If a producer crashes between
  `fetch_add(tail)` and `store(seq)`, the consumer parks forever.
  A reservation timestamp + skip-with-log on timeout is the minimum.
* `task::spawn_local` + `LocalSet`. capnp-rpc types are `!Send`, so
  the whole runtime is single-threaded. Splitting RPC and consumer
  onto different threads is a `tokio::sync::mpsc` away.
* The JSON `payload` field is `&str`, not `&[u8]`. A real binary
  payload would require base64 or a separate shm segment.

---

## License

MIT or Apache-2.0, your pick.
