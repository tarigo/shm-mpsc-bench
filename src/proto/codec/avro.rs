//! Avro codec через `apache-avro`. Используем raw-datum (без container
//! framing). Avro — schema-driven и dynamic-typed: encode/decode идут
//! через промежуточный `Value`, поэтому аллокаций тут хватает; это и есть
//! честная цена «обычного» Avro в Rust.

use anyhow::{anyhow, Result};
use apache_avro::types::Value;
use apache_avro::{from_avro_datum, to_avro_datum, Schema};

use crate::proto::{Codec, DecodedEvent, Kind};
use crate::ring::RingConfig;

const SCHEMA_JSON: &str = r#"
{
  "type": "record",
  "name": "Event",
  "fields": [
    {"name": "seq", "type": "long"},
    {"name": "timestamp", "type": "long"},
    {"name": "source", "type": "string"},
    {"name": "kind", "type": {
      "type": "enum",
      "name": "Kind",
      "symbols": ["Info", "Warn", "Error", "Metric"]
    }},
    {"name": "payload", "type": "bytes"}
  ]
}
"#;

pub struct AvroCodec {
    schema: Schema,
}

impl Codec for AvroCodec {
    const NAME: &'static str = "avro";

    fn new(_: RingConfig) -> Self {
        let schema = Schema::parse_str(SCHEMA_JSON).expect("valid avro schema");
        Self { schema }
    }

    fn encode(
        &mut self,
        seq: u64,
        ts: u64,
        source: &str,
        kind: Kind,
        payload: &[u8],
        dst: &mut Vec<u8>,
    ) -> Result<()> {
        let value = Value::Record(vec![
            ("seq".into(), Value::Long(seq as i64)),
            ("timestamp".into(), Value::Long(ts as i64)),
            ("source".into(), Value::String(source.to_string())),
            (
                "kind".into(),
                Value::Enum(kind_to_idx(kind) as u32, kind_symbol(kind).into()),
            ),
            ("payload".into(), Value::Bytes(payload.to_vec())),
        ]);
        let bytes = to_avro_datum(&self.schema, value)?;
        dst.extend_from_slice(&bytes);
        Ok(())
    }

    fn decode_into(&mut self, raw: &[u8], out: &mut DecodedEvent) -> Result<()> {
        let mut reader = raw;
        let value = from_avro_datum(&self.schema, &mut reader, None)?;
        let fields = match value {
            Value::Record(fs) => fs,
            other => return Err(anyhow!("avro: expected record, got {other:?}")),
        };
        for (name, v) in fields {
            match (name.as_str(), v) {
                ("seq", Value::Long(n)) => out.seq = n as u64,
                ("timestamp", Value::Long(n)) => out.timestamp = n as u64,
                ("source", Value::String(s)) => {
                    out.source.clear();
                    out.source.push_str(&s);
                }
                ("kind", Value::Enum(idx, _)) => out.kind = kind_from_idx(idx as u32),
                ("payload", Value::Bytes(b)) => {
                    out.payload.clear();
                    out.payload.extend_from_slice(&b);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn kind_to_idx(k: Kind) -> u8 {
    match k {
        Kind::Info => 0,
        Kind::Warn => 1,
        Kind::Error => 2,
        Kind::Metric => 3,
    }
}

fn kind_from_idx(i: u32) -> Kind {
    match i {
        1 => Kind::Warn,
        2 => Kind::Error,
        3 => Kind::Metric,
        _ => Kind::Info,
    }
}

fn kind_symbol(k: Kind) -> &'static str {
    match k {
        Kind::Info => "Info",
        Kind::Warn => "Warn",
        Kind::Error => "Error",
        Kind::Metric => "Metric",
    }
}
