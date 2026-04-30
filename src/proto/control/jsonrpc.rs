//! JSON-RPC 2.0 (newline-delimited) реализация Client / Server (control plane).

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::Ordering::*;
use std::sync::Arc;
use std::thread::Thread;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use crate::proto::{AttachInfo, Client, Server};
use crate::ring::RingConfig;
use crate::stats::ControlStats;

#[derive(Debug, Deserialize)]
struct Incoming {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct Response<'a, R: Serialize> {
    jsonrpc: &'static str,
    id: &'a Value,
    result: R,
}

#[derive(Debug, Serialize)]
struct ErrorResponse<'a> {
    jsonrpc: &'static str,
    id: &'a Value,
    error: ErrorObject,
}

#[derive(Debug, Serialize)]
struct ErrorObject {
    code: i32,
    message: String,
}

#[derive(Debug, Serialize)]
struct Request<'a, P: Serialize> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: P,
}

#[derive(Debug, Serialize)]
struct Notification<'a, P: Serialize> {
    jsonrpc: &'static str,
    method: &'a str,
    params: P,
}

#[derive(Debug, Serialize)]
struct AttachParams<'a> {
    name: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
struct AttachResult {
    slots: u32,
    #[serde(rename = "slotSize")]
    slot_size: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct PostedParams {
    seq: u64,
}

pub struct JsonClient {
    posted_tx: mpsc::UnboundedSender<u64>,
    writer_task: tokio::task::JoinHandle<Result<()>>,
}

#[async_trait(?Send)]
impl Client for JsonClient {
    async fn attach(stream: UnixStream, name: &str) -> Result<(Self, AttachInfo)> {
        let (rd, mut wr) = stream.into_split();
        let mut rd = BufReader::new(rd);

        let req = Request {
            jsonrpc: "2.0",
            id: 1,
            method: "attach",
            params: AttachParams { name },
        };
        let mut bytes = serde_json::to_vec(&req)?;
        bytes.push(b'\n');
        wr.write_all(&bytes).await?;

        let mut resp_line = String::new();
        let n = rd.read_line(&mut resp_line).await?;
        ensure!(n > 0, "server closed before attach response");
        let resp: Value = serde_json::from_str(resp_line.trim_end())?;
        let result: AttachResult = serde_json::from_value(
            resp.get("result")
                .cloned()
                .ok_or_else(|| anyhow!("attach failed: {resp}"))?,
        )?;
        let info = AttachInfo {
            slots: result.slots,
            slot_size: result.slot_size,
        };

        let (posted_tx, mut posted_rx) = mpsc::unbounded_channel::<u64>();
        let writer_task: tokio::task::JoinHandle<Result<()>> = tokio::task::spawn_local(
            async move {
                drop(rd);
                let mut buf: Vec<u8> = Vec::with_capacity(128);
                while let Some(seq) = posted_rx.recv().await {
                    if let Err(e) = send_posted(&mut wr, &mut buf, seq).await {
                        eprintln!("[jsonrpc-client] writer error: {e}");
                        break;
                    }
                }
                let _ = wr.shutdown().await;
                Ok(())
            },
        );

        Ok((
            JsonClient {
                posted_tx,
                writer_task,
            },
            info,
        ))
    }

    fn signal_posted(&self, seq: u64) {
        let _ = self.posted_tx.send(seq);
    }

    async fn shutdown(self) -> Result<()> {
        drop(self.posted_tx);
        let _ = self.writer_task.await;
        Ok(())
    }
}

async fn send_posted(wr: &mut OwnedWriteHalf, buf: &mut Vec<u8>, seq: u64) -> Result<()> {
    let notif = Notification {
        jsonrpc: "2.0",
        method: "posted",
        params: PostedParams { seq },
    };
    buf.clear();
    serde_json::to_writer(&mut *buf, &notif)?;
    buf.push(b'\n');
    wr.write_all(buf).await?;
    Ok(())
}

pub struct JsonServer;

#[async_trait(?Send)]
impl Server for JsonServer {
    async fn serve(
        stream: UnixStream,
        consumer: Arc<Thread>,
        stats: Arc<ControlStats>,
        cfg: RingConfig,
    ) -> Result<()> {
        let (rd, mut wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        let mut line = String::new();

        loop {
            line.clear();
            let n = rd.read_line(&mut line).await?;
            if n == 0 {
                break;
            }
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                continue;
            }
            let msg: Incoming = match serde_json::from_str(trimmed) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("[jsonrpc-server] parse err: {e}");
                    continue;
                }
            };
            match msg.method.as_str() {
                "attach" => {
                    let name = msg
                        .params
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("<anon>");
                    eprintln!("[jsonrpc-server] attach: {name}");
                    if let Some(id) = &msg.id {
                        let resp = Response {
                            jsonrpc: "2.0",
                            id,
                            result: AttachResult {
                                slots: cfg.num_slots as u32,
                                slot_size: cfg.slot_size as u32,
                            },
                        };
                        let mut bytes = serde_json::to_vec(&resp)?;
                        bytes.push(b'\n');
                        wr.write_all(&bytes).await?;
                    }
                }
                "posted" => {
                    stats.posted_calls.fetch_add(1, Relaxed);
                    consumer.unpark();
                }
                other => {
                    if let Some(id) = &msg.id {
                        let resp = ErrorResponse {
                            jsonrpc: "2.0",
                            id,
                            error: ErrorObject {
                                code: -32601,
                                message: format!("method not found: {other}"),
                            },
                        };
                        let mut bytes = serde_json::to_vec(&resp)?;
                        bytes.push(b'\n');
                        wr.write_all(&bytes).await?;
                    }
                }
            }
        }
        Ok(())
    }
}
