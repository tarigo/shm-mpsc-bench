//! capnp-rpc реализация Client / Server (control plane).

use anyhow::Result;
use async_trait::async_trait;
use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use std::sync::atomic::Ordering::*;
use std::sync::Arc;
use std::thread::Thread;
use tokio::net::UnixStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::control_capnp::control;
use crate::proto::{AttachInfo, Client, Server};
use crate::ring::RingConfig;
use crate::stats::ControlStats;

pub struct CapnpClient {
    ctrl: control::Client,
    disconnector: capnp_rpc::Disconnector<rpc_twoparty_capnp::Side>,
    rpc_task: tokio::task::JoinHandle<()>,
}

#[async_trait(?Send)]
impl Client for CapnpClient {
    async fn attach(stream: UnixStream, name: &str) -> Result<(Self, AttachInfo)> {
        let (rd, wr) = stream.into_split();
        let net = twoparty::VatNetwork::new(
            rd.compat(),
            wr.compat_write(),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        );
        let mut rpc = RpcSystem::new(Box::new(net), None);
        let ctrl: control::Client = rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
        let disconnector = rpc.get_disconnector();
        let rpc_task = tokio::task::spawn_local(async move {
            if let Err(e) = rpc.await {
                eprintln!("[capnp-client] rpc end: {e}");
            }
        });

        let mut req = ctrl.attach_request();
        req.get().set_name(name);
        let r = req.send().promise.await?;
        let r = r.get()?;
        let info = AttachInfo {
            slots: r.get_slots(),
            slot_size: r.get_slot_size(),
        };

        Ok((
            CapnpClient {
                ctrl,
                disconnector,
                rpc_task,
            },
            info,
        ))
    }

    fn signal_posted(&self, seq: u64) {
        let mut req = self.ctrl.posted_request();
        req.get().set_seq(seq);
        tokio::task::spawn_local(async move {
            let _ = req.send().promise.await;
        });
    }

    async fn shutdown(self) -> Result<()> {
        let _ = self.disconnector.await;
        let _ = self.rpc_task.await;
        Ok(())
    }
}

struct ControlImpl {
    consumer: Arc<Thread>,
    stats: Arc<ControlStats>,
    cfg: RingConfig,
}

impl control::Server for ControlImpl {
    fn attach(
        &mut self,
        params: control::AttachParams,
        mut results: control::AttachResults,
    ) -> Promise<(), capnp::Error> {
        let name = params
            .get()
            .and_then(|p| p.get_name())
            .and_then(|t| t.to_string().map_err(Into::into))
            .unwrap_or_default();
        eprintln!("[capnp-server] attach: {name}");
        let mut r = results.get();
        r.set_slots(self.cfg.num_slots as u32);
        r.set_slot_size(self.cfg.slot_size as u32);
        Promise::ok(())
    }

    fn posted(
        &mut self,
        _params: control::PostedParams,
        _results: control::PostedResults,
    ) -> Promise<(), capnp::Error> {
        self.stats.posted_calls.fetch_add(1, Relaxed);
        self.consumer.unpark();
        Promise::ok(())
    }
}

pub struct CapnpServer;

#[async_trait(?Send)]
impl Server for CapnpServer {
    async fn serve(
        stream: UnixStream,
        consumer: Arc<Thread>,
        stats: Arc<ControlStats>,
        cfg: RingConfig,
    ) -> Result<()> {
        let (rd, wr) = stream.into_split();
        let net = twoparty::VatNetwork::new(
            rd.compat(),
            wr.compat_write(),
            rpc_twoparty_capnp::Side::Server,
            Default::default(),
        );
        let client: control::Client =
            capnp_rpc::new_client(ControlImpl { consumer, stats, cfg });
        let rpc = RpcSystem::new(Box::new(net), Some(client.client));
        rpc.await?;
        Ok(())
    }
}
