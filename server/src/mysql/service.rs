// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{net::SocketAddr, sync::Arc};

use common_util::runtime::JoinHandle;
use log::{error, info};
use opensrv_mysql::AsyncMysqlIntermediary;
use query_engine::executor::Executor as QueryExecutor;
use snafu::ResultExt;
use table_engine::engine::EngineRuntimes;
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::{
    instance::{Instance, InstanceRef},
    mysql::{
        error::{Result, ServerNotRunning},
        worker::MysqlWorker,
    },
};

pub struct MysqlService<Q> {
    instance: InstanceRef<Q>,
    runtimes: Arc<EngineRuntimes>,
    socket_addr: SocketAddr,
    join_handler: Option<JoinHandle<()>>,
    tx: Option<Sender<()>>,
}

impl<Q> MysqlService<Q> {
    pub fn new(
        instance: Arc<Instance<Q>>,
        runtimes: Arc<EngineRuntimes>,
        socket_addr: SocketAddr,
    ) -> MysqlService<Q> {
        Self {
            instance,
            runtimes,
            socket_addr,
            join_handler: None,
            tx: None,
        }
    }
}

impl<Q: QueryExecutor + 'static> MysqlService<Q> {
    pub async fn start(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        let rt = self.runtimes.clone();
        self.tx = Some(tx);
        self.join_handler = Some(rt.bg_runtime.spawn(Self::loop_accept(
            self.instance.clone(),
            self.runtimes.clone(),
            self.socket_addr,
            rx,
        )));
        info!("Mysql service listens on {}", self.socket_addr);
        Ok(())
    }

    pub fn shutdown(self) {
        if let Some(tx) = self.tx {
            let _ = tx.send(());
        }
    }

    async fn loop_accept(
        instance: InstanceRef<Q>,
        runtimes: Arc<EngineRuntimes>,
        socket_addr: SocketAddr,
        mut rx: Receiver<()>,
    ) {
        let listener = match tokio::net::TcpListener::bind(socket_addr)
            .await
            .context(ServerNotRunning)
        {
            Ok(l) => l,
            Err(err) => {
                error!("Mysql server binds failed, err:{}", err);
                return;
            }
        };
        loop {
            tokio::select! {
                conn_result = listener.accept() => {
                    let (stream, _) = match conn_result {
                        Ok((s, addr)) => (s, addr),
                        Err(err) => {
                            error!("Mysql Server accept new connection fail. err: {}", err);
                            break;
                        }
                    };
                    let instance = instance.clone();
                    let runtimes = runtimes.clone();

                    let rt = runtimes.read_runtime.clone();
                    rt.spawn(AsyncMysqlIntermediary::run_on(
                        MysqlWorker::new(instance, runtimes),
                        stream,
                    ));
                },
                _ = &mut rx => {
                    break;
                }
            }
        }
    }
}
