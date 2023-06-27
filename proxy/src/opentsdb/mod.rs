// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! This module implements [put][1] for OpenTSDB
//! [1]: http://opentsdb.net/docs/build/html/api_http/put.html

use ceresdbproto::storage::{
    RequestContext as GrpcRequestContext, WriteRequest as GrpcWriteRequest,
};
use log::debug;
use query_engine::executor::Executor as QueryExecutor;

use crate::{
    context::RequestContext,
    error::Result,
    opentsdb::types::{convert_put_request, PutRequest, PutResponse},
    Context, Proxy,
};

pub mod types;

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_opentsdb_put(
        &self,
        ctx: RequestContext,
        req: PutRequest,
    ) -> Result<PutResponse> {
        let table_request = GrpcWriteRequest {
            context: Some(GrpcRequestContext {
                database: ctx.schema.clone(),
            }),
            table_requests: convert_put_request(req)?,
        };
        let proxy_context = Context {
            timeout: ctx.timeout,
            runtime: self.engine_runtimes.write_runtime.clone(),
            enable_partition_table_access: false,
            forwarded_from: None,
        };
        let result = self
            .handle_write_internal(proxy_context, table_request)
            .await?;

        debug!(
            "OpenTSDB write finished, catalog:{}, schema:{}, result:{result:?}",
            ctx.catalog, ctx.schema
        );

        Ok(())
    }
}
