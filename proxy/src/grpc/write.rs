// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{cmp::max, collections::HashMap, time::Instant};

use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, RouteRequest, WriteRequest, WriteResponse,
    WriteTableRequest,
};
use cluster::config::SchemaConfig;
use common_types::request_id::RequestId;
use common_util::error::BoxError;
use futures::{future::try_join_all, FutureExt};
use http::StatusCode;
use interpreters::interpreter::Output;
use log::debug;
use query_engine::executor::Executor as QueryExecutor;
use query_frontend::plan::{InsertPlan, Plan};
use router::endpoint::Endpoint;
use snafu::{OptionExt, ResultExt};
use tonic::transport::Channel;

use crate::{
    create_table, error,
    error::{build_ok_header, ErrNoCause, ErrWithCause, InternalNoCause, Result},
    execute_add_columns_plan, execute_plan, find_new_columns,
    forward::{ForwardResult, ForwarderRef},
    instance::InstanceRef,
    try_get_table, write_table_request_to_insert_plan, Context, Proxy,
};

#[derive(Debug)]
pub struct WriteContext {
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub catalog: String,
    pub schema: String,
    pub auto_create_table: bool,
}

impl WriteContext {
    pub fn new(
        request_id: RequestId,
        deadline: Option<Instant>,
        catalog: String,
        schema: String,
    ) -> Self {
        let auto_create_table = true;
        Self {
            request_id,
            deadline,
            catalog,
            schema,
            auto_create_table,
        }
    }
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub async fn handle_write(&self, ctx: Context, req: WriteRequest) -> WriteResponse {
        self.hotspot_recorder.inc_write_reqs(&req).await;
        match self.handle_write_internal(ctx, req).await {
            Err(e) => {
                error!("Failed to handle write, err:{e}");
                WriteResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                }
            }
            Ok(v) => v,
        }
    }

    async fn handle_write_internal(
        &self,
        ctx: Context,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let write_context = req.context.clone().context(ErrNoCause {
            msg: "Missing context",
            code: StatusCode::BAD_REQUEST,
        })?;

        let (write_request_to_local, write_requests_to_forward) =
            self.split_write_request(req).await?;

        let mut futures = Vec::with_capacity(write_requests_to_forward.len() + 1);

        // Write to remote.
        for (endpoint, table_write_request) in write_requests_to_forward {
            let forwarder = self.forwarder.clone();
            let write_handle = self.engine_runtimes.io_runtime.spawn(async move {
                Self::write_to_remote(forwarder, endpoint, table_write_request).await
            });

            futures.push(write_handle.boxed());
        }

        // Write to local.
        if !write_request_to_local.table_requests.is_empty() {
            let local_handle =
                async move { Ok(self.write_to_local(ctx, write_request_to_local).await) };
            futures.push(local_handle.boxed());
        }

        let resps = try_join_all(futures)
            .await
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to join task",
            })?;

        debug!(
            "Grpc handle write finished, schema:{}, resps:{:?}",
            write_context.database, resps
        );

        let mut success = 0;
        for resp in resps {
            success += resp?.success;
        }

        Ok(WriteResponse {
            success,
            header: Some(build_ok_header()),
            ..Default::default()
        })
    }

    async fn write_to_remote(
        forwarder: ForwarderRef,
        endpoint: Endpoint,
        table_write_request: WriteRequest,
    ) -> Result<WriteResponse> {
        let do_write = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<WriteRequest>,
                        _: &Endpoint| {
            let write = async move {
                client
                    .write(request)
                    .await
                    .map(|resp| resp.into_inner())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded write request failed",
                    })
            }
            .boxed();

            Box::new(write) as _
        };

        let forward_result = forwarder
            .forward_with_endpoint(endpoint, tonic::Request::new(table_write_request), do_write)
            .await;
        let forward_res = forward_result
            .map_err(|e| {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                e
            })
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Local response is not expected",
            })?;

        match forward_res {
            ForwardResult::Forwarded(resp) => resp,
            ForwardResult::Local => InternalNoCause {
                msg: "Local response is not expected".to_string(),
            }
            .fail(),
        }
    }

    async fn write_to_local(&self, ctx: Context, req: WriteRequest) -> Result<WriteResponse> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog = self.instance.catalog_manager.default_catalog_name();
        let req_ctx = req.context.context(ErrNoCause {
            msg: "Missing context",
            code: StatusCode::BAD_REQUEST,
        })?;
        let schema = req_ctx.database;
        let schema_config = self
            .schema_config_provider
            .schema_config(&schema)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Fail to fetch schema config, schema_name:{schema}"),
            })?;

        debug!(
            "Grpc handle write begin, catalog:{catalog}, schema:{schema}, request_id:{request_id}, first_table:{:?}, num_tables:{}",
            req.table_requests
                .first()
                .map(|m| (&m.table, &m.tag_names, &m.field_names)),
            req.table_requests.len(),
        );

        let write_context = WriteContext {
            request_id,
            deadline,
            catalog: catalog.to_string(),
            schema: schema.clone(),
            auto_create_table: self.auto_create_table,
        };

        let plan_vec = self
            .write_request_to_insert_plan(req.table_requests, schema_config, write_context)
            .await?;

        let mut success = 0;
        for insert_plan in plan_vec {
            success += execute_insert_plan(
                request_id,
                catalog,
                &schema,
                self.instance.clone(),
                insert_plan,
                deadline,
            )
            .await?;
        }

        Ok(WriteResponse {
            success: success as u32,
            header: Some(build_ok_header()),
            ..Default::default()
        })
    }

    async fn split_write_request(
        &self,
        req: WriteRequest,
    ) -> Result<(WriteRequest, HashMap<Endpoint, WriteRequest>)> {
        // Split write request into multiple requests, each request contains table
        // belong to one remote engine.
        let tables = req
            .table_requests
            .iter()
            .map(|table_request| table_request.table.clone())
            .collect();

        // TODO: Make the router can accept an iterator over the tables to avoid the
        // memory allocation here.
        let route_data = self
            .router
            .route(RouteRequest {
                context: req.context.clone(),
                tables,
            })
            .await?;
        let forwarded_table_routes = route_data
            .into_iter()
            .filter_map(|router| {
                router
                    .endpoint
                    .map(|endpoint| (router.table, endpoint.into()))
            })
            .filter(|router| !self.forwarder.is_local_endpoint(&router.1))
            .collect::<HashMap<_, _>>();

        // No table need to be forwarded.
        if forwarded_table_routes.is_empty() {
            return Ok((req, HashMap::default()));
        }

        let mut table_requests_to_local = WriteRequest {
            table_requests: Vec::with_capacity(max(
                req.table_requests.len() - forwarded_table_routes.len(),
                0,
            )),
            context: req.context.clone(),
        };

        let mut table_requests_to_forward = HashMap::with_capacity(forwarded_table_routes.len());

        let write_context = req.context;
        for table_request in req.table_requests {
            let route = forwarded_table_routes.get(&table_request.table);
            match route {
                Some(endpoint) => {
                    let table_requests = table_requests_to_forward
                        .entry(endpoint.clone())
                        .or_insert_with(|| WriteRequest {
                            context: write_context.clone(),
                            table_requests: Vec::new(),
                        });
                    table_requests.table_requests.push(table_request);
                }
                _ => {
                    table_requests_to_local.table_requests.push(table_request);
                }
            }
        }
        Ok((table_requests_to_local, table_requests_to_forward))
    }
}

// TODO: use write_request_to_insert_plan in proxy, and remove following code.
pub async fn write_request_to_insert_plan<Q: QueryExecutor + 'static>(
    instance: InstanceRef<Q>,
    table_requests: Vec<WriteTableRequest>,
    schema_config: Option<&SchemaConfig>,
    write_context: WriteContext,
) -> Result<Vec<InsertPlan>> {
    let mut plan_vec = Vec::with_capacity(table_requests.len());

    let WriteContext {
        request_id,
        catalog,
        schema,
        deadline,
        auto_create_table,
    } = write_context;
    let schema_config = schema_config.cloned().unwrap_or_default();
    for write_table_req in table_requests {
        let table_name = &write_table_req.table;
        let mut table = try_get_table(&catalog, &schema, instance.clone(), table_name)?;

        match table.clone() {
            None => {
                if auto_create_table {
                    create_table(
                        request_id,
                        &catalog,
                        &schema,
                        instance.clone(),
                        &write_table_req,
                        &schema_config,
                        deadline,
                    )
                    .await?;
                    // try to get table again
                    table = try_get_table(&catalog, &schema, instance.clone(), table_name)?;
                }
            }
            Some(t) => {
                if auto_create_table {
                    // The reasons for making the decision to add columns before writing are as
                    // follows:
                    // * If judged based on the error message returned, it may cause data that has
                    //   already been successfully written to be written again and affect the
                    //   accuracy of the data.
                    // * Currently, the decision to add columns is made at the request level, not at
                    //   the row level, so the cost is relatively small.
                    let table_schema = t.schema();
                    let columns =
                        find_new_columns(&table_schema, &schema_config, &write_table_req)?;
                    if !columns.is_empty() {
                        execute_add_columns_plan(
                            request_id,
                            &catalog,
                            &schema,
                            instance.clone(),
                            t,
                            columns,
                            deadline,
                        )
                        .await?;
                    }
                }
            }
        }

        match table {
            Some(table) => {
                let plan = write_table_request_to_insert_plan(table, write_table_req)?;
                plan_vec.push(plan);
            }
            None => {
                return ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!("Table not found, schema:{schema}, table:{table_name}"),
                }
                .fail();
            }
        }
    }

    Ok(plan_vec)
}

pub async fn execute_insert_plan<Q: QueryExecutor + 'static>(
    request_id: RequestId,
    catalog: &str,
    schema: &str,
    instance: InstanceRef<Q>,
    insert_plan: InsertPlan,
    deadline: Option<Instant>,
) -> Result<usize> {
    debug!(
        "Grpc handle write table begin, table:{}, row_num:{}",
        insert_plan.table.name(),
        insert_plan.rows.num_rows()
    );
    let plan = Plan::Insert(insert_plan);
    let output = execute_plan(request_id, catalog, schema, instance, plan, deadline).await;
    output.and_then(|output| match output {
        Output::AffectedRows(n) => Ok(n),
        Output::Records(_) => ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: "Invalid output type, expect AffectedRows, found Records",
        }
        .fail(),
    })
}
